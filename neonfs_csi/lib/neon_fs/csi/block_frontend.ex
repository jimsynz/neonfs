defmodule NeonFS.CSI.BlockFrontend do
  @moduledoc """
  Which frontend a block volume is attached through, and the attach itself.

  The two frontends are not interchangeable from here, and the difference is
  not preference:

    * **NBD** is a network protocol. `nbd-client` runs on *this* host and
      dials the block service wherever it is, so any block service will do.
    * **ublk** is local. The device node is created by the kernel of the host
      running the block target, so it only helps when a block target is on
      *this* host — otherwise `/dev/ublkbN` appears on a machine the kubelet
      cannot see.

  So availability has three parts, and each can fail on its own: this host
  has a block service, that service advertises `:ublk`, and its node still
  answers. A forced `:ublk` reports which of the three failed rather than
  falling back, because a silent fallback makes a benchmark measure NBD and
  call it ublk.

  ## The shipped Kubernetes chart cannot use ublk

  The node DaemonSet runs no block target: the plugin discovers one over the
  cluster and dials it. So `:auto` resolves to NBD there, always, and forcing
  `:ublk` fails saying no local block service was found — which is accurate
  rather than a defect to work around here. Co-locating a block target with
  the node plugin is a deployment change, not a selection one.

  ## Configuration

  `:neonfs_csi, :block_frontend` is `:auto` (the default), `:ublk` or `:nbd`.

  `:block_call_fn` replaces the call into the block node, the same seam
  `:block_attach_fn` and `:fuse_mount_fn` give the other two: a test can drive
  the ublk branch on a host with no ublk and no second node.
  """

  alias NeonFS.Client.Discovery

  require Logger

  @block_module NeonFS.Block

  @type frontend :: :nbd | :ublk
  @type attachment :: %{device_path: Path.t(), frontend: frontend()}

  @doc "The configured preference."
  @spec preference() :: :auto | frontend()
  def preference, do: Application.get_env(:neonfs_csi, :block_frontend, :auto)

  @doc """
  Resolves the preference against what is reachable from this host.

  Answers the frontend and, for ublk, the node that will own the device —
  the caller needs it to attach and again to detach.
  """
  @spec select() :: {:ok, :nbd} | {:ok, :ublk, node()} | {:error, term()}
  def select, do: select(preference())

  @spec select(:auto | frontend()) :: {:ok, :nbd} | {:ok, :ublk, node()} | {:error, term()}
  def select(:nbd), do: {:ok, :nbd}

  def select(:ublk) do
    case local_ublk_node() do
      {:ok, node} -> {:ok, :ublk, node}
      {:error, reason} -> {:error, {:frontend_forced_unavailable, :ublk, reason}}
    end
  end

  def select(:auto) do
    case local_ublk_node() do
      {:ok, node} ->
        {:ok, :ublk, node}

      {:error, reason} ->
        Logger.debug("ublk not available here; attaching over NBD", reason: inspect(reason))
        {:ok, :nbd}
    end
  end

  def select(other), do: {:error, {:unknown_frontend, other}}

  @doc """
  A block service on this very host that says it can serve ublk.

  Each failure is distinct because each has a different fix: no block
  service here means co-locate one, a service that does not advertise
  `:ublk` means the driver or the helper is missing *there* — which that
  node can say precisely, and does when asked to force it.
  """
  @spec local_ublk_node() :: {:ok, node()} | {:error, term()}
  def local_ublk_node do
    :block
    |> Discovery.list_by_type()
    |> Enum.filter(&local?/1)
    |> case do
      [] ->
        {:error, {:no_local_block_service, this_host()}}

      local ->
        case Enum.find(local, &advertises_ublk?/1) do
          nil -> {:error, {:local_block_service_lacks_ublk, Enum.map(local, & &1.node)}}
          service -> {:ok, service.node}
        end
    end
  end

  @doc """
  Attaches `volume_id` through whichever frontend this host can use.

  The frontend travels with the device path because the detach differs: NBD
  is undone here with `nbd-client -d`, ublk by the node that owns the target.
  """
  @spec attach(String.t(), (String.t() -> {:ok, Path.t()} | {:error, term()})) ::
          {:ok, attachment()} | {:error, term()}
  def attach(volume_id, nbd_attach) do
    case select() do
      {:ok, :nbd} ->
        with {:ok, device_path} <- nbd_attach.(volume_id) do
          {:ok, %{device_path: device_path, frontend: :nbd, node: Node.self()}}
        end

      {:ok, :ublk, node} ->
        case block_call(node, :attach_ublk, [volume_id, []]) do
          {:ok, device_path} -> {:ok, %{device_path: device_path, frontend: :ublk, node: node}}
          {:error, reason} -> {:error, {:ublk_attach_failed, node, reason}}
        end

      {:error, _reason} = error ->
        error
    end
  end

  @doc "Undoes `attach/2`, by whichever route took the device."
  @spec detach(map(), (Path.t() -> :ok | {:error, term()})) :: :ok | {:error, term()}
  def detach(%{frontend: :ublk, node: node} = record, _nbd_detach) do
    case block_call(node, :detach_ublk, [export_of(record)]) do
      :ok -> :ok
      {:error, reason} -> {:error, {:ublk_detach_failed, node, reason}}
    end
  end

  def detach(%{device_path: device_path}, nbd_detach), do: nbd_detach.(device_path)

  defp block_call(node, function, args) do
    Application.get_env(:neonfs_csi, :block_call_fn, &:erpc.call/4).(
      node,
      @block_module,
      function,
      args
    )
  end

  # A record staged before this field existed, or one whose export was not
  # kept, still names its volume — which is the export the block target
  # resolves for a bare name.
  defp export_of(record), do: Map.get(record, :export) || Map.fetch!(record, :volume_id)

  defp advertises_ublk?(service) do
    :ublk in Map.get(service.metadata || %{}, :capabilities, [])
  end

  # Same host, not same node: in an omnibus deployment CSI and the block
  # target are one node and this is trivially true, but they can also be two
  # nodes sharing a machine, where the device one creates is still the one
  # the other opens.
  defp local?(service), do: host_of(service.node) == this_host()

  defp this_host, do: host_of(Node.self())

  defp host_of(node) do
    node |> Atom.to_string() |> String.split("@", parts: 2) |> List.last()
  end
end
