defmodule NeonFS.Block.DeviceRegistry do
  @moduledoc """
  Node-wide record of which devices are attached, and by whom.

  A device is attached by *connections*, not by one connection: a client
  opening several sockets to the same export is how blk-mq gets its
  parallelism, and each of those has to see the same device. Resolving the
  export once and sharing the handle also keeps an attach from costing a
  metadata round trip per socket.

  Each attaching connection is monitored, so a client that dies without
  detaching still releases its hold. The device is released when its last
  connection goes — not before, or a surviving connection would be left
  holding a handle to a detached device.

  ## The cluster-wide claim

  A device gaining its first connection also takes an exclusive
  `NeonFS.Core.NamespaceCoordinator` claim on
  `NeonFS.Core.BlockAttachment.path/2`, released when the last connection
  goes. That claim is the cluster's one record of a block attachment by any
  route: `NeonFS.CSI.AttachRegistry` takes the same path, so an export whose
  device Kubernetes already has attached is refused here, and `volume show`
  reports an `nbd-client` attachment instead of describing it as an absence.

  The claim is per *device*, not per connection — several sockets to one
  export is how blk-mq gets its parallelism, and a per-connection claim would
  refuse `nbd-client -C` outright. The holder is this registry's pid, which
  is the only pid the mechanism can use: an NBD client is an arbitrary host
  rather than a BEAM node, so the node a claim reports for an NBD attachment
  is this one, serving the socket. Two clients reaching the same device
  through this node are therefore both admitted.

  An export whose claim cannot be taken is refused, including when the
  coordinator cannot be reached at all — a control-plane outage is exactly
  the window a split-brain double attach happens in.

  ## Telemetry

    * `[:neonfs, :block, :attached]` — Measurements: `holders`. Metadata:
      `export`.
    * `[:neonfs, :block, :detached]` — Measurements: `holders`, which is `0`
      when the device was released. Metadata: `export`.
  """

  use GenServer

  alias NeonFS.Block.{Device, Frontend}
  alias NeonFS.Client.Router
  alias NeonFS.Core.BlockAttachment
  alias NeonFS.Core.NamespaceCoordinator

  @type export :: String.t()

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @doc """
  Attaches `holder` to `export`, resolving the device on first use.

  Returns the shared device handle. A second attach of an already-attached
  export does not re-resolve it, nor re-claim it.

  The first attach of a device takes the cluster-wide attachment claim, so
  this fails with `{:attached_elsewhere, export}` when the device is already
  attached — including through CSI — and with
  `{:attachment_claim_unavailable, reason}` when exclusivity could not be
  established at all.
  """
  @spec attach(export(), pid(), GenServer.server()) :: {:ok, Device.t()} | {:error, term()}
  def attach(export, holder, server \\ __MODULE__) do
    GenServer.call(server, {:attach, export, holder}, 30_000)
  end

  @doc """
  Releases `holder`'s hold on `export`.

  Idempotent: detaching something this holder does not hold is `:ok`, so a
  connection closing after an error it already handled is not an error.
  """
  @spec detach(export(), pid(), GenServer.server()) :: :ok
  def detach(export, holder, server \\ __MODULE__) do
    GenServer.call(server, {:detach, export, holder}, 30_000)
  end

  @doc """
  The exports currently attached, and how many connections hold each.
  """
  @spec attached(GenServer.server()) :: %{export() => pos_integer()}
  def attached(server \\ __MODULE__) do
    GenServer.call(server, :attached)
  end

  @impl GenServer
  def init(_opts) do
    {:ok, %{devices: %{}, holders: %{}}}
  end

  @impl GenServer
  def handle_call({:attach, export, holder}, _from, state) do
    case Map.fetch(state.devices, export) do
      {:ok, %{device: device}} ->
        {:reply, {:ok, device}, note_attach(state, export, holder)}

      :error ->
        with {:ok, device} <- Frontend.impl().open(export),
             {:ok, claim_id} <- claim_device(device) do
          state =
            put_in(state.devices[export], %{
              device: device,
              claim_id: claim_id,
              holders: MapSet.new()
            })

          {:reply, {:ok, device}, note_attach(state, export, holder)}
        else
          {:error, _reason} = error -> {:reply, error, state}
        end
    end
  end

  def handle_call({:detach, export, holder}, _from, state) do
    {:reply, :ok, note_detach(state, export, holder)}
  end

  def handle_call(:attached, _from, state) do
    counts =
      Map.new(state.devices, fn {export, %{holders: holders}} ->
        {export, MapSet.size(holders)}
      end)

    {:reply, counts, state}
  end

  @impl GenServer
  def handle_info({:DOWN, _ref, :process, pid, _reason}, state) do
    exports = Map.get(state.holders, pid, MapSet.new())
    state = %{state | holders: Map.delete(state.holders, pid)}

    {:noreply, Enum.reduce(exports, state, &note_detach(&2, &1, pid, monitored: false))}
  end

  def handle_info(_message, state), do: {:noreply, state}

  defp note_attach(state, export, holder) do
    state = add_holder(state, export, holder)
    emit(:attached, export, holder_count(state, export))
    state
  end

  defp note_detach(state, export, holder, opts \\ []) do
    state = drop_holder(state, export, holder, opts)
    emit(:detached, export, holder_count(state, export))
    state
  end

  defp holder_count(state, export) do
    case Map.fetch(state.devices, export) do
      {:ok, %{holders: holders}} -> MapSet.size(holders)
      :error -> 0
    end
  end

  defp emit(event, export, holders) do
    :telemetry.execute([:neonfs, :block, event], %{holders: holders}, %{export: export})
  end

  defp add_holder(state, export, holder) do
    unless Map.has_key?(state.holders, holder), do: Process.monitor(holder)

    state
    |> update_in([:devices, export, :holders], &MapSet.put(&1, holder))
    |> update_in([:holders], fn holders ->
      Map.update(holders, holder, MapSet.new([export]), &MapSet.put(&1, export))
    end)
  end

  defp drop_holder(state, export, holder, opts) do
    case Map.fetch(state.devices, export) do
      {:ok, %{holders: holders}} ->
        remaining = MapSet.delete(holders, holder)

        state
        |> release_if_last(export, remaining)
        |> forget_holder(holder, export, Keyword.get(opts, :monitored, true))

      :error ->
        state
    end
  end

  defp release_if_last(state, export, remaining) do
    if MapSet.size(remaining) == 0 do
      release_claim(state.devices[export])
      %{state | devices: Map.delete(state.devices, export)}
    else
      put_in(state.devices[export].holders, remaining)
    end
  end

  # Held by this process rather than the connection: several connections
  # share one device, and the claim's lifetime is the device's.
  defp claim_device(device) do
    device.volume
    |> BlockAttachment.path(device.path)
    |> then(&coordinator_call(:claim_path_for, [&1, :exclusive, self()]))
    |> case do
      {:ok, claim_id} ->
        {:ok, claim_id}

      {:error, %NeonFS.Error.Conflict{}} ->
        {:error, {:attached_elsewhere, device.export}}

      other ->
        {:error, {:attachment_claim_unavailable, other}}
    end
  end

  defp release_claim(%{claim_id: claim_id}) do
    _ = coordinator_call(:release, [claim_id])
    :ok
  end

  # A block node runs as its own interface node, so the coordinator is
  # always a hop away. Configured as a module or closure so a test can drive
  # the registry without a cluster behind it, the same seam
  # `NeonFS.CSI.AttachRegistry` uses.
  defp coordinator_call(function, args) do
    case Application.get_env(:neonfs_block, :coordinator_call_fn) do
      nil -> Router.call(NamespaceCoordinator, function, args)
      module when is_atom(module) -> apply(module, function, args)
      fun when is_function(fun, 2) -> fun.(function, args)
    end
  end

  defp forget_holder(state, _holder, _export, false), do: state

  defp forget_holder(state, holder, export, true) do
    update_in(state.holders, &drop_export(&1, holder, export))
  end

  defp drop_export(holders, holder, export) do
    holders
    |> Map.get(holder, MapSet.new())
    |> MapSet.delete(export)
    |> case do
      remaining -> retain_holder(holders, holder, remaining)
    end
  end

  # A holder with no exports left is forgotten rather than kept as an empty
  # set, so the monitor bookkeeping does not grow with every closed connection.
  defp retain_holder(holders, holder, remaining) do
    if MapSet.size(remaining) == 0 do
      Map.delete(holders, holder)
    else
      Map.put(holders, holder, remaining)
    end
  end
end
