defmodule NeonFS.CSI.Supervisor do
  @moduledoc """
  Top-level supervisor for `neonfs_csi`.

  Owns the gRPC endpoint that exposes the CSI Identity (and, in
  later slices, Controller / Node) services, plus the registrar that
  advertises this node as a `:csi` service in the cluster's service
  registry.

  ## Configuration

    * `:mode` — `:controller` (default) or `:node`. Determines which
      services the gRPC endpoint exposes; both expose Identity.
    * `:socket_path` — UDS path. CSI defaults:
      * `:controller` → `/var/lib/csi/sockets/pluginproxy/csi.sock`
      * `:node` → `/var/lib/kubelet/plugins/neonfs.csi.harton.dev/csi.sock`
    * `:listener` — `:socket` (default) or `{:tcp, port}` for tests.
    * `:register_service` — `true` (default) registers as `:csi` in
      the cluster service registry. Tests usually disable.

  ## Mounting in-pod

  A node plugin stages volumes by mounting them itself: the release carries
  `neonfs_fuse` and this supervisor starts its `InodeTable`,
  `MountSupervisor` and `MountManager` directly, with `neonfs_fuse`'s own
  application supervisor left inert (`start_supervisor: false`). That keeps
  the CSI pod out of the service registry as a `:fuse` service it does not
  serve, and off a second metrics port.

  The mounts live and die with this pod. Restarting the plugin — a
  DaemonSet rollout, say — leaves every staged mountpoint at `ENOTCONN`
  until something unmounts it, because `MountManager` holds its mount table
  in memory with no recovery path.
  """

  use Supervisor
  require Logger

  alias NeonFS.Client.Registrar

  @controller_socket "/var/lib/csi/sockets/pluginproxy/csi.sock"
  @node_socket "/var/lib/kubelet/plugins/neonfs.csi.harton.dev/csi.sock"

  # Errors that mean "the host can't host the plugin socket" rather
  # than a misconfiguration we should crash on. The canonical CSI
  # deployment runs this daemon as a privileged sidecar with the
  # kubelet plugin path hostPath-mounted, so these only trip when
  # someone runs the daemon outside that context.
  @skip_errors [:eacces, :enoent, :enotdir, :erofs]

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    register? = Application.get_env(:neonfs_csi, :register_service, true)

    # Node staged/published state lives in public ETS tables the
    # gRPC handler processes read/write directly. Initialise them
    # regardless of mode so the tables exist before the gRPC layer
    # accepts the first request.
    NeonFS.CSI.NodeServer.init_state_tables()
    NeonFS.CSI.VolumeHealth.init_table()

    children =
      case endpoint_child_spec() do
        {:ok, endpoint} ->
          (mount_stack_children() ++ [endpoint] ++ attach_holder_children())
          |> maybe_add_registrar(register?)

        {:skip, message} ->
          Logger.warning("CSI plugin disabled: #{message}")
          []
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  # `GRPC.Server.Supervisor` accepts only `:endpoint, :servers, :start_server,
  # :port, :adapter_opts, :exception_log_filter, :max_body_size` at the top
  # level and raises on anything else, so the `:ip` binding — including the
  # `{:local, path}` that makes this a unix socket — belongs under
  # `:adapter_opts`. `:start_server` defaults to false, which loads the
  # supervisor with no children: the driver would boot, log nothing, and never
  # open the socket every CSI sidecar dials.
  defp endpoint_child_spec do
    case Application.get_env(:neonfs_csi, :listener, :socket) do
      :socket ->
        socket_path =
          Application.get_env(:neonfs_csi, :socket_path, default_socket_path())

        prepare_socket(socket_path)

      {:tcp, port} ->
        {:ok,
         {GRPC.Server.Supervisor,
          endpoint: NeonFS.CSI.Endpoint,
          port: port,
          start_server: true,
          adapter_opts: [ip: {127, 0, 0, 1}]}}
    end
  end

  defp prepare_socket(socket_path) do
    socket_dir = Path.dirname(socket_path)

    case File.mkdir_p(socket_dir) do
      :ok ->
        File.rm(socket_path)

        {:ok,
         {GRPC.Server.Supervisor,
          endpoint: NeonFS.CSI.Endpoint,
          port: 0,
          start_server: true,
          adapter_opts: [ip: {:local, socket_path}]}}

      {:error, reason} when reason in @skip_errors ->
        {:skip,
         "cannot prepare socket directory #{inspect(socket_dir)} (#{reason}). " <>
           "Check `:socket_path` and the daemon's filesystem permissions."}
    end
  end

  defp default_socket_path do
    case Application.get_env(:neonfs_csi, :mode, :controller) do
      :node -> @node_socket
      _ -> @controller_socket
    end
  end

  # Only a node-mode plugin holds attachments, so only it needs the pid
  # whose death releases them.
  defp attach_holder_children do
    if node_mode?() do
      [NeonFS.CSI.AttachHolder]
    else
      []
    end
  end

  # Before the endpoint, so a mount call cannot arrive at a `MountManager`
  # that has not started. A controller never stages, so it never needs one.
  defp mount_stack_children do
    if node_mode?() do
      [NeonFS.FUSE.InodeTable, NeonFS.FUSE.MountSupervisor, NeonFS.FUSE.MountManager]
    else
      []
    end
  end

  defp node_mode?, do: Application.get_env(:neonfs_csi, :mode, :controller) == :node

  defp maybe_add_registrar(children, false), do: children

  defp maybe_add_registrar(children, true) do
    children ++
      [
        {Registrar,
         metadata: registration_metadata(), type: :csi, name: NeonFS.Client.Registrar.CSI}
      ]
  end

  # A node-mode plugin advertises the `node_id` it reports to the CO.
  # `ServiceInfo` already carries the BEAM node, so the pair is what lets a
  # controller turn the node name Kubernetes uses into the node a claim can
  # be held on — see `NeonFS.CSI.NodeResolver`.
  @doc """
  The metadata this plugin registers itself with. Public so the
  node_id-to-BEAM-node mapping it carries can be asserted directly.
  """
  @spec registration_metadata() :: map()
  def registration_metadata do
    base = %{
      capabilities: [:csi_identity],
      mode: Application.get_env(:neonfs_csi, :mode, :controller),
      version: to_string(Application.spec(:neonfs_csi, :vsn) || "0.0.0")
    }

    case base.mode do
      :node -> Map.put(base, :node_id, NeonFS.CSI.NodeServer.node_id())
      _controller -> base
    end
  end
end
