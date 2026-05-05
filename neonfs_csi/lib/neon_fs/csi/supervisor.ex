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

    # Controller publish state (#314) and Node staged/published state
    # (#315) live in public ETS tables their gRPC handler processes
    # read/write directly. Initialise both regardless of mode so the
    # tables exist before the gRPC layer accepts the first request.
    NeonFS.CSI.ControllerServer.init_publish_table()
    NeonFS.CSI.NodeServer.init_state_tables()
    NeonFS.CSI.VolumeHealth.init_table()

    children =
      case endpoint_child_spec() do
        {:ok, endpoint} ->
          [endpoint] |> maybe_add_registrar(register?)

        {:skip, message} ->
          Logger.warning("CSI plugin disabled: #{message}")
          []
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp endpoint_child_spec do
    case Application.get_env(:neonfs_csi, :listener, :socket) do
      :socket ->
        socket_path =
          Application.get_env(:neonfs_csi, :socket_path, default_socket_path())

        prepare_socket(socket_path)

      {:tcp, port} ->
        {:ok,
         {GRPC.Server.Supervisor, endpoint: NeonFS.CSI.Endpoint, port: port, ip: {127, 0, 0, 1}}}
    end
  end

  defp prepare_socket(socket_path) do
    socket_dir = Path.dirname(socket_path)

    case File.mkdir_p(socket_dir) do
      :ok ->
        File.rm(socket_path)

        {:ok, {GRPC.Server.Supervisor, endpoint: NeonFS.CSI.Endpoint, ip: {:local, socket_path}}}

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

  defp maybe_add_registrar(children, false), do: children

  defp maybe_add_registrar(children, true) do
    children ++
      [
        {Registrar,
         metadata: registration_metadata(), type: :csi, name: NeonFS.Client.Registrar.CSI}
      ]
  end

  defp registration_metadata do
    %{
      capabilities: [:csi_identity],
      mode: Application.get_env(:neonfs_csi, :mode, :controller),
      version: to_string(Application.spec(:neonfs_csi, :vsn) || "0.0.0")
    }
  end
end
