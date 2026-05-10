defmodule NeonFS.Containerd.Supervisor do
  @moduledoc """
  Top-level supervisor for `neonfs_containerd`.

  Owns the gRPC endpoint that exposes the `containerd.services.content.v1.Content`
  service plus the standard `grpc.health.v1.Health` checking protocol, and
  the registrar that advertises this node as a `:containerd` service in
  the cluster's service registry.

  ## Configuration

    * `:socket_path` — UDS path. Default `/run/neonfs/containerd.sock`
      (the daemon owns its own `RuntimeDirectory`; containerd's
      `[proxy_plugins.neonfs] address` should point here).
    * `:listener` — `:socket` (default) or `{:tcp, port}` for tests.
    * `:register_service` — `true` (default) registers as `:containerd` in
      the cluster service registry. Tests usually disable.
  """

  use Supervisor
  require Logger

  alias NeonFS.Client.Registrar
  alias NeonFS.Containerd.{HealthCheck, WriteRegistry, WriteSupervisor}

  # The plugin owns its socket inside its own RuntimeDirectory; operators
  # point containerd's `[proxy_plugins.neonfs] address` at it via
  # `/etc/containerd/config.toml`. Putting the socket under
  # `/run/containerd/proxy-plugins` would require write access to a
  # path that containerd creates as root:root, which the daemon's
  # unprivileged user can't satisfy.
  @default_socket_path "/run/neonfs/containerd.sock"

  # Errors that mean "the host can't host the plugin socket" rather
  # than a misconfiguration we should crash on. The daemon owns its
  # RuntimeDirectory by default so these are unusual, but they can
  # surface if `:socket_path` is overridden to a path the daemon
  # can't reach (ProtectSystem=strict without a matching
  # ReadWritePaths, a missing parent, a read-only mount, or something
  # else holding the path).
  @skip_errors [:eacces, :enoent, :enotdir, :erofs]

  @doc """
  Starts the containerd-plugin supervision tree. Honoured options
  are read from the `:neonfs_containerd` application env (see the
  module doc for the full list); the keyword list is currently
  unused but reserved for future per-instance overrides.
  """
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    register? = Application.get_env(:neonfs_containerd, :register_service, true)

    HealthCheck.register_checks()

    children =
      case endpoint_child_spec() do
        {:ok, endpoint} ->
          [WriteRegistry, WriteSupervisor, endpoint]
          |> maybe_add_registrar(register?)

        {:skip, message} ->
          Logger.warning("containerd content store plugin disabled: #{message}")
          []
      end

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp endpoint_child_spec do
    # GRPC.Server.Supervisor (grpc 0.11+) only takes
    # `:endpoint, :servers, :start_server, :port, :adapter_opts,
    # :exception_log_filter` at the top level — anything else
    # (including the `:ip` binding) lives under `:adapter_opts`.
    # `:start_server` defaults to `false`, which loads the
    # supervisor but never opens the listener — so it has to be
    # set explicitly here for the gRPC endpoint to be reachable.
    case Application.get_env(:neonfs_containerd, :listener, :socket) do
      :socket ->
        socket_path =
          Application.get_env(:neonfs_containerd, :socket_path, @default_socket_path)

        prepare_socket(socket_path)

      {:tcp, port} ->
        {:ok,
         {GRPC.Server.Supervisor,
          endpoint: NeonFS.Containerd.Endpoint,
          port: port,
          start_server: true,
          adapter_opts: [ip: {127, 0, 0, 1}],
          exception_log_filter: {__MODULE__, :log_grpc_exception?}}}
    end
  end

  @doc """
  Filter for `GRPC.Server.Supervisor`'s `:exception_log_filter` —
  drops noisy `:not_found` exceptions emitted by `Status` / `Info` /
  `Delete` during normal containerd pull behaviour, where containerd
  probes for blobs / writers it knows might not exist.

  Everything else still logs as before.
  """
  @spec log_grpc_exception?(GRPC.Server.Adapters.ReportException.t()) :: boolean()
  # `GRPC.RPCError.status` is the integer gRPC status code (e.g. `5`
  # for NOT_FOUND), not the atom — `RPCError.exception/1` resolves
  # the atom to its number via `GRPC.Status` at construction time.
  # Matching on the atom would miss everything; match on the code.
  @grpc_status_not_found 5

  def log_grpc_exception?(%GRPC.Server.Adapters.ReportException{
        reason: %GRPC.RPCError{status: @grpc_status_not_found}
      }) do
    false
  end

  def log_grpc_exception?(_), do: true

  defp prepare_socket(socket_path) do
    socket_dir = Path.dirname(socket_path)

    case File.mkdir_p(socket_dir) do
      :ok ->
        File.rm(socket_path)

        {:ok,
         {GRPC.Server.Supervisor,
          endpoint: NeonFS.Containerd.Endpoint,
          port: 0,
          start_server: true,
          adapter_opts: [ip: {:local, socket_path}],
          exception_log_filter: {__MODULE__, :log_grpc_exception?}}}

      {:error, reason} when reason in @skip_errors ->
        {:skip,
         "cannot prepare socket directory #{inspect(socket_dir)} (#{reason}). " <>
           "Check `:socket_path` and the daemon's filesystem permissions."}
    end
  end

  defp maybe_add_registrar(children, false), do: children

  defp maybe_add_registrar(children, true) do
    children ++
      [
        {Registrar,
         metadata: registration_metadata(),
         type: :containerd,
         name: NeonFS.Client.Registrar.Containerd}
      ]
  end

  defp registration_metadata do
    %{
      capabilities: [:content_store],
      version: NeonFS.Containerd.version()
    }
  end
end
