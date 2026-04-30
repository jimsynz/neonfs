defmodule NeonFS.Containerd.Supervisor do
  @moduledoc """
  Top-level supervisor for `neonfs_containerd`.

  Owns the gRPC endpoint that exposes the `containerd.services.content.v1.Content`
  service plus the standard `grpc.health.v1.Health` checking protocol, and
  the registrar that advertises this node as a `:containerd` service in
  the cluster's service registry.

  ## Configuration

    * `:socket_path` — UDS path. Default `/run/containerd/proxy-plugins/neonfs.sock`
      (the path containerd's `[proxy_plugins]` config dials).
    * `:listener` — `:socket` (default) or `{:tcp, port}` for tests.
    * `:register_service` — `true` (default) registers as `:containerd` in
      the cluster service registry. Tests usually disable.
  """

  use Supervisor

  alias NeonFS.Client.Registrar
  alias NeonFS.Containerd.{HealthCheck, WriteRegistry, WriteSupervisor}

  @default_socket_path "/run/containerd/proxy-plugins/neonfs.sock"

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
      [
        WriteRegistry,
        WriteSupervisor,
        endpoint_child_spec()
      ]
      |> maybe_add_registrar(register?)

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp endpoint_child_spec do
    # GRPC.Server.Supervisor (grpc 0.11+) only takes
    # `:endpoint, :servers, :start_server, :port, :adapter_opts,
    # :exception_log_filter` at the top level — anything else
    # (including the `:ip` binding) lives under `:adapter_opts`.
    case Application.get_env(:neonfs_containerd, :listener, :socket) do
      :socket ->
        socket_path =
          Application.get_env(:neonfs_containerd, :socket_path, @default_socket_path)

        socket_path |> Path.dirname() |> File.mkdir_p!()
        File.rm(socket_path)

        {GRPC.Server.Supervisor,
         endpoint: NeonFS.Containerd.Endpoint,
         port: 0,
         adapter_opts: [ip: {:local, socket_path}]}

      {:tcp, port} ->
        {GRPC.Server.Supervisor,
         endpoint: NeonFS.Containerd.Endpoint,
         port: port,
         adapter_opts: [ip: {127, 0, 0, 1}]}
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
