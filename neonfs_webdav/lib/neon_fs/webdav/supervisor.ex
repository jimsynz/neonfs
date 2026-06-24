defmodule NeonFS.WebDAV.Supervisor do
  @moduledoc """
  Top-level supervisor for neonfs_webdav application.

  Supervises:
  - Client connectivity (Connection, Discovery, CostFunction)
  - Service registration with the core cluster
  - Bandit HTTP server running Davy.Plug via HealthPlug
  """

  use Supervisor

  alias NeonFS.Client.Registrar
  alias NeonFS.WebDAV.{Backend, HealthCheck, HealthPlug, LockStore}
  alias NeonFS.WebDAV.LockStore.{Cleaner, NamespaceHolder}

  # On shutdown ThousandIsland stops accepting new connections and drains
  # in-flight requests for up to `shutdown_timeout`. The default leaves
  # headroom under the systemd `TimeoutStopSec=30` budget (#1377).
  @default_drain_deadline_ms 25_000

  @doc "Starts the WebDAV supervisor."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    HealthCheck.register_checks()

    # Registrar last so it terminates first on shutdown: it deregisters the
    # service (stopping new client work) before the listener drains (#1386).
    children = [
      NamespaceHolder,
      Cleaner,
      listener_child_spec(),
      {Registrar,
       metadata: registration_metadata(), type: :webdav, name: NeonFS.Client.Registrar.WebDAV}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end

  @doc false
  @spec listener_child_spec() :: {Bandit, keyword()}
  def listener_child_spec do
    port = Application.get_env(:neonfs_webdav, :webdav_port, 8081)
    bind = Application.get_env(:neonfs_webdav, :webdav_bind, "0.0.0.0")

    {Bandit,
     plug: {HealthPlug, backend: Backend, lock_store: LockStore},
     port: port,
     ip: parse_bind_address(bind),
     scheme: :http,
     thousand_island_options: [shutdown_timeout: drain_deadline_ms()]}
  end

  @doc false
  @spec drain_deadline_ms() :: timeout()
  def drain_deadline_ms do
    Application.get_env(:neonfs_webdav, :drain_deadline_ms, @default_drain_deadline_ms)
  end

  defp registration_metadata do
    %{
      capabilities: [:read, :write],
      version: to_string(Application.spec(:neonfs_webdav, :vsn) || "0.0.0")
    }
  end

  defp parse_bind_address(bind) when is_binary(bind) do
    {:ok, ip} = :inet.parse_address(String.to_charlist(bind))
    ip
  end
end
