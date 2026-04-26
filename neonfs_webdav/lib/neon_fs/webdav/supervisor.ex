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

  @doc "Starts the WebDAV supervisor."
  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    port = Application.get_env(:neonfs_webdav, :webdav_port, 8081)
    bind = Application.get_env(:neonfs_webdav, :webdav_bind, "0.0.0.0")

    HealthCheck.register_checks()
    LockStore.init()

    children = [
      NamespaceHolder,
      Cleaner,
      {Registrar,
       metadata: registration_metadata(), type: :webdav, name: NeonFS.Client.Registrar.WebDAV},
      {Bandit,
       plug: {HealthPlug, backend: Backend, lock_store: LockStore},
       port: port,
       ip: parse_bind_address(bind),
       scheme: :http}
    ]

    Supervisor.init(children, strategy: :one_for_one)
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
