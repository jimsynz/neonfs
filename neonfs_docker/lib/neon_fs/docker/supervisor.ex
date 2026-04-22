defmodule NeonFS.Docker.Supervisor do
  @moduledoc """
  Top-level supervisor for `neonfs_docker`.

  Supervises the local volume store, service registration with the
  core cluster, and the Bandit HTTP server that speaks the Docker
  Volume Plugin protocol. Bandit can listen on a Unix socket path
  (the default Docker plugin location) or a TCP port (test mode).
  """

  use Supervisor

  alias NeonFS.Client.Registrar
  alias NeonFS.Docker.{Plug, VolumeStore}

  @default_socket_path "/run/docker/plugins/neonfs.sock"

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    register? = Application.get_env(:neonfs_docker, :register_service, true)

    children =
      [VolumeStore]
      |> maybe_add_registrar(register?)
      |> Kernel.++([bandit_child_spec()])

    Supervisor.init(children, strategy: :one_for_one)
  end

  defp maybe_add_registrar(children, false), do: children

  defp maybe_add_registrar(children, true) do
    children ++
      [
        {Registrar,
         metadata: registration_metadata(), type: :docker, name: NeonFS.Client.Registrar.Docker}
      ]
  end

  defp bandit_child_spec do
    case Application.get_env(:neonfs_docker, :listener, :socket) do
      :socket ->
        socket_path = Application.get_env(:neonfs_docker, :socket_path, @default_socket_path)
        socket_path |> Path.dirname() |> File.mkdir_p!()
        File.rm(socket_path)

        {Bandit,
         plug: Plug,
         scheme: :http,
         thousand_island_options: [
           transport_module: ThousandIsland.Transports.Unix,
           transport_options: [path: socket_path]
         ]}

      {:tcp, port} ->
        {Bandit, plug: Plug, scheme: :http, port: port, ip: :loopback}
    end
  end

  defp registration_metadata do
    %{
      capabilities: [:volume_driver],
      version: to_string(Application.spec(:neonfs_docker, :vsn) || "0.0.0")
    }
  end
end
