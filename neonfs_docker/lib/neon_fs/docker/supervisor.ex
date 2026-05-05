defmodule NeonFS.Docker.Supervisor do
  @moduledoc """
  Top-level supervisor for `neonfs_docker`.

  Supervises the local volume store, service registration with the
  core cluster, and the Bandit HTTP server that speaks the Docker
  Volume Plugin protocol. Bandit can listen on a Unix socket path
  (the default Docker plugin location) or a TCP port (test mode).
  """

  use Supervisor
  require Logger

  alias NeonFS.Client.Registrar
  alias NeonFS.Docker.{MountTracker, Plug, VolumeStore}

  # The plugin owns its socket inside its own RuntimeDirectory; Docker
  # discovers it via `/etc/docker/plugins/neonfs.spec`. Putting the
  # socket under `/run/docker/plugins` would require write access to a
  # path that Docker creates as root:root 0700, which the daemon's
  # unprivileged user can't satisfy.
  @default_socket_path "/run/neonfs/docker.sock"

  # Errors that mean "the host can't host the plugin socket" rather
  # than a misconfiguration we should crash on. The daemon owns its
  # RuntimeDirectory by default so these are unusual, but they can
  # surface if `:socket_path` is overridden to a path the daemon
  # can't reach (ProtectSystem=strict without a matching
  # ReadWritePaths, a missing parent, a read-only mount, or something
  # else holding the path).
  @skip_errors [:eacces, :enoent, :enotdir, :erofs]

  @spec start_link(keyword()) :: Supervisor.on_start()
  def start_link(opts \\ []) do
    Supervisor.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(_opts) do
    register? = Application.get_env(:neonfs_docker, :register_service, true)

    children =
      case bandit_child_spec() do
        {:ok, listener} ->
          [VolumeStore, MountTracker]
          |> maybe_add_registrar(register?)
          |> Kernel.++([listener])

        {:skip, message} ->
          Logger.warning("Docker volume plugin disabled: #{message}")
          []
      end

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
        prepare_socket(socket_path)

      {:tcp, port} ->
        {:ok, {Bandit, plug: Plug, scheme: :http, port: port, ip: :loopback}}
    end
  end

  defp prepare_socket(socket_path) do
    socket_dir = Path.dirname(socket_path)

    case File.mkdir_p(socket_dir) do
      :ok ->
        File.rm(socket_path)
        {:ok, {Bandit, plug: Plug, scheme: :http, port: 0, ip: {:local, socket_path}}}

      {:error, reason} when reason in @skip_errors ->
        {:skip,
         "cannot prepare socket directory #{inspect(socket_dir)} (#{reason}). " <>
           "Check `:socket_path` and the daemon's filesystem permissions."}
    end
  end

  defp registration_metadata do
    %{
      capabilities: [:volume_driver],
      version: to_string(Application.spec(:neonfs_docker, :vsn) || "0.0.0")
    }
  end
end
