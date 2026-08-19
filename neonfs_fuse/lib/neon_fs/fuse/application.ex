defmodule NeonFS.FUSE.Application do
  @moduledoc """
  OTP Application for neonfs_fuse.

  Starts the FUSE supervision tree and handles graceful shutdown.
  """

  use Application
  require Logger

  alias NeonFS.FUSE.{HealthCheck, MountManager, Supervisor}

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    start_supervisor? = Application.get_env(:neonfs_fuse, :start_supervisor, true)

    result =
      if start_supervisor? do
        Supervisor.start_link()
      else
        Elixir.Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
      end

    if start_supervisor? do
      HealthCheck.register_checks()
    end

    unless Application.spec(:neonfs_omnibus) do
      NeonFS.Systemd.notify_ready()
    end

    result
  end

  # Drop the kernel mounts, but through `detach_all/0` rather than
  # `unmount/1` — shutting down is not the operator saying "stop serving
  # this", so the mount records survive and the next boot puts them back.
  # Unmounting each one individually here would clear them and turn every
  # restart into a silent loss of every mount the node held.
  @impl true
  def stop(_state) do
    case Process.whereis(MountManager) do
      nil ->
        :ok

      _pid ->
        detach_all()
        :ok
    end
  end

  defp detach_all do
    case MountManager.list_mounts() do
      [] ->
        :ok

      mounts ->
        Logger.info("Unmounting filesystems before shutdown", count: length(mounts))
        MountManager.detach_all()
    end
  end
end
