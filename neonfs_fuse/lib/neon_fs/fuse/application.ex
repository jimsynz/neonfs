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

  @impl true
  def stop(_state) do
    # Graceful shutdown: unmount all filesystems before stopping
    case Process.whereis(MountManager) do
      nil -> :ok
      _pid -> unmount_all()
    end

    :ok
  end

  defp unmount_all do
    mounts = MountManager.list_mounts()

    case mounts do
      [] ->
        :ok

      _ ->
        Logger.info("Unmounting filesystems before shutdown", count: length(mounts))
        Enum.each(mounts, &unmount_one/1)
    end
  end

  defp unmount_one(mount) do
    Logger.debug("Unmounting filesystem",
      volume_name: mount.volume_name,
      mount_point: mount.mount_point
    )

    case MountManager.unmount(mount.id) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to unmount",
          mount_point: mount.mount_point,
          reason: inspect(reason)
        )
    end
  end
end
