defmodule NeonFS.FUSE.Application do
  @moduledoc """
  OTP Application for neonfs_fuse.

  Starts the FUSE supervision tree and handles graceful shutdown.
  """

  use Application
  require Logger

  @impl true
  def start(_type, _args) do
    # Start the supervision tree
    NeonFS.FUSE.Supervisor.start_link()
  end

  @impl true
  def stop(_state) do
    # Graceful shutdown: unmount all filesystems before stopping
    case Process.whereis(NeonFS.FUSE.MountManager) do
      nil ->
        :ok

      _pid ->
        mounts = NeonFS.FUSE.MountManager.list_mounts()

        if length(mounts) > 0 do
          Logger.info("Unmounting #{length(mounts)} filesystem(s) before shutdown...")

          Enum.each(mounts, fn mount ->
            Logger.debug("Unmounting #{mount.volume_name} from #{mount.mount_point}")

            case NeonFS.FUSE.MountManager.unmount(mount.id) do
              :ok ->
                :ok

              {:error, reason} ->
                Logger.warning("Failed to unmount #{mount.mount_point}: #{inspect(reason)}")
            end
          end)
        end
    end

    :ok
  end
end
