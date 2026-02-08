defmodule NeonFS.FUSE.Application do
  @moduledoc """
  OTP Application for neonfs_fuse.

  Starts the FUSE supervision tree and handles graceful shutdown.
  """

  use Application
  require Logger

  alias NeonFS.FUSE.{MountManager, Supervisor}

  @impl true
  def start(_type, _args) do
    # In test mode, don't start the supervisor - tests use start_supervised
    # for the specific components they need
    result =
      if Application.get_env(:neonfs_fuse, :start_supervisor, true) do
        Supervisor.start_link()
      else
        # Return a minimal supervisor for test mode
        Elixir.Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
      end

    # Register with the cluster after supervisor is running
    # (Connection GenServer must be started first)
    case result do
      {:ok, _} -> register_with_cluster()
      _ -> :ok
    end

    result
  end

  defp register_with_cluster do
    # Async registration — don't block startup if core is unreachable
    spawn(fn ->
      Process.sleep(500)

      case NeonFS.Client.register(:fuse, %{
             capabilities: [:mount, :unmount],
             version: to_string(Application.spec(:neonfs_fuse, :vsn) || "0.0.0")
           }) do
        :ok ->
          Logger.info("Registered as FUSE service with cluster")

        {:error, reason} ->
          Logger.warning("Failed to register with cluster: #{inspect(reason)}")
      end
    end)
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
        Logger.info("Unmounting #{length(mounts)} filesystem(s) before shutdown...")
        Enum.each(mounts, &unmount_one/1)
    end
  end

  defp unmount_one(mount) do
    Logger.debug("Unmounting #{mount.volume_name} from #{mount.mount_point}")

    case MountManager.unmount(mount.id) do
      :ok ->
        :ok

      {:error, reason} ->
        Logger.warning("Failed to unmount #{mount.mount_point}: #{inspect(reason)}")
    end
  end
end
