defmodule NeonFS.Integration.Helpers do
  @moduledoc """
  Helper functions for integration tests.
  """

  alias NeonFS.CLI.Handler
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.FUSE.MountManager

  @doc """
  Creates a temporary directory for test artifacts.
  Returns the path to the directory.
  """
  def create_temp_dir do
    base = System.tmp_dir!()
    timestamp = DateTime.utc_now() |> DateTime.to_unix(:millisecond)
    random = :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
    dir = Path.join([base, "neonfs_test_#{timestamp}_#{random}"])
    File.mkdir_p!(dir)
    dir
  end

  @doc """
  Cleans up test artifacts including unmounting and removing directories.
  """
  def cleanup(temp_dir, mount_point \\ nil) do
    # Try to unmount if mount point is provided
    if mount_point && File.exists?(mount_point) do
      case Handler.unmount(mount_point) do
        :ok -> :ok
        {:error, _} -> :ok
      end
    end

    # Remove temp directory
    if File.exists?(temp_dir) do
      File.rm_rf!(temp_dir)
    end

    :ok
  end

  @doc """
  Checks if FUSE support is available in the current environment.
  """
  def fuse_available? do
    case Code.ensure_loaded?(MountManager) do
      true ->
        # Try to call a simple function to see if NIF is loaded
        case Handler.list_mounts() do
          {:ok, _} -> true
          {:error, :fuse_not_available} -> false
          _ -> false
        end

      false ->
        false
    end
  end

  @doc """
  Creates a test volume with default configuration.
  Returns {:ok, volume_id} or {:error, reason}.
  """
  def create_test_volume(name, opts \\ []) do
    default_opts = [
      durability: %{type: :replicate, factor: 1, min_copies: 1},
      compression: %{algorithm: :zstd, level: 3, min_size: 4096},
      verification: %{on_read: :never}
    ]

    merged_opts = Keyword.merge(default_opts, opts)

    # Use VolumeRegistry directly instead of CLI Handler to avoid string conversion issues
    case VolumeRegistry.create(name, merged_opts) do
      {:ok, volume} ->
        # Convert to map format similar to CLI Handler
        {:ok,
         %{
           "id" => volume.id,
           "name" => volume.name,
           "owner" => volume.owner,
           "durability" => volume.durability,
           "compression" => volume.compression
         }}

      {:error, reason} ->
        {:error, reason}
    end
  end

  @doc """
  Waits for a condition to become true, with timeout.
  """
  def wait_until(fun, timeout \\ 5000, interval \\ 100) do
    end_time = System.monotonic_time(:millisecond) + timeout

    do_wait_until(fun, end_time, interval)
  end

  defp do_wait_until(fun, end_time, interval) do
    if fun.() do
      :ok
    else
      now = System.monotonic_time(:millisecond)

      if now < end_time do
        Process.sleep(interval)
        do_wait_until(fun, end_time, interval)
      else
        {:error, :timeout}
      end
    end
  end

  @doc """
  Generates random test data of the specified size.
  """
  def random_data(size) do
    :crypto.strong_rand_bytes(size)
  end
end
