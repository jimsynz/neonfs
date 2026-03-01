defmodule NeonFS.Integration.LoopbackDevice do
  @moduledoc """
  Test helpers for creating and tearing down loopback block devices.

  Loopback devices allow integration tests to exercise real space-related
  behaviours (ENOSPC, capacity thresholds) using small real filesystems
  rather than mocked blob stores.

  These operations require root privileges. Tests using loopback devices
  should be tagged with `@tag :loopback` and skipped when `available?/0`
  returns `false`.

  ## Usage

      setup do
        {:ok, device} = LoopbackDevice.create(size_mb: 50)
        on_exit(fn -> LoopbackDevice.destroy(device) end)
        {:ok, device: device}
      end

      test "write to device", %{device: device} do
        File.write!(Path.join(device.path, "test.txt"), "hello")
      end
  """

  @type t :: %__MODULE__{
          path: String.t(),
          loop_device: String.t(),
          image_file: String.t()
        }

  defstruct [:path, :loop_device, :image_file]

  @doc """
  Checks whether loopback device creation is possible.

  Returns `true` if running as root and `losetup` is available.
  """
  @spec available?() :: boolean()
  def available? do
    root?() and losetup_present?()
  end

  @doc """
  Creates a loopback device of the specified size.

  ## Options

    * `:size_mb` — size in megabytes (default: 50)

  Returns `{:ok, %LoopbackDevice{}}` on success or `{:error, reason}` on failure.
  """
  @spec create(keyword()) :: {:ok, t()} | {:error, term()}
  def create(opts \\ []) do
    size_mb = Keyword.get(opts, :size_mb, 50)
    unique_id = System.unique_integer([:positive])
    tmp_dir = System.tmp_dir!()
    image_file = Path.join(tmp_dir, "neonfs_loop_#{unique_id}.img")
    mount_path = Path.join(tmp_dir, "neonfs_mount_#{unique_id}")

    with :ok <- create_image(image_file, size_mb),
         {:ok, loop_device} <- attach_loop(image_file),
         :ok <- format_ext4(loop_device),
         :ok <- mount_device(loop_device, mount_path) do
      {:ok,
       %__MODULE__{
         path: mount_path,
         loop_device: loop_device,
         image_file: image_file
       }}
    else
      {:error, reason} ->
        # attach_loop may or may not have succeeded — find any attached device
        loop_device = find_attached_loop(image_file)
        cleanup_partial(image_file, loop_device, mount_path)
        {:error, reason}
    end
  end

  @doc """
  Destroys a loopback device, cleaning up all artifacts.

  Idempotent — safe to call multiple times. Returns `:ok` always.
  """
  @spec destroy(t()) :: :ok
  def destroy(%__MODULE__{} = device) do
    unmount(device.path)
    detach_loop(device.loop_device)
    remove_file(device.image_file)
    remove_dir(device.path)
    :ok
  end

  # --- Private helpers ---

  defp root? do
    case System.cmd("id", ["-u"], stderr_to_stdout: true) do
      {"0\n", 0} -> true
      _ -> false
    end
  end

  defp losetup_present? do
    case System.find_executable("losetup") do
      nil -> false
      _ -> true
    end
  end

  defp create_image(path, size_mb) do
    case System.cmd(
           "dd",
           [
             "if=/dev/zero",
             "of=#{path}",
             "bs=1M",
             "count=#{size_mb}"
           ],
           stderr_to_stdout: true
         ) do
      {_, 0} -> :ok
      {output, code} -> {:error, {:dd_failed, code, output}}
    end
  end

  defp attach_loop(image_path) do
    case System.cmd("losetup", ["--find", "--show", image_path], stderr_to_stdout: true) do
      {output, 0} -> {:ok, String.trim(output)}
      {output, code} -> {:error, {:losetup_failed, code, output}}
    end
  end

  defp format_ext4(loop_device) do
    case System.cmd("mkfs.ext4", ["-q", loop_device], stderr_to_stdout: true) do
      {_, 0} -> :ok
      {output, code} -> {:error, {:mkfs_failed, code, output}}
    end
  end

  defp mount_device(loop_device, mount_path) do
    File.mkdir_p!(mount_path)

    case System.cmd("mount", [loop_device, mount_path], stderr_to_stdout: true) do
      {_, 0} -> :ok
      {output, code} -> {:error, {:mount_failed, code, output}}
    end
  end

  defp unmount(path) do
    System.cmd("umount", [path], stderr_to_stdout: true)
  end

  defp detach_loop(loop_device) do
    System.cmd("losetup", ["-d", loop_device], stderr_to_stdout: true)
  end

  defp remove_file(path) do
    File.rm(path)
  end

  defp remove_dir(path) do
    File.rmdir(path)
  end

  defp find_attached_loop(image_file) do
    case System.cmd("losetup", ["-j", image_file], stderr_to_stdout: true) do
      {output, 0} ->
        case String.split(output, ":", parts: 2) do
          [device | _] when device != "" -> String.trim(device)
          _ -> nil
        end

      _ ->
        nil
    end
  end

  @spec cleanup_partial(String.t(), String.t() | nil, String.t()) :: :ok
  defp cleanup_partial(image_file, nil, mount_path) do
    unmount(mount_path)
    remove_file(image_file)
    remove_dir(mount_path)
    :ok
  end

  defp cleanup_partial(image_file, loop_device, mount_path) do
    unmount(mount_path)
    detach_loop(loop_device)
    remove_file(image_file)
    remove_dir(mount_path)
    :ok
  end
end
