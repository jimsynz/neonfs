defmodule NeonFS.TestSupport.LoopbackDeviceTest do
  use ExUnit.Case, async: false

  alias NeonFS.TestSupport.LoopbackDevice

  @moduletag :loopback
  @moduletag :requires_root
  @moduletag timeout: 60_000

  describe "create and destroy" do
    test "creates a loopback device and mounts it" do
      {:ok, device} = LoopbackDevice.create(size_mb: 10)
      on_exit(fn -> LoopbackDevice.destroy(device) end)

      assert %LoopbackDevice{} = device
      assert File.dir?(device.path)
      assert String.starts_with?(device.loop_device, "/dev/loop")
      assert File.exists?(device.image_file)
    end

    test "write and read data on the mount point" do
      {:ok, device} = LoopbackDevice.create(size_mb: 10)
      on_exit(fn -> LoopbackDevice.destroy(device) end)

      test_file = Path.join(device.path, "test.txt")
      content = "hello from loopback device"

      File.write!(test_file, content)
      assert File.read!(test_file) == content
    end

    test "destroy removes all artifacts" do
      {:ok, device} = LoopbackDevice.create(size_mb: 10)

      # Capture paths before destroy
      mount_path = device.path
      image_file = device.image_file

      :ok = LoopbackDevice.destroy(device)

      refute File.exists?(image_file)
      refute File.dir?(mount_path)
    end

    test "destroy is idempotent" do
      {:ok, device} = LoopbackDevice.create(size_mb: 10)

      :ok = LoopbackDevice.destroy(device)
      # Second call should not raise
      :ok = LoopbackDevice.destroy(device)
    end
  end

  describe "available?" do
    test "returns a boolean" do
      result = LoopbackDevice.available?()
      assert is_boolean(result)
    end
  end
end
