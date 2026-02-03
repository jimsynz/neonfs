defmodule NeonFS.FUSE.NativeTest do
  use ExUnit.Case, async: true

  alias NeonFS.FUSE.Native

  doctest Native

  describe "FUSE server lifecycle" do
    test "start_fuse_server/1 creates server resource" do
      assert {:ok, server} = Native.start_fuse_server(self())
      assert is_reference(server)
    end

    test "stop_fuse_server/1 stops the server" do
      assert {:ok, server} = Native.start_fuse_server(self())
      assert :ok = Native.stop_fuse_server(server)
    end

    test "server_stats/1 returns initial state" do
      assert {:ok, server} = Native.start_fuse_server(self())
      assert {:ok, {pending, shutdown}} = Native.server_stats(server)
      assert pending == 0
      assert shutdown == false
    end

    test "server_stats/1 shows shutdown state" do
      assert {:ok, server} = Native.start_fuse_server(self())
      assert :ok = Native.stop_fuse_server(server)
      assert {:ok, {_pending, shutdown}} = Native.server_stats(server)
      assert shutdown == true
    end
  end

  describe "operation submission" do
    test "test_operation/2 can submit read operation" do
      assert {:ok, server} = Native.start_fuse_server(self())
      assert {:ok, msg} = Native.test_operation(server, "read")
      assert msg =~ "submitted"
    end

    test "test_operation/2 can submit lookup operation" do
      assert {:ok, server} = Native.start_fuse_server(self())
      assert {:ok, msg} = Native.test_operation(server, "lookup")
      assert msg =~ "submitted"
    end

    test "test_operation/2 rejects unknown operations" do
      assert {:ok, server} = Native.start_fuse_server(self())
      assert {:error, msg} = Native.test_operation(server, "invalid")
      assert msg =~ "Unknown operation"
    end

    test "test_operation/2 fails after shutdown" do
      assert {:ok, server} = Native.start_fuse_server(self())
      assert :ok = Native.stop_fuse_server(server)
      assert {:error, msg} = Native.test_operation(server, "read")
      assert msg =~ "shut down"
    end
  end

  describe "reply handling" do
    test "reply_fuse_operation/3 with :ok reply" do
      assert {:ok, server} = Native.start_fuse_server(self())

      # Note: In the current implementation, we can't easily test actual
      # request/reply flow without a real FUSE operation. This test just
      # verifies the NIF accepts valid reply formats.

      # Try replying with a non-existent request ID (will fail)
      assert {:error, msg} = Native.reply_fuse_operation(server, 999, :ok)
      assert msg =~ "No pending request"
    end

    test "reply_fuse_operation/3 with error reply" do
      assert {:ok, server} = Native.start_fuse_server(self())

      # Try replying with error
      assert {:error, msg} = Native.reply_fuse_operation(server, 999, {:error, 2})
      assert msg =~ "No pending request"
    end
  end

  describe "graceful shutdown" do
    test "stopping server prevents new operations" do
      assert {:ok, server} = Native.start_fuse_server(self())

      # Submit operation before shutdown
      assert {:ok, _} = Native.test_operation(server, "read")

      # Stop server
      assert :ok = Native.stop_fuse_server(server)

      # Verify shutdown state
      assert {:ok, {_pending, shutdown}} = Native.server_stats(server)
      assert shutdown == true

      # New operations should fail
      assert {:error, _} = Native.test_operation(server, "read")
    end
  end

  describe "mount operations" do
    @tag :fuse_integration
    test "mount/3 with invalid mount point returns error" do
      assert {:error, msg} = Native.mount("/nonexistent/path", self(), [])
      assert msg =~ "not exist"
    end

    @tag :fuse_integration
    test "mount/3 with valid mount point and unmount" do
      mount_point =
        System.tmp_dir!() |> Path.join("neonfs_test_mount_#{System.unique_integer([:positive])}")

      File.mkdir_p!(mount_point)

      try do
        assert {:ok, session} = Native.mount(mount_point, self(), ["auto_unmount"])
        assert is_reference(session)
        assert File.exists?(mount_point)
        assert {:ok, :ok} = Native.unmount(session, "fusermount3")
      after
        File.rm_rf(mount_point)
      end
    end

    @tag :fuse_integration
    test "unmount/1 on already unmounted session returns error" do
      mount_point =
        System.tmp_dir!() |> Path.join("neonfs_test_mount_#{System.unique_integer([:positive])}")

      File.mkdir_p!(mount_point)

      try do
        assert {:ok, session} = Native.mount(mount_point, self(), ["auto_unmount"])
        assert {:ok, :ok} = Native.unmount(session, "fusermount3")
        assert {:error, msg} = Native.unmount(session, "fusermount3")
        assert msg =~ "already unmounted"
      after
        File.rm_rf(mount_point)
      end
    end

    @tag :fuse_integration
    test "mount/3 validates mount point is a directory" do
      file_path =
        System.tmp_dir!() |> Path.join("neonfs_test_file_#{System.unique_integer([:positive])}")

      File.write!(file_path, "test")

      try do
        assert {:error, msg} = Native.mount(file_path, self(), [])
        assert msg =~ "not a directory"
      after
        File.rm!(file_path)
      end
    end
  end
end
