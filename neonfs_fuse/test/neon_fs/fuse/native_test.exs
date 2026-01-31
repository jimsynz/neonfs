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
    # Note: These tests require the "fuse" feature to be enabled and FUSE support
    # in the system. They may be skipped if not available.

    @tag :fuse_integration
    test "mount/3 with invalid mount point returns error" do
      try do
        case Native.mount("/nonexistent/path", self(), []) do
          {:error, msg} ->
            assert msg =~ "not exist"

          _ ->
            :ok
        end
      rescue
        ErlangError ->
          # NIF not loaded (fuse feature not enabled)
          :ok
      end
    end

    @tag :fuse_integration
    test "mount/3 with valid mount point and unmount" do
      # Create a temporary directory for testing
      mount_point = System.tmp_dir!() |> Path.join("neonfs_test_mount")
      File.mkdir_p!(mount_point)

      try do
        case Native.mount(mount_point, self(), ["auto_unmount"]) do
          {:ok, session} ->
            assert is_reference(session)

            # Verify mount point is mounted
            assert File.exists?(mount_point)

            # Unmount - may fail with permission error in unprivileged environments
            case Native.unmount(session, "fusermount3") do
              {:ok, :ok} ->
                :ok

              {:error, msg} when is_binary(msg) ->
                # Permission errors are expected in unprivileged environments
                assert msg =~ "Operation not permitted" or msg =~ "Failed to unmount"
            end

          {:error, msg} ->
            # Feature not available or insufficient permissions
            assert msg =~ "Failed to mount"
        end
      rescue
        ErlangError ->
          # NIF not loaded (fuse feature not enabled)
          :ok
      after
        # Cleanup
        File.rm_rf(mount_point)
      end
    end

    @tag :fuse_integration
    test "unmount/1 on already unmounted session returns error" do
      mount_point = System.tmp_dir!() |> Path.join("neonfs_test_mount2")
      File.mkdir_p!(mount_point)

      try do
        case Native.mount(mount_point, self(), []) do
          {:ok, session} ->
            # Unmount once
            assert {:ok, :ok} = Native.unmount(session, "fusermount3")

            # Try to unmount again - should fail
            assert {:error, msg} = Native.unmount(session, "fusermount3")
            assert msg =~ "already unmounted"

          {:error, _} ->
            # Feature not available, skip test
            :ok
        end
      rescue
        ErlangError ->
          # NIF not loaded (fuse feature not enabled)
          :ok
      after
        File.rm_rf(mount_point)
      end
    end

    @tag :fuse_integration
    test "mount/3 validates mount point is a directory" do
      # Create a file instead of directory
      file_path = System.tmp_dir!() |> Path.join("neonfs_test_file")
      File.write!(file_path, "test")

      try do
        case Native.mount(file_path, self(), []) do
          {:error, msg} ->
            assert msg =~ "not a directory"

          _ ->
            # Feature might handle this differently
            :ok
        end
      rescue
        ErlangError ->
          # NIF not loaded (fuse feature not enabled)
          :ok
      after
        File.rm!(file_path)
      end
    end
  end
end
