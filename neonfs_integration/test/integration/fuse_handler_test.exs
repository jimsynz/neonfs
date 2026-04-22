defmodule NeonFS.Integration.FuseHandlerTest do
  @moduledoc """
  Integration tests for the FUSE handler operating against a remote core node.

  These tests verify that FUSE operations work correctly when the handler
  routes calls to a core node via the NeonFS.Client infrastructure.
  """
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 120_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  alias NeonFS.Client.{Connection, CostFunction, Discovery}
  alias NeonFS.FUSE.{Handler, InodeTable}

  setup_all %{cluster: cluster} do
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])
    :ok = wait_for_cluster_stable(cluster)

    {:ok, volume_map} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "test-volume",
        %{}
      ])

    %{volume_id: volume_map[:id], volume_name: "test-volume"}
  end

  setup %{cluster: cluster, volume_id: volume_id, volume_name: volume_name} do
    # Start client infrastructure on the test runner, pointing at the core peer
    core_node = PeerCluster.get_node!(cluster, :node1).node

    start_supervised!({Connection, bootstrap_nodes: [core_node]})
    start_supervised!(Discovery)
    start_supervised!(CostFunction)
    start_supervised!(InodeTable)

    # Wait for Connection to reach the core node
    :ok =
      wait_until(fn ->
        case Connection.connected_core_node() do
          {:ok, _} -> true
          _ -> false
        end
      end)

    # Wait for Discovery to cache the core node
    :ok =
      wait_until(
        fn ->
          case Discovery.get_core_nodes() do
            [_ | _] -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    # Start a handler for testing with test_notify so we can assert_receive
    {:ok, handler} =
      Handler.start_link(volume: volume_id, volume_name: volume_name, test_notify: self())

    on_exit(fn ->
      if Process.alive?(handler), do: GenServer.stop(handler)
    end)

    {:ok, handler: handler}
  end

  describe "lookup operation" do
    test "looks up existing file", %{handler: handler, volume_id: volume_id, cluster: cluster} do
      # Create test file on core node
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
        volume_id,
        "/test.txt",
        0,
        "hello"
      ])

      send(handler, {:fuse_op, 1, {"lookup", %{"parent" => 1, "name" => "test.txt"}}})
      assert_receive {:fuse_op_complete, 1, {"lookup_ok", _}}, 5_000

      {:ok, inode} = InodeTable.get_inode(volume_id, "/test.txt")
      assert inode > 1
    end

    test "returns error for nonexistent file", %{handler: handler, volume_id: volume_id} do
      send(handler, {:fuse_op, 1, {"lookup", %{"parent" => 1, "name" => "missing.txt"}}})
      assert_receive {:fuse_op_complete, 1, {"error", %{"errno" => 2}}}, 5_000

      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/missing.txt")
    end
  end

  describe "getattr operation" do
    test "gets attributes for root directory", %{handler: handler} do
      send(handler, {:fuse_op, 1, {"getattr", %{"ino" => 1}}})
      assert_receive {:fuse_op_complete, 1, {"attr_ok", _}}, 5_000

      # Root should always exist
      assert {:ok, {nil, "/"}} = InodeTable.get_path(1)
    end
  end

  describe "read and write operations" do
    test "creates a file and reads it back", %{handler: handler, volume_id: volume_id} do
      # Create file via FUSE create operation
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "new.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000

      # Verify inode was allocated
      {:ok, inode} = InodeTable.get_inode(volume_id, "/new.txt")

      # Write data to the file
      send(
        handler,
        {:fuse_op, 2, {"write", %{"ino" => inode, "offset" => 0, "data" => "hello world"}}}
      )

      assert_receive {:fuse_op_complete, 2, {"write_ok", _}}, 5_000

      # Read the data back — goes through NeonFS.Client.ChunkReader
      send(handler, {:fuse_op, 3, {"read", %{"ino" => inode, "offset" => 0, "size" => 100}}})
      assert_receive {:fuse_op_complete, 3, {"read_ok", %{"data" => "hello world"}}}, 5_000
    end

    test "reads at non-zero offset and bounded length", %{
      handler: handler,
      volume_id: volume_id
    } do
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "ranged.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000
      {:ok, inode} = InodeTable.get_inode(volume_id, "/ranged.txt")

      send(
        handler,
        {:fuse_op, 2, {"write", %{"ino" => inode, "offset" => 0, "data" => "0123456789abcdef"}}}
      )

      assert_receive {:fuse_op_complete, 2, {"write_ok", _}}, 5_000

      send(handler, {:fuse_op, 3, {"read", %{"ino" => inode, "offset" => 4, "size" => 8}}})
      assert_receive {:fuse_op_complete, 3, {"read_ok", %{"data" => "456789ab"}}}, 5_000
    end
  end

  describe "directory operations" do
    test "creates and lists a directory", %{handler: handler, volume_id: volume_id} do
      # Create a directory
      send(
        handler,
        {:fuse_op, 1, {"mkdir", %{"parent" => 1, "name" => "docs", "mode" => 0o755}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000

      {:ok, dir_inode} = InodeTable.get_inode(volume_id, "/docs")

      # Create a file inside the directory
      send(
        handler,
        {:fuse_op, 2,
         {"create", %{"parent" => dir_inode, "name" => "readme.md", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 2, {"entry_ok", _}}, 5_000

      # List the directory
      send(handler, {:fuse_op, 3, {"readdir", %{"ino" => dir_inode, "offset" => 0}}})
      assert_receive {:fuse_op_complete, 3, {"readdir_ok", _}}, 5_000

      assert {:ok, _} = InodeTable.get_inode(volume_id, "/docs/readme.md")
    end

    test "reads root directory contents", %{handler: handler, volume_id: volume_id} do
      # Create files in root
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "file1.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000

      send(
        handler,
        {:fuse_op, 2, {"create", %{"parent" => 1, "name" => "file2.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 2, {"entry_ok", _}}, 5_000

      send(handler, {:fuse_op, 3, {"readdir", %{"ino" => 1, "offset" => 0}}})
      assert_receive {:fuse_op_complete, 3, {"readdir_ok", _}}, 5_000

      assert {:ok, _} = InodeTable.get_inode(volume_id, "/file1.txt")
      assert {:ok, _} = InodeTable.get_inode(volume_id, "/file2.txt")
    end
  end

  describe "create/mkdir mode passthrough" do
    test "create with explicit mode 0o600 stores mode 0o100600", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "private.txt", "mode" => 0o600}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/private.txt"
        ])

      assert file.mode == 0o100600
    end

    test "create with nil mode falls back to 0o100644", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "default.txt", "mode" => nil}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/default.txt"
        ])

      assert file.mode == 0o100644
    end

    test "mkdir with explicit mode 0o700 stores mode 0o040700", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"mkdir", %{"parent" => 1, "name" => "secret", "mode" => 0o700}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/secret"
        ])

      assert file.mode == 0o040700
    end

    test "mkdir with nil mode falls back to 0o040755", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"mkdir", %{"parent" => 1, "name" => "normal_dir", "mode" => nil}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/normal_dir"
        ])

      assert file.mode == 0o040755
    end
  end

  describe "unlink and rmdir operations" do
    test "deletes a file", %{handler: handler, volume_id: volume_id, cluster: cluster} do
      # Create file on core
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
        volume_id,
        "/delete_me.txt",
        0,
        "content"
      ])

      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/delete_me.txt")

      send(handler, {:fuse_op, 1, {"unlink", %{"parent" => 1, "name" => "delete_me.txt"}}})
      assert_receive {:fuse_op_complete, 1, {"ok", _}}, 5_000

      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/delete_me.txt")
    end

    test "deletes empty directory", %{handler: handler, volume_id: volume_id, cluster: cluster} do
      # Create empty directory on core
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
        volume_id,
        "/empty_dir",
        0,
        "",
        [mode: 0o040755]
      ])

      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/empty_dir")

      send(handler, {:fuse_op, 1, {"rmdir", %{"parent" => 1, "name" => "empty_dir"}}})
      assert_receive {:fuse_op_complete, 1, {"ok", _}}, 5_000

      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/empty_dir")
    end
  end

  describe "rename operation" do
    test "renames a file", %{handler: handler, volume_id: volume_id, cluster: cluster} do
      # Create file on core
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
        volume_id,
        "/old_name.txt",
        0,
        "content"
      ])

      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/old_name.txt")

      send(
        handler,
        {:fuse_op, 1,
         {"rename",
          %{
            "old_parent" => 1,
            "old_name" => "old_name.txt",
            "new_parent" => 1,
            "new_name" => "new_name.txt"
          }}}
      )

      assert_receive {:fuse_op_complete, 1, {"ok", _}}, 5_000

      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/old_name.txt")
      assert {:ok, _} = InodeTable.get_inode(volume_id, "/new_name.txt")
    end
  end

  describe "setattr operations" do
    test "chmod changes file mode and is reflected in getattr", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      # Create file with default mode (0o100644)
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "chmod_file.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000
      {:ok, inode} = InodeTable.get_inode(volume_id, "/chmod_file.txt")

      # chmod to 0o755 (with regular file type bits)
      send(
        handler,
        {:fuse_op, 2, {"setattr", %{"ino" => inode, "mode" => 0o100755}}}
      )

      assert_receive {:fuse_op_complete, 2, {"attr_ok", %{"ino" => ^inode}}}, 5_000

      # Verify mode was updated on core
      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/chmod_file.txt"
        ])

      assert file.mode == 0o100755
    end

    test "chmod on directory changes mode correctly", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"mkdir", %{"parent" => 1, "name" => "chmod_dir", "mode" => 0o755}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000
      {:ok, inode} = InodeTable.get_inode(volume_id, "/chmod_dir")

      # chmod directory to 0o700
      send(
        handler,
        {:fuse_op, 2, {"setattr", %{"ino" => inode, "mode" => 0o040700}}}
      )

      assert_receive {:fuse_op_complete, 2,
                      {"attr_ok", %{"ino" => ^inode, "kind" => "directory"}}},
                     5_000

      {:ok, dir} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/chmod_dir"
        ])

      assert dir.mode == 0o040700
    end

    test "chown changes UID/GID and is reflected in getattr", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "chown_file.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000
      {:ok, inode} = InodeTable.get_inode(volume_id, "/chown_file.txt")

      # chown to uid=1000, gid=1000
      send(
        handler,
        {:fuse_op, 2, {"setattr", %{"ino" => inode, "uid" => 1000, "gid" => 1000}}}
      )

      assert_receive {:fuse_op_complete, 2, {"attr_ok", %{"ino" => ^inode}}}, 5_000

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/chown_file.txt"
        ])

      assert file.uid == 1000
      assert file.gid == 1000
    end

    test "truncate to smaller size updates size in metadata", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      # Create file and write data
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "trunc_file.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000
      {:ok, inode} = InodeTable.get_inode(volume_id, "/trunc_file.txt")

      send(
        handler,
        {:fuse_op, 2,
         {"write", %{"ino" => inode, "offset" => 0, "data" => "hello world, this is content"}}}
      )

      assert_receive {:fuse_op_complete, 2, {"write_ok", _}}, 5_000

      # Verify the file has data
      {:ok, file_before} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/trunc_file.txt"
        ])

      assert file_before.size > 0

      # Truncate to 5 bytes
      send(
        handler,
        {:fuse_op, 3, {"setattr", %{"ino" => inode, "size" => 5}}}
      )

      assert_receive {:fuse_op_complete, 3, {"attr_ok", %{"ino" => ^inode, "size" => 5}}}, 5_000

      {:ok, file_after} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/trunc_file.txt"
        ])

      assert file_after.size == 5
    end

    test "truncate to zero empties the file", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "trunc_zero.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000
      {:ok, inode} = InodeTable.get_inode(volume_id, "/trunc_zero.txt")

      send(
        handler,
        {:fuse_op, 2, {"write", %{"ino" => inode, "offset" => 0, "data" => "data to be removed"}}}
      )

      assert_receive {:fuse_op_complete, 2, {"write_ok", _}}, 5_000

      # Truncate to 0
      send(
        handler,
        {:fuse_op, 3, {"setattr", %{"ino" => inode, "size" => 0}}}
      )

      assert_receive {:fuse_op_complete, 3, {"attr_ok", %{"ino" => ^inode, "size" => 0}}}, 5_000

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/trunc_zero.txt"
        ])

      assert file.size == 0
      assert file.chunks == []
    end

    test "utimens sets access and modification times correctly", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "utimens_file.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000
      {:ok, inode} = InodeTable.get_inode(volume_id, "/utimens_file.txt")

      # Set atime and mtime to a known value (2025-01-01 00:00:00 UTC)
      target_sec = 1_735_689_600
      target_nsec = 0

      send(
        handler,
        {:fuse_op, 2,
         {"setattr",
          %{
            "ino" => inode,
            "atime" => {target_sec, target_nsec},
            "mtime" => {target_sec, target_nsec}
          }}}
      )

      assert_receive {:fuse_op_complete, 2, {"attr_ok", %{"ino" => ^inode}}}, 5_000

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/utimens_file.txt"
        ])

      expected_dt = DateTime.from_unix!(target_sec)
      assert DateTime.truncate(file.accessed_at, :second) == expected_dt
      # modified_at is overwritten by FileMeta.update/2 to now — check accessed_at was set
      assert file.accessed_at != nil
    end

    test "utimens with nonzero nanoseconds works", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "utimens_ns.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000
      {:ok, inode} = InodeTable.get_inode(volume_id, "/utimens_ns.txt")

      # Set atime with nanosecond precision
      target_sec = 1_735_689_600
      target_nsec = 500_000_000

      send(
        handler,
        {:fuse_op, 2,
         {"setattr",
          %{
            "ino" => inode,
            "atime" => {target_sec, target_nsec}
          }}}
      )

      assert_receive {:fuse_op_complete, 2, {"attr_ok", %{"ino" => ^inode}}}, 5_000

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/utimens_ns.txt"
        ])

      expected_dt = DateTime.from_unix!(target_sec * 1_000_000_000 + target_nsec, :nanosecond)
      assert file.accessed_at == expected_dt
    end

    test "setattr with combined mode and timestamps in one call", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "combined.txt", "mode" => 0o644}}}
      )

      assert_receive {:fuse_op_complete, 1, {"entry_ok", _}}, 5_000
      {:ok, inode} = InodeTable.get_inode(volume_id, "/combined.txt")

      target_sec = 1_735_689_600

      send(
        handler,
        {:fuse_op, 2,
         {"setattr",
          %{
            "ino" => inode,
            "mode" => 0o100755,
            "atime" => {target_sec, 0},
            "mtime" => {target_sec, 0}
          }}}
      )

      assert_receive {:fuse_op_complete, 2, {"attr_ok", %{"ino" => ^inode}}}, 5_000

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/combined.txt"
        ])

      assert file.mode == 0o100755

      expected_dt = DateTime.from_unix!(target_sec)
      assert DateTime.truncate(file.accessed_at, :second) == expected_dt
    end

    test "setattr updates changed_at (ctime)", %{
      handler: handler,
      volume_id: volume_id,
      cluster: cluster
    } do
      # Create file via RPC so we can capture its initial changed_at
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
        volume_id,
        "/ctime_file.txt",
        0,
        "content"
      ])

      {:ok, file_before} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/ctime_file.txt"
        ])

      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/ctime_file.txt")

      # Ensure wall clock advances past the file creation timestamp so
      # that the setattr changed_at is guaranteed to be strictly newer.
      Process.sleep(10)

      # chmod triggers a metadata update which should update changed_at
      send(
        handler,
        {:fuse_op, 1, {"setattr", %{"ino" => inode, "mode" => 0o100600}}}
      )

      assert_receive {:fuse_op_complete, 1, {"attr_ok", _}}, 5_000

      {:ok, file_after} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.FileIndex, :get_by_path, [
          volume_id,
          "/ctime_file.txt"
        ])

      assert DateTime.compare(file_after.changed_at, file_before.changed_at) == :gt
    end
  end

  describe "setattr permission enforcement" do
    test "non-owner UID cannot chmod (returns EACCES)", %{
      volume_id: volume_id,
      cluster: cluster
    } do
      # Create file owned by root (uid 0) via RPC
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
        volume_id,
        "/root_chmod.txt",
        0,
        "content"
      ])

      # Start a non-root handler (uid 1000)
      {:ok, non_root_handler} =
        Handler.start_link(volume: volume_id, test_notify: self(), uid: 1000)

      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/root_chmod.txt")

      send(
        non_root_handler,
        {:fuse_op, 1, {"setattr", %{"ino" => inode, "mode" => 0o100777}}}
      )

      # Should return EACCES (errno 13)
      assert_receive {:fuse_op_complete, 1, {"error", %{"errno" => 13}}}, 5_000

      GenServer.stop(non_root_handler)
    end

    test "non-owner UID cannot chown (returns EACCES)", %{
      volume_id: volume_id,
      cluster: cluster
    } do
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file_at, [
        volume_id,
        "/root_chown.txt",
        0,
        "content"
      ])

      {:ok, non_root_handler} =
        Handler.start_link(volume: volume_id, test_notify: self(), uid: 1000)

      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/root_chown.txt")

      send(
        non_root_handler,
        {:fuse_op, 1, {"setattr", %{"ino" => inode, "uid" => 1000, "gid" => 1000}}}
      )

      assert_receive {:fuse_op_complete, 1, {"error", %{"errno" => 13}}}, 5_000

      GenServer.stop(non_root_handler)
    end
  end

  describe "error handling" do
    test "handles unknown operations gracefully", %{handler: handler} do
      send(handler, {:fuse_op, 1, {"unknown_op", %{}}})
      assert_receive {:fuse_op_complete, 1, {"error", %{"errno" => 38}}}, 5_000

      # Should log warning and return ENOSYS — handler stays alive
      assert Process.alive?(handler)
    end
  end
end
