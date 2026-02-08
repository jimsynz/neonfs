defmodule NeonFS.Integration.FuseHandlerTest do
  @moduledoc """
  Integration tests for the FUSE handler operating against a remote core node.

  These tests verify that FUSE operations work correctly when the handler
  routes calls to a core node via the NeonFS.Client infrastructure.
  """
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 120_000
  @moduletag nodes: 1

  alias NeonFS.Client.{Connection, CostFunction, Discovery}
  alias NeonFS.FUSE.{Handler, InodeTable}

  setup %{cluster: cluster} do
    # Initialise the cluster and create a test volume
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

    :ok =
      wait_until(fn ->
        case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
          {:ok, _status} -> true
          _ -> false
        end
      end)

    {:ok, volume_map} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "test-volume",
        %{}
      ])

    volume_id = volume_map[:id]

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

    # Start a handler for testing
    {:ok, handler} = Handler.start_link(volume: volume_id)

    on_exit(fn ->
      if Process.alive?(handler), do: GenServer.stop(handler)
    end)

    {:ok, handler: handler, volume_id: volume_id, cluster: cluster}
  end

  describe "lookup operation" do
    test "looks up existing file", %{handler: handler, volume_id: volume_id, cluster: cluster} do
      # Create test file on core node
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file, [
        volume_id,
        "/test.txt",
        "hello"
      ])

      send(handler, {:fuse_op, 1, {"lookup", %{"parent" => 1, "name" => "test.txt"}}})
      :timer.sleep(200)

      assert {:ok, inode} = InodeTable.get_inode(volume_id, "/test.txt")
      assert inode > 1
    end

    test "returns error for nonexistent file", %{handler: handler, volume_id: volume_id} do
      send(handler, {:fuse_op, 1, {"lookup", %{"parent" => 1, "name" => "missing.txt"}}})
      :timer.sleep(200)

      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/missing.txt")
    end
  end

  describe "getattr operation" do
    test "gets attributes for root directory", %{handler: handler} do
      send(handler, {:fuse_op, 1, {"getattr", %{"ino" => 1}}})
      :timer.sleep(200)

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

      :timer.sleep(200)

      # Verify inode was allocated
      assert {:ok, inode} = InodeTable.get_inode(volume_id, "/new.txt")

      # Write data to the file
      send(
        handler,
        {:fuse_op, 2, {"write", %{"ino" => inode, "offset" => 0, "data" => "hello world"}}}
      )

      :timer.sleep(200)

      # Read the data back
      send(handler, {:fuse_op, 3, {"read", %{"ino" => inode, "offset" => 0, "size" => 100}}})
      :timer.sleep(200)
    end
  end

  describe "directory operations" do
    test "creates and lists a directory", %{handler: handler, volume_id: volume_id} do
      # Create a directory
      send(
        handler,
        {:fuse_op, 1, {"mkdir", %{"parent" => 1, "name" => "docs", "mode" => 0o755}}}
      )

      :timer.sleep(200)

      assert {:ok, dir_inode} = InodeTable.get_inode(volume_id, "/docs")

      # Create a file inside the directory
      send(
        handler,
        {:fuse_op, 2,
         {"create", %{"parent" => dir_inode, "name" => "readme.md", "mode" => 0o644}}}
      )

      :timer.sleep(200)

      # List the directory
      send(handler, {:fuse_op, 3, {"readdir", %{"ino" => dir_inode, "offset" => 0}}})
      :timer.sleep(200)

      assert {:ok, _} = InodeTable.get_inode(volume_id, "/docs/readme.md")
    end

    test "reads root directory contents", %{handler: handler, volume_id: volume_id} do
      # Create files in root
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "file1.txt", "mode" => 0o644}}}
      )

      send(
        handler,
        {:fuse_op, 2, {"create", %{"parent" => 1, "name" => "file2.txt", "mode" => 0o644}}}
      )

      :timer.sleep(200)

      send(handler, {:fuse_op, 3, {"readdir", %{"ino" => 1, "offset" => 0}}})
      :timer.sleep(200)

      assert {:ok, _} = InodeTable.get_inode(volume_id, "/file1.txt")
      assert {:ok, _} = InodeTable.get_inode(volume_id, "/file2.txt")
    end
  end

  describe "unlink and rmdir operations" do
    test "deletes a file", %{handler: handler, volume_id: volume_id, cluster: cluster} do
      # Create file on core
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file, [
        volume_id,
        "/delete_me.txt",
        "content"
      ])

      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/delete_me.txt")

      send(handler, {:fuse_op, 1, {"unlink", %{"parent" => 1, "name" => "delete_me.txt"}}})
      :timer.sleep(200)

      # Verify inode was released
      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/delete_me.txt")
    end

    test "deletes empty directory", %{handler: handler, volume_id: volume_id, cluster: cluster} do
      # Create empty directory on core
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file, [
        volume_id,
        "/empty_dir",
        "",
        [mode: 0o040755]
      ])

      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/empty_dir")

      send(handler, {:fuse_op, 1, {"rmdir", %{"parent" => 1, "name" => "empty_dir"}}})
      :timer.sleep(200)

      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/empty_dir")
    end
  end

  describe "rename operation" do
    test "renames a file", %{handler: handler, volume_id: volume_id, cluster: cluster} do
      # Create file on core
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.WriteOperation, :write_file, [
        volume_id,
        "/old_name.txt",
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

      :timer.sleep(200)

      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/old_name.txt")
      assert {:ok, _} = InodeTable.get_inode(volume_id, "/new_name.txt")
    end
  end

  describe "error handling" do
    test "handles unknown operations gracefully", %{handler: handler} do
      send(handler, {:fuse_op, 1, {"unknown_op", %{}}})
      :timer.sleep(100)

      # Should log warning and return ENOSYS — handler stays alive
      assert Process.alive?(handler)
    end
  end
end
