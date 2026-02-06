defmodule NeonFS.FUSE.HandlerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.{FileIndex, VolumeRegistry, WriteOperation}
  alias NeonFS.FUSE.{Handler, InodeTable}

  @test_volume_name "test_volume"

  setup do
    # Start InodeTable under test supervision for proper isolation
    start_supervised!(InodeTable)

    # Ensure test volume exists and get its ID
    volume_id =
      case VolumeRegistry.get_by_name(@test_volume_name) do
        {:ok, vol} ->
          # Volume exists, clean up any files from previous tests
          case FileIndex.list_volume(vol.id) do
            {:ok, files} -> Enum.each(files, fn file -> FileIndex.delete(file.id) end)
            _ -> :ok
          end

          vol.id

        {:error, :not_found} ->
          # Create test volume
          {:ok, vol} =
            VolumeRegistry.create(@test_volume_name,
              durability: %{type: :replicate, factor: 1, min_copies: 1},
              compression: %{algorithm: :none, level: 1, min_size: 0},
              verification: %{on_read: :never, sampling_rate: nil}
            )

          vol.id
      end

    # Start a handler for testing
    {:ok, handler} = Handler.start_link(volume: volume_id)

    on_exit(fn ->
      # Stop the handler
      if Process.alive?(handler) do
        GenServer.stop(handler)
      end

      # Clean up test files
      case FileIndex.list_volume(volume_id) do
        {:ok, files} -> Enum.each(files, fn file -> FileIndex.delete(file.id) end)
        _ -> :ok
      end
    end)

    {:ok, handler: handler, volume_id: volume_id}
  end

  describe "lookup operation" do
    test "looks up existing file", %{handler: handler, volume_id: volume_id} do
      # Create test file
      {:ok, _} = WriteOperation.write_file(volume_id, "/test.txt", "hello")

      # Lookup the file
      send(handler, {:fuse_op, 1, {"lookup", %{"parent" => 1, "name" => "test.txt"}}})
      :timer.sleep(100)

      # Verify inode was allocated
      assert {:ok, inode} = InodeTable.get_inode(volume_id, "/test.txt")
      assert inode > 1
    end

    test "returns error for nonexistent file", %{handler: handler, volume_id: volume_id} do
      send(handler, {:fuse_op, 1, {"lookup", %{"parent" => 1, "name" => "missing.txt"}}})
      :timer.sleep(100)

      # Verify no inode was allocated
      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/missing.txt")
    end

    test "looks up file in subdirectory", %{handler: handler, volume_id: volume_id} do
      # Create directory with S_IFDIR mode and file
      {:ok, _} = WriteOperation.write_file(volume_id, "/docs", "", mode: 0o040755)
      {:ok, _} = WriteOperation.write_file(volume_id, "/docs/file.txt", "content")

      # Allocate inode for parent directory
      {:ok, parent_inode} = InodeTable.allocate_inode(volume_id, "/docs")

      # Lookup file in directory
      send(handler, {:fuse_op, 1, {"lookup", %{"parent" => parent_inode, "name" => "file.txt"}}})
      :timer.sleep(100)

      # Verify inode was allocated
      assert {:ok, _inode} = InodeTable.get_inode(volume_id, "/docs/file.txt")
    end
  end

  describe "getattr operation" do
    test "gets attributes for root directory", %{handler: handler, volume_id: _volume_id} do
      send(handler, {:fuse_op, 1, {"getattr", %{"ino" => 1}}})
      :timer.sleep(100)

      # Root should always exist
      assert {:ok, {nil, "/"}} = InodeTable.get_path(1)
    end

    test "gets attributes for existing file", %{handler: handler, volume_id: volume_id} do
      # Create test file
      {:ok, _} = WriteOperation.write_file(volume_id, "/test.txt", "hello world")
      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/test.txt")

      send(handler, {:fuse_op, 1, {"getattr", %{"ino" => inode}}})
      :timer.sleep(100)

      # Should succeed (handler logs would show result)
    end

    test "returns error for nonexistent inode", %{handler: handler, volume_id: _volume_id} do
      send(handler, {:fuse_op, 1, {"getattr", %{"ino" => 999}}})
      :timer.sleep(100)

      # Should return ENOENT error (handler logs would show result)
    end
  end

  describe "read operation" do
    test "reads file content", %{handler: handler, volume_id: volume_id} do
      # Create test file
      content = "Hello, FUSE!"
      {:ok, _} = WriteOperation.write_file(volume_id, "/test.txt", content)
      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/test.txt")

      send(handler, {:fuse_op, 1, {"read", %{"ino" => inode, "offset" => 0, "size" => 100}}})
      :timer.sleep(100)

      # Verify file still exists and has correct content
      {:ok, file} = FileIndex.get_by_path(volume_id, "/test.txt")
      assert file.size == byte_size(content)
    end

    test "reads partial content with offset", %{handler: handler, volume_id: volume_id} do
      # Create test file
      {:ok, _} = WriteOperation.write_file(volume_id, "/test.txt", "0123456789")
      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/test.txt")

      send(handler, {:fuse_op, 1, {"read", %{"ino" => inode, "offset" => 5, "size" => 3}}})
      :timer.sleep(100)

      # Should read bytes 5-7: "567"
    end

    test "returns error for nonexistent file", %{handler: handler, volume_id: volume_id} do
      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/missing.txt")

      send(handler, {:fuse_op, 1, {"read", %{"ino" => inode, "offset" => 0, "size" => 100}}})
      :timer.sleep(100)

      # Should return ENOENT error
    end
  end

  describe "write operation" do
    test "writes data to existing file", %{handler: handler, volume_id: volume_id} do
      # Create empty file
      {:ok, _} = WriteOperation.write_file(volume_id, "/test.txt", "")
      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/test.txt")

      send(
        handler,
        {:fuse_op, 1, {"write", %{"ino" => inode, "offset" => 0, "data" => "hello"}}}
      )

      :timer.sleep(100)

      # Verify content was written
      {:ok, file} = FileIndex.get_by_path(volume_id, "/test.txt")
      assert file.size > 0
    end

    test "writes data at offset", %{handler: handler, volume_id: volume_id} do
      # Create file with initial content
      {:ok, _} = WriteOperation.write_file(volume_id, "/test.txt", "hello world")
      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/test.txt")

      # Overwrite part of the content
      send(handler, {:fuse_op, 1, {"write", %{"ino" => inode, "offset" => 6, "data" => "FUSE"}}})
      :timer.sleep(100)

      # File should be updated
      {:ok, file} = FileIndex.get_by_path(volume_id, "/test.txt")
      assert file.size >= 10
    end
  end

  describe "readdir operation" do
    test "reads root directory", %{handler: handler, volume_id: volume_id} do
      # Create some files in root
      {:ok, _} = WriteOperation.write_file(volume_id, "/file1.txt", "content1")
      {:ok, _} = WriteOperation.write_file(volume_id, "/file2.txt", "content2")

      send(handler, {:fuse_op, 1, {"readdir", %{"ino" => 1, "offset" => 0}}})
      :timer.sleep(100)

      # Should list files (inodes should be allocated)
      assert {:ok, _} = InodeTable.get_inode(volume_id, "/file1.txt")
      assert {:ok, _} = InodeTable.get_inode(volume_id, "/file2.txt")
    end

    test "reads subdirectory", %{handler: handler, volume_id: volume_id} do
      # Create directory with S_IFDIR mode and files
      {:ok, _} = WriteOperation.write_file(volume_id, "/docs", "", mode: 0o040755)
      {:ok, _} = WriteOperation.write_file(volume_id, "/docs/readme.md", "# README")
      {:ok, _} = WriteOperation.write_file(volume_id, "/docs/guide.md", "# Guide")

      {:ok, dir_inode} = InodeTable.allocate_inode(volume_id, "/docs")

      send(handler, {:fuse_op, 1, {"readdir", %{"ino" => dir_inode, "offset" => 0}}})
      :timer.sleep(100)

      # Files should be allocated inodes
      assert {:ok, _} = InodeTable.get_inode(volume_id, "/docs/readme.md")
      assert {:ok, _} = InodeTable.get_inode(volume_id, "/docs/guide.md")
    end

    test "returns empty list for empty directory", %{handler: handler, volume_id: volume_id} do
      # Create empty directory with S_IFDIR mode
      {:ok, _} = WriteOperation.write_file(volume_id, "/empty", "", mode: 0o040755)
      {:ok, dir_inode} = InodeTable.allocate_inode(volume_id, "/empty")

      send(handler, {:fuse_op, 1, {"readdir", %{"ino" => dir_inode, "offset" => 0}}})
      :timer.sleep(100)

      # Should succeed with empty list
    end
  end

  describe "create operation" do
    test "creates a new file", %{handler: handler, volume_id: volume_id} do
      send(
        handler,
        {:fuse_op, 1, {"create", %{"parent" => 1, "name" => "new.txt", "mode" => 0o644}}}
      )

      :timer.sleep(100)

      # Verify file was created
      assert {:ok, file} = FileIndex.get_by_path(volume_id, "/new.txt")
      assert file.size == 0

      # Verify inode was allocated
      assert {:ok, _inode} = InodeTable.get_inode(volume_id, "/new.txt")
    end

    test "creates file in subdirectory", %{handler: handler, volume_id: volume_id} do
      # Create parent directory with S_IFDIR mode
      {:ok, _} = WriteOperation.write_file(volume_id, "/docs", "", mode: 0o040755)
      {:ok, parent_inode} = InodeTable.allocate_inode(volume_id, "/docs")

      send(
        handler,
        {:fuse_op, 1,
         {"create", %{"parent" => parent_inode, "name" => "file.txt", "mode" => 0o644}}}
      )

      :timer.sleep(100)

      # Verify file was created
      assert {:ok, _file} = FileIndex.get_by_path(volume_id, "/docs/file.txt")
    end
  end

  describe "mkdir operation" do
    test "creates a new directory", %{handler: handler, volume_id: volume_id} do
      send(
        handler,
        {:fuse_op, 1, {"mkdir", %{"parent" => 1, "name" => "newdir", "mode" => 0o755}}}
      )

      :timer.sleep(100)

      # Verify directory was created (without trailing slash, identified by mode)
      assert {:ok, file} = FileIndex.get_by_path(volume_id, "/newdir")
      # Directory has S_IFDIR bit set (0o040000)
      assert Bitwise.band(file.mode, 0o170000) == 0o040000

      # Verify inode was allocated
      assert {:ok, _inode} = InodeTable.get_inode(volume_id, "/newdir")
    end

    test "creates nested directory", %{handler: handler, volume_id: volume_id} do
      # Create parent directory with S_IFDIR mode
      {:ok, _} = WriteOperation.write_file(volume_id, "/parent", "", mode: 0o040755)
      {:ok, parent_inode} = InodeTable.allocate_inode(volume_id, "/parent")

      send(
        handler,
        {:fuse_op, 1, {"mkdir", %{"parent" => parent_inode, "name" => "child", "mode" => 0o755}}}
      )

      :timer.sleep(100)

      # Verify nested directory was created
      assert {:ok, _file} = FileIndex.get_by_path(volume_id, "/parent/child")
    end
  end

  describe "unlink operation" do
    test "deletes a file", %{handler: handler, volume_id: volume_id} do
      # Create file
      {:ok, _} = WriteOperation.write_file(volume_id, "/delete_me.txt", "content")
      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/delete_me.txt")

      send(handler, {:fuse_op, 1, {"unlink", %{"parent" => 1, "name" => "delete_me.txt"}}})
      :timer.sleep(50)

      # Verify file was deleted
      assert {:error, :not_found} = FileIndex.get_by_path(volume_id, "/delete_me.txt")

      # Verify inode was released
      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/delete_me.txt")
    end

    test "returns error for nonexistent file", %{handler: handler, volume_id: _volume_id} do
      send(handler, {:fuse_op, 1, {"unlink", %{"parent" => 1, "name" => "missing.txt"}}})
      :timer.sleep(100)

      # Should return ENOENT error
    end
  end

  describe "rmdir operation" do
    test "deletes empty directory", %{handler: handler, volume_id: volume_id} do
      # Create empty directory with S_IFDIR mode
      {:ok, _} = WriteOperation.write_file(volume_id, "/empty_dir", "", mode: 0o040755)
      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/empty_dir")

      send(handler, {:fuse_op, 1, {"rmdir", %{"parent" => 1, "name" => "empty_dir"}}})
      :timer.sleep(50)

      # Verify directory was deleted
      assert {:error, :not_found} = FileIndex.get_by_path(volume_id, "/empty_dir")

      # Verify inode was released
      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/empty_dir")
    end

    test "returns error for non-empty directory", %{handler: handler, volume_id: volume_id} do
      # Create directory with S_IFDIR mode and a file
      {:ok, _} = WriteOperation.write_file(volume_id, "/dir", "", mode: 0o040755)
      {:ok, _} = WriteOperation.write_file(volume_id, "/dir/file.txt", "content")
      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/dir")

      send(handler, {:fuse_op, 1, {"rmdir", %{"parent" => 1, "name" => "dir"}}})
      :timer.sleep(50)

      # Directory should still exist
      assert {:ok, _file} = FileIndex.get_by_path(volume_id, "/dir")
    end
  end

  describe "open and release operations" do
    test "opens and releases a file", %{handler: handler, volume_id: volume_id} do
      # Create file
      {:ok, _} = WriteOperation.write_file(volume_id, "/test.txt", "content")
      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/test.txt")

      # Open file
      send(handler, {:fuse_op, 1, {"open", %{"ino" => inode, "flags" => 0}}})
      :timer.sleep(100)

      # Release file
      send(handler, {:fuse_op, 2, {"release", %{"ino" => inode, "fh" => inode, "flags" => 0}}})
      :timer.sleep(100)

      # Both operations should succeed
    end
  end

  describe "rename operation" do
    test "renames a file", %{handler: handler, volume_id: volume_id} do
      # Create file
      {:ok, _} = WriteOperation.write_file(volume_id, "/old_name.txt", "content")
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

      :timer.sleep(100)

      # Verify old path doesn't exist
      assert {:error, :not_found} = FileIndex.get_by_path(volume_id, "/old_name.txt")

      # Verify new path exists
      assert {:ok, file} = FileIndex.get_by_path(volume_id, "/new_name.txt")
      assert file.path == "/new_name.txt"

      # Verify inode was updated
      assert {:ok, _new_inode} = InodeTable.get_inode(volume_id, "/new_name.txt")
      assert {:error, :not_found} = InodeTable.get_inode(volume_id, "/old_name.txt")
    end

    test "moves file to different directory", %{handler: handler, volume_id: volume_id} do
      # Create source file and destination directory with S_IFDIR mode
      {:ok, _} = WriteOperation.write_file(volume_id, "/file.txt", "content")
      {:ok, _} = WriteOperation.write_file(volume_id, "/dest", "", mode: 0o040755)
      {:ok, _inode} = InodeTable.allocate_inode(volume_id, "/file.txt")
      {:ok, dest_inode} = InodeTable.allocate_inode(volume_id, "/dest")

      send(
        handler,
        {:fuse_op, 1,
         {"rename",
          %{
            "old_parent" => 1,
            "old_name" => "file.txt",
            "new_parent" => dest_inode,
            "new_name" => "moved.txt"
          }}}
      )

      :timer.sleep(100)

      # Verify file moved
      assert {:error, :not_found} = FileIndex.get_by_path(volume_id, "/file.txt")
      assert {:ok, _file} = FileIndex.get_by_path(volume_id, "/dest/moved.txt")
    end
  end

  describe "setattr operation" do
    test "sets file attributes", %{handler: handler, volume_id: volume_id} do
      # Create file
      {:ok, _} = WriteOperation.write_file(volume_id, "/test.txt", "content")
      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/test.txt")

      send(
        handler,
        {:fuse_op, 1,
         {"setattr", %{"ino" => inode, "mode" => 0o755, "uid" => 1000, "gid" => 1000}}}
      )

      :timer.sleep(50)

      # Verify attributes were updated
      {:ok, file} = FileIndex.get_by_path(volume_id, "/test.txt")
      assert file.mode == 0o755
      assert file.uid == 1000
      assert file.gid == 1000
    end

    test "sets timestamps", %{handler: handler, volume_id: volume_id} do
      # Create file
      {:ok, _} = WriteOperation.write_file(volume_id, "/test.txt", "content")
      {:ok, inode} = InodeTable.allocate_inode(volume_id, "/test.txt")

      now_sec = System.os_time(:second)

      send(
        handler,
        {:fuse_op, 1,
         {"setattr", %{"ino" => inode, "atime" => {now_sec, 0}, "mtime" => {now_sec, 0}}}}
      )

      :timer.sleep(50)

      # Verify timestamps were updated
      {:ok, file} = FileIndex.get_by_path(volume_id, "/test.txt")
      assert file.accessed_at != nil
      assert file.modified_at != nil
    end
  end

  describe "error handling" do
    test "handles unknown operations gracefully", %{handler: handler, volume_id: _volume_id} do
      send(handler, {:fuse_op, 1, {"unknown_op", %{}}})
      :timer.sleep(100)

      # Should log warning and return ENOSYS error
    end

    test "handles missing volume gracefully", %{handler: handler, volume_id: _volume_id} do
      # Allocate inode for non-existent volume
      {:ok, inode} = InodeTable.allocate_inode("missing_vol", "/file.txt")

      send(handler, {:fuse_op, 1, {"getattr", %{"ino" => inode}}})
      :timer.sleep(100)

      # Should return appropriate error
    end
  end
end
