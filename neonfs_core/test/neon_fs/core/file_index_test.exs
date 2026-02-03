defmodule NeonFS.Core.FileIndexTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.FileMeta

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_file_index()
    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  describe "create/1" do
    test "creates a new file metadata entry" do
      file = FileMeta.new("vol1", "/test.txt")
      assert {:ok, ^file} = FileIndex.create(file)

      # Verify it was stored
      assert {:ok, retrieved} = FileIndex.get(file.id)
      assert retrieved.id == file.id
      assert retrieved.path == "/test.txt"
      assert retrieved.volume_id == "vol1"
    end

    test "returns error if file ID already exists" do
      file = FileMeta.new("vol1", "/test.txt", id: "custom-id")
      assert {:ok, _} = FileIndex.create(file)

      # Try to create another file with same ID
      duplicate = FileMeta.new("vol1", "/other.txt", id: "custom-id")
      assert {:error, :already_exists} = FileIndex.create(duplicate)
    end

    test "returns error if path already exists in volume" do
      file1 = FileMeta.new("vol1", "/test.txt")
      assert {:ok, _} = FileIndex.create(file1)

      # Try to create another file at same path
      file2 = FileMeta.new("vol1", "/test.txt")
      assert {:error, :already_exists} = FileIndex.create(file2)
    end

    test "allows same path in different volumes" do
      file1 = FileMeta.new("vol1", "/test.txt")
      file2 = FileMeta.new("vol2", "/test.txt")

      assert {:ok, _} = FileIndex.create(file1)
      assert {:ok, _} = FileIndex.create(file2)
    end

    test "normalizes paths when creating" do
      file = FileMeta.new("vol1", "/test/path/")
      assert {:ok, created} = FileIndex.create(file)
      assert created.path == "/test/path"
    end

    test "rejects invalid paths" do
      file = %FileMeta{FileMeta.new("vol1", "/valid") | path: "no-leading-slash"}
      assert {:error, :invalid_path} = FileIndex.create(file)
    end
  end

  describe "get/1" do
    test "retrieves file by ID" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, _} = FileIndex.create(file)

      assert {:ok, retrieved} = FileIndex.get(file.id)
      assert retrieved.id == file.id
    end

    test "returns error if file not found" do
      assert {:error, :not_found} = FileIndex.get("nonexistent-id")
    end

    test "reads directly from ETS without GenServer call" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, _} = FileIndex.create(file)

      # This should work even if we bypass the GenServer
      assert [{_id, retrieved}] = :ets.lookup(:file_index_by_id, file.id)
      assert retrieved.id == file.id
    end
  end

  describe "get_by_path/2" do
    test "retrieves file by volume and path" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, _} = FileIndex.create(file)

      assert {:ok, retrieved} = FileIndex.get_by_path("vol1", "/test.txt")
      assert retrieved.id == file.id
      assert retrieved.path == "/test.txt"
    end

    test "normalizes path when looking up" do
      file = FileMeta.new("vol1", "/test/path")
      {:ok, _} = FileIndex.create(file)

      assert {:ok, retrieved} = FileIndex.get_by_path("vol1", "/test/path/")
      assert retrieved.id == file.id
    end

    test "returns error if file not found" do
      assert {:error, :not_found} = FileIndex.get_by_path("vol1", "/nonexistent.txt")
    end

    test "distinguishes between volumes" do
      file1 = FileMeta.new("vol1", "/test.txt")
      file2 = FileMeta.new("vol2", "/test.txt")

      {:ok, _} = FileIndex.create(file1)
      {:ok, _} = FileIndex.create(file2)

      assert {:ok, found1} = FileIndex.get_by_path("vol1", "/test.txt")
      assert {:ok, found2} = FileIndex.get_by_path("vol2", "/test.txt")

      assert found1.id == file1.id
      assert found2.id == file2.id
      refute found1.id == found2.id
    end
  end

  describe "update/2" do
    test "updates file metadata and increments version" do
      file = FileMeta.new("vol1", "/test.txt", size: 0, mode: 0o644)
      {:ok, created} = FileIndex.create(file)

      assert {:ok, updated} = FileIndex.update(created.id, size: 1024, mode: 0o755)
      assert updated.size == 1024
      assert updated.mode == 0o755
      assert updated.version == 2
      assert DateTime.compare(updated.modified_at, created.modified_at) == :gt
    end

    test "updates chunks list" do
      file = FileMeta.new("vol1", "/test.txt", chunks: [])
      {:ok, created} = FileIndex.create(file)

      chunk_hash = :crypto.hash(:sha256, "test data")
      assert {:ok, updated} = FileIndex.update(created.id, chunks: [chunk_hash])
      assert updated.chunks == [chunk_hash]
      assert updated.version == 2
    end

    test "handles path updates" do
      file = FileMeta.new("vol1", "/old.txt")
      {:ok, created} = FileIndex.create(file)

      assert {:ok, updated} = FileIndex.update(created.id, path: "/new.txt")
      assert updated.path == "/new.txt"
      assert updated.version == 2

      # Old path should not exist
      assert {:error, :not_found} = FileIndex.get_by_path("vol1", "/old.txt")

      # New path should work
      assert {:ok, found} = FileIndex.get_by_path("vol1", "/new.txt")
      assert found.id == created.id
    end

    test "rejects invalid path updates" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, created} = FileIndex.create(file)

      assert {:error, :invalid_path} = FileIndex.update(created.id, path: "no-leading-slash")
    end

    test "returns error if file not found" do
      assert {:error, :not_found} = FileIndex.update("nonexistent-id", size: 1024)
    end

    test "preserves fields not included in update" do
      file = FileMeta.new("vol1", "/test.txt", size: 100, mode: 0o644, uid: 1000)
      {:ok, created} = FileIndex.create(file)

      assert {:ok, updated} = FileIndex.update(created.id, size: 200)
      assert updated.size == 200
      assert updated.mode == 0o644
      assert updated.uid == 1000
    end
  end

  describe "delete/1" do
    test "deletes file metadata" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, created} = FileIndex.create(file)

      assert :ok = FileIndex.delete(created.id)

      # File should not be found
      assert {:error, :not_found} = FileIndex.get(created.id)
      assert {:error, :not_found} = FileIndex.get_by_path("vol1", "/test.txt")
    end

    test "removes from both ETS tables" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, created} = FileIndex.create(file)

      assert :ok = FileIndex.delete(created.id)

      # Check ETS tables directly
      assert [] = :ets.lookup(:file_index_by_id, created.id)
      assert [] = :ets.lookup(:file_index_by_path, {"vol1", "/test.txt"})
    end

    test "returns error if file not found" do
      assert {:error, :not_found} = FileIndex.delete("nonexistent-id")
    end
  end

  describe "list_dir/2" do
    setup do
      # Create a directory structure
      files = [
        FileMeta.new("vol1", "/file1.txt"),
        FileMeta.new("vol1", "/file2.txt"),
        FileMeta.new("vol1", "/docs/readme.md"),
        FileMeta.new("vol1", "/docs/guide.md"),
        FileMeta.new("vol1", "/docs/api/reference.md"),
        FileMeta.new("vol2", "/file1.txt")
      ]

      Enum.each(files, &FileIndex.create/1)

      :ok
    end

    test "lists files in root directory" do
      files = FileIndex.list_dir("vol1", "/")
      paths = Enum.map(files, & &1.path) |> Enum.sort()

      assert "/docs/api/reference.md" in paths
      assert "/docs/guide.md" in paths
      assert "/docs/readme.md" in paths
      assert "/file1.txt" in paths
      assert "/file2.txt" in paths
    end

    test "lists files in subdirectory" do
      files = FileIndex.list_dir("vol1", "/docs")
      paths = Enum.map(files, & &1.path) |> Enum.sort()

      assert paths == ["/docs/api/reference.md", "/docs/guide.md", "/docs/readme.md"]
    end

    test "lists files in nested subdirectory" do
      files = FileIndex.list_dir("vol1", "/docs/api")
      paths = Enum.map(files, & &1.path)

      assert paths == ["/docs/api/reference.md"]
    end

    test "returns empty list for directory with no files" do
      files = FileIndex.list_dir("vol1", "/empty")
      assert files == []
    end

    test "distinguishes between volumes" do
      files = FileIndex.list_dir("vol2", "/")
      paths = Enum.map(files, & &1.path)

      assert paths == ["/file1.txt"]
    end
  end

  describe "list_volume/1" do
    test "lists all files in a volume" do
      files = [
        FileMeta.new("vol1", "/file1.txt"),
        FileMeta.new("vol1", "/file2.txt"),
        FileMeta.new("vol2", "/file3.txt")
      ]

      Enum.each(files, &FileIndex.create/1)

      vol1_files = FileIndex.list_volume("vol1")
      assert length(vol1_files) == 2

      vol2_files = FileIndex.list_volume("vol2")
      assert length(vol2_files) == 1
    end

    test "returns empty list for volume with no files" do
      assert [] = FileIndex.list_volume("empty-volume")
    end
  end

  describe "list_all/0" do
    test "lists all files across all volumes" do
      files = [
        FileMeta.new("vol1", "/file1.txt"),
        FileMeta.new("vol2", "/file2.txt"),
        FileMeta.new("vol3", "/file3.txt")
      ]

      Enum.each(files, &FileIndex.create/1)

      all_files = FileIndex.list_all()
      assert length(all_files) == 3
    end
  end

  describe "path validation and normalization" do
    test "validates paths during creation" do
      invalid_paths = [
        "no-leading-slash",
        "/../escape",
        "/path/../escape",
        "/trailing/slash/"
      ]

      for path <- invalid_paths do
        file = %FileMeta{FileMeta.new("vol1", "/valid") | path: path}
        assert {:error, :invalid_path} = FileIndex.create(file)
      end
    end

    test "normalizes trailing slashes" do
      file = FileMeta.new("vol1", "/test/path/")
      {:ok, created} = FileIndex.create(file)

      assert created.path == "/test/path"
    end
  end

  describe "version tracking" do
    test "tracks version increments" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, v1} = FileIndex.create(file)
      assert v1.version == 1

      {:ok, v2} = FileIndex.update(v1.id, size: 100)
      assert v2.version == 2

      {:ok, v3} = FileIndex.update(v2.id, mode: 0o755)
      assert v3.version == 3
    end

    test "tracks previous version ID" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, v1} = FileIndex.create(file)

      # Create a new version with previous_version_id set
      file_v2 = FileMeta.new("vol1", "/test.txt.v2", previous_version_id: v1.id)
      {:ok, v2} = FileIndex.create(file_v2)

      assert v2.previous_version_id == v1.id
    end
  end

  describe "concurrent operations" do
    test "handles concurrent reads" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, created} = FileIndex.create(file)

      # Spawn multiple processes reading simultaneously
      tasks =
        for _ <- 1..100 do
          Task.async(fn -> FileIndex.get(created.id) end)
        end

      results = Task.await_many(tasks)

      # All should succeed
      assert Enum.all?(results, fn
               {:ok, _} -> true
               _ -> false
             end)
    end
  end

  describe "FileMeta helpers" do
    test "generates unique IDs" do
      file1 = FileMeta.new("vol1", "/test1.txt")
      file2 = FileMeta.new("vol1", "/test2.txt")

      refute file1.id == file2.id
    end

    test "IDs are UUIDv7 format" do
      file = FileMeta.new("vol1", "/test.txt")

      # UUIDv7 format: 8-4-4-4-12 hex digits
      assert file.id =~ ~r/^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$/
    end

    test "update helper increments version" do
      file = FileMeta.new("vol1", "/test.txt")
      updated = FileMeta.update(file, size: 1024)

      assert updated.version == 2
      assert updated.size == 1024
    end

    test "touch updates accessed_at without incrementing version" do
      file = FileMeta.new("vol1", "/test.txt")
      original_accessed = file.accessed_at

      # Sleep to ensure timestamp difference
      Process.sleep(10)

      touched = FileMeta.touch(file)

      assert touched.version == file.version
      assert DateTime.compare(touched.accessed_at, original_accessed) == :gt
    end

    test "parent_path extracts parent directory" do
      assert FileMeta.parent_path("/docs/api/file.txt") == "/docs/api"
      assert FileMeta.parent_path("/docs/file.txt") == "/docs"
      assert FileMeta.parent_path("/file.txt") == "/"
      assert FileMeta.parent_path("/") == nil
    end
  end
end
