defmodule NeonFS.Core.FileIndexTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{ChunkMeta, DirectoryEntry, FileIndex, FileMeta, MetadataRing}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    # Set up mock quorum infrastructure
    store = :ets.new(:test_file_store, [:set, :public])

    ring =
      MetadataRing.new([node()],
        virtual_nodes_per_physical: 4,
        replicas: 1
      )

    write_fn = fn _node, _segment, key, value ->
      :ets.insert(store, {key, value})
      :ok
    end

    read_fn = fn _node, _segment, key ->
      case :ets.lookup(store, key) do
        [{^key, value}] -> {:ok, value, {1_000_000, 0, node()}}
        [] -> {:error, :not_found}
      end
    end

    delete_fn = fn _node, _segment, key ->
      :ets.delete(store, key)
      :ok
    end

    quorum_opts = [
      ring: ring,
      write_fn: write_fn,
      read_fn: read_fn,
      delete_fn: delete_fn,
      quarantine_checker: fn _ -> false end,
      read_repair_fn: fn _work_fn, _opts -> {:ok, "noop"} end,
      local_node: node()
    ]

    stop_if_running(NeonFS.Core.FileIndex)
    cleanup_ets_table(:file_index_by_id)

    start_supervised!(
      {NeonFS.Core.FileIndex, quorum_opts: quorum_opts},
      restart: :temporary
    )

    on_exit(fn ->
      cleanup_test_dirs()

      try do
        :ets.delete(store)
      rescue
        ArgumentError -> :ok
      end
    end)

    %{store: store, quorum_opts: quorum_opts}
  end

  describe "create/1" do
    test "creates a new file metadata entry" do
      file = FileMeta.new("vol1", "/test.txt")
      assert {:ok, ^file} = FileIndex.create(file)

      # Verify it was stored in ETS cache
      assert {:ok, retrieved} = FileIndex.get(file.id)
      assert retrieved.id == file.id
      assert retrieved.path == "/test.txt"
      assert retrieved.volume_id == "vol1"
    end

    test "writes both FileMeta and DirectoryEntry", %{store: store} do
      file = FileMeta.new("vol1", "/test.txt")
      assert {:ok, _} = FileIndex.create(file)

      # Check file metadata is in quorum store
      file_key = "file:" <> file.id
      assert [{^file_key, stored}] = :ets.lookup(store, file_key)
      assert stored[:id] == file.id

      # Check directory entry is in quorum store
      dir_key = "dir:vol1:/"
      assert [{^dir_key, dir_data}] = :ets.lookup(store, dir_key)
      assert dir_data[:children]["test.txt"] == %{type: :file, id: file.id}
    end

    test "creates nested parent directories automatically", %{store: store} do
      file = FileMeta.new("vol1", "/a/b/c/file.txt")
      assert {:ok, _} = FileIndex.create(file)

      # All intermediate directories should exist
      for path <- ["/", "/a", "/a/b", "/a/b/c"] do
        dir_key = "dir:vol1:#{path}"
        assert [{^dir_key, _}] = :ets.lookup(store, dir_key)
      end
    end

    test "allows same path in different volumes" do
      file1 = FileMeta.new("vol1", "/test.txt")
      file2 = FileMeta.new("vol2", "/test.txt")

      assert {:ok, _} = FileIndex.create(file1)
      assert {:ok, _} = FileIndex.create(file2)
    end

    test "normalises paths when creating" do
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

    test "quorum read populates ETS cache on miss", %{store: store} do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, _} = FileIndex.create(file)

      # Remove from ETS cache manually
      :ets.delete(:file_index_by_id, file.id)
      assert [] = :ets.lookup(:file_index_by_id, file.id)

      # Verify still in quorum store
      file_key = "file:" <> file.id
      assert [{^file_key, _}] = :ets.lookup(store, file_key)

      # get/1 should fall back to quorum and re-populate ETS
      assert {:ok, retrieved} = FileIndex.get(file.id)
      assert retrieved.id == file.id
      assert [{_, _}] = :ets.lookup(:file_index_by_id, file.id)
    end
  end

  describe "get_by_path/2" do
    test "retrieves file by volume and path via DirectoryEntry" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, _} = FileIndex.create(file)

      assert {:ok, retrieved} = FileIndex.get_by_path("vol1", "/test.txt")
      assert retrieved.id == file.id
      assert retrieved.path == "/test.txt"
    end

    test "resolves nested paths through directory entries" do
      file = FileMeta.new("vol1", "/docs/api/guide.md")
      {:ok, _} = FileIndex.create(file)

      assert {:ok, retrieved} = FileIndex.get_by_path("vol1", "/docs/api/guide.md")
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
    end

    test "updates chunks list" do
      file = FileMeta.new("vol1", "/test.txt", chunks: [])
      {:ok, created} = FileIndex.create(file)

      chunk_hash = :crypto.hash(:sha256, "test data")
      assert {:ok, updated} = FileIndex.update(created.id, chunks: [chunk_hash])
      assert updated.chunks == [chunk_hash]
    end

    test "returns error if file not found" do
      assert {:error, :not_found} = FileIndex.update("nonexistent-id", size: 1024)
    end
  end

  describe "delete/1" do
    test "deletes file metadata from quorum and ETS" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, created} = FileIndex.create(file)

      assert :ok = FileIndex.delete(created.id)

      # File should not be found
      assert {:error, :not_found} = FileIndex.get(created.id)
    end

    test "removes from both quorum store and ETS cache", %{store: store} do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, created} = FileIndex.create(file)

      assert :ok = FileIndex.delete(created.id)

      # ETS should be empty
      assert [] = :ets.lookup(:file_index_by_id, created.id)

      # Quorum store should be empty
      file_key = "file:" <> created.id
      assert [] = :ets.lookup(store, file_key)
    end

    test "removes child from parent DirectoryEntry", %{store: store} do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, created} = FileIndex.create(file)

      # Verify child exists in directory
      dir_key = "dir:vol1:/"
      [{^dir_key, dir_data}] = :ets.lookup(store, dir_key)
      assert Map.has_key?(dir_data[:children], "test.txt")

      assert :ok = FileIndex.delete(created.id)

      # Child should be removed from directory
      [{^dir_key, updated_dir}] = :ets.lookup(store, dir_key)
      refute Map.has_key?(updated_dir[:children], "test.txt")
    end

    test "returns error if file not found" do
      assert {:error, :not_found} = FileIndex.delete("nonexistent-id")
    end
  end

  describe "list_dir/2" do
    test "returns children of a directory" do
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/file1.txt"))
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/file2.txt"))

      assert {:ok, children} = FileIndex.list_dir("vol1", "/")
      assert Map.has_key?(children, "file1.txt")
      assert Map.has_key?(children, "file2.txt")
    end

    test "returns empty map for directory with no files" do
      assert {:ok, children} = FileIndex.list_dir("vol1", "/empty")
      assert children == %{}
    end

    test "returns children of subdirectory" do
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/docs/readme.md"))
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/docs/guide.md"))

      assert {:ok, children} = FileIndex.list_dir("vol1", "/docs")
      assert Map.has_key?(children, "readme.md")
      assert Map.has_key?(children, "guide.md")
    end

    test "distinguishes between volumes" do
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/file.txt"))
      {:ok, _} = FileIndex.create(FileMeta.new("vol2", "/other.txt"))

      assert {:ok, vol1_children} = FileIndex.list_dir("vol1", "/")
      assert {:ok, vol2_children} = FileIndex.list_dir("vol2", "/")

      assert Map.has_key?(vol1_children, "file.txt")
      refute Map.has_key?(vol1_children, "other.txt")
      assert Map.has_key?(vol2_children, "other.txt")
    end
  end

  describe "mkdir/3" do
    test "creates a directory and adds to parent" do
      assert {:ok, dir} = FileIndex.mkdir("vol1", "/documents")

      assert %DirectoryEntry{} = dir
      assert dir.parent_path == "/documents"
      assert dir.volume_id == "vol1"
    end

    test "created directory appears in parent listing" do
      {:ok, _} = FileIndex.mkdir("vol1", "/documents")

      assert {:ok, children} = FileIndex.list_dir("vol1", "/")
      assert Map.has_key?(children, "documents")
      assert children["documents"].type == :dir
    end

    test "creates nested directories" do
      {:ok, _} = FileIndex.mkdir("vol1", "/a/b/c")

      assert {:ok, root_children} = FileIndex.list_dir("vol1", "/")
      assert Map.has_key?(root_children, "a")

      assert {:ok, a_children} = FileIndex.list_dir("vol1", "/a")
      assert Map.has_key?(a_children, "b")

      assert {:ok, b_children} = FileIndex.list_dir("vol1", "/a/b")
      assert Map.has_key?(b_children, "c")
    end
  end

  describe "rename/4" do
    test "renames within directory" do
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/old.txt"))

      assert :ok = FileIndex.rename("vol1", "/", "old.txt", "new.txt")

      assert {:ok, children} = FileIndex.list_dir("vol1", "/")
      assert Map.has_key?(children, "new.txt")
      refute Map.has_key?(children, "old.txt")
    end

    test "returns error for non-existent source name" do
      # Ensure root exists
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/placeholder.txt"))

      assert {:error, :not_found} = FileIndex.rename("vol1", "/", "nope", "new.txt")
    end

    test "returns error if target name already exists" do
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/a.txt"))
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/b.txt"))

      assert {:error, :already_exists} = FileIndex.rename("vol1", "/", "a.txt", "b.txt")
    end
  end

  describe "move/4" do
    test "moves file across directories" do
      {:ok, _} = FileIndex.mkdir("vol1", "/src")
      {:ok, _} = FileIndex.mkdir("vol1", "/dest")
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/src/file.txt"))

      assert :ok = FileIndex.move("vol1", "/src", "/dest", "file.txt")

      # Should be gone from source
      assert {:ok, src_children} = FileIndex.list_dir("vol1", "/src")
      refute Map.has_key?(src_children, "file.txt")

      # Should be in destination
      assert {:ok, dest_children} = FileIndex.list_dir("vol1", "/dest")
      assert Map.has_key?(dest_children, "file.txt")
    end

    test "returns error if source doesn't contain the item" do
      {:ok, _} = FileIndex.mkdir("vol1", "/src")
      {:ok, _} = FileIndex.mkdir("vol1", "/dest")

      assert {:error, :not_found} = FileIndex.move("vol1", "/src", "/dest", "nope.txt")
    end
  end

  describe "ensure_root_dir/1" do
    test "creates root directory entry for volume", %{store: store} do
      assert :ok = FileIndex.ensure_root_dir("vol1")

      dir_key = "dir:vol1:/"
      assert [{^dir_key, dir_data}] = :ets.lookup(store, dir_key)
      assert dir_data[:parent_path] == "/"
      assert dir_data[:volume_id] == "vol1"
    end

    test "is idempotent" do
      assert :ok = FileIndex.ensure_root_dir("vol1")
      assert :ok = FileIndex.ensure_root_dir("vol1")
    end
  end

  describe "list_all/0" do
    test "lists all files from ETS cache" do
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

  describe "list_volume/1" do
    test "lists files for a specific volume" do
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/a.txt"))
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/b.txt"))
      {:ok, _} = FileIndex.create(FileMeta.new("vol2", "/c.txt"))

      vol1_files = FileIndex.list_volume("vol1")
      assert length(vol1_files) == 2

      vol2_files = FileIndex.list_volume("vol2")
      assert length(vol2_files) == 1
    end

    test "returns empty list for volume with no files" do
      assert [] = FileIndex.list_volume("empty-volume")
    end
  end

  describe "key format" do
    test "uses file: prefix for file metadata", %{store: store} do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, _} = FileIndex.create(file)

      expected_key = "file:" <> file.id
      assert [{^expected_key, _}] = :ets.lookup(store, expected_key)
    end

    test "uses dir: prefix for directory entries", %{store: store} do
      {:ok, _} = FileIndex.create(FileMeta.new("vol1", "/test.txt"))

      expected_key = "dir:vol1:/"
      assert [{^expected_key, _}] = :ets.lookup(store, expected_key)
    end
  end

  describe "path traversal" do
    test "create nested path and resolve via get_by_path" do
      file = FileMeta.new("vol1", "/a/b/c/file.txt")
      {:ok, _} = FileIndex.create(file)

      assert {:ok, retrieved} = FileIndex.get_by_path("vol1", "/a/b/c/file.txt")
      assert retrieved.id == file.id
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
  end

  describe "concurrent operations" do
    test "handles concurrent reads" do
      file = FileMeta.new("vol1", "/test.txt")
      {:ok, created} = FileIndex.create(file)

      tasks =
        for _ <- 1..100 do
          Task.async(fn -> FileIndex.get(created.id) end)
        end

      results = Task.await_many(tasks)

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

    test "hlc_timestamp field defaults to nil" do
      file = FileMeta.new("vol1", "/test.txt")
      assert file.hlc_timestamp == nil
    end

    test "parent_path extracts parent directory" do
      assert FileMeta.parent_path("/docs/api/file.txt") == "/docs/api"
      assert FileMeta.parent_path("/docs/file.txt") == "/docs"
      assert FileMeta.parent_path("/file.txt") == "/"
      assert FileMeta.parent_path("/") == nil
    end
  end

  describe "truncate/2" do
    setup do
      # Ensure chunk_index ETS table exists for ChunkIndex.get lookups
      case :ets.whereis(:chunk_index) do
        :undefined ->
          :ets.new(:chunk_index, [:set, :named_table, :public, read_concurrency: true])

        _ ->
          :ok
      end

      :ok
    end

    test "truncate to smaller size trims chunks list" do
      # Create 4 chunks of 256 bytes each (1024 bytes total)
      chunk_hashes = for i <- 1..4, do: :crypto.hash(:sha256, "chunk#{i}")

      Enum.each(chunk_hashes, fn hash ->
        :ets.insert(:chunk_index, {hash, ChunkMeta.new(hash, 256, 256)})
      end)

      file = FileMeta.new("vol1", "/test.txt", chunks: chunk_hashes, size: 1024)
      {:ok, created} = FileIndex.create(file)

      # Truncate to 512 bytes — should keep first 2 chunks
      assert {:ok, truncated} = FileIndex.truncate(created.id, 512)
      assert truncated.size == 512
      assert length(truncated.chunks) == 2
      assert truncated.chunks == Enum.take(chunk_hashes, 2)
      assert truncated.version == 2
    end

    test "truncate to mid-chunk boundary keeps partial chunk" do
      chunk_hashes = for i <- 1..4, do: :crypto.hash(:sha256, "chunk#{i}")

      Enum.each(chunk_hashes, fn hash ->
        :ets.insert(:chunk_index, {hash, ChunkMeta.new(hash, 256, 256)})
      end)

      file = FileMeta.new("vol1", "/mid.txt", chunks: chunk_hashes, size: 1024)
      {:ok, created} = FileIndex.create(file)

      # Truncate to 300 bytes — falls mid-way in chunk 2 (bytes 256-512)
      # Chunk 2 starts at offset 256, which is < 300, so it's kept
      assert {:ok, truncated} = FileIndex.truncate(created.id, 300)
      assert truncated.size == 300
      assert length(truncated.chunks) == 2
      assert truncated.chunks == Enum.take(chunk_hashes, 2)
    end

    test "truncate to 0 clears all chunks" do
      chunk_hashes = for i <- 1..3, do: :crypto.hash(:sha256, "chunk#{i}")

      Enum.each(chunk_hashes, fn hash ->
        :ets.insert(:chunk_index, {hash, ChunkMeta.new(hash, 256, 256)})
      end)

      file = FileMeta.new("vol1", "/empty.txt", chunks: chunk_hashes, size: 768)
      {:ok, created} = FileIndex.create(file)

      assert {:ok, truncated} = FileIndex.truncate(created.id, 0)
      assert truncated.size == 0
      assert truncated.chunks == []
      assert truncated.stripes == nil
    end

    test "truncate to current size is a no-op on chunks" do
      chunk_hashes = for i <- 1..2, do: :crypto.hash(:sha256, "chunk#{i}")

      Enum.each(chunk_hashes, fn hash ->
        :ets.insert(:chunk_index, {hash, ChunkMeta.new(hash, 512, 512)})
      end)

      file = FileMeta.new("vol1", "/same.txt", chunks: chunk_hashes, size: 1024)
      {:ok, created} = FileIndex.create(file)

      assert {:ok, same} = FileIndex.truncate(created.id, 1024)
      assert same.size == 1024
      assert same.chunks == chunk_hashes
    end

    test "truncate to larger size extends without adding chunks" do
      chunk_hash = :crypto.hash(:sha256, "only-chunk")
      :ets.insert(:chunk_index, {chunk_hash, ChunkMeta.new(chunk_hash, 512, 512)})

      file = FileMeta.new("vol1", "/grow.txt", chunks: [chunk_hash], size: 512)
      {:ok, created} = FileIndex.create(file)

      assert {:ok, extended} = FileIndex.truncate(created.id, 2048)
      assert extended.size == 2048
      assert extended.chunks == [chunk_hash]
    end

    test "truncate updates modified_at and changed_at" do
      chunk_hash = :crypto.hash(:sha256, "data")
      :ets.insert(:chunk_index, {chunk_hash, ChunkMeta.new(chunk_hash, 1024, 1024)})

      file = FileMeta.new("vol1", "/timestamps.txt", chunks: [chunk_hash], size: 1024)
      {:ok, created} = FileIndex.create(file)

      assert {:ok, truncated} = FileIndex.truncate(created.id, 100)
      assert DateTime.compare(truncated.modified_at, created.modified_at) in [:gt, :eq]
      assert DateTime.compare(truncated.changed_at, created.changed_at) in [:gt, :eq]
    end

    test "truncate trims stripes for erasure-coded files" do
      stripes = [
        %{stripe_id: "s1", byte_range: {0, 262_144}},
        %{stripe_id: "s2", byte_range: {262_144, 524_288}},
        %{stripe_id: "s3", byte_range: {524_288, 786_432}}
      ]

      file =
        FileMeta.new("vol1", "/ec.txt", size: 786_432)
        |> Map.put(:stripes, stripes)
        |> Map.put(:chunks, [])

      {:ok, created} = FileIndex.create(file)

      # Truncate to 300_000 — keeps stripe 1 (0..262144) fully and trims stripe 2
      assert {:ok, truncated} = FileIndex.truncate(created.id, 300_000)
      assert truncated.size == 300_000
      assert length(truncated.stripes) == 2

      [s1, s2] = truncated.stripes
      assert s1.byte_range == {0, 262_144}
      assert s2.byte_range == {262_144, 300_000}
    end

    test "truncate to 0 clears stripes" do
      stripes = [%{stripe_id: "s1", byte_range: {0, 262_144}}]

      file =
        FileMeta.new("vol1", "/ec-empty.txt", size: 262_144)
        |> Map.put(:stripes, stripes)
        |> Map.put(:chunks, [])

      {:ok, created} = FileIndex.create(file)

      assert {:ok, truncated} = FileIndex.truncate(created.id, 0)
      assert truncated.stripes == nil
      assert truncated.chunks == []
    end

    test "truncate returns error for non-existent file" do
      assert {:error, :not_found} = FileIndex.truncate("nonexistent-id", 100)
    end
  end

  describe "ACL serialisation round-trip" do
    test "acl_entries and default_acl survive create/get round-trip" do
      acl_entries = [
        %{type: :user, id: 1000, permissions: MapSet.new([:r, :w])},
        %{type: :group, id: 100, permissions: MapSet.new([:r])},
        %{type: :mask, id: nil, permissions: MapSet.new([:r, :w, :x])},
        %{type: :other, id: nil, permissions: MapSet.new([:r])}
      ]

      default_acl = [
        %{type: :user, id: nil, permissions: MapSet.new([:r, :w, :x])},
        %{type: :group, id: nil, permissions: MapSet.new([:r, :x])},
        %{type: :other, id: nil, permissions: MapSet.new([:r])}
      ]

      file =
        FileMeta.new("vol1", "/acl-test.txt",
          acl_entries: acl_entries,
          default_acl: default_acl
        )

      {:ok, created} = FileIndex.create(file)

      # Clear ETS cache to force quorum read + deserialization
      :ets.delete(:file_index_by_id, created.id)

      {:ok, retrieved} = FileIndex.get(created.id)

      assert retrieved.acl_entries == acl_entries
      assert retrieved.default_acl == default_acl
    end

    test "file without ACLs round-trips with defaults" do
      file = FileMeta.new("vol1", "/no-acl.txt")

      {:ok, created} = FileIndex.create(file)
      :ets.delete(:file_index_by_id, created.id)

      {:ok, retrieved} = FileIndex.get(created.id)

      assert retrieved.acl_entries == []
      assert retrieved.default_acl == nil
    end

    test "ACL entries survive update round-trip" do
      file = FileMeta.new("vol1", "/update-acl.txt")
      {:ok, created} = FileIndex.create(file)

      acl_entries = [
        %{type: :user, id: 1000, permissions: MapSet.new([:r, :w, :x])}
      ]

      {:ok, updated} = FileIndex.update(created.id, acl_entries: acl_entries)

      # Clear ETS cache to force quorum read
      :ets.delete(:file_index_by_id, updated.id)

      {:ok, retrieved} = FileIndex.get(updated.id)
      assert retrieved.acl_entries == acl_entries
    end

    test "ACL entries survive MetadataCodec round-trip" do
      alias NeonFS.Core.MetadataCodec

      acl_entries = [
        %{type: :user, id: 1000, permissions: MapSet.new([:r, :w])},
        %{type: :group, id: nil, permissions: MapSet.new([:r, :x])}
      ]

      default_acl = [
        %{type: :other, id: nil, permissions: MapSet.new([:r])}
      ]

      file =
        FileMeta.new("vol1", "/codec-test.txt",
          acl_entries: acl_entries,
          default_acl: default_acl
        )

      # Build a storable map matching what file_to_storable_map produces,
      # then round-trip through MetadataCodec to verify atom-to-string
      # conversion is handled correctly on decode.
      storable_map = %{
        id: file.id,
        volume_id: file.volume_id,
        path: file.path,
        chunks: file.chunks,
        stripes: file.stripes,
        size: file.size,
        content_type: file.content_type,
        mode: file.mode,
        uid: file.uid,
        gid: file.gid,
        acl_entries: file.acl_entries,
        default_acl: file.default_acl,
        created_at: file.created_at,
        modified_at: file.modified_at,
        accessed_at: file.accessed_at,
        changed_at: file.changed_at,
        version: file.version,
        previous_version_id: file.previous_version_id,
        hlc_timestamp: file.hlc_timestamp
      }

      record = %{
        value: storable_map,
        hlc_timestamp: {1_000_000, 0, node()},
        tombstone: false
      }

      {:ok, encoded} = MetadataCodec.encode_record(record)
      {:ok, decoded} = MetadataCodec.decode_record(encoded)

      # After msgpax round-trip, atom keys become strings and atom values
      # become strings. Verify the decoded value has ACL data in the
      # string-key form that storable_map_to_file must handle.
      decoded_value = decoded.value
      assert is_list(decoded_value["acl_entries"])
      assert length(decoded_value["acl_entries"]) == 2

      first_entry = hd(decoded_value["acl_entries"])
      assert first_entry["type"] == "user"
      assert first_entry["id"] == 1000
      assert MapSet.new(["r", "w"]) == first_entry["permissions"]

      assert [default_entry] = decoded_value["default_acl"]
      assert default_entry["type"] == "other"
    end

    test "storable_map_to_file reconstructs ACLs from string-keyed data", %{store: store} do
      file_id = UUIDv7.generate()

      # Simulate what MetadataCodec produces after msgpax round-trip:
      # all atom keys become strings, atom values become strings,
      # MapSets are preserved but contain strings instead of atoms.
      stringified = %{
        "id" => file_id,
        "volume_id" => "vol1",
        "path" => "/string-keys.txt",
        "chunks" => [],
        "stripes" => nil,
        "size" => 42,
        "content_type" => "text/plain",
        "mode" => 0o644,
        "uid" => 1000,
        "gid" => 1000,
        "acl_entries" => [
          %{"type" => "user", "id" => 1000, "permissions" => MapSet.new(["r", "w"])},
          %{"type" => "mask", "id" => nil, "permissions" => MapSet.new(["r", "w", "x"])}
        ],
        "default_acl" => [
          %{"type" => "other", "id" => nil, "permissions" => MapSet.new(["r"])}
        ],
        "created_at" => "2026-01-01T00:00:00Z",
        "modified_at" => "2026-01-01T00:00:00Z",
        "accessed_at" => "2026-01-01T00:00:00Z",
        "changed_at" => "2026-01-01T00:00:00Z",
        "version" => 1,
        "previous_version_id" => nil,
        "hlc_timestamp" => nil
      }

      # Inject directly into mock quorum store
      file_key = "file:" <> file_id
      :ets.insert(store, {file_key, stringified})

      # Also insert into ETS so get_by_path can find it... actually, just use get/1
      # which falls back to quorum when ETS misses.
      {:ok, retrieved} = FileIndex.get(file_id)

      assert retrieved.acl_entries == [
               %{type: :user, id: 1000, permissions: MapSet.new([:r, :w])},
               %{type: :mask, id: nil, permissions: MapSet.new([:r, :w, :x])}
             ]

      assert retrieved.default_acl == [
               %{type: :other, id: nil, permissions: MapSet.new([:r])}
             ]
    end
  end

  # Private helpers

  defp stop_if_running(name) do
    case Process.whereis(name) do
      nil ->
        :ok

      pid ->
        ref = Process.monitor(pid)
        GenServer.stop(pid, :normal, 5000)

        receive do
          {:DOWN, ^ref, :process, ^pid, _} -> :ok
        after
          1_000 -> :ok
        end
    end
  end

  defp cleanup_ets_table(table) do
    case :ets.whereis(table) do
      :undefined -> :ok
      ref -> :ets.delete(ref)
    end
  end
end
