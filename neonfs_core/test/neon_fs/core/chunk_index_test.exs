defmodule NeonFS.Core.ChunkIndexTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.ChunkIndex
  alias NeonFS.Core.ChunkMeta
  alias NeonFS.Core.MetadataRing

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    # Set up mock quorum infrastructure
    store = :ets.new(:test_quorum_store, [:set, :public])

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

    stop_if_running(NeonFS.Core.ChunkIndex)
    cleanup_ets_table(:chunk_index)

    start_supervised!(
      {NeonFS.Core.ChunkIndex, quorum_opts: quorum_opts},
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

  describe "put/1 and get/1" do
    test "stores and retrieves chunk metadata via quorum" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512, :zstd)

      assert :ok = ChunkIndex.put(chunk_meta)
      assert {:ok, retrieved} = ChunkIndex.get(hash)
      assert retrieved.hash == hash
      assert retrieved.original_size == 1024
      assert retrieved.stored_size == 512
      assert retrieved.compression == :zstd
      assert retrieved.commit_state == :uncommitted
    end

    test "returns error for non-existent chunk" do
      hash = :crypto.strong_rand_bytes(32)
      assert {:error, :not_found} = ChunkIndex.get(hash)
    end

    test "updates existing chunk metadata" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta1 = ChunkMeta.new(hash, 1024, 512, :none)
      chunk_meta2 = %{chunk_meta1 | stored_size: 256, compression: :zstd}

      assert :ok = ChunkIndex.put(chunk_meta1)
      assert :ok = ChunkIndex.put(chunk_meta2)

      assert {:ok, retrieved} = ChunkIndex.get(hash)
      assert retrieved.stored_size == 256
      assert retrieved.compression == :zstd
    end

    test "quorum read populates ETS cache on miss", %{store: store} do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512, :zstd)

      # Write via put (goes to quorum + ETS)
      assert :ok = ChunkIndex.put(chunk_meta)

      # Remove from ETS cache manually
      :ets.delete(:chunk_index, hash)
      assert [] = :ets.lookup(:chunk_index, hash)

      # Verify data is still in quorum store
      key = "chunk:" <> Base.encode16(hash)
      assert [{^key, _}] = :ets.lookup(store, key)

      # get/1 should fall back to quorum and re-populate ETS
      assert {:ok, retrieved} = ChunkIndex.get(hash)
      assert retrieved.hash == hash

      # ETS should be populated again
      assert [{^hash, _}] = :ets.lookup(:chunk_index, hash)
    end
  end

  describe "delete/1" do
    test "removes chunk metadata from quorum and ETS" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      assert :ok = ChunkIndex.put(chunk_meta)
      assert {:ok, _} = ChunkIndex.get(hash)

      assert :ok = ChunkIndex.delete(hash)
      assert {:error, :not_found} = ChunkIndex.get(hash)
    end

    test "deleting non-existent chunk is idempotent" do
      hash = :crypto.strong_rand_bytes(32)
      assert :ok = ChunkIndex.delete(hash)
      assert :ok = ChunkIndex.delete(hash)
    end
  end

  describe "exists?/1" do
    test "returns true for existing chunk" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      assert :ok = ChunkIndex.put(chunk_meta)
      assert ChunkIndex.exists?(hash)
    end

    test "returns false for non-existent chunk" do
      hash = :crypto.strong_rand_bytes(32)
      refute ChunkIndex.exists?(hash)
    end

    test "finds chunk via quorum when not in ETS cache", %{store: store} do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      assert :ok = ChunkIndex.put(chunk_meta)

      # Remove from ETS cache
      :ets.delete(:chunk_index, hash)

      # Verify data is still in quorum store
      key = "chunk:" <> Base.encode16(hash)
      assert [{^key, _}] = :ets.lookup(store, key)

      # exists? should find it via quorum
      assert ChunkIndex.exists?(hash)
    end
  end

  describe "list_all/0" do
    test "returns all chunks from ETS" do
      hashes =
        for _ <- 1..5 do
          hash = :crypto.strong_rand_bytes(32)
          chunk_meta = ChunkMeta.new(hash, 1024, 512)
          ChunkIndex.put(chunk_meta)
          hash
        end

      all = ChunkIndex.list_all()
      assert length(all) == 5
      all_hashes = Enum.map(all, & &1.hash) |> Enum.sort()
      assert Enum.sort(hashes) == all_hashes
    end

    test "returns empty list when no chunks exist" do
      assert [] = ChunkIndex.list_all()
    end
  end

  describe "list_by_location/1" do
    test "finds chunks at specific location" do
      hash1 = :crypto.strong_rand_bytes(32)
      hash2 = :crypto.strong_rand_bytes(32)
      hash3 = :crypto.strong_rand_bytes(32)

      location1 = %{node: :node1, drive_id: "drive1", tier: :hot}
      location2 = %{node: :node1, drive_id: "drive2", tier: :hot}
      location3 = %{node: :node2, drive_id: "drive1", tier: :cold}

      chunk1 = ChunkMeta.new(hash1, 1024, 512) |> ChunkMeta.add_location(location1)
      chunk2 = ChunkMeta.new(hash2, 2048, 1024) |> ChunkMeta.add_location(location2)

      chunk3 =
        ChunkMeta.new(hash3, 4096, 2048)
        |> ChunkMeta.add_location(location1)
        |> ChunkMeta.add_location(location3)

      ChunkIndex.put(chunk1)
      ChunkIndex.put(chunk2)
      ChunkIndex.put(chunk3)

      # Query location1
      chunks = ChunkIndex.list_by_location(location1)
      hashes = Enum.map(chunks, & &1.hash) |> Enum.sort()
      assert Enum.sort([hash1, hash3]) == hashes

      # Query location2
      chunks = ChunkIndex.list_by_location(location2)
      assert length(chunks) == 1
      assert hd(chunks).hash == hash2

      # Query location3
      chunks = ChunkIndex.list_by_location(location3)
      assert length(chunks) == 1
      assert hd(chunks).hash == hash3

      # Query non-existent location
      chunks = ChunkIndex.list_by_location(%{node: :node999, drive_id: "drive999", tier: :hot})
      assert chunks == []
    end
  end

  describe "list_by_node/1" do
    test "finds all chunks on a specific node" do
      hash1 = :crypto.strong_rand_bytes(32)
      hash2 = :crypto.strong_rand_bytes(32)
      hash3 = :crypto.strong_rand_bytes(32)

      location1 = %{node: :node1, drive_id: "drive1", tier: :hot}
      location2 = %{node: :node1, drive_id: "drive2", tier: :cold}
      location3 = %{node: :node2, drive_id: "drive1", tier: :warm}

      chunk1 = ChunkMeta.new(hash1, 1024, 512) |> ChunkMeta.add_location(location1)
      chunk2 = ChunkMeta.new(hash2, 2048, 1024) |> ChunkMeta.add_location(location2)
      chunk3 = ChunkMeta.new(hash3, 4096, 2048) |> ChunkMeta.add_location(location3)

      ChunkIndex.put(chunk1)
      ChunkIndex.put(chunk2)
      ChunkIndex.put(chunk3)

      # Query node1 - should return chunk1 and chunk2
      chunks = ChunkIndex.list_by_node(:node1)
      hashes = Enum.map(chunks, & &1.hash) |> Enum.sort()
      assert Enum.sort([hash1, hash2]) == hashes

      # Query node2 - should return only chunk3
      chunks = ChunkIndex.list_by_node(:node2)
      assert length(chunks) == 1
      assert hd(chunks).hash == hash3

      # Query non-existent node
      chunks = ChunkIndex.list_by_node(:node999)
      assert chunks == []
    end
  end

  describe "list_by_drive/2" do
    test "finds chunks on a specific node and drive" do
      hash1 = :crypto.strong_rand_bytes(32)
      hash2 = :crypto.strong_rand_bytes(32)
      hash3 = :crypto.strong_rand_bytes(32)

      location1 = %{node: :node1, drive_id: "drive1", tier: :hot}
      location2 = %{node: :node1, drive_id: "drive2", tier: :hot}
      location3 = %{node: :node2, drive_id: "drive1", tier: :cold}

      chunk1 = ChunkMeta.new(hash1, 1024, 512) |> ChunkMeta.add_location(location1)
      chunk2 = ChunkMeta.new(hash2, 2048, 1024) |> ChunkMeta.add_location(location2)

      chunk3 =
        ChunkMeta.new(hash3, 4096, 2048)
        |> ChunkMeta.add_location(location1)
        |> ChunkMeta.add_location(location3)

      ChunkIndex.put(chunk1)
      ChunkIndex.put(chunk2)
      ChunkIndex.put(chunk3)

      # Query node1/drive1 - should return chunk1 and chunk3
      chunks = ChunkIndex.list_by_drive(:node1, "drive1")
      hashes = Enum.map(chunks, & &1.hash) |> Enum.sort()
      assert Enum.sort([hash1, hash3]) == hashes

      # Query node1/drive2 - should return only chunk2
      chunks = ChunkIndex.list_by_drive(:node1, "drive2")
      assert length(chunks) == 1
      assert hd(chunks).hash == hash2

      # Query node2/drive1 - should return only chunk3
      chunks = ChunkIndex.list_by_drive(:node2, "drive1")
      assert length(chunks) == 1
      assert hd(chunks).hash == hash3

      # Query non-existent combination
      chunks = ChunkIndex.list_by_drive(:node1, "drive999")
      assert chunks == []
    end
  end

  describe "list_uncommitted/0" do
    test "finds all uncommitted chunks" do
      hash1 = :crypto.strong_rand_bytes(32)
      hash2 = :crypto.strong_rand_bytes(32)
      hash3 = :crypto.strong_rand_bytes(32)

      chunk1 = ChunkMeta.new(hash1, 1024, 512)
      chunk2 = ChunkMeta.new(hash2, 2048, 1024)
      chunk3 = ChunkMeta.new(hash3, 4096, 2048)

      ChunkIndex.put(chunk1)
      ChunkIndex.put(chunk2)
      ChunkIndex.put(chunk3)

      # All chunks should be uncommitted initially
      chunks = ChunkIndex.list_uncommitted()
      hashes = Enum.map(chunks, & &1.hash) |> Enum.sort()
      assert Enum.sort([hash1, hash2, hash3]) == hashes

      # Commit chunk2
      assert :ok = ChunkIndex.commit(hash2)

      # Should now only return chunk1 and chunk3
      chunks = ChunkIndex.list_uncommitted()
      hashes = Enum.map(chunks, & &1.hash) |> Enum.sort()
      assert Enum.sort([hash1, hash3]) == hashes
    end

    test "returns empty list when all chunks are committed" do
      hash1 = :crypto.strong_rand_bytes(32)
      chunk1 = ChunkMeta.new(hash1, 1024, 512)

      ChunkIndex.put(chunk1)
      assert :ok = ChunkIndex.commit(hash1)

      chunks = ChunkIndex.list_uncommitted()
      assert chunks == []
    end
  end

  describe "add_write_ref/2 and remove_write_ref/2" do
    test "adds and removes write references (local-only)" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      ChunkIndex.put(chunk_meta)

      # Add write refs
      assert :ok = ChunkIndex.add_write_ref(hash, "write1")
      assert :ok = ChunkIndex.add_write_ref(hash, "write2")

      {:ok, retrieved} = ChunkIndex.get(hash)
      assert MapSet.size(retrieved.active_write_refs) == 2
      assert MapSet.member?(retrieved.active_write_refs, "write1")
      assert MapSet.member?(retrieved.active_write_refs, "write2")

      # Remove one write ref
      assert :ok = ChunkIndex.remove_write_ref(hash, "write1")

      {:ok, retrieved} = ChunkIndex.get(hash)
      assert MapSet.size(retrieved.active_write_refs) == 1
      assert MapSet.member?(retrieved.active_write_refs, "write2")
      refute MapSet.member?(retrieved.active_write_refs, "write1")

      # Remove second write ref
      assert :ok = ChunkIndex.remove_write_ref(hash, "write2")

      {:ok, retrieved} = ChunkIndex.get(hash)
      assert MapSet.size(retrieved.active_write_refs) == 0
    end

    test "returns error for non-existent chunk" do
      hash = :crypto.strong_rand_bytes(32)
      assert {:error, :not_found} = ChunkIndex.add_write_ref(hash, "write1")
      assert {:error, :not_found} = ChunkIndex.remove_write_ref(hash, "write1")
    end

    test "adding same write ref multiple times is idempotent" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      ChunkIndex.put(chunk_meta)

      assert :ok = ChunkIndex.add_write_ref(hash, "write1")
      assert :ok = ChunkIndex.add_write_ref(hash, "write1")

      {:ok, retrieved} = ChunkIndex.get(hash)
      assert MapSet.size(retrieved.active_write_refs) == 1
    end

    test "removing non-existent write ref is idempotent" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      ChunkIndex.put(chunk_meta)

      assert :ok = ChunkIndex.remove_write_ref(hash, "write1")
      {:ok, retrieved} = ChunkIndex.get(hash)
      assert MapSet.size(retrieved.active_write_refs) == 0
    end
  end

  describe "commit/1" do
    test "commits a chunk without active write refs" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      ChunkIndex.put(chunk_meta)

      assert :ok = ChunkIndex.commit(hash)

      {:ok, retrieved} = ChunkIndex.get(hash)
      assert retrieved.commit_state == :committed
    end

    test "cannot commit chunk with active write refs" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      ChunkIndex.put(chunk_meta)
      ChunkIndex.add_write_ref(hash, "write1")

      assert {:error, :has_active_writes} = ChunkIndex.commit(hash)

      {:ok, retrieved} = ChunkIndex.get(hash)
      assert retrieved.commit_state == :uncommitted
    end

    test "can commit after removing all write refs" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      ChunkIndex.put(chunk_meta)
      ChunkIndex.add_write_ref(hash, "write1")
      ChunkIndex.add_write_ref(hash, "write2")

      assert {:error, :has_active_writes} = ChunkIndex.commit(hash)

      ChunkIndex.remove_write_ref(hash, "write1")
      assert {:error, :has_active_writes} = ChunkIndex.commit(hash)

      ChunkIndex.remove_write_ref(hash, "write2")
      assert :ok = ChunkIndex.commit(hash)

      {:ok, retrieved} = ChunkIndex.get(hash)
      assert retrieved.commit_state == :committed
    end

    test "committing already committed chunk is idempotent" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      ChunkIndex.put(chunk_meta)
      assert :ok = ChunkIndex.commit(hash)
      assert :ok = ChunkIndex.commit(hash)

      {:ok, retrieved} = ChunkIndex.get(hash)
      assert retrieved.commit_state == :committed
    end

    test "returns error for non-existent chunk" do
      hash = :crypto.strong_rand_bytes(32)
      assert {:error, :not_found} = ChunkIndex.commit(hash)
    end
  end

  describe "concurrent access" do
    test "multiple processes can read and write concurrently" do
      # Create some chunks
      hashes = for _ <- 1..10, do: :crypto.strong_rand_bytes(32)

      for hash <- hashes do
        chunk_meta = ChunkMeta.new(hash, 1024, 512)
        ChunkIndex.put(chunk_meta)
      end

      # Spawn multiple processes that read and update chunks
      tasks =
        for hash <- hashes do
          Task.async(fn ->
            # Read chunk
            {:ok, chunk} = ChunkIndex.get(hash)
            assert chunk.hash == hash

            # Add write ref
            ChunkIndex.add_write_ref(hash, "concurrent_write")

            # Read again
            {:ok, chunk} = ChunkIndex.get(hash)
            assert MapSet.member?(chunk.active_write_refs, "concurrent_write")

            # Remove write ref
            ChunkIndex.remove_write_ref(hash, "concurrent_write")

            # Commit
            ChunkIndex.commit(hash)
          end)
        end

      # Wait for all tasks to complete
      Task.await_many(tasks)

      # Verify all chunks created by this test are committed
      for hash <- hashes do
        {:ok, chunk} = ChunkIndex.get(hash)
        assert chunk.commit_state == :committed, "Chunk should be committed"
        assert MapSet.size(chunk.active_write_refs) == 0, "No active write refs should remain"
      end
    end
  end

  describe "ETS cache behaviour" do
    test "writes update both quorum and ETS", %{store: store} do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512, :zstd)

      assert :ok = ChunkIndex.put(chunk_meta)

      # Check ETS
      assert [{^hash, cached}] = :ets.lookup(:chunk_index, hash)
      assert cached.hash == hash

      # Check quorum store
      key = "chunk:" <> Base.encode16(hash)
      assert [{^key, stored}] = :ets.lookup(store, key)
      assert stored[:hash] == hash
    end

    test "deletes remove from both quorum and ETS", %{store: store} do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      assert :ok = ChunkIndex.put(chunk_meta)
      assert :ok = ChunkIndex.delete(hash)

      # ETS should be empty
      assert [] = :ets.lookup(:chunk_index, hash)

      # Quorum store should be empty
      key = "chunk:" <> Base.encode16(hash)
      assert [] = :ets.lookup(store, key)
    end
  end

  describe "key format" do
    test "uses chunk: prefix with hex-encoded hash", %{store: store} do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 512)

      assert :ok = ChunkIndex.put(chunk_meta)

      expected_key = "chunk:" <> Base.encode16(hash)
      assert [{^expected_key, _}] = :ets.lookup(store, expected_key)
    end
  end

  # Private helper to match test_case.ex pattern
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
