defmodule NeonFS.Core.ChunkIndexDistributedTest do
  @moduledoc """
  Tests for ChunkIndex with Ra-backed distributed storage.

  These tests verify:
  - Chunk metadata is replicated via Ra consensus
  - Reads use local ETS cache (no network hop)
  - Writes go through Ra
  - API remains stable
  """

  use ExUnit.Case, async: false

  alias NeonFS.Core.{ChunkIndex, ChunkMeta, RaSupervisor}

  @moduletag :ra

  setup_all do
    # Clean up Ra data directory before test suite
    ra_dir = "/tmp/neonfs_test/ra"
    File.rm_rf!(ra_dir)
    File.mkdir_p!(ra_dir)

    # Ensure Ra application is started first
    Application.ensure_all_started(:ra)

    # Start the application once for all tests
    case Application.ensure_all_started(:neonfs_core) do
      {:ok, _apps} -> :ok
      {:error, {app, reason}} -> raise "Failed to start #{app}: #{inspect(reason)}"
    end

    # Wait for Ra to be fully initialized
    Process.sleep(2000)

    :ok
  end

  setup do
    # Clean up test data between tests
    # Delete all chunks from the previous test
    try do
      :ets.foldl(
        fn {hash, _meta}, acc ->
          try do
            ChunkIndex.delete(hash)
          catch
            :exit, _ -> :ok
          end

          acc
        end,
        :ok,
        :chunk_index
      )
    catch
      :error, :badarg -> :ok
    end

    :ok
  end

  describe "put/1 and get/1" do
    test "stores and retrieves chunk metadata via Ra" do
      hash = :crypto.strong_rand_bytes(32)

      chunk_meta =
        ChunkMeta.new(hash, 1024, 1024, :none)
        |> ChunkMeta.add_location(%{node: node(), drive_id: "drive0", tier: :hot})

      # Put chunk metadata (goes through Ra)
      assert :ok = ChunkIndex.put(chunk_meta)

      # Get chunk metadata (reads from local ETS)
      assert {:ok, retrieved_meta} = ChunkIndex.get(hash)
      assert retrieved_meta.hash == hash
      assert retrieved_meta.original_size == 1024
      assert length(retrieved_meta.locations) == 1
    end

    test "updates existing chunk metadata" do
      hash = :crypto.strong_rand_bytes(32)

      chunk_meta1 = ChunkMeta.new(hash, 1024, 1024, :none)
      assert :ok = ChunkIndex.put(chunk_meta1)

      chunk_meta2 =
        ChunkMeta.new(hash, 2048, 2048, :zstd)
        |> ChunkMeta.add_location(%{node: node(), drive_id: "drive1", tier: :warm})

      assert :ok = ChunkIndex.put(chunk_meta2)

      assert {:ok, retrieved_meta} = ChunkIndex.get(hash)
      assert retrieved_meta.original_size == 2048
      assert retrieved_meta.compression == :zstd
    end
  end

  describe "delete/1" do
    test "removes chunk metadata via Ra" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 1024, :none)

      assert :ok = ChunkIndex.put(chunk_meta)
      assert {:ok, _} = ChunkIndex.get(hash)

      assert :ok = ChunkIndex.delete(hash)
      assert {:error, :not_found} = ChunkIndex.get(hash)
    end
  end

  describe "add_write_ref/2 and remove_write_ref/2" do
    test "manages write references via Ra" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 1024, :none)

      assert :ok = ChunkIndex.put(chunk_meta)

      # Add write ref
      assert :ok = ChunkIndex.add_write_ref(hash, "write-1")

      {:ok, meta} = ChunkIndex.get(hash)
      assert MapSet.member?(meta.active_write_refs, "write-1")

      # Remove write ref
      assert :ok = ChunkIndex.remove_write_ref(hash, "write-1")

      {:ok, meta} = ChunkIndex.get(hash)
      refute MapSet.member?(meta.active_write_refs, "write-1")
    end

    test "returns error for non-existent chunk" do
      hash = :crypto.strong_rand_bytes(32)

      assert {:error, :not_found} = ChunkIndex.add_write_ref(hash, "write-1")
      assert {:error, :not_found} = ChunkIndex.remove_write_ref(hash, "write-1")
    end
  end

  describe "commit/1" do
    test "commits chunk when no active writes via Ra" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 1024, :none)

      assert :ok = ChunkIndex.put(chunk_meta)
      assert :ok = ChunkIndex.commit(hash)

      {:ok, meta} = ChunkIndex.get(hash)
      assert meta.commit_state == :committed
    end

    test "refuses to commit with active write refs" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 1024, :none)

      assert :ok = ChunkIndex.put(chunk_meta)
      assert :ok = ChunkIndex.add_write_ref(hash, "write-1")

      assert {:error, :has_active_writes} = ChunkIndex.commit(hash)

      {:ok, meta} = ChunkIndex.get(hash)
      assert meta.commit_state == :uncommitted
    end

    test "allows commit after removing all write refs" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 1024, :none)

      assert :ok = ChunkIndex.put(chunk_meta)
      assert :ok = ChunkIndex.add_write_ref(hash, "write-1")
      assert :ok = ChunkIndex.add_write_ref(hash, "write-2")

      assert {:error, :has_active_writes} = ChunkIndex.commit(hash)

      assert :ok = ChunkIndex.remove_write_ref(hash, "write-1")
      assert :ok = ChunkIndex.remove_write_ref(hash, "write-2")

      assert :ok = ChunkIndex.commit(hash)

      {:ok, meta} = ChunkIndex.get(hash)
      assert meta.commit_state == :committed
    end
  end

  describe "list_by_location/1" do
    test "queries chunks by location from local ETS" do
      hash1 = :crypto.strong_rand_bytes(32)
      hash2 = :crypto.strong_rand_bytes(32)
      hash3 = :crypto.strong_rand_bytes(32)

      location1 = %{node: node(), drive_id: "drive0", tier: :hot}
      location2 = %{node: node(), drive_id: "drive1", tier: :warm}

      chunk1 = ChunkMeta.new(hash1, 1024, 1024) |> ChunkMeta.add_location(location1)
      chunk2 = ChunkMeta.new(hash2, 2048, 2048) |> ChunkMeta.add_location(location1)
      chunk3 = ChunkMeta.new(hash3, 4096, 4096) |> ChunkMeta.add_location(location2)

      assert :ok = ChunkIndex.put(chunk1)
      assert :ok = ChunkIndex.put(chunk2)
      assert :ok = ChunkIndex.put(chunk3)

      # Query by location1
      chunks_at_loc1 = ChunkIndex.list_by_location(location1)
      assert length(chunks_at_loc1) == 2
      assert Enum.any?(chunks_at_loc1, fn c -> c.hash == hash1 end)
      assert Enum.any?(chunks_at_loc1, fn c -> c.hash == hash2 end)

      # Query by location2
      chunks_at_loc2 = ChunkIndex.list_by_location(location2)
      assert length(chunks_at_loc2) == 1
      assert hd(chunks_at_loc2).hash == hash3
    end
  end

  describe "list_by_node/1" do
    test "queries chunks by node from local ETS" do
      hash1 = :crypto.strong_rand_bytes(32)
      hash2 = :crypto.strong_rand_bytes(32)

      location1 = %{node: node(), drive_id: "drive0", tier: :hot}
      location2 = %{node: node(), drive_id: "drive1", tier: :warm}

      chunk1 = ChunkMeta.new(hash1, 1024, 1024) |> ChunkMeta.add_location(location1)
      chunk2 = ChunkMeta.new(hash2, 2048, 2048) |> ChunkMeta.add_location(location2)

      assert :ok = ChunkIndex.put(chunk1)
      assert :ok = ChunkIndex.put(chunk2)

      # Query by current node
      chunks_at_node = ChunkIndex.list_by_node(node())
      assert length(chunks_at_node) >= 2
      assert Enum.any?(chunks_at_node, fn c -> c.hash == hash1 end)
      assert Enum.any?(chunks_at_node, fn c -> c.hash == hash2 end)
    end
  end

  describe "list_uncommitted/0" do
    test "queries uncommitted chunks from local ETS" do
      hash1 = :crypto.strong_rand_bytes(32)
      hash2 = :crypto.strong_rand_bytes(32)
      hash3 = :crypto.strong_rand_bytes(32)

      chunk1 = ChunkMeta.new(hash1, 1024, 1024)
      chunk2 = ChunkMeta.new(hash2, 2048, 2048)
      chunk3 = ChunkMeta.new(hash3, 4096, 4096)

      assert :ok = ChunkIndex.put(chunk1)
      assert :ok = ChunkIndex.put(chunk2)
      assert :ok = ChunkIndex.put(chunk3)

      # Commit one chunk
      assert :ok = ChunkIndex.commit(hash2)

      # Query uncommitted
      uncommitted = ChunkIndex.list_uncommitted()
      uncommitted_hashes = Enum.map(uncommitted, & &1.hash)

      assert hash1 in uncommitted_hashes
      refute hash2 in uncommitted_hashes
      assert hash3 in uncommitted_hashes
    end
  end

  describe "Ra consistency" do
    test "chunk metadata persists in Ra state" do
      hash = :crypto.strong_rand_bytes(32)
      chunk_meta = ChunkMeta.new(hash, 1024, 1024, :none)

      assert :ok = ChunkIndex.put(chunk_meta)

      # Query Ra state directly
      {:ok, ra_state} = RaSupervisor.get_state()
      assert is_map(ra_state.chunks)
      assert Map.has_key?(ra_state.chunks, hash)

      ra_chunk = ra_state.chunks[hash]
      assert ra_chunk.hash == hash
      assert ra_chunk.original_size == 1024
    end
  end
end
