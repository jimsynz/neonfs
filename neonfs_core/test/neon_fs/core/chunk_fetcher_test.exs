defmodule NeonFS.Core.ChunkFetcherTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.{BlobStore, ChunkFetcher, ChunkIndex, ChunkMeta}

  @test_base_dir "/tmp/neonfs_test_chunk_fetcher"

  setup do
    # Clean up test directory
    File.rm_rf!(@test_base_dir)
    File.mkdir_p!(@test_base_dir)

    # Clear all data from ETS tables (services are already started by the application)
    :ets.delete_all_objects(:chunk_index)

    on_exit(fn ->
      File.rm_rf!(@test_base_dir)
    end)

    :ok
  end

  describe "fetch_chunk/2 - local chunks" do
    test "returns {:ok, data, :local} when chunk exists locally" do
      # Write a test chunk
      data = "hello world from local node"
      {:ok, hash, info} = BlobStore.write_chunk(data, "hot")

      # Create chunk metadata
      chunk_meta = %ChunkMeta{
        hash: hash,
        original_size: info.original_size,
        stored_size: info.stored_size,
        compression: String.to_atom(info.compression),
        locations: [%{node: Node.self(), drive_id: "default", tier: :hot}],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      # Fetch the chunk
      assert {:ok, ^data, :local} = ChunkFetcher.fetch_chunk(hash)
    end

    test "uses correct tier when fetching locally" do
      # Write chunk to warm tier
      data = "warm tier data"
      {:ok, hash, info} = BlobStore.write_chunk(data, "warm")

      chunk_meta = %ChunkMeta{
        hash: hash,
        original_size: info.original_size,
        stored_size: info.stored_size,
        compression: String.to_atom(info.compression),
        locations: [%{node: Node.self(), drive_id: "default", tier: :warm}],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      # Fetch with correct tier
      assert {:ok, ^data, :local} = ChunkFetcher.fetch_chunk(hash, tier: "warm")
    end

    test "returns error when chunk not found locally and not in metadata" do
      # Create a fake hash that doesn't exist
      fake_hash = :crypto.hash(:sha256, "nonexistent")

      # Try to fetch it
      assert {:error, :chunk_not_found} = ChunkFetcher.fetch_chunk(fake_hash)
    end

    test "verifies chunk when verify option is true" do
      data = "data to verify"
      {:ok, hash, info} = BlobStore.write_chunk(data, "hot")

      chunk_meta = %ChunkMeta{
        hash: hash,
        original_size: info.original_size,
        stored_size: info.stored_size,
        compression: String.to_atom(info.compression),
        locations: [%{node: Node.self(), drive_id: "default", tier: :hot}],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      # Fetch with verification
      assert {:ok, ^data, :local} = ChunkFetcher.fetch_chunk(hash, verify: true)
    end

    test "decompresses chunk when decompress option is true" do
      # Write compressed chunk
      data = String.duplicate("compress me! ", 100)
      {:ok, hash, info} = BlobStore.write_chunk(data, "hot", compression: "zstd")

      chunk_meta = %ChunkMeta{
        hash: hash,
        original_size: info.original_size,
        stored_size: info.stored_size,
        compression: String.to_atom(info.compression),
        locations: [%{node: Node.self(), drive_id: "default", tier: :hot}],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      # Fetch with decompression
      assert {:ok, ^data, :local} = ChunkFetcher.fetch_chunk(hash, decompress: true)
    end
  end

  describe "fetch_chunk/2 - remote chunks" do
    @tag :skip
    # This test requires multi-node setup which is better suited for integration tests
    test "fetches chunk from remote node when not available locally"

    test "returns error when chunk metadata exists but all locations are unavailable" do
      # Create chunk metadata with a non-existent remote node
      fake_hash = :crypto.hash(:sha256, "remote chunk")

      chunk_meta = %ChunkMeta{
        hash: fake_hash,
        original_size: 100,
        stored_size: 100,
        compression: :none,
        locations: [
          %{node: :nonexistent@localhost, drive_id: "default", tier: :hot}
        ],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      # Try to fetch - should fail as remote node doesn't exist
      assert {:error, :all_replicas_failed} = ChunkFetcher.fetch_chunk(fake_hash)
    end

    test "filters out local node from remote fetch attempts" do
      data = "local data, not remote"
      {:ok, hash, info} = BlobStore.write_chunk(data, "hot")

      # Create metadata with both local and remote locations
      chunk_meta = %ChunkMeta{
        hash: hash,
        original_size: info.original_size,
        stored_size: info.stored_size,
        compression: String.to_atom(info.compression),
        locations: [
          %{node: Node.self(), drive_id: "default", tier: :hot},
          %{node: :remote@localhost, drive_id: "default", tier: :hot}
        ],
        target_replicas: 2,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      # Should fetch locally without trying remote
      assert {:ok, ^data, :local} = ChunkFetcher.fetch_chunk(hash)
    end
  end

  describe "telemetry events" do
    test "emits local_hit event when chunk is found locally" do
      # Attach telemetry handler
      test_pid = self()

      :telemetry.attach(
        "test-local-hit",
        [:neonfs, :chunk_fetcher, :local_hit],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_event, event, measurements, metadata})
        end,
        nil
      )

      # Write and fetch chunk
      data = "telemetry test data"
      {:ok, hash, info} = BlobStore.write_chunk(data, "hot")

      chunk_meta = %ChunkMeta{
        hash: hash,
        original_size: info.original_size,
        stored_size: info.stored_size,
        compression: String.to_atom(info.compression),
        locations: [%{node: Node.self(), drive_id: "default", tier: :hot}],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      {:ok, ^data, :local} = ChunkFetcher.fetch_chunk(hash)

      # Verify telemetry event was emitted
      assert_receive {:telemetry_event, [:neonfs, :chunk_fetcher, :local_hit], measurements,
                      metadata}

      assert is_number(measurements.duration)
      assert measurements.bytes == byte_size(data)
      assert metadata.hash == Base.encode16(hash, case: :lower)

      :telemetry.detach("test-local-hit")
    end

    test "emits remote_fetch exception event when remote fetch fails" do
      # Attach telemetry handler
      test_pid = self()

      :telemetry.attach(
        "test-remote-exception",
        [:neonfs, :chunk_fetcher, :remote_fetch, :exception],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_event, event, measurements, metadata})
        end,
        nil
      )

      # Create metadata with non-existent remote node
      fake_hash = :crypto.hash(:sha256, "remote exception test")

      chunk_meta = %ChunkMeta{
        hash: fake_hash,
        original_size: 100,
        stored_size: 100,
        compression: :none,
        locations: [
          %{node: :nonexistent@localhost, drive_id: "default", tier: :hot}
        ],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      # Try to fetch - should fail and emit exception event
      assert {:error, :all_replicas_failed} = ChunkFetcher.fetch_chunk(fake_hash)

      # Verify telemetry event was emitted
      assert_receive {:telemetry_event, [:neonfs, :chunk_fetcher, :remote_fetch, :exception],
                      measurements, metadata}

      assert is_number(measurements.duration)
      assert metadata.hash == Base.encode16(fake_hash, case: :lower)
      assert metadata.error == :all_replicas_failed

      :telemetry.detach("test-remote-exception")
    end
  end
end
