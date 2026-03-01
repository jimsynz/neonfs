defmodule NeonFS.Core.ChunkFetcherTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{BlobStore, ChunkCache, ChunkFetcher, ChunkIndex, ChunkMeta, VolumeRegistry}
  alias NeonFS.Core.Volume

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    ensure_chunk_access_tracker()
    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  describe "fetch_chunk/2 - local chunks" do
    test "returns {:ok, data, :local} when chunk exists locally" do
      # Write a test chunk
      data = "hello world from local node"
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot")

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
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "warm")

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
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot")

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
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot", compression: "zstd")

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
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot")

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

  describe "score_location/2" do
    @local_node :test@localhost

    test "scores local SSD (hot tier) as 0" do
      location = %{node: @local_node, drive_id: "ssd1", tier: :hot, state: :active}
      assert ChunkFetcher.score_location(location, @local_node) == 0
    end

    test "scores remote SSD (hot tier) as 10" do
      location = %{node: :remote@host, drive_id: "ssd1", tier: :hot, state: :active}
      assert ChunkFetcher.score_location(location, @local_node) == 10
    end

    test "scores local active HDD (warm tier) as 20" do
      location = %{node: @local_node, drive_id: "hdd1", tier: :warm, state: :active}
      assert ChunkFetcher.score_location(location, @local_node) == 20
    end

    test "scores local active HDD (cold tier) as 20" do
      location = %{node: @local_node, drive_id: "hdd1", tier: :cold, state: :active}
      assert ChunkFetcher.score_location(location, @local_node) == 20
    end

    test "scores remote active HDD as 30" do
      location = %{node: :remote@host, drive_id: "hdd1", tier: :warm, state: :active}
      assert ChunkFetcher.score_location(location, @local_node) == 30
    end

    test "scores standby HDD as 50 regardless of locality" do
      local_standby = %{node: @local_node, drive_id: "hdd1", tier: :cold, state: :standby}
      remote_standby = %{node: :remote@host, drive_id: "hdd2", tier: :warm, state: :standby}

      assert ChunkFetcher.score_location(local_standby, @local_node) == 50
      assert ChunkFetcher.score_location(remote_standby, @local_node) == 50
    end

    test "scores unknown drive (nil tier) as 100" do
      location = %{node: :remote@host, drive_id: "unknown", tier: nil, state: nil}
      assert ChunkFetcher.score_location(location, @local_node) == 100
    end

    test "scores location with missing tier/state keys as 100" do
      location = %{node: :remote@host, drive_id: "unknown"}
      assert ChunkFetcher.score_location(location, @local_node) == 100
    end

    test "standby SSD is still scored as SSD (no spin-up penalty)" do
      location = %{node: @local_node, drive_id: "ssd1", tier: :hot, state: :standby}
      assert ChunkFetcher.score_location(location, @local_node) == 0
    end
  end

  describe "sort_locations_by_score/2" do
    @local_node :test@localhost

    test "sorts by score ascending" do
      locations = [
        %{node: :remote@host, drive_id: "hdd1", tier: :warm, state: :active},
        %{node: @local_node, drive_id: "ssd1", tier: :hot, state: :active},
        %{node: :remote@host, drive_id: "hdd2", tier: :cold, state: :standby},
        %{node: :remote@host, drive_id: "ssd2", tier: :hot, state: :active}
      ]

      sorted = ChunkFetcher.sort_locations_by_score(locations, @local_node)

      # Local SSD (0), Remote SSD (10), Remote HDD active (30), HDD standby (50)
      assert Enum.at(sorted, 0).drive_id == "ssd1"
      assert Enum.at(sorted, 1).drive_id == "ssd2"
      assert Enum.at(sorted, 2).drive_id == "hdd1"
      assert Enum.at(sorted, 3).drive_id == "hdd2"
    end

    test "unknown drives sorted last" do
      locations = [
        %{node: :remote@host, drive_id: "unknown1"},
        %{node: @local_node, drive_id: "ssd1", tier: :hot, state: :active}
      ]

      sorted = ChunkFetcher.sort_locations_by_score(locations, @local_node)

      assert Enum.at(sorted, 0).drive_id == "ssd1"
      assert Enum.at(sorted, 1).drive_id == "unknown1"
    end

    test "tie-breaking within same score varies across calls" do
      # Two remote SSDs should have the same score (10) but random ordering
      locations =
        for i <- 1..20 do
          %{node: :remote@host, drive_id: "ssd#{i}", tier: :hot, state: :active}
        end

      # Run multiple sorts and check that at least one produces different first element
      first_ids =
        for _ <- 1..10 do
          sorted = ChunkFetcher.sort_locations_by_score(locations, @local_node)
          Enum.at(sorted, 0).drive_id
        end

      # With 20 locations and 10 attempts, it's astronomically unlikely
      # that the same one would be first every time if randomization works
      unique_firsts = Enum.uniq(first_ids)
      assert length(unique_firsts) > 1
    end

    test "empty list returns empty list" do
      assert ChunkFetcher.sort_locations_by_score([], @local_node) == []
    end

    test "single location returns that location" do
      locations = [%{node: @local_node, drive_id: "ssd1", tier: :hot, state: :active}]
      sorted = ChunkFetcher.sort_locations_by_score(locations, @local_node)
      assert length(sorted) == 1
      assert Enum.at(sorted, 0).drive_id == "ssd1"
    end

    test "preserves all location fields" do
      locations = [
        %{node: @local_node, drive_id: "ssd1", tier: :hot, state: :active, extra: :data}
      ]

      [sorted_loc] = ChunkFetcher.sort_locations_by_score(locations, @local_node)
      assert sorted_loc.extra == :data
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
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot")

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

  describe "fetch_chunk/2 - volume caching flags" do
    setup do
      start_volume_registry()
      start_chunk_cache()
      :ok
    end

    defp start_chunk_cache do
      case GenServer.whereis(ChunkCache) do
        nil -> start_supervised!(ChunkCache)
        _pid -> :ok
      end

      clear_cache_tables()
    end

    defp clear_cache_tables do
      for table <- [:chunk_cache_data, :chunk_cache_lru] do
        case :ets.whereis(table) do
          :undefined -> :ok
          ref -> :ets.delete_all_objects(ref)
        end
      end
    end

    defp create_volume_with_caching(caching_overrides) do
      caching = Map.merge(Volume.default_caching(), caching_overrides)

      {:ok, volume} =
        VolumeRegistry.create("test-vol-#{:rand.uniform(999_999)}", caching: caching)

      volume
    end

    defp write_compressed_chunk do
      data = String.duplicate("cache test data! ", 100)
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot", compression: "zstd")

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

      {data, hash}
    end

    test "caches decompressed chunk when transformed_chunks is true" do
      volume = create_volume_with_caching(%{transformed_chunks: true})
      {_data, hash} = write_compressed_chunk()

      ChunkFetcher.fetch_chunk(hash, volume_id: volume.id, decompress: true)

      # Drain ChunkCache mailbox so the async cast is processed
      :sys.get_state(ChunkCache)
      assert {:ok, _cached} = ChunkCache.get(volume.id, hash)
    end

    test "does not cache decompressed chunk when transformed_chunks is false" do
      volume = create_volume_with_caching(%{transformed_chunks: false})
      {_data, hash} = write_compressed_chunk()

      ChunkFetcher.fetch_chunk(hash, volume_id: volume.id, decompress: true)

      # Drain ChunkCache mailbox so the async cast is processed
      :sys.get_state(ChunkCache)
      assert :miss = ChunkCache.get(volume.id, hash)
    end

    test "does not cache when all caching flags are false" do
      volume =
        create_volume_with_caching(%{
          transformed_chunks: false,
          reconstructed_stripes: false,
          remote_chunks: false
        })

      {_data, hash} = write_compressed_chunk()

      ChunkFetcher.fetch_chunk(hash, volume_id: volume.id, decompress: true)

      # Drain ChunkCache mailbox so the async cast is processed
      :sys.get_state(ChunkCache)
      assert :miss = ChunkCache.get(volume.id, hash)
    end

    test "default volume config (all flags true) still caches as before" do
      {:ok, volume} = VolumeRegistry.create("default-caching-vol")
      {_data, hash} = write_compressed_chunk()

      ChunkFetcher.fetch_chunk(hash, volume_id: volume.id, decompress: true)

      # Drain ChunkCache mailbox so the async cast is processed
      :sys.get_state(ChunkCache)
      assert {:ok, _cached} = ChunkCache.get(volume.id, hash)
    end

    test "does not cache when decompress is false regardless of flags" do
      volume = create_volume_with_caching(%{transformed_chunks: true})
      data = "uncompressed data"
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot")

      chunk_meta = %ChunkMeta{
        hash: hash,
        original_size: info.original_size,
        stored_size: info.stored_size,
        compression: :none,
        locations: [%{node: Node.self(), drive_id: "default", tier: :hot}],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      ChunkFetcher.fetch_chunk(hash, volume_id: volume.id, decompress: false)

      # Drain ChunkCache mailbox so the async cast is processed
      :sys.get_state(ChunkCache)
      assert :miss = ChunkCache.get(volume.id, hash)
    end

    test "respects reconstructed_stripes flag via cache_reason opt" do
      volume = create_volume_with_caching(%{reconstructed_stripes: true})
      data = "reconstructed data"
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot")

      chunk_meta = %ChunkMeta{
        hash: hash,
        original_size: info.original_size,
        stored_size: info.stored_size,
        compression: :none,
        locations: [%{node: Node.self(), drive_id: "default", tier: :hot}],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      ChunkFetcher.fetch_chunk(hash, volume_id: volume.id, cache_reason: :reconstructed)

      # Drain ChunkCache mailbox so the async cast is processed
      :sys.get_state(ChunkCache)
      assert {:ok, _cached} = ChunkCache.get(volume.id, hash)
    end

    test "blocks caching when reconstructed_stripes is false" do
      volume = create_volume_with_caching(%{reconstructed_stripes: false})
      data = "reconstructed data blocked"
      {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot")

      chunk_meta = %ChunkMeta{
        hash: hash,
        original_size: info.original_size,
        stored_size: info.stored_size,
        compression: :none,
        locations: [%{node: Node.self(), drive_id: "default", tier: :hot}],
        target_replicas: 1,
        commit_state: :committed
      }

      ChunkIndex.put(chunk_meta)

      ChunkFetcher.fetch_chunk(hash, volume_id: volume.id, cache_reason: :reconstructed)

      # Drain ChunkCache mailbox so the async cast is processed
      :sys.get_state(ChunkCache)
      assert :miss = ChunkCache.get(volume.id, hash)
    end

    test "falls back to default behaviour when volume not found" do
      {_data, hash} = write_compressed_chunk()

      # Use a non-existent volume ID
      ChunkFetcher.fetch_chunk(hash, volume_id: "nonexistent-volume", decompress: true)

      # Drain ChunkCache mailbox so the async cast is processed
      :sys.get_state(ChunkCache)
      # Should cache because default caching has transformed_chunks: true
      assert {:ok, _cached} = ChunkCache.get("nonexistent-volume", hash)
    end
  end
end
