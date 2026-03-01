defmodule NeonFS.Core.ChunkCacheTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.{ChunkCache, Volume}

  setup do
    ensure_cache_running()
    clear_cache()
    clear_volume_cache_table()

    on_exit(fn ->
      Application.delete_env(:neonfs_core, :chunk_cache_max_memory)
    end)

    :ok
  end

  defp ensure_cache_running do
    case GenServer.whereis(ChunkCache) do
      nil ->
        start_supervised!(ChunkCache)

      _pid ->
        :ok
    end
  end

  defp clear_cache do
    if :ets.whereis(:chunk_cache_data) != :undefined do
      :ets.delete_all_objects(:chunk_cache_data)
    end

    if :ets.whereis(:chunk_cache_lru) != :undefined do
      :ets.delete_all_objects(:chunk_cache_lru)
    end

    if :ets.whereis(:chunk_cache_stats) != :undefined do
      :ets.insert(:chunk_cache_stats, [{:hits, 0}, {:misses, 0}, {:evictions, 0}])
    end
  end

  defp clear_cache_tables do
    for table <- [:chunk_cache_data, :chunk_cache_lru, :chunk_cache_stats] do
      if :ets.whereis(table) != :undefined, do: :ets.delete(table)
    end
  end

  defp ensure_volume_cache_table do
    if :ets.whereis(:volumes_by_id) == :undefined do
      :ets.new(:volumes_by_id, [:named_table, :set, :public, read_concurrency: true])
    end
  end

  defp clear_volume_cache_table do
    if :ets.whereis(:volumes_by_id) != :undefined do
      :ets.delete_all_objects(:volumes_by_id)
    end
  end

  defp put_volume_caching(volume_id, caching) do
    ensure_volume_cache_table()

    volume =
      Volume.new("volume-")
      |> Map.put(:id, volume_id)
      |> Map.put(:caching, caching)

    :ets.insert(:volumes_by_id, {volume_id, volume})
  end

  defp restart_cache_with_limit(limit) do
    stop_supervised(ChunkCache)
    clear_cache_tables()
    Application.put_env(:neonfs_core, :chunk_cache_max_memory, limit)
    start_supervised!(ChunkCache)
  end

  describe "get/2" do
    test "returns :miss for non-existent entry" do
      assert :miss = ChunkCache.get("vol1", <<1, 2, 3>>)
    end

    test "returns {:ok, data} for cached entry" do
      ChunkCache.put("vol1", <<1, 2, 3>>, "hello world")
      # Wait for cast to process
      ChunkCache.flush()

      assert {:ok, "hello world"} = ChunkCache.get("vol1", <<1, 2, 3>>)
    end

    test "returns :miss for different volume" do
      ChunkCache.put("vol1", <<1, 2, 3>>, "hello")
      ChunkCache.flush()

      assert :miss = ChunkCache.get("vol2", <<1, 2, 3>>)
    end

    test "returns :miss for different hash" do
      ChunkCache.put("vol1", <<1, 2, 3>>, "hello")
      ChunkCache.flush()

      assert :miss = ChunkCache.get("vol1", <<4, 5, 6>>)
    end
  end

  describe "put/3" do
    test "stores data in cache" do
      data = :crypto.strong_rand_bytes(1000)
      ChunkCache.put("vol1", <<10>>, data)
      ChunkCache.flush()

      assert {:ok, ^data} = ChunkCache.get("vol1", <<10>>)
    end

    test "overwrites existing entry" do
      ChunkCache.put("vol1", <<10>>, "first")
      ChunkCache.flush()
      ChunkCache.put("vol1", <<10>>, "second")
      ChunkCache.flush()

      assert {:ok, "second"} = ChunkCache.get("vol1", <<10>>)
    end

    test "different volumes have independent caches" do
      ChunkCache.put("vol1", <<10>>, "data_vol1")
      ChunkCache.put("vol2", <<10>>, "data_vol2")
      ChunkCache.flush()

      assert {:ok, "data_vol1"} = ChunkCache.get("vol1", <<10>>)
      assert {:ok, "data_vol2"} = ChunkCache.get("vol2", <<10>>)
    end
  end

  describe "invalidate/1" do
    test "removes chunk from all volumes" do
      ChunkCache.put("vol1", <<10>>, "data1")
      ChunkCache.put("vol2", <<10>>, "data2")
      ChunkCache.flush()

      ChunkCache.invalidate(<<10>>)
      ChunkCache.flush()

      assert :miss = ChunkCache.get("vol1", <<10>>)
      assert :miss = ChunkCache.get("vol2", <<10>>)
    end

    test "does not affect other chunks" do
      ChunkCache.put("vol1", <<10>>, "keep")
      ChunkCache.put("vol1", <<20>>, "remove")
      ChunkCache.flush()

      ChunkCache.invalidate(<<20>>)
      ChunkCache.flush()

      assert {:ok, "keep"} = ChunkCache.get("vol1", <<10>>)
      assert :miss = ChunkCache.get("vol1", <<20>>)
    end
  end

  describe "invalidate/2" do
    test "removes chunk from specific volume" do
      ChunkCache.put("vol1", <<10>>, "data1")
      ChunkCache.put("vol2", <<10>>, "data2")
      ChunkCache.flush()

      ChunkCache.invalidate("vol1", <<10>>)
      ChunkCache.flush()

      assert :miss = ChunkCache.get("vol1", <<10>>)
      assert {:ok, "data2"} = ChunkCache.get("vol2", <<10>>)
    end
  end

  describe "LRU eviction" do
    test "evicts oldest entries when cache exceeds max_memory" do
      # Use a small max_memory to trigger eviction
      restart_cache_with_limit(100)

      # Fill cache beyond max_memory
      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 50))
      ChunkCache.flush()
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 50))
      ChunkCache.flush()

      # This should evict <<1>> since it's oldest and we're over 100 bytes
      ChunkCache.put("vol1", <<3>>, String.duplicate("c", 50))
      ChunkCache.flush()

      # Oldest entry should be evicted
      assert :miss = ChunkCache.get("vol1", <<1>>)
      # Newest entries should remain
      assert {:ok, _} = ChunkCache.get("vol1", <<3>>)
    end

    test "eviction is global across volumes" do
      restart_cache_with_limit(80)

      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 50))
      ChunkCache.flush()
      ChunkCache.put("vol2", <<2>>, String.duplicate("b", 50))
      ChunkCache.flush()

      assert :miss = ChunkCache.get("vol1", <<1>>)
      assert {:ok, _} = ChunkCache.get("vol2", <<2>>)
    end
  end

  describe "stats/0" do
    test "returns zeroed stats initially" do
      stats = ChunkCache.stats()
      assert stats.hits == 0
      assert stats.misses == 0
      assert stats.evictions == 0
      assert stats.memory_used == 0
    end

    test "tracks hits and misses" do
      ChunkCache.put("vol1", <<1>>, "data")
      ChunkCache.flush()

      ChunkCache.get("vol1", <<1>>)
      ChunkCache.get("vol1", <<99>>)

      stats = ChunkCache.stats()
      assert stats.hits == 1
      assert stats.misses == 1
    end

    test "tracks memory used" do
      data = String.duplicate("x", 100)
      ChunkCache.put("vol1", <<1>>, data)
      ChunkCache.flush()

      stats = ChunkCache.stats()
      assert stats.memory_used == 100
    end

    test "tracks evictions" do
      restart_cache_with_limit(50)

      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 40))
      ChunkCache.flush()
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 40))
      ChunkCache.flush()

      stats = ChunkCache.stats()
      assert stats.evictions >= 1
    end
  end

  describe "telemetry" do
    test "emits hit events" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :chunk_cache, :hit]])

      ChunkCache.put("vol1", <<1>>, "data")
      ChunkCache.flush()
      ChunkCache.get("vol1", <<1>>)

      assert_receive {[:neonfs, :chunk_cache, :hit], ^ref, %{bytes: 4}, %{volume_id: "vol1"}}
    end

    test "emits miss events" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :chunk_cache, :miss]])

      ChunkCache.get("vol1", <<99>>)

      assert_receive {[:neonfs, :chunk_cache, :miss], ^ref, %{}, %{volume_id: "vol1"}}
    end

    test "emits eviction events" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :chunk_cache, :eviction]])

      restart_cache_with_limit(50)

      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 40))
      ChunkCache.flush()
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 40))
      ChunkCache.flush()

      assert_receive {[:neonfs, :chunk_cache, :eviction], ^ref, %{bytes: 40},
                      %{volume_id: "vol1"}}
    end
  end

  describe "configurable memory limit" do
    test "defaults to 256 MiB when no config is set" do
      # No config set — put/3 should use the default 256 MiB limit
      # Inserting small data should not cause eviction
      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 50))
      ChunkCache.flush()
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 50))
      ChunkCache.flush()

      assert {:ok, _} = ChunkCache.get("vol1", <<1>>)
      assert {:ok, _} = ChunkCache.get("vol1", <<2>>)
    end

    test "configured limit triggers eviction sooner" do
      # Stop and restart with a small configured limit
      stop_supervised(ChunkCache)
      clear_cache_tables()
      Application.put_env(:neonfs_core, :chunk_cache_max_memory, 80)

      start_supervised!(ChunkCache)

      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 50))
      ChunkCache.flush()
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 50))
      ChunkCache.flush()

      # Oldest entry should be evicted due to 80-byte limit
      assert :miss = ChunkCache.get("vol1", <<1>>)
      assert {:ok, _} = ChunkCache.get("vol1", <<2>>)
    after
      Application.delete_env(:neonfs_core, :chunk_cache_max_memory)
    end
  end

  describe "LRU ordering" do
    test "recently accessed entries survive eviction" do
      restart_cache_with_limit(100)

      # Insert three entries
      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 40))
      ChunkCache.flush()
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 40))
      ChunkCache.flush()

      # Access <<1>> to make it recently used
      ChunkCache.get("vol1", <<1>>)
      ChunkCache.flush()

      # Insert <<3>> which should evict <<2>> (oldest accessed), not <<1>>
      ChunkCache.put("vol1", <<3>>, String.duplicate("c", 40))
      ChunkCache.flush()

      # <<1>> should survive (recently accessed), <<2>> should be evicted
      assert {:ok, _} = ChunkCache.get("vol1", <<1>>)
      assert :miss = ChunkCache.get("vol1", <<2>>)
    end
  end

  describe "cache policy flags" do
    test "transformed chunk not cached when transformed_chunks is false" do
      put_volume_caching("vol1", %{
        transformed_chunks: false,
        reconstructed_stripes: true,
        remote_chunks: true
      })

      ChunkCache.put("vol1", <<1>>, "data", chunk_type: :transformed)
      ChunkCache.flush()

      assert :miss = ChunkCache.get("vol1", <<1>>)
    end

    test "remote chunk not cached when remote_chunks is false" do
      put_volume_caching("vol1", %{
        transformed_chunks: true,
        reconstructed_stripes: true,
        remote_chunks: false
      })

      ChunkCache.put("vol1", <<1>>, "data", chunk_type: :remote)
      ChunkCache.flush()

      assert :miss = ChunkCache.get("vol1", <<1>>)
    end
  end
end
