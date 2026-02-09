defmodule NeonFS.Core.ChunkCacheTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.ChunkCache

  setup do
    ensure_cache_running()
    clear_cache()
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

  describe "get/2" do
    test "returns :miss for non-existent entry" do
      assert :miss = ChunkCache.get("vol1", <<1, 2, 3>>)
    end

    test "returns {:ok, data} for cached entry" do
      ChunkCache.put("vol1", <<1, 2, 3>>, "hello world")
      # Wait for cast to process
      Process.sleep(10)

      assert {:ok, "hello world"} = ChunkCache.get("vol1", <<1, 2, 3>>)
    end

    test "returns :miss for different volume" do
      ChunkCache.put("vol1", <<1, 2, 3>>, "hello")
      Process.sleep(10)

      assert :miss = ChunkCache.get("vol2", <<1, 2, 3>>)
    end

    test "returns :miss for different hash" do
      ChunkCache.put("vol1", <<1, 2, 3>>, "hello")
      Process.sleep(10)

      assert :miss = ChunkCache.get("vol1", <<4, 5, 6>>)
    end
  end

  describe "put/4" do
    test "stores data in cache" do
      data = :crypto.strong_rand_bytes(1000)
      ChunkCache.put("vol1", <<10>>, data)
      Process.sleep(10)

      assert {:ok, ^data} = ChunkCache.get("vol1", <<10>>)
    end

    test "overwrites existing entry" do
      ChunkCache.put("vol1", <<10>>, "first")
      Process.sleep(10)
      ChunkCache.put("vol1", <<10>>, "second")
      Process.sleep(10)

      assert {:ok, "second"} = ChunkCache.get("vol1", <<10>>)
    end

    test "different volumes have independent caches" do
      ChunkCache.put("vol1", <<10>>, "data_vol1")
      ChunkCache.put("vol2", <<10>>, "data_vol2")
      Process.sleep(10)

      assert {:ok, "data_vol1"} = ChunkCache.get("vol1", <<10>>)
      assert {:ok, "data_vol2"} = ChunkCache.get("vol2", <<10>>)
    end
  end

  describe "invalidate/1" do
    test "removes chunk from all volumes" do
      ChunkCache.put("vol1", <<10>>, "data1")
      ChunkCache.put("vol2", <<10>>, "data2")
      Process.sleep(10)

      ChunkCache.invalidate(<<10>>)
      Process.sleep(10)

      assert :miss = ChunkCache.get("vol1", <<10>>)
      assert :miss = ChunkCache.get("vol2", <<10>>)
    end

    test "does not affect other chunks" do
      ChunkCache.put("vol1", <<10>>, "keep")
      ChunkCache.put("vol1", <<20>>, "remove")
      Process.sleep(10)

      ChunkCache.invalidate(<<20>>)
      Process.sleep(10)

      assert {:ok, "keep"} = ChunkCache.get("vol1", <<10>>)
      assert :miss = ChunkCache.get("vol1", <<20>>)
    end
  end

  describe "invalidate/2" do
    test "removes chunk from specific volume" do
      ChunkCache.put("vol1", <<10>>, "data1")
      ChunkCache.put("vol2", <<10>>, "data2")
      Process.sleep(10)

      ChunkCache.invalidate("vol1", <<10>>)
      Process.sleep(10)

      assert :miss = ChunkCache.get("vol1", <<10>>)
      assert {:ok, "data2"} = ChunkCache.get("vol2", <<10>>)
    end
  end

  describe "LRU eviction" do
    test "evicts oldest entries when volume exceeds max_memory" do
      # Use a small max_memory to trigger eviction
      max_memory = 100

      # Fill cache beyond max_memory
      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 50), max_memory)
      Process.sleep(10)
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 50), max_memory)
      Process.sleep(10)

      # This should evict <<1>> since it's oldest and we're over 100 bytes
      ChunkCache.put("vol1", <<3>>, String.duplicate("c", 50), max_memory)
      Process.sleep(10)

      # Oldest entry should be evicted
      assert :miss = ChunkCache.get("vol1", <<1>>)
      # Newest entries should remain
      assert {:ok, _} = ChunkCache.get("vol1", <<3>>)
    end

    test "eviction is per-volume" do
      max_memory = 80

      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 50), max_memory)
      Process.sleep(10)
      ChunkCache.put("vol2", <<2>>, String.duplicate("b", 50), max_memory)
      Process.sleep(10)

      # Adding to vol1 should only evict from vol1, not vol2
      ChunkCache.put("vol1", <<3>>, String.duplicate("c", 50), max_memory)
      Process.sleep(10)

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
      Process.sleep(10)

      ChunkCache.get("vol1", <<1>>)
      ChunkCache.get("vol1", <<99>>)

      stats = ChunkCache.stats()
      assert stats.hits == 1
      assert stats.misses == 1
    end

    test "tracks memory used" do
      data = String.duplicate("x", 100)
      ChunkCache.put("vol1", <<1>>, data)
      Process.sleep(10)

      stats = ChunkCache.stats()
      assert stats.memory_used == 100
    end

    test "tracks evictions" do
      max_memory = 50

      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 40), max_memory)
      Process.sleep(10)
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 40), max_memory)
      Process.sleep(10)

      stats = ChunkCache.stats()
      assert stats.evictions >= 1
    end
  end

  describe "telemetry" do
    test "emits hit events" do
      ref = :telemetry_test.attach_event_handlers(self(), [[:neonfs, :chunk_cache, :hit]])

      ChunkCache.put("vol1", <<1>>, "data")
      Process.sleep(10)
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

      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 40), 50)
      Process.sleep(10)
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 40), 50)
      Process.sleep(10)

      assert_receive {[:neonfs, :chunk_cache, :eviction], ^ref, %{bytes: 40},
                      %{volume_id: "vol1"}}
    end
  end

  describe "LRU ordering" do
    test "recently accessed entries survive eviction" do
      max_memory = 100

      # Insert three entries
      ChunkCache.put("vol1", <<1>>, String.duplicate("a", 40), max_memory)
      Process.sleep(10)
      ChunkCache.put("vol1", <<2>>, String.duplicate("b", 40), max_memory)
      Process.sleep(10)

      # Access <<1>> to make it recently used
      ChunkCache.get("vol1", <<1>>)
      Process.sleep(10)

      # Insert <<3>> which should evict <<2>> (oldest accessed), not <<1>>
      ChunkCache.put("vol1", <<3>>, String.duplicate("c", 40), max_memory)
      Process.sleep(10)

      # <<1>> should survive (recently accessed), <<2>> should be evicted
      assert {:ok, _} = ChunkCache.get("vol1", <<1>>)
      assert :miss = ChunkCache.get("vol1", <<2>>)
    end
  end
end
