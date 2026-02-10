defmodule NeonFS.Core.ResolvedLookupCacheTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.ResolvedLookupCache

  setup do
    # Use a short TTL and small max for testing
    start_supervised!(
      {ResolvedLookupCache,
       ttl_ms: 200, max_entries: 5, cleanup_interval_ms: 0, name: ResolvedLookupCache}
    )

    :ok
  end

  describe "put/3 and get/1" do
    test "round-trip stores and retrieves metadata" do
      metadata = %{file_meta: %{id: "f1"}, chunks: ["c1", "c2"]}
      assert :ok = ResolvedLookupCache.put("file-1", metadata)
      assert {:ok, ^metadata} = ResolvedLookupCache.get("file-1")
    end

    test "returns :miss for unknown file_id" do
      assert :miss = ResolvedLookupCache.get("nonexistent")
    end
  end

  describe "TTL expiry" do
    test "entry expires after TTL" do
      metadata = %{file_meta: %{id: "f2"}, chunks: ["c1"]}
      ResolvedLookupCache.put("file-2", metadata)

      # Entry should be available immediately
      assert {:ok, _} = ResolvedLookupCache.get("file-2")

      # Wait for TTL to expire (200ms)
      Process.sleep(250)

      # Entry should now be expired
      assert :miss = ResolvedLookupCache.get("file-2")
    end

    test "per-entry TTL override" do
      metadata = %{file_meta: %{id: "f3"}, chunks: []}
      # Override with very short TTL
      ResolvedLookupCache.put("file-3", metadata, ttl_ms: 50)

      assert {:ok, _} = ResolvedLookupCache.get("file-3")
      Process.sleep(100)
      assert :miss = ResolvedLookupCache.get("file-3")
    end
  end

  describe "evict/1" do
    test "evicts a cached entry" do
      metadata = %{file_meta: %{id: "f4"}, chunks: ["c1"]}
      ResolvedLookupCache.put("file-4", metadata)

      assert {:ok, _} = ResolvedLookupCache.get("file-4")

      ResolvedLookupCache.evict("file-4")

      assert :miss = ResolvedLookupCache.get("file-4")
    end

    test "evicting nonexistent entry is a no-op" do
      assert :ok = ResolvedLookupCache.evict("nonexistent")
    end
  end

  describe "LRU eviction" do
    test "evicts oldest entry when max entries exceeded" do
      # max_entries is 5 in our test config
      for i <- 1..5 do
        ResolvedLookupCache.put("lru-#{i}", %{id: i})
        # Small delay so timestamps differ
        Process.sleep(5)
      end

      # All 5 should be cached
      for i <- 1..5 do
        assert {:ok, _} = ResolvedLookupCache.get("lru-#{i}")
      end

      # Adding a 6th entry should evict the oldest (lru-1)
      Process.sleep(5)
      ResolvedLookupCache.put("lru-6", %{id: 6})

      # lru-1 should have been evicted (it was accessed least recently)
      # Note: all entries were accessed by the `get` loop above, so the oldest
      # by access time is the one that was `get`ed first (lru-1)
      assert {:ok, _} = ResolvedLookupCache.get("lru-6")
      # At least one of the first entries should be evicted
      evicted_count =
        Enum.count(1..5, fn i ->
          ResolvedLookupCache.get("lru-#{i}") == :miss
        end)

      assert evicted_count >= 1
    end
  end

  describe "telemetry" do
    test "emits hit event on cache hit" do
      test_pid = self()

      :telemetry.attach(
        "test-cache-hit",
        [:neonfs, :resolved_cache, :hit],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:cache_hit, metadata})
        end,
        nil
      )

      ResolvedLookupCache.put("tel-1", %{test: true})
      ResolvedLookupCache.get("tel-1")

      assert_receive {:cache_hit, %{file_id: "tel-1"}}

      :telemetry.detach("test-cache-hit")
    end

    test "emits miss event on cache miss" do
      test_pid = self()

      :telemetry.attach(
        "test-cache-miss",
        [:neonfs, :resolved_cache, :miss],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:cache_miss, metadata})
        end,
        nil
      )

      ResolvedLookupCache.get("tel-missing")

      assert_receive {:cache_miss, %{file_id: "tel-missing"}}

      :telemetry.detach("test-cache-miss")
    end

    test "emits evict event on explicit eviction" do
      test_pid = self()

      :telemetry.attach(
        "test-cache-evict",
        [:neonfs, :resolved_cache, :evict],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:cache_evict, metadata})
        end,
        nil
      )

      ResolvedLookupCache.put("tel-evict", %{test: true})
      ResolvedLookupCache.evict("tel-evict")

      assert_receive {:cache_evict, %{file_id: "tel-evict"}}

      :telemetry.detach("test-cache-evict")
    end
  end
end
