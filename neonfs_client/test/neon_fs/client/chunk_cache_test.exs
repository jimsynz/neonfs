defmodule NeonFS.Client.ChunkCacheTest do
  @moduledoc """
  Unit tests for the per-node byte-bounded LRU chunk cache (#1355).
  The cache is not auto-started in `:test` (`start_children?: false`),
  so each test that needs it starts its own with a tight byte cap.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Client.ChunkCache

  @hit [:neonfs, :client, :chunk_cache, :hit]
  @miss [:neonfs, :client, :chunk_cache, :miss]
  @eviction [:neonfs, :client, :chunk_cache, :eviction]

  defp key(n), do: {"vol", :crypto.hash(:sha256, "chunk-#{n}")}

  describe "when the cache is not running" do
    test "get reports a miss and put is a no-op" do
      assert :miss = ChunkCache.get(key(1))
      assert :ok = ChunkCache.put(key(1), "bytes")
      assert :miss = ChunkCache.get(key(1))
    end
  end

  describe "get/put" do
    setup do
      start_supervised!({ChunkCache, max_bytes: 1_000})
      :ok
    end

    test "stores and returns chunk bytes" do
      assert :miss = ChunkCache.get(key(1))
      assert :ok = ChunkCache.put(key(1), "alpha")
      assert {:ok, "alpha"} = ChunkCache.get(key(1))
    end

    test "emits hit and miss telemetry" do
      tel = :telemetry_test.attach_event_handlers(self(), [@hit, @miss])

      assert :miss = ChunkCache.get(key(2))
      assert_receive {@miss, ^tel, %{}, %{}}

      ChunkCache.put(key(2), "beta")
      assert {:ok, "beta"} = ChunkCache.get(key(2))
      assert_receive {@hit, ^tel, %{size: 4}, %{}}
    end

    test "re-putting the same key keeps a single entry" do
      ChunkCache.put(key(3), "gamma")
      ChunkCache.put(key(3), "gamma")
      assert %{entries: 1, bytes: 5} = ChunkCache.stats()
    end
  end

  describe "byte-bounded eviction" do
    setup do
      # Cap fits three 10-byte chunks exactly.
      start_supervised!({ChunkCache, max_bytes: 30})
      :ok
    end

    test "evicts the least-recently-used entry when over the cap" do
      tel = :telemetry_test.attach_event_handlers(self(), [@eviction])

      for n <- 1..3, do: ChunkCache.put(key(n), ten_bytes())
      assert %{entries: 3, bytes: 30} = ChunkCache.stats()

      ChunkCache.put(key(4), ten_bytes())

      assert_receive {@eviction, ^tel, %{size: 10}, %{}}
      assert %{entries: 3, bytes: 30} = ChunkCache.stats()
      assert :miss = ChunkCache.get(key(1))
      assert {:ok, _} = ChunkCache.get(key(4))
    end

    test "a recent get bumps the entry so it survives the next eviction" do
      for n <- 1..3, do: ChunkCache.put(key(n), ten_bytes())

      # Touch key(1) so key(2) becomes the LRU victim. get/1 bumps via an
      # async cast — sync on the owner's mailbox before the next put.
      assert {:ok, _} = ChunkCache.get(key(1))
      :sys.get_state(ChunkCache)

      ChunkCache.put(key(4), ten_bytes())

      assert {:ok, _} = ChunkCache.get(key(1))
      assert :miss = ChunkCache.get(key(2))
    end

    test "a chunk larger than the whole cap is not cached" do
      assert :ok = ChunkCache.put(key(5), :binary.copy("x", 40))
      assert :miss = ChunkCache.get(key(5))
      assert %{entries: 0, bytes: 0} = ChunkCache.stats()
    end
  end

  defp ten_bytes, do: :binary.copy("y", 10)
end
