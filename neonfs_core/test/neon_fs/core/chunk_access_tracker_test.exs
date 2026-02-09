defmodule NeonFS.Core.ChunkAccessTrackerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.ChunkAccessTracker

  setup do
    # Ensure tracker is running
    ensure_tracker()

    # Clear ETS table for clean test state
    :ets.delete_all_objects(:chunk_access_tracker)

    :ok
  end

  defp ensure_tracker do
    case GenServer.whereis(ChunkAccessTracker) do
      nil ->
        {:ok, _pid} = ChunkAccessTracker.start_link(decay_interval_ms: 0)

      _pid ->
        :ok
    end
  end

  defp make_hash(n) do
    :crypto.hash(:sha256, "chunk_#{n}")
  end

  describe "record_access/1" do
    test "records first access for a chunk" do
      hash = make_hash(1)
      assert :ok = ChunkAccessTracker.record_access(hash)

      stats = ChunkAccessTracker.get_stats(hash)
      assert stats.hourly == 1
      assert stats.daily == 1
      assert stats.last_accessed != nil
    end

    test "increments counts on repeated access" do
      hash = make_hash(2)

      for _ <- 1..5 do
        ChunkAccessTracker.record_access(hash)
      end

      stats = ChunkAccessTracker.get_stats(hash)
      assert stats.hourly == 5
      assert stats.daily == 5
    end

    test "tracks multiple chunks independently" do
      hash_a = make_hash(:a)
      hash_b = make_hash(:b)

      for _ <- 1..3 do
        ChunkAccessTracker.record_access(hash_a)
      end

      ChunkAccessTracker.record_access(hash_b)

      assert ChunkAccessTracker.get_stats(hash_a).hourly == 3
      assert ChunkAccessTracker.get_stats(hash_b).hourly == 1
    end

    test "uses direct ETS operations (no GenServer bottleneck)" do
      hash = make_hash(:direct)

      # Record access should work even under concurrent load
      tasks =
        for i <- 1..100 do
          Task.async(fn ->
            ChunkAccessTracker.record_access(hash)
            i
          end)
        end

      Task.await_many(tasks)

      stats = ChunkAccessTracker.get_stats(hash)
      assert stats.hourly == 100
      assert stats.daily == 100
    end
  end

  describe "get_stats/1" do
    test "returns zeros for untracked chunk" do
      hash = make_hash(:unknown)
      stats = ChunkAccessTracker.get_stats(hash)

      assert stats.hourly == 0
      assert stats.daily == 0
      assert stats.last_accessed == nil
    end

    test "returns correct last_accessed timestamp" do
      hash = make_hash(:timestamp)
      before = DateTime.utc_now() |> DateTime.add(-1, :second)

      ChunkAccessTracker.record_access(hash)

      stats = ChunkAccessTracker.get_stats(hash)
      assert DateTime.compare(stats.last_accessed, before) in [:gt, :eq]
    end
  end

  describe "list_hot_chunks/2" do
    test "returns chunks above threshold" do
      hot_hash = make_hash(:hot)
      warm_hash = make_hash(:warm)

      for _ <- 1..10 do
        ChunkAccessTracker.record_access(hot_hash)
      end

      for _ <- 1..2 do
        ChunkAccessTracker.record_access(warm_hash)
      end

      hot_chunks = ChunkAccessTracker.list_hot_chunks(5)
      assert length(hot_chunks) == 1
      assert {^hot_hash, 10} = hd(hot_chunks)
    end

    test "returns empty when no chunks above threshold" do
      ChunkAccessTracker.record_access(make_hash(:low))

      assert ChunkAccessTracker.list_hot_chunks(100) == []
    end

    test "sorts by count descending" do
      hash_a = make_hash(:sort_a)
      hash_b = make_hash(:sort_b)

      for _ <- 1..5 do
        ChunkAccessTracker.record_access(hash_a)
      end

      for _ <- 1..10 do
        ChunkAccessTracker.record_access(hash_b)
      end

      hot_chunks = ChunkAccessTracker.list_hot_chunks(3)
      assert length(hot_chunks) == 2
      [{first_hash, _}, {second_hash, _}] = hot_chunks
      assert first_hash == hash_b
      assert second_hash == hash_a
    end

    test "respects limit" do
      for i <- 1..5 do
        hash = make_hash("limit_#{i}")

        for _ <- 1..10 do
          ChunkAccessTracker.record_access(hash)
        end
      end

      hot_chunks = ChunkAccessTracker.list_hot_chunks(1, 3)
      assert length(hot_chunks) == 3
    end
  end

  describe "list_cold_chunks/2" do
    test "returns chunks not accessed within N hours" do
      hash = make_hash(:cold)

      # Insert with a very old timestamp (48 hours ago)
      old_ts = System.system_time(:second) - 48 * 3600
      :ets.insert(:chunk_access_tracker, {hash, 0, 5, old_ts})

      cold_chunks = ChunkAccessTracker.list_cold_chunks(24)
      assert hash in cold_chunks
    end

    test "excludes recently accessed chunks" do
      hash = make_hash(:recent)
      ChunkAccessTracker.record_access(hash)

      cold_chunks = ChunkAccessTracker.list_cold_chunks(24)
      refute hash in cold_chunks
    end

    test "respects limit" do
      old_ts = System.system_time(:second) - 48 * 3600

      for i <- 1..5 do
        hash = make_hash("cold_limit_#{i}")
        :ets.insert(:chunk_access_tracker, {hash, 0, 1, old_ts})
      end

      cold_chunks = ChunkAccessTracker.list_cold_chunks(24, 3)
      assert length(cold_chunks) == 3
    end
  end

  describe "decay" do
    test "resets hourly counts and updates daily" do
      hash = make_hash(:decay)

      for _ <- 1..10 do
        ChunkAccessTracker.record_access(hash)
      end

      assert ChunkAccessTracker.get_stats(hash).hourly == 10

      # Trigger decay manually
      send(GenServer.whereis(ChunkAccessTracker), :decay)
      Process.sleep(50)

      stats = ChunkAccessTracker.get_stats(hash)
      assert stats.hourly == 0
      # Daily should have the rolled-over counts (10 - 10/24 + 10 approx)
      assert stats.daily > 0
    end

    test "cleans up entries with no activity" do
      hash = make_hash(:cleanup)

      # Insert an entry with zero counts
      :ets.insert(:chunk_access_tracker, {hash, 0, 0, System.system_time(:second)})

      # Trigger decay
      send(GenServer.whereis(ChunkAccessTracker), :decay)
      Process.sleep(50)

      # Entry should be cleaned up
      assert ChunkAccessTracker.get_stats(hash).hourly == 0
      assert ChunkAccessTracker.get_stats(hash).daily == 0
    end
  end
end
