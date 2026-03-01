defmodule NeonFS.Core.ChunkAccessTrackerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.ChunkAccessTracker

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    # Stop any existing tracker
    case GenServer.whereis(ChunkAccessTracker) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal, 5_000)
    end

    # Clean up persistent_term from previous runs
    try do
      :persistent_term.erase({ChunkAccessTracker, :max_chunks})
    rescue
      ArgumentError -> :ok
    end

    dets_path = Path.join(tmp_dir, "chunk_access_tracker.dets")
    {:ok, _pid} = ChunkAccessTracker.start_link(decay_interval_ms: 0, dets_path: dets_path)

    on_exit(fn ->
      case GenServer.whereis(ChunkAccessTracker) do
        nil ->
          :ok

        pid ->
          try do
            GenServer.stop(pid, :normal, 5_000)
          catch
            :exit, _ -> :ok
          end
      end

      try do
        :persistent_term.erase({ChunkAccessTracker, :max_chunks})
      rescue
        ArgumentError -> :ok
      end
    end)

    {:ok, dets_path: dets_path, tmp_dir: tmp_dir}
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
      :ets.insert(:chunk_access_tracker, {hash, 0, 5, old_ts, 0})

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
        :ets.insert(:chunk_access_tracker, {hash, 0, 1, old_ts, 0})
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
      :sys.get_state(ChunkAccessTracker)

      stats = ChunkAccessTracker.get_stats(hash)
      assert stats.hourly == 0
      # Daily should have the rolled-over counts (10 - 10/24 + 10 approx)
      assert stats.daily > 0
    end

    test "cleans up entries with no activity after staleness threshold" do
      hash = make_hash(:cleanup)

      # Insert an entry with zero counts at staleness 2 (one more cycle = pruned)
      :ets.insert(:chunk_access_tracker, {hash, 0, 0, System.system_time(:second), 2})

      # Trigger decay — staleness reaches 3, entry should be pruned
      send(GenServer.whereis(ChunkAccessTracker), :decay)
      :sys.get_state(ChunkAccessTracker)

      assert ChunkAccessTracker.get_stats(hash).hourly == 0
      assert ChunkAccessTracker.get_stats(hash).daily == 0
      assert :ets.lookup(:chunk_access_tracker, hash) == []
    end

    test "increments staleness for zero-activity entries below threshold" do
      hash = make_hash(:stale_increment)

      # Insert with zero activity and staleness 0
      :ets.insert(:chunk_access_tracker, {hash, 0, 0, System.system_time(:second), 0})

      # Trigger decay
      send(GenServer.whereis(ChunkAccessTracker), :decay)
      :sys.get_state(ChunkAccessTracker)

      # Entry should still exist with incremented staleness
      [{^hash, 0, 0, _ts, staleness}] = :ets.lookup(:chunk_access_tracker, hash)
      assert staleness == 1
    end

    test "resets staleness when entry has activity" do
      hash = make_hash(:stale_reset)

      # Insert with some staleness
      :ets.insert(:chunk_access_tracker, {hash, 5, 10, System.system_time(:second), 2})

      # Trigger decay — entry has activity (hourly=5), staleness should reset
      send(GenServer.whereis(ChunkAccessTracker), :decay)
      :sys.get_state(ChunkAccessTracker)

      [{^hash, 0, new_daily, _ts, staleness}] = :ets.lookup(:chunk_access_tracker, hash)
      assert new_daily > 0
      assert staleness == 0
    end
  end

  describe "DETS persistence" do
    test "stats survive a process restart", %{dets_path: dets_path} do
      hash = make_hash(:persist)

      for _ <- 1..7 do
        ChunkAccessTracker.record_access(hash)
      end

      assert ChunkAccessTracker.get_stats(hash).hourly == 7

      # Stop the tracker (triggers terminate → DETS sync)
      GenServer.stop(GenServer.whereis(ChunkAccessTracker), :normal, 5_000)

      # Verify DETS file was written
      assert File.exists?(dets_path)

      # Restart with same DETS path
      {:ok, _pid} = ChunkAccessTracker.start_link(decay_interval_ms: 0, dets_path: dets_path)

      # Stats should be restored
      stats = ChunkAccessTracker.get_stats(hash)
      assert stats.hourly == 7
      assert stats.daily == 7
    end

    test "DETS sync occurs on decay timer", %{dets_path: dets_path} do
      hash = make_hash(:decay_sync)
      ChunkAccessTracker.record_access(hash)

      # Trigger decay (which also syncs to DETS)
      send(GenServer.whereis(ChunkAccessTracker), :decay)
      :sys.get_state(ChunkAccessTracker)

      assert File.exists?(dets_path)

      # Verify DETS contains the entry
      {:ok, dets_ref} =
        :dets.open_file(:test_verify_dets, type: :set, file: String.to_charlist(dets_path))

      entries = :dets.match_object(dets_ref, :_)
      :dets.close(dets_ref)

      assert entries != []
      assert Enum.any?(entries, fn {h, _hourly, _daily, _ts, _stale} -> h == hash end)
    end
  end

  describe "table size management" do
    test "inserting beyond max triggers pruning", %{tmp_dir: tmp_dir} do
      # Stop current tracker
      GenServer.stop(GenServer.whereis(ChunkAccessTracker), :normal, 5_000)

      # Start with a very small max
      dets_path = Path.join(tmp_dir, "prune_test.dets")

      {:ok, _pid} =
        ChunkAccessTracker.start_link(
          decay_interval_ms: 0,
          dets_path: dets_path,
          max_chunks: 10
        )

      # Insert 10 stale entries (zero activity, high staleness)
      for i <- 1..10 do
        hash = make_hash("stale_#{i}")
        :ets.insert(:chunk_access_tracker, {hash, 0, 0, System.system_time(:second) - 1000, 2})
      end

      assert :ets.info(:chunk_access_tracker, :size) == 10

      # This record_access pushes us over the limit, triggering async prune
      ChunkAccessTracker.record_access(make_hash(:trigger))
      :sys.get_state(ChunkAccessTracker)

      # Table should be pruned back to at most max_chunks
      assert :ets.info(:chunk_access_tracker, :size) <= 10
    end

    test "stale entries are pruned after consecutive zero-activity cycles" do
      hash = make_hash(:multi_stale)

      # Insert with zero activity
      :ets.insert(:chunk_access_tracker, {hash, 0, 0, System.system_time(:second), 0})

      # Run 3 decay cycles — entry should be pruned after the 3rd
      for _ <- 1..3 do
        send(GenServer.whereis(ChunkAccessTracker), :decay)
        :sys.get_state(ChunkAccessTracker)
      end

      assert :ets.lookup(:chunk_access_tracker, hash) == []
    end
  end
end
