defmodule NeonFS.Core.TieringManagerTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.TieringManager
  alias NeonFS.TestSupport.TieringMocks.AccessTracker, as: MockAccessTracker
  alias NeonFS.TestSupport.TieringMocks.BackgroundWorker, as: MockBackgroundWorker
  alias NeonFS.TestSupport.TieringMocks.ChunkIndex, as: MockChunkIndex
  alias NeonFS.TestSupport.TieringMocks.DriveRegistry, as: MockDriveRegistry
  alias NeonFS.TestSupport.TieringMocks.VolumeRegistry, as: MockVolumeRegistry

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    MockChunkIndex.init()
    MockAccessTracker.init()
    MockDriveRegistry.init(tmp_dir)
    MockBackgroundWorker.init()

    pid = start_test_manager()

    on_exit(fn ->
      MockChunkIndex.cleanup()
      MockAccessTracker.cleanup()
      MockDriveRegistry.cleanup()
      MockBackgroundWorker.cleanup()
    end)

    %{manager: pid}
  end

  describe "status/0" do
    test "returns configuration and last evaluation", %{manager: pid} do
      status = GenServer.call(pid, :status)
      assert is_integer(status.eval_interval_ms)
      assert is_integer(status.max_chunks_per_cycle)
      assert is_number(status.eviction_threshold)
      assert is_integer(status.queue_full_threshold)
      assert is_boolean(status.dry_run)
    end
  end

  describe "evaluate_now/0" do
    test "returns evaluation result with no chunks", %{manager: pid} do
      result = GenServer.call(pid, :evaluate_now, 30_000)
      assert is_map(result)
      assert result.promotions == 0
      assert result.demotions == 0
    end
  end

  describe "with mock modules" do
    setup %{tmp_dir: tmp_dir} do
      # Initialize mock ETS tables
      MockChunkIndex.init()
      MockAccessTracker.init()
      MockDriveRegistry.init(tmp_dir)
      MockBackgroundWorker.init()

      on_exit(fn ->
        MockChunkIndex.cleanup()
        MockAccessTracker.cleanup()
        MockDriveRegistry.cleanup()
        MockBackgroundWorker.cleanup()
      end)

      :ok
    end

    test "promotes chunk with high daily access count" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :warm)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, high_access_stats())

      pid = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 1
      assert result.demotions == 0
    end

    test "promotes cold chunk to warm (not directly to hot)" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :cold)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, high_access_stats())

      pid = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 1

      submitted = MockBackgroundWorker.get_submitted()
      assert length(submitted) == 1
      [{_fn, opts}] = submitted
      assert opts[:label] =~ "promote"
      assert opts[:label] =~ "cold->warm"
    end

    test "demotes chunk with no recent access past delay" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, stale_access_stats())

      pid = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.demotions == 1
      assert result.promotions == 0
    end

    test "demotes hot chunk to warm (not directly to cold)" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, stale_access_stats())

      pid = start_test_manager()
      GenServer.call(pid, :evaluate_now, 10_000)

      submitted = MockBackgroundWorker.get_submitted()
      assert length(submitted) == 1
      [{_fn, opts}] = submitted
      assert opts[:label] =~ "demote"
      assert opts[:label] =~ "hot->warm"
    end

    test "does not promote chunk already on hot tier" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, high_access_stats())

      pid = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 0
    end

    test "does not demote chunk already on cold tier" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :cold)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, stale_access_stats())

      pid = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.demotions == 0
    end

    test "skips evaluation when background worker queue is full" do
      MockBackgroundWorker.set_queue_full(true)

      pid = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.skipped == true
      assert result.reason == :queue_full
    end

    test "dry run mode logs but does not submit work" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :warm)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, high_access_stats())

      pid = start_test_manager(dry_run: true)

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 1

      submitted = MockBackgroundWorker.get_submitted()
      assert submitted == []
    end

    test "eviction under pressure forces demotion regardless of delay" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, recent_access_stats())
      MockDriveRegistry.set_tier_usage(:hot, 0.95)

      pid = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.evictions >= 1
    end

    test "does not promote chunks with low access count" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :warm)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, low_access_stats())

      pid = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 0
    end

    test "handles chunk with nil last_accessed as demotion candidate" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, %{hourly: 0, daily: 0, last_accessed: nil})

      pid = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.demotions == 1
    end
  end

  describe "telemetry" do
    setup %{tmp_dir: tmp_dir} do
      MockChunkIndex.init()
      MockAccessTracker.init()
      MockDriveRegistry.init(tmp_dir)
      MockBackgroundWorker.init()

      on_exit(fn ->
        MockChunkIndex.cleanup()
        MockAccessTracker.cleanup()
        MockDriveRegistry.cleanup()
        MockBackgroundWorker.cleanup()
      end)

      :ok
    end

    test "emits evaluation telemetry event" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :tiering_manager, :evaluation]
        ])

      pid = start_test_manager()
      GenServer.call(pid, :evaluate_now, 10_000)

      assert_receive {[:neonfs, :tiering_manager, :evaluation], ^ref, measurements, %{}}
      assert is_integer(measurements.chunks_evaluated)
      assert is_integer(measurements.promotions)
      assert is_integer(measurements.demotions)
    end

    test "emits promotion telemetry event" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :warm)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, high_access_stats())

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :tiering_manager, :promotion]
        ])

      pid = start_test_manager()
      GenServer.call(pid, :evaluate_now, 10_000)

      assert_receive {[:neonfs, :tiering_manager, :promotion], ^ref, %{},
                      %{hash: ^chunk_hash, from_tier: :warm, to_tier: :hot, dry_run: false}}
    end

    test "emits demotion telemetry event" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, stale_access_stats())

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :tiering_manager, :demotion]
        ])

      pid = start_test_manager()
      GenServer.call(pid, :evaluate_now, 10_000)

      assert_receive {[:neonfs, :tiering_manager, :demotion], ^ref, %{},
                      %{hash: ^chunk_hash, from_tier: :hot, to_tier: :warm, dry_run: false}}
    end
  end

  ## Helpers

  defp start_test_manager(opts \\ []) do
    name = :"tiering_test_#{:erlang.unique_integer([:positive])}"

    start_supervised!(
      {TieringManager,
       [
         name: name,
         eval_interval_ms: 600_000,
         dry_run: Keyword.get(opts, :dry_run, false),
         chunk_index_mod: MockChunkIndex,
         access_tracker_mod: MockAccessTracker,
         drive_registry_mod: MockDriveRegistry,
         volume_registry_mod: MockVolumeRegistry,
         background_worker_mod: MockBackgroundWorker
       ]},
      id: name,
      restart: :temporary
    )
  end

  defp mock_chunk(hash, tier) do
    %NeonFS.Core.ChunkMeta{
      hash: hash,
      original_size: 1024,
      stored_size: 1024,
      compression: :none,
      locations: [%{node: Node.self(), drive_id: "default", tier: tier}],
      target_replicas: 1,
      commit_state: :committed,
      active_write_refs: MapSet.new(),
      created_at: DateTime.utc_now(),
      last_verified: nil
    }
  end

  defp high_access_stats do
    %{hourly: 20, daily: 50, last_accessed: DateTime.utc_now()}
  end

  defp low_access_stats do
    %{hourly: 1, daily: 3, last_accessed: DateTime.utc_now()}
  end

  defp stale_access_stats do
    %{
      hourly: 0,
      daily: 0,
      last_accessed: DateTime.add(DateTime.utc_now(), -172_800, :second)
    }
  end

  defp recent_access_stats do
    %{hourly: 5, daily: 15, last_accessed: DateTime.add(DateTime.utc_now(), -3600, :second)}
  end
end
