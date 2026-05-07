defmodule NeonFS.Core.TieringManagerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.TieringManager

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    Application.put_env(:neonfs_core, :mock_drive_dir, tmp_dir)

    MockChunkIndex.init()
    MockAccessTracker.init()
    MockDriveRegistry.init()
    MockBackgroundWorker.init()

    {:ok, pid} = start_test_manager()

    on_exit(fn ->
      try do
        GenServer.stop(pid)
      catch
        :exit, _ -> :ok
      end

      MockChunkIndex.cleanup()
      MockAccessTracker.cleanup()
      MockDriveRegistry.cleanup()
      MockBackgroundWorker.cleanup()
      Application.delete_env(:neonfs_core, :mock_drive_dir)
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
    setup do
      # Initialize mock ETS tables
      MockChunkIndex.init()
      MockAccessTracker.init()
      MockDriveRegistry.init()
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

      {:ok, pid} = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 1
      assert result.demotions == 0

      GenServer.stop(pid)
    end

    test "promotes cold chunk to warm (not directly to hot)" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :cold)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, high_access_stats())

      {:ok, pid} = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 1

      submitted = MockBackgroundWorker.get_submitted()
      assert length(submitted) == 1
      [{_fn, opts}] = submitted
      assert opts[:label] =~ "promote"
      assert opts[:label] =~ "cold->warm"

      GenServer.stop(pid)
    end

    test "demotes chunk with no recent access past delay" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, stale_access_stats())

      {:ok, pid} = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.demotions == 1
      assert result.promotions == 0

      GenServer.stop(pid)
    end

    test "demotes hot chunk to warm (not directly to cold)" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, stale_access_stats())

      {:ok, pid} = start_test_manager()
      GenServer.call(pid, :evaluate_now, 10_000)

      submitted = MockBackgroundWorker.get_submitted()
      assert length(submitted) == 1
      [{_fn, opts}] = submitted
      assert opts[:label] =~ "demote"
      assert opts[:label] =~ "hot->warm"

      GenServer.stop(pid)
    end

    test "does not promote chunk already on hot tier" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, high_access_stats())

      {:ok, pid} = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 0

      GenServer.stop(pid)
    end

    test "does not demote chunk already on cold tier" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :cold)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, stale_access_stats())

      {:ok, pid} = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.demotions == 0

      GenServer.stop(pid)
    end

    test "skips evaluation when background worker queue is full" do
      MockBackgroundWorker.set_queue_full(true)

      {:ok, pid} = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.skipped == true
      assert result.reason == :queue_full

      GenServer.stop(pid)
    end

    test "dry run mode logs but does not submit work" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :warm)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, high_access_stats())

      {:ok, pid} = start_test_manager(dry_run: true)

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 1

      submitted = MockBackgroundWorker.get_submitted()
      assert submitted == []

      GenServer.stop(pid)
    end

    test "eviction under pressure forces demotion regardless of delay" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, recent_access_stats())
      MockDriveRegistry.set_tier_usage(:hot, 0.95)

      {:ok, pid} = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.evictions >= 1

      GenServer.stop(pid)
    end

    test "does not promote chunks with low access count" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :warm)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, low_access_stats())

      {:ok, pid} = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.promotions == 0

      GenServer.stop(pid)
    end

    test "handles chunk with nil last_accessed as demotion candidate" do
      chunk_hash = :crypto.strong_rand_bytes(32)
      chunk = mock_chunk(chunk_hash, :hot)

      MockChunkIndex.set_chunks([chunk])
      MockAccessTracker.set_stats(chunk_hash, %{hourly: 0, daily: 0, last_accessed: nil})

      {:ok, pid} = start_test_manager()

      result = GenServer.call(pid, :evaluate_now, 10_000)
      assert result.demotions == 1

      GenServer.stop(pid)
    end
  end

  describe "telemetry" do
    setup do
      MockChunkIndex.init()
      MockAccessTracker.init()
      MockDriveRegistry.init()
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

      {:ok, pid} = start_test_manager()
      GenServer.call(pid, :evaluate_now, 10_000)

      assert_receive {[:neonfs, :tiering_manager, :evaluation], ^ref, measurements, %{}}
      assert is_integer(measurements.chunks_evaluated)
      assert is_integer(measurements.promotions)
      assert is_integer(measurements.demotions)

      GenServer.stop(pid)
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

      {:ok, pid} = start_test_manager()
      GenServer.call(pid, :evaluate_now, 10_000)

      assert_receive {[:neonfs, :tiering_manager, :promotion], ^ref, %{},
                      %{hash: ^chunk_hash, from_tier: :warm, to_tier: :hot, dry_run: false}}

      GenServer.stop(pid)
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

      {:ok, pid} = start_test_manager()
      GenServer.call(pid, :evaluate_now, 10_000)

      assert_receive {[:neonfs, :tiering_manager, :demotion], ^ref, %{},
                      %{hash: ^chunk_hash, from_tier: :hot, to_tier: :warm, dry_run: false}}

      GenServer.stop(pid)
    end
  end

  ## Helpers

  defp start_test_manager(opts \\ []) do
    TieringManager.start_link(
      name: :"tiering_test_#{:erlang.unique_integer([:positive])}",
      eval_interval_ms: 600_000,
      dry_run: Keyword.get(opts, :dry_run, false),
      chunk_index_mod: MockChunkIndex,
      access_tracker_mod: MockAccessTracker,
      drive_registry_mod: MockDriveRegistry,
      volume_registry_mod: MockVolumeRegistry,
      background_worker_mod: MockBackgroundWorker
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

# Mock modules using ETS for test state sharing

defmodule MockChunkIndex do
  @table :mock_chunk_index_data

  def init do
    safe_delete_table()
    :ets.new(@table, [:named_table, :set, :public])
    :ets.insert(@table, {:chunks, []})
  end

  def cleanup do
    safe_delete_table()
  end

  defp safe_delete_table do
    :ets.delete(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  def set_chunks(chunks) do
    :ets.insert(@table, {:chunks, chunks})
  end

  def list_by_node(_node) do
    case :ets.lookup(@table, :chunks) do
      [{:chunks, chunks}] -> chunks
      [] -> []
    end
  end
end

defmodule MockAccessTracker do
  @table :mock_access_tracker_data

  def init do
    safe_delete_table()
    :ets.new(@table, [:named_table, :set, :public])
  end

  def cleanup do
    safe_delete_table()
  end

  defp safe_delete_table do
    :ets.delete(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  def set_stats(chunk_hash, stats) do
    :ets.insert(@table, {chunk_hash, stats})
  end

  def get_stats(chunk_hash) do
    case :ets.lookup(@table, chunk_hash) do
      [{^chunk_hash, stats}] -> stats
      [] -> %{hourly: 0, daily: 0, last_accessed: nil}
    end
  end
end

defmodule MockDriveRegistry do
  @table :mock_drive_registry_data

  def init do
    safe_delete_table()
    :ets.new(@table, [:named_table, :set, :public])
  end

  def cleanup do
    safe_delete_table()
  end

  defp safe_delete_table do
    :ets.delete(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  def set_tier_usage(tier, ratio) do
    :ets.insert(@table, {{:tier_usage, tier}, ratio})
  end

  def list_drives do
    base_dir = Application.get_env(:neonfs_core, :mock_drive_dir, "/tmp/mock")

    :ets.tab2list(@table)
    |> Enum.filter(fn
      {{:tier_usage, _}, _} -> true
      _ -> false
    end)
    |> Enum.map(fn {{:tier_usage, tier}, ratio} ->
      capacity = 1_000_000_000
      used = trunc(capacity * ratio)

      %NeonFS.Core.Drive{
        id: "mock_#{tier}",
        node: Node.self(),
        path: Path.join(base_dir, "#{tier}"),
        tier: tier,
        capacity_bytes: capacity,
        used_bytes: used,
        state: :active
      }
    end)
  end
end

defmodule MockVolumeRegistry do
  def list, do: []
end

defmodule MockBackgroundWorker do
  @table :mock_bg_worker_data

  def init do
    safe_delete_table()
    :ets.new(@table, [:named_table, :set, :public])
    :ets.insert(@table, {:submitted, []})
    :ets.insert(@table, {:queue_full, false})
  end

  def cleanup do
    safe_delete_table()
  end

  defp safe_delete_table do
    :ets.delete(@table)
    :ok
  rescue
    ArgumentError -> :ok
  end

  def set_queue_full(full) do
    :ets.insert(@table, {:queue_full, full})
  end

  def get_submitted do
    case :ets.lookup(@table, :submitted) do
      [{:submitted, list}] -> list
      [] -> []
    end
  end

  def status do
    queue_full =
      case :ets.lookup(@table, :queue_full) do
        [{:queue_full, val}] -> val
        [] -> false
      end

    %{
      queued: if(queue_full, do: 100, else: 0),
      running: 0,
      completed: 0,
      by_priority: %{high: 0, normal: 0, low: 0}
    }
  end

  def submit(work_fn, opts) do
    existing =
      case :ets.lookup(@table, :submitted) do
        [{:submitted, list}] -> list
        [] -> []
      end

    :ets.insert(@table, {:submitted, existing ++ [{work_fn, opts}]})
    {:ok, "mock_work_#{:erlang.unique_integer([:positive])}"}
  end
end
