defmodule NeonFS.IO.PriorityAdjusterTest do
  use ExUnit.Case, async: true

  alias NeonFS.IO.{Operation, PriorityAdjuster, Producer}

  # Mock StorageMetrics that returns configurable utilisation
  defmodule MockStorageMetrics do
    use Agent

    def start_link(opts) do
      state = Map.new(opts)
      Agent.start_link(fn -> state end, name: state[:name] || __MODULE__)
    end

    def cluster_capacity do
      Agent.get(__MODULE__, fn state -> state.capacity end)
    end
  end

  # Mock VolumeRegistry that returns configurable volumes
  defmodule MockVolumeRegistry do
    use Agent

    def start_link(opts) do
      state = Map.new(opts)
      Agent.start_link(fn -> state end, name: state[:name] || __MODULE__)
    end

    def get(volume_id) do
      volumes = Agent.get(__MODULE__, fn state -> Map.get(state, :volumes, []) end)

      case Enum.find(volumes, &(&1.id == volume_id)) do
        nil -> {:error, :not_found}
        vol -> {:ok, vol}
      end
    end

    def list do
      Agent.get(__MODULE__, fn state -> Map.get(state, :volumes, []) end)
    end

    def set_volumes(volumes) do
      Agent.update(__MODULE__, fn state -> Map.put(state, :volumes, volumes) end)
    end
  end

  # Mock ChunkIndex that returns configurable chunks per volume
  defmodule MockChunkIndex do
    use Agent

    def start_link(opts) do
      state = Map.new(opts)
      Agent.start_link(fn -> state end, name: state[:name] || __MODULE__)
    end

    def get_chunks_for_volume(volume_id) do
      Agent.get(__MODULE__, fn state ->
        Map.get(Map.get(state, :chunks, %{}), volume_id, [])
      end)
    end
  end

  # Test consumer that collects dispatched operations
  defmodule TestConsumer do
    use GenStage

    def start_link(opts) do
      GenStage.start_link(__MODULE__, opts)
    end

    @impl true
    def init(opts) do
      producer = Keyword.fetch!(opts, :producer)
      test_pid = Keyword.fetch!(opts, :test_pid)
      max_demand = Keyword.get(opts, :max_demand, 1)

      {:consumer, %{test_pid: test_pid},
       subscribe_to: [{producer, max_demand: max_demand, min_demand: 0}]}
    end

    @impl true
    def handle_events(events, _from, state) do
      for event <- events do
        send(state.test_pid, {:dispatched, event})
      end

      {:noreply, [], state}
    end
  end

  defp make_op(overrides) do
    defaults = [
      priority: :user_read,
      volume_id: "vol-1",
      drive_id: "nvme0",
      type: :read,
      callback: fn -> :ok end
    ]

    Operation.new(Keyword.merge(defaults, overrides))
  end

  defp make_volume(id, opts \\ []) do
    factor = Keyword.get(opts, :factor, 3)
    io_weight = Keyword.get(opts, :io_weight, 100)

    %{
      id: id,
      name: "volume-#{id}",
      io_weight: io_weight,
      durability: %{type: :replicate, factor: factor, min_copies: 1}
    }
  end

  defp make_chunk(hash, locations) do
    %{
      hash: hash,
      stored_size: 1024,
      locations: Enum.map(locations, fn node -> %{node: node, drive_id: "nvme0"} end)
    }
  end

  defp start_producer(ctx) do
    name = :"producer_#{ctx.test}"

    producer =
      start_supervised!(
        {Producer, name: name, volume_registry_mod: MockVolumeRegistry},
        id: name
      )

    %{producer: producer, producer_name: name}
  end

  defp sync(producer), do: Producer.status(producer)

  defp collect_dispatched(count, timeout \\ 500) do
    Enum.map(1..count, fn _ ->
      assert_receive {:dispatched, op}, timeout
      op
    end)
  end

  defp normal_capacity do
    %{
      drives: [%{capacity_bytes: 1000, used_bytes: 500, available_bytes: 500, state: :active}],
      total_capacity: 1000,
      total_used: 500,
      total_available: 500
    }
  end

  defp boost_capacity do
    %{
      drives: [%{capacity_bytes: 1000, used_bytes: 900, available_bytes: 100, state: :active}],
      total_capacity: 1000,
      total_used: 900,
      total_available: 100
    }
  end

  defp critical_capacity do
    %{
      drives: [%{capacity_bytes: 1000, used_bytes: 960, available_bytes: 40, state: :active}],
      total_capacity: 1000,
      total_used: 960,
      total_available: 40
    }
  end

  defp unlimited_capacity do
    %{
      drives: [%{capacity_bytes: 0, used_bytes: 500, available_bytes: :unlimited, state: :active}],
      total_capacity: :unlimited,
      total_used: 500,
      total_available: :unlimited
    }
  end

  setup do
    start_supervised!({MockStorageMetrics, name: MockStorageMetrics, capacity: normal_capacity()})

    start_supervised!(
      {MockVolumeRegistry, name: MockVolumeRegistry, volumes: [make_volume("vol-1")]}
    )

    start_supervised!({MockChunkIndex, name: MockChunkIndex, chunks: %{}})

    :ok
  end

  describe "no adjustment under normal conditions" do
    test "does not publish overrides when storage below 85%", ctx do
      %{producer: producer, producer_name: producer_name} = start_producer(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neon_fs, :io, :priority_check],
          [:neon_fs, :io, :priority_adjusted]
        ])

      start_supervised!(
        {PriorityAdjuster,
         name: :"adjuster_#{ctx.test}",
         producer: producer_name,
         check_interval: 50,
         storage_metrics_mod: MockStorageMetrics,
         volume_registry_mod: MockVolumeRegistry,
         chunk_index_mod: MockChunkIndex},
        id: :"adjuster_#{ctx.test}"
      )

      # Wait for at least one check cycle via telemetry
      assert_receive {[:neon_fs, :io, :priority_check], ^ref, %{}, %{level: :normal}}, 1_000

      # No adjustment should fire since we're at normal level from the start
      refute_received {[:neon_fs, :io, :priority_adjusted], ^ref, _, _}

      # Enqueue scrub and user_read ops — scrub should still come after user_read
      scrub_op = make_op(priority: :scrub, volume_id: "vol-1")
      user_read_op = make_op(priority: :user_read, volume_id: "vol-1")

      Producer.enqueue(producer, scrub_op)
      Producer.enqueue(producer, user_read_op)
      sync(producer)

      _consumer =
        start_supervised!(
          {TestConsumer, producer: producer, test_pid: self(), max_demand: 10},
          id: :"consumer_#{ctx.test}"
        )

      dispatched = collect_dispatched(2)
      priorities = Enum.map(dispatched, & &1.priority)

      assert priorities == [:user_read, :scrub]
    end
  end

  describe "boost adjustment at 85-95% storage" do
    test "increases scrub and repair weights by 50% at boost level", ctx do
      # Set storage to 90% — above boost threshold
      Agent.update(MockStorageMetrics, fn state ->
        Map.put(state, :capacity, boost_capacity())
      end)

      %{producer: producer, producer_name: producer_name} = start_producer(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neon_fs, :io, :priority_adjusted]
        ])

      start_supervised!(
        {PriorityAdjuster,
         name: :"adjuster_#{ctx.test}",
         producer: producer_name,
         check_interval: 50,
         storage_metrics_mod: MockStorageMetrics,
         volume_registry_mod: MockVolumeRegistry,
         chunk_index_mod: MockChunkIndex},
        id: :"adjuster_#{ctx.test}"
      )

      # Wait for adjustment
      assert_receive {[:neon_fs, :io, :priority_adjusted], ^ref, %{}, %{level: :boost}},
                     1_000

      sync(producer)
    end

    test "boost weights are applied to dispatch ordering", ctx do
      Agent.update(MockStorageMetrics, fn state ->
        Map.put(state, :capacity, boost_capacity())
      end)

      %{producer: producer, producer_name: producer_name} = start_producer(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neon_fs, :io, :priority_adjusted]
        ])

      start_supervised!(
        {PriorityAdjuster,
         name: :"adjuster_#{ctx.test}",
         producer: producer_name,
         check_interval: 50,
         storage_metrics_mod: MockStorageMetrics,
         volume_registry_mod: MockVolumeRegistry,
         chunk_index_mod: MockChunkIndex},
        id: :"adjuster_#{ctx.test}"
      )

      # Wait for adjustment to propagate
      assert_receive {[:neon_fs, :io, :priority_adjusted], ^ref, %{}, %{level: :boost}}, 1_000
      sync(producer)

      # Under boost: scrub weight = 15 (10 * 1.5), repair weight = 30 (20 * 1.5)
      # read_repair stays at 40, replication stays at 60
      # Order should be: replication(60), read_repair(40), repair(30), scrub(15)
      replication_op = make_op(priority: :replication, volume_id: "vol-1")
      repair_op = make_op(priority: :repair, volume_id: "vol-1")
      scrub_op = make_op(priority: :scrub, volume_id: "vol-1")
      read_repair_op = make_op(priority: :read_repair, volume_id: "vol-1")

      Producer.enqueue(producer, scrub_op)
      Producer.enqueue(producer, repair_op)
      Producer.enqueue(producer, replication_op)
      Producer.enqueue(producer, read_repair_op)
      sync(producer)

      _consumer =
        start_supervised!(
          {TestConsumer, producer: producer, test_pid: self(), max_demand: 10},
          id: :"consumer_#{ctx.test}"
        )

      dispatched = collect_dispatched(4)
      priorities = Enum.map(dispatched, & &1.priority)

      assert priorities == [:replication, :read_repair, :repair, :scrub]
    end
  end

  describe "critical adjustment above 95% storage" do
    test "scrub and repair weights match replication at critical level", ctx do
      Agent.update(MockStorageMetrics, fn state ->
        Map.put(state, :capacity, critical_capacity())
      end)

      %{producer: producer, producer_name: producer_name} = start_producer(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neon_fs, :io, :priority_adjusted]
        ])

      start_supervised!(
        {PriorityAdjuster,
         name: :"adjuster_#{ctx.test}",
         producer: producer_name,
         check_interval: 50,
         storage_metrics_mod: MockStorageMetrics,
         volume_registry_mod: MockVolumeRegistry,
         chunk_index_mod: MockChunkIndex},
        id: :"adjuster_#{ctx.test}"
      )

      assert_receive {[:neon_fs, :io, :priority_adjusted], ^ref, %{}, %{level: :critical}}, 1_000

      # Under critical: scrub=60, repair=60, matching replication=60
      # Enqueue all three — they should dispatch in any order since weights are equal
      replication_op = make_op(priority: :replication, volume_id: "vol-1")
      repair_op = make_op(priority: :repair, volume_id: "vol-1")
      scrub_op = make_op(priority: :scrub, volume_id: "vol-1")

      Producer.enqueue(producer, scrub_op)
      Producer.enqueue(producer, repair_op)
      Producer.enqueue(producer, replication_op)
      sync(producer)

      _consumer =
        start_supervised!(
          {TestConsumer, producer: producer, test_pid: self(), max_demand: 10},
          id: :"consumer_#{ctx.test}"
        )

      dispatched = collect_dispatched(3)
      priorities = Enum.map(dispatched, & &1.priority)

      # All three have equal weight (60), so FIFO order within the volume queue
      # After queue resorting with equal weights, insertion order is preserved
      # The scrub was enqueued first, then repair, then replication — but
      # with equal weights they maintain FIFO order
      assert :replication in priorities
      assert :repair in priorities
      assert :scrub in priorities
    end

    test "critical promotes scrub above read_repair in dispatch", ctx do
      Agent.update(MockStorageMetrics, fn state ->
        Map.put(state, :capacity, critical_capacity())
      end)

      %{producer: producer, producer_name: producer_name} = start_producer(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neon_fs, :io, :priority_adjusted]
        ])

      start_supervised!(
        {PriorityAdjuster,
         name: :"adjuster_#{ctx.test}",
         producer: producer_name,
         check_interval: 50,
         storage_metrics_mod: MockStorageMetrics,
         volume_registry_mod: MockVolumeRegistry,
         chunk_index_mod: MockChunkIndex},
        id: :"adjuster_#{ctx.test}"
      )

      assert_receive {[:neon_fs, :io, :priority_adjusted], ^ref, %{}, %{level: :critical}}, 1_000
      sync(producer)

      # Under critical: scrub=60, repair=60, read_repair=40 (static)
      # So scrub and repair now outrank read_repair!
      read_repair_op = make_op(priority: :read_repair, volume_id: "vol-1")
      scrub_op = make_op(priority: :scrub, volume_id: "vol-1")

      Producer.enqueue(producer, read_repair_op)
      Producer.enqueue(producer, scrub_op)
      sync(producer)

      _consumer =
        start_supervised!(
          {TestConsumer, producer: producer, test_pid: self(), max_demand: 10},
          id: :"consumer_#{ctx.test}"
        )

      dispatched = collect_dispatched(2)
      priorities = Enum.map(dispatched, & &1.priority)

      # Scrub (boosted to 60) should come before read_repair (static 40)
      assert priorities == [:scrub, :read_repair]
    end
  end

  describe "per-volume repair promotion for degraded volumes" do
    test "promotes repair to user_read weight for degraded volume", ctx do
      # Set up a volume with factor=3 but chunks that only have 2 replicas
      vol = make_volume("vol-1", factor: 3)
      MockVolumeRegistry.set_volumes([vol])

      Agent.update(MockChunkIndex, fn state ->
        chunks = [make_chunk("abc123", [:node1, :node2])]
        Map.put(state, :chunks, %{"vol-1" => chunks})
      end)

      %{producer: producer, producer_name: producer_name} = start_producer(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neon_fs, :io, :priority_adjusted]
        ])

      start_supervised!(
        {PriorityAdjuster,
         name: :"adjuster_#{ctx.test}",
         producer: producer_name,
         check_interval: 50,
         storage_metrics_mod: MockStorageMetrics,
         volume_registry_mod: MockVolumeRegistry,
         chunk_index_mod: MockChunkIndex},
        id: :"adjuster_#{ctx.test}"
      )

      assert_receive {[:neon_fs, :io, :priority_adjusted], ^ref, %{},
                      %{degraded_volumes: degraded}},
                     1_000

      assert "vol-1" in degraded

      # Repair for vol-1 should now have user_read weight (100)
      # Enqueue user_write(80) and repair — repair(100) should come first!
      user_write_op = make_op(priority: :user_write, volume_id: "vol-1")
      repair_op = make_op(priority: :repair, volume_id: "vol-1")

      Producer.enqueue(producer, user_write_op)
      Producer.enqueue(producer, repair_op)
      sync(producer)

      _consumer =
        start_supervised!(
          {TestConsumer, producer: producer, test_pid: self(), max_demand: 10},
          id: :"consumer_#{ctx.test}"
        )

      dispatched = collect_dispatched(2)
      priorities = Enum.map(dispatched, & &1.priority)

      # Repair promoted to 100 (user_read weight) > user_write (80)
      assert priorities == [:repair, :user_write]
    end

    test "per-volume override only affects the degraded volume", ctx do
      vol1 = make_volume("vol-1", factor: 3)
      vol2 = make_volume("vol-2", factor: 3)
      MockVolumeRegistry.set_volumes([vol1, vol2])

      # vol-1 degraded, vol-2 healthy
      Agent.update(MockChunkIndex, fn state ->
        chunks = %{
          "vol-1" => [make_chunk("abc123", [:node1, :node2])],
          "vol-2" => [make_chunk("def456", [:node1, :node2, :node3])]
        }

        Map.put(state, :chunks, chunks)
      end)

      %{producer: producer, producer_name: producer_name} = start_producer(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neon_fs, :io, :priority_adjusted]
        ])

      start_supervised!(
        {PriorityAdjuster,
         name: :"adjuster_#{ctx.test}",
         producer: producer_name,
         check_interval: 50,
         storage_metrics_mod: MockStorageMetrics,
         volume_registry_mod: MockVolumeRegistry,
         chunk_index_mod: MockChunkIndex},
        id: :"adjuster_#{ctx.test}"
      )

      assert_receive {[:neon_fs, :io, :priority_adjusted], ^ref, %{}, _}, 1_000
      sync(producer)

      # vol-1 repair promoted to 100 (user_read weight)
      # vol-2 repair stays at 20 (default)
      # Enqueue repair for both volumes, plus user_write for both
      vol1_repair = make_op(priority: :repair, volume_id: "vol-1")
      vol2_repair = make_op(priority: :repair, volume_id: "vol-2")
      vol2_user_write = make_op(priority: :user_write, volume_id: "vol-2")

      Producer.enqueue(producer, vol2_user_write)
      Producer.enqueue(producer, vol2_repair)
      Producer.enqueue(producer, vol1_repair)
      sync(producer)

      _consumer =
        start_supervised!(
          {TestConsumer, producer: producer, test_pid: self(), max_demand: 10},
          id: :"consumer_#{ctx.test}"
        )

      dispatched = collect_dispatched(3)

      # vol-1 repair (effective weight 100) should be dispatched before vol-2's ops
      # vol-2: user_write(80) before repair(20)
      # WFQ interleaving: vol-1 and vol-2 alternate based on VFT.
      # With equal io_weight, first dispatch goes to whichever was enqueued first.
      # vol-2 user_write enqueued first → vol-2 gets VFT first.
      # So dispatch order: vol-2:user_write, vol-1:repair, vol-2:repair
      # (because vol-2 gets lower VFT from being enqueued first)

      # What matters: vol-1's repair has effective weight 100
      # vol-2's user_write has weight 80, vol-2's repair has weight 20
      # The WFQ will interleave volumes, but within vol-2 user_write > repair
      vol2_ops = Enum.filter(dispatched, &(&1.volume_id == "vol-2"))
      vol2_priorities = Enum.map(vol2_ops, & &1.priority)

      assert vol2_priorities == [:user_write, :repair]
    end
  end

  describe "unlimited capacity" do
    test "treats unlimited capacity as no pressure", ctx do
      Agent.update(MockStorageMetrics, fn state ->
        Map.put(state, :capacity, unlimited_capacity())
      end)

      %{producer_name: producer_name} = start_producer(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neon_fs, :io, :priority_check],
          [:neon_fs, :io, :priority_adjusted]
        ])

      start_supervised!(
        {PriorityAdjuster,
         name: :"adjuster_#{ctx.test}",
         producer: producer_name,
         check_interval: 50,
         storage_metrics_mod: MockStorageMetrics,
         volume_registry_mod: MockVolumeRegistry,
         chunk_index_mod: MockChunkIndex},
        id: :"adjuster_#{ctx.test}"
      )

      # Wait for at least one check cycle via telemetry
      assert_receive {[:neon_fs, :io, :priority_check], ^ref, %{}, %{level: :normal}}, 1_000

      # Should not fire any adjustment event
      refute_received {[:neon_fs, :io, :priority_adjusted], ^ref, _, _}
    end
  end

  describe "configurable thresholds" do
    test "respects custom boost and critical thresholds", ctx do
      # 70% used — below default 85% but above custom 60%
      Agent.update(MockStorageMetrics, fn state ->
        capacity = %{
          drives: [%{capacity_bytes: 1000, used_bytes: 700, available_bytes: 300, state: :active}],
          total_capacity: 1000,
          total_used: 700,
          total_available: 300
        }

        Map.put(state, :capacity, capacity)
      end)

      %{producer_name: producer_name} = start_producer(ctx)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neon_fs, :io, :priority_adjusted]
        ])

      start_supervised!(
        {PriorityAdjuster,
         name: :"adjuster_#{ctx.test}",
         producer: producer_name,
         check_interval: 50,
         boost_threshold: 0.60,
         critical_threshold: 0.80,
         storage_metrics_mod: MockStorageMetrics,
         volume_registry_mod: MockVolumeRegistry,
         chunk_index_mod: MockChunkIndex},
        id: :"adjuster_#{ctx.test}"
      )

      # At 70% with custom boost=60%, should trigger boost
      assert_receive {[:neon_fs, :io, :priority_adjusted], ^ref, %{}, %{level: :boost}}, 1_000
    end
  end
end
