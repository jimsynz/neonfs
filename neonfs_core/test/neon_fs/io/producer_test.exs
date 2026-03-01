defmodule NeonFS.IO.ProducerTest do
  use ExUnit.Case, async: true

  alias NeonFS.IO.{Operation, Producer}

  # A simple test consumer that collects dispatched operations
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

  # Mock volume registry that returns configurable io_weight per volume
  defmodule MockVolumeRegistry do
    use Agent

    def start_link(weights) do
      Agent.start_link(fn -> weights end, name: __MODULE__)
    end

    def get(volume_id) do
      weights = Agent.get(__MODULE__, & &1)

      case Map.get(weights, volume_id) do
        nil -> {:error, :not_found}
        weight -> {:ok, %{io_weight: weight}}
      end
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

  defp start_producer(ctx, opts \\ []) do
    name = :"producer_#{ctx.test}"

    volume_registry_mod =
      Keyword.get(opts, :volume_registry_mod, NeonFS.IO.ProducerTest.MockVolumeRegistry)

    producer =
      start_supervised!(
        {Producer, name: name, volume_registry_mod: volume_registry_mod},
        id: name
      )

    %{producer: producer}
  end

  defp start_consumer(producer, opts) do
    max_demand = Keyword.get(opts, :max_demand, 1)

    start_supervised!(
      {TestConsumer, producer: producer, test_pid: self(), max_demand: max_demand},
      id: :"consumer_#{System.unique_integer([:positive])}"
    )
  end

  # Use status/1 (a GenStage.call) to synchronise — the call is processed
  # after any pending casts, so when it returns all prior casts have been handled.
  defp sync(producer), do: Producer.status(producer)

  defp collect_dispatched(count, timeout \\ 500) do
    Enum.map(1..count, fn _ ->
      assert_receive {:dispatched, op}, timeout
      op
    end)
  end

  setup do
    start_supervised!({MockVolumeRegistry, %{"vol-1" => 100, "vol-2" => 100, "vol-3" => 300}})
    :ok
  end

  describe "single volume, priority ordering" do
    test "dispatches operations in priority order", ctx do
      %{producer: producer} = start_producer(ctx)

      # Enqueue in reverse priority order (lowest priority first).
      # No consumer yet, so operations buffer in the queue.
      scrub_op = make_op(priority: :scrub, volume_id: "vol-1")
      repair_op = make_op(priority: :repair, volume_id: "vol-1")
      user_read_op = make_op(priority: :user_read, volume_id: "vol-1")

      Producer.enqueue(producer, scrub_op)
      Producer.enqueue(producer, repair_op)
      Producer.enqueue(producer, user_read_op)

      # Synchronise — ensures all enqueue casts are processed
      sync(producer)

      # Now start consumer — demand triggers dispatch from buffered queue
      _consumer = start_consumer(producer, max_demand: 10)

      dispatched = collect_dispatched(3)
      priorities = Enum.map(dispatched, & &1.priority)

      assert priorities == [:user_read, :repair, :scrub]
    end

    test "dispatches same-priority operations in FIFO order", ctx do
      %{producer: producer} = start_producer(ctx)

      op1 = make_op(priority: :user_read, volume_id: "vol-1")
      op2 = make_op(priority: :user_read, volume_id: "vol-1")
      op3 = make_op(priority: :user_read, volume_id: "vol-1")

      Producer.enqueue(producer, op1)
      Producer.enqueue(producer, op2)
      Producer.enqueue(producer, op3)

      sync(producer)

      _consumer = start_consumer(producer, max_demand: 10)

      dispatched = collect_dispatched(3)
      ids = Enum.map(dispatched, & &1.id)

      assert ids == [op1.id, op2.id, op3.id]
    end
  end

  describe "WFQ fairness with different io_weights" do
    test "volume with higher weight gets proportionally more dispatches", ctx do
      # vol-1 has weight 100, vol-3 has weight 300 (3:1 ratio)
      %{producer: producer} = start_producer(ctx)

      # Enqueue all operations first (no consumer, so they buffer)
      for _ <- 1..4 do
        Producer.enqueue(producer, make_op(volume_id: "vol-1"))
        Producer.enqueue(producer, make_op(volume_id: "vol-3"))
      end

      sync(producer)

      # Now start consumer — triggers WFQ dispatch from buffered queues
      _consumer = start_consumer(producer, max_demand: 100)

      dispatched = collect_dispatched(8)
      volume_ids = Enum.map(dispatched, & &1.volume_id)

      # vol-3 (weight 300) advances VFT more slowly (1/300 vs 1/100 per op)
      # After all enqueues:
      #   vol-3 VFT = 4/300 ≈ 0.0133
      #   vol-1 VFT = 4/100 = 0.04
      # vol-3 has lower VFT, so its queue drains first
      vol3_count_in_first_half =
        Enum.count(Enum.take(volume_ids, 4), &(&1 == "vol-3"))

      assert vol3_count_in_first_half == 4,
             "vol-3 (weight 300) should get all first 4 dispatches, got #{vol3_count_in_first_half}; order: #{inspect(volume_ids)}"
    end

    test "equal weight volumes get fair interleaving", ctx do
      # vol-1 and vol-2 both have weight 100
      %{producer: producer} = start_producer(ctx)

      for _ <- 1..4 do
        Producer.enqueue(producer, make_op(volume_id: "vol-1"))
        Producer.enqueue(producer, make_op(volume_id: "vol-2"))
      end

      sync(producer)

      _consumer = start_consumer(producer, max_demand: 100)

      dispatched = collect_dispatched(8)
      volume_ids = Enum.map(dispatched, & &1.volume_id)

      vol1_count = Enum.count(volume_ids, &(&1 == "vol-1"))
      vol2_count = Enum.count(volume_ids, &(&1 == "vol-2"))

      assert vol1_count == 4
      assert vol2_count == 4
    end
  end

  describe "cancel/2" do
    test "removes operation from queue", ctx do
      %{producer: producer} = start_producer(ctx)

      op1 = make_op(volume_id: "vol-1")
      op2 = make_op(volume_id: "vol-1")
      op3 = make_op(volume_id: "vol-1")

      Producer.enqueue(producer, op1)
      Producer.enqueue(producer, op2)
      Producer.enqueue(producer, op3)

      # Cancel the middle operation
      Producer.cancel(producer, op2.id)

      sync(producer)

      _consumer = start_consumer(producer, max_demand: 10)

      dispatched = collect_dispatched(2)
      ids = Enum.map(dispatched, & &1.id)

      assert op2.id not in ids
      assert op1.id in ids
      assert op3.id in ids
    end

    test "cancel reflects in status counters", ctx do
      %{producer: producer} = start_producer(ctx)

      op = make_op(priority: :user_read, volume_id: "vol-1")
      Producer.enqueue(producer, op)

      # sync ensures the enqueue cast has been processed
      status = sync(producer)
      assert status.queue_depths[:user_read] == 1
      assert status.total_pending == 1

      Producer.cancel(producer, op.id)

      status = sync(producer)
      assert status.queue_depths[:user_read] == 0
      assert status.total_pending == 0
    end
  end

  describe "status/1" do
    test "returns correct queue depths", ctx do
      %{producer: producer} = start_producer(ctx)

      Producer.enqueue(producer, make_op(priority: :user_read, volume_id: "vol-1"))
      Producer.enqueue(producer, make_op(priority: :user_read, volume_id: "vol-1"))
      Producer.enqueue(producer, make_op(priority: :scrub, volume_id: "vol-1"))
      Producer.enqueue(producer, make_op(priority: :replication, volume_id: "vol-2"))

      status = sync(producer)

      assert status.queue_depths[:user_read] == 2
      assert status.queue_depths[:scrub] == 1
      assert status.queue_depths[:replication] == 1
      assert status.queue_depths[:repair] == 0
      assert status.total_pending == 4
    end

    test "returns zero depths when empty", ctx do
      %{producer: producer} = start_producer(ctx)

      status = sync(producer)

      assert status.total_pending == 0

      for {_priority, count} <- status.queue_depths do
        assert count == 0
      end
    end
  end

  describe "demand buffering" do
    test "buffers demand and dispatches when operations arrive", ctx do
      %{producer: producer} = start_producer(ctx)

      # Start consumer first — it will request demand with nothing to dispatch
      _consumer = start_consumer(producer, max_demand: 5)

      # Synchronise to ensure consumer subscription and demand are registered
      sync(producer)

      # Now enqueue an operation — it should be dispatched immediately
      op = make_op(volume_id: "vol-1")
      Producer.enqueue(producer, op)

      assert_receive {:dispatched, dispatched_op}, 500
      assert dispatched_op.id == op.id
    end
  end
end
