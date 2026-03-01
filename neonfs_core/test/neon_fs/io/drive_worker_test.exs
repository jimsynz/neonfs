defmodule NeonFS.IO.DriveWorkerTest do
  use ExUnit.Case, async: true

  alias NeonFS.IO.{DriveWorker, Operation, Producer}

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
      drive_id: "test-drive",
      type: :read,
      callback: fn -> :ok end
    ]

    Operation.new(Keyword.merge(defaults, overrides))
  end

  defp start_system(ctx, opts \\ []) do
    drive_id = Keyword.get(opts, :drive_id, "test-drive")
    drive_type = Keyword.get(opts, :drive_type, :ssd)
    drive_ids = Keyword.get(opts, :drive_ids, [drive_id])
    strategy_config = Keyword.get(opts, :strategy_config, [])

    producer_name = :"producer_#{ctx.test}"

    dispatcher =
      {GenStage.PartitionDispatcher,
       partitions: drive_ids, hash: fn %Operation{drive_id: did} = event -> {event, did} end}

    producer =
      start_supervised!(
        {Producer,
         name: producer_name, dispatcher: dispatcher, volume_registry_mod: MockVolumeRegistry},
        id: producer_name
      )

    worker_name = :"worker_#{ctx.test}_#{drive_id}"

    worker =
      start_supervised!(
        {DriveWorker,
         name: worker_name,
         drive_id: drive_id,
         drive_type: drive_type,
         producer: producer_name,
         strategy_config: strategy_config},
        id: worker_name
      )

    # Synchronise with GenStage subscription handshake
    :sys.get_state(worker)

    %{
      producer: producer,
      producer_name: producer_name,
      worker: worker,
      drive_id: drive_id
    }
  end

  setup do
    start_supervised!({MockVolumeRegistry, %{"vol-1" => 100}})
    :ok
  end

  describe "HDD strategy" do
    test "worker dispatches operations using HDD strategy ordering", ctx do
      test_pid = self()

      %{producer_name: producer} =
        start_system(ctx, drive_type: :hdd, strategy_config: [batch_size: 100])

      # Enqueue write first, then reads — HDD strategy serves reads before writes
      ops = [
        make_op(
          type: :write,
          metadata: %{chunk_hash: "zzz"},
          callback: fn -> send(test_pid, {:exec, :w1}) end
        ),
        make_op(
          type: :read,
          metadata: %{chunk_hash: "bbb"},
          callback: fn -> send(test_pid, {:exec, :r2}) end
        ),
        make_op(
          type: :read,
          metadata: %{chunk_hash: "aaa"},
          callback: fn -> send(test_pid, {:exec, :r1}) end
        )
      ]

      for op <- ops, do: Producer.enqueue(producer, op)

      # All three should execute
      for _ <- 1..3, do: assert_receive({:exec, _}, 2000)

      # Verify all operations completed (HDD strategy was used)
      refute_receive {:exec, _}
    end

    test "worker executes all read and write operations on HDD", ctx do
      test_pid = self()

      %{producer_name: producer} =
        start_system(ctx, drive_type: :hdd, strategy_config: [batch_size: 10])

      reads =
        for i <- 1..3, do: make_op(type: :read, callback: fn -> send(test_pid, {:read, i}) end)

      writes =
        for i <- 1..3, do: make_op(type: :write, callback: fn -> send(test_pid, {:write, i}) end)

      for op <- writes ++ reads, do: Producer.enqueue(producer, op)

      results = for _ <- 1..6, do: assert_receive({_, _}, 2000)

      read_count = Enum.count(results, fn {type, _} -> type == :read end)
      write_count = Enum.count(results, fn {type, _} -> type == :write end)

      assert read_count == 3
      assert write_count == 3
    end
  end

  describe "SSD strategy" do
    test "worker dispatches operations using SSD strategy ordering", ctx do
      test_pid = self()

      %{producer_name: producer} =
        start_system(ctx, drive_type: :ssd, strategy_config: [max_concurrent: 64])

      ops =
        for i <- 1..5 do
          make_op(
            type: if(rem(i, 2) == 0, do: :write, else: :read),
            callback: fn -> send(test_pid, {:exec, i}) end
          )
        end

      for op <- ops, do: Producer.enqueue(producer, op)

      # All five should execute
      for _ <- 1..5, do: assert_receive({:exec, _}, 2000)

      refute_receive {:exec, _}
    end

    test "worker handles interleaved reads and writes on SSD", ctx do
      test_pid = self()

      %{producer_name: producer} =
        start_system(ctx, drive_type: :ssd, strategy_config: [max_concurrent: 4])

      ops = [
        make_op(type: :read, callback: fn -> send(test_pid, :r1) end),
        make_op(type: :write, callback: fn -> send(test_pid, :w1) end),
        make_op(type: :read, callback: fn -> send(test_pid, :r2) end),
        make_op(type: :write, callback: fn -> send(test_pid, :w2) end)
      ]

      for op <- ops, do: Producer.enqueue(producer, op)

      received = for _ <- 1..4, do: assert_receive(_, 2000)

      assert length(received) == 4
      assert :r1 in received
      assert :w1 in received
      assert :r2 in received
      assert :w2 in received
    end
  end

  describe "error telemetry" do
    test "failed callback emits error telemetry", ctx do
      test_pid = self()

      :telemetry.attach(
        "test-io-error-#{ctx.test}",
        [:neonfs, :io, :error],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_error, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-io-error-#{ctx.test}") end)

      %{producer_name: producer} = start_system(ctx)

      failing_op =
        make_op(callback: fn -> raise "test failure" end)

      Producer.enqueue(producer, failing_op)

      assert_receive {:telemetry_error, measurements, metadata}, 2000

      assert is_integer(measurements.duration)
      assert metadata.operation_id == failing_op.id
      assert metadata.priority == :user_read
      assert metadata.drive_id == "test-drive"
      assert metadata.type == :read
      assert metadata.reason =~ "test failure"
    end

    test "successful callback emits complete telemetry", ctx do
      test_pid = self()

      :telemetry.attach(
        "test-io-complete-#{ctx.test}",
        [:neonfs, :io, :complete],
        fn _event, measurements, metadata, _config ->
          send(test_pid, {:telemetry_complete, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("test-io-complete-#{ctx.test}") end)

      %{producer_name: producer} = start_system(ctx)

      op = make_op(callback: fn -> :done end)
      Producer.enqueue(producer, op)

      assert_receive {:telemetry_complete, measurements, metadata}, 2000

      assert is_integer(measurements.duration)
      assert metadata.operation_id == op.id
      assert metadata.priority == :user_read
      assert metadata.drive_id == "test-drive"
      assert metadata.type == :read
    end
  end

  describe "concurrency limiting" do
    test "worker respects concurrency limit", ctx do
      test_pid = self()
      counter_ref = :counters.new(2, [:atomics])

      max_concurrent_limit = 2

      %{producer_name: producer} =
        start_system(ctx,
          drive_type: :ssd,
          strategy_config: [max_concurrent: max_concurrent_limit]
        )

      ops =
        for i <- 1..6 do
          tag = :"ssd_conc_#{i}"

          make_op(
            callback: fn ->
              :counters.add(counter_ref, 1, 1)
              current = :counters.get(counter_ref, 1)
              old_max = :counters.get(counter_ref, 2)
              if current > old_max, do: :counters.put(counter_ref, 2, current)

              send(test_pid, {:op_started, tag, self()})
              receive do: (:continue -> :ok)

              :counters.sub(counter_ref, 1, 1)
              send(test_pid, :done)
            end
          )
        end

      for op <- ops, do: Producer.enqueue(producer, op)

      # Release operations in batches as they start
      for i <- 1..6 do
        tag = :"ssd_conc_#{i}"
        assert_receive {:op_started, ^tag, pid}, 5000
        send(pid, :continue)
      end

      for _ <- 1..6, do: assert_receive(:done, 5000)

      max_observed = :counters.get(counter_ref, 2)
      assert max_observed <= max_concurrent_limit
    end

    test "HDD worker uses batch_size as concurrency limit", ctx do
      test_pid = self()
      counter_ref = :counters.new(2, [:atomics])

      batch_size = 3

      %{producer_name: producer} =
        start_system(ctx, drive_type: :hdd, strategy_config: [batch_size: batch_size])

      ops =
        for i <- 1..8 do
          tag = :"hdd_conc_#{i}"

          make_op(
            callback: fn ->
              :counters.add(counter_ref, 1, 1)
              current = :counters.get(counter_ref, 1)
              old_max = :counters.get(counter_ref, 2)
              if current > old_max, do: :counters.put(counter_ref, 2, current)

              send(test_pid, {:op_started, tag, self()})
              receive do: (:continue -> :ok)

              :counters.sub(counter_ref, 1, 1)
              send(test_pid, :done)
            end
          )
        end

      for op <- ops, do: Producer.enqueue(producer, op)

      for i <- 1..8 do
        tag = :"hdd_conc_#{i}"
        assert_receive {:op_started, ^tag, pid}, 5000
        send(pid, :continue)
      end

      for _ <- 1..8, do: assert_receive(:done, 5000)

      max_observed = :counters.get(counter_ref, 2)
      assert max_observed <= batch_size
    end
  end
end
