defmodule NeonFS.IO.SchedulerTest do
  use ExUnit.Case, async: true

  alias NeonFS.IO.{Operation, Priority, Producer, Scheduler}

  # Mock volume registry — returns default io_weight for all volumes
  defmodule MockVolumeRegistry do
    def get(_volume_id), do: {:ok, %{io_weight: 100}}
  end

  setup do
    suffix = System.unique_integer([:positive])
    producer_name = :"producer_#{suffix}"

    producer =
      start_supervised!(
        {Producer, name: producer_name, volume_registry_mod: MockVolumeRegistry},
        id: producer_name
      )

    scheduler =
      start_supervised!(
        {Scheduler, name: :"scheduler_#{suffix}", producer: producer_name},
        id: :"scheduler_#{suffix}"
      )

    %{scheduler: scheduler, producer: producer}
  end

  defp make_op(overrides \\ []) do
    defaults = [
      priority: :user_read,
      volume_id: "vol-1",
      drive_id: "nvme0",
      type: :read,
      callback: fn -> :ok end
    ]

    Operation.new(Keyword.merge(defaults, overrides))
  end

  describe "submit/1" do
    test "accepts a valid operation", %{scheduler: scheduler} do
      op = make_op()
      assert {:ok, id} = GenServer.call(scheduler, {:submit, op, []})
      assert id == op.id
    end

    test "rejects an operation with invalid fields" do
      op = %Operation{
        id: "test",
        priority: :bogus,
        volume_id: "vol-1",
        drive_id: "nvme0",
        type: :read,
        callback: fn -> :ok end,
        submitted_at: 0
      }

      assert {:error, _reason} = Scheduler.submit(op)
    end
  end

  describe "cancel/1" do
    test "cancels a submitted operation", %{scheduler: scheduler} do
      op = make_op()
      {:ok, id} = GenServer.call(scheduler, {:submit, op, []})
      assert :ok = GenServer.call(scheduler, {:cancel, id})
    end

    test "returns :ok for unknown operation (cancel is idempotent)", %{scheduler: scheduler} do
      assert :ok = GenServer.call(scheduler, {:cancel, "nonexistent"})
    end

    test "removes the operation from queue depths", %{scheduler: scheduler} do
      op = make_op()
      {:ok, _id} = GenServer.call(scheduler, {:submit, op, []})

      status = GenServer.call(scheduler, :status)
      assert status.queue_depths[:user_read] == 1

      GenServer.call(scheduler, {:cancel, op.id})

      # Synchronise with the producer to ensure the cancel cast is processed
      status = GenServer.call(scheduler, :status)
      assert status.queue_depths[:user_read] == 0
    end
  end

  describe "status/0" do
    test "returns zero depths when empty", %{scheduler: scheduler} do
      status = GenServer.call(scheduler, :status)

      for priority <- Priority.all() do
        assert status.queue_depths[priority] == 0
      end
    end

    test "tracks queue depths by priority", %{scheduler: scheduler} do
      for _ <- 1..3 do
        op = make_op(priority: :user_read)
        GenServer.call(scheduler, {:submit, op, []})
      end

      for _ <- 1..2 do
        op = make_op(priority: :scrub)
        GenServer.call(scheduler, {:submit, op, []})
      end

      status = GenServer.call(scheduler, :status)
      assert status.queue_depths[:user_read] == 3
      assert status.queue_depths[:scrub] == 2
      assert status.queue_depths[:replication] == 0
    end
  end

  describe "submit_sync/2 fallback" do
    test "executes callback directly when scheduler is not running" do
      # Ensure the default-named scheduler is NOT running
      assert Process.whereis(Scheduler) == nil

      op = make_op(callback: fn -> {:ok, :direct_result} end)
      assert {:ok, :direct_result} = Scheduler.submit_sync(op)
    end

    test "returns callback errors directly in fallback mode" do
      assert Process.whereis(Scheduler) == nil

      op = make_op(callback: fn -> {:error, :some_reason} end)
      assert {:error, :some_reason} = Scheduler.submit_sync(op)
    end
  end

  describe "submit_async/1 fallback" do
    test "spawns callback when scheduler is not running" do
      assert Process.whereis(Scheduler) == nil

      test_pid = self()
      op = make_op(callback: fn -> send(test_pid, :async_ran) end)
      assert :ok = Scheduler.submit_async(op)
      assert_receive :async_ran, 1000
    end
  end

  describe "scheduler_available?/0" do
    test "returns false when scheduler is not registered" do
      refute Scheduler.scheduler_available?()
    end
  end
end
