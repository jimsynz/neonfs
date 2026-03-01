defmodule NeonFS.IO.SchedulerSyncTest do
  @moduledoc """
  Tests for submit_sync/2 and submit_async/1 with a full I/O pipeline.
  """
  use ExUnit.Case, async: false

  alias NeonFS.IO.{Operation, Producer, Scheduler, WorkerSupervisor}

  defmodule MockVolumeRegistry do
    def get(_volume_id), do: {:ok, %{io_weight: 100}}
  end

  setup do
    # Start the worker registry
    case Registry.start_link(keys: :unique, name: NeonFS.IO.WorkerRegistry) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end

    suffix = System.unique_integer([:positive])
    producer_name = :"sync_prod_#{suffix}"

    start_supervised!(
      {Producer, name: producer_name, volume_registry_mod: MockVolumeRegistry},
      id: producer_name
    )

    # Use the default module name so submit_sync/submit_async can find it
    start_supervised!(
      {Scheduler, name: Scheduler, producer: producer_name},
      id: Scheduler
    )

    sup_name = :"sync_wsup_#{suffix}"

    start_supervised!(
      {WorkerSupervisor, name: sup_name},
      id: sup_name
    )

    # Start a drive worker for "nvme0"
    WorkerSupervisor.start_worker(
      drive_id: "nvme0",
      drive_type: :ssd,
      producer: producer_name,
      supervisor: sup_name
    )

    %{producer: producer_name, worker_sup: sup_name}
  end

  describe "submit_sync/2 with pipeline" do
    test "returns the callback result synchronously" do
      op =
        Operation.new(
          priority: :user_read,
          volume_id: "vol-1",
          drive_id: "nvme0",
          type: :read,
          callback: fn -> {:ok, "data_from_disk"} end
        )

      assert {:ok, "data_from_disk"} = Scheduler.submit_sync(op)
    end

    test "returns error results from callback" do
      op =
        Operation.new(
          priority: :user_write,
          volume_id: "vol-1",
          drive_id: "nvme0",
          type: :write,
          callback: fn -> {:error, :disk_full} end
        )

      assert {:error, :disk_full} = Scheduler.submit_sync(op)
    end

    test "handles callback exceptions by sending error to caller" do
      op =
        Operation.new(
          priority: :user_read,
          volume_id: "vol-1",
          drive_id: "nvme0",
          type: :read,
          callback: fn -> raise "boom" end
        )

      assert {:error, {:callback_exception, "boom"}} = Scheduler.submit_sync(op)
    end

    test "respects timeout option" do
      op =
        Operation.new(
          priority: :user_read,
          volume_id: "vol-1",
          drive_id: "nvme0",
          type: :read,
          callback: fn ->
            receive do
            after
              :infinity -> :ok
            end
          end
        )

      assert {:error, :io_scheduler_timeout} = Scheduler.submit_sync(op, timeout: 100)
    end
  end

  describe "submit_async/1 with pipeline" do
    test "returns :ok immediately" do
      test_pid = self()

      op =
        Operation.new(
          priority: :replication,
          volume_id: "vol-1",
          drive_id: "nvme0",
          type: :write,
          callback: fn -> send(test_pid, :async_complete) end
        )

      assert :ok = Scheduler.submit_async(op)
      assert_receive :async_complete, 2000
    end
  end

  describe "scheduler_available?/0 with pipeline" do
    test "returns true when scheduler is registered" do
      assert Scheduler.scheduler_available?()
    end
  end
end
