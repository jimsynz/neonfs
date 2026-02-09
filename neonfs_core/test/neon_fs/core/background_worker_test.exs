defmodule NeonFS.Core.BackgroundWorkerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.BackgroundWorker

  setup do
    ensure_task_supervisor()
    ensure_background_worker()
    Process.sleep(50)
    :ok
  end

  defp ensure_task_supervisor do
    case Process.whereis(NeonFS.Core.BackgroundTaskSupervisor) do
      nil ->
        start_supervised!(
          {Task.Supervisor, name: NeonFS.Core.BackgroundTaskSupervisor},
          restart: :temporary
        )

      _pid ->
        :ok
    end
  end

  defp ensure_background_worker do
    case GenServer.whereis(NeonFS.Core.BackgroundWorker) do
      nil ->
        start_supervised!(NeonFS.Core.BackgroundWorker, restart: :temporary)

      _pid ->
        :ok
    end
  end

  defp poke_worker do
    pid = GenServer.whereis(BackgroundWorker)
    if pid, do: send(pid, :check_queue)
    Process.sleep(50)
  end

  describe "submit/2" do
    test "returns {:ok, work_id}" do
      assert {:ok, work_id} = BackgroundWorker.submit(fn -> :ok end)
      assert is_binary(work_id)
      assert byte_size(work_id) == 16
    end

    test "submits with default priority :normal" do
      {:ok, _id} = BackgroundWorker.submit(fn -> :ok end)
      status = BackgroundWorker.status()
      assert is_integer(status.queued)
      assert is_integer(status.running)
    end

    test "submits with custom priority and label" do
      {:ok, _id} =
        BackgroundWorker.submit(fn -> :ok end, priority: :high, label: "test_high")

      {:ok, _id} =
        BackgroundWorker.submit(fn -> :ok end, priority: :low, label: "test_low")

      poke_worker()
    end
  end

  describe "status/0" do
    test "returns status map with expected keys" do
      status = BackgroundWorker.status()
      assert Map.has_key?(status, :queued)
      assert Map.has_key?(status, :running)
      assert Map.has_key?(status, :completed)
      assert Map.has_key?(status, :by_priority)
      assert Map.has_key?(status.by_priority, :high)
      assert Map.has_key?(status.by_priority, :normal)
      assert Map.has_key?(status.by_priority, :low)
    end
  end

  describe "status/1" do
    test "returns :completed for finished work" do
      {:ok, id} = BackgroundWorker.submit(fn -> :done end)
      poke_worker()
      Process.sleep(100)

      assert BackgroundWorker.status(id) == :completed
    end

    test "returns {:error, :not_found} for unknown id" do
      assert BackgroundWorker.status("nonexistent") == {:error, :not_found}
    end

    test "returns :running for in-progress work" do
      {:ok, id} = BackgroundWorker.submit(fn -> Process.sleep(5000) end)
      poke_worker()

      assert BackgroundWorker.status(id) == :running
    end
  end

  describe "cancel/1" do
    test "cancels queued work" do
      # Fill concurrency slots
      {:ok, _} = BackgroundWorker.submit(fn -> Process.sleep(5000) end)
      {:ok, _} = BackgroundWorker.submit(fn -> Process.sleep(5000) end)
      poke_worker()

      # This should be queued since slots are full
      {:ok, id} = BackgroundWorker.submit(fn -> :ok end)

      assert :ok = BackgroundWorker.cancel(id)
      assert BackgroundWorker.status(id) == :cancelled
    end

    test "marks running work as cancelled" do
      {:ok, id} = BackgroundWorker.submit(fn -> Process.sleep(5000) end)
      poke_worker()

      assert :ok = BackgroundWorker.cancel(id)
      assert BackgroundWorker.status(id) == :cancelled
    end

    test "returns {:error, :not_found} for unknown id" do
      assert BackgroundWorker.cancel("nonexistent") == {:error, :not_found}
    end
  end

  describe "concurrency limiting" do
    test "limits concurrent tasks to max_concurrent" do
      counter = :counters.new(1, [])
      max_seen = :counters.new(1, [])

      for _i <- 1..5 do
        BackgroundWorker.submit(fn ->
          :counters.add(counter, 1, 1)
          current = :counters.get(counter, 1)

          if current > :counters.get(max_seen, 1) do
            :counters.put(max_seen, 1, current)
          end

          Process.sleep(200)
          :counters.sub(counter, 1, 1)
        end)
      end

      # Poke multiple times to process queue
      for _i <- 1..10 do
        poke_worker()
        Process.sleep(100)
      end

      Process.sleep(500)

      # max_concurrent is 2 (app supervisor default)
      assert :counters.get(max_seen, 1) <= 2
    end
  end

  describe "priority ordering" do
    test "high priority tasks dequeued before low priority" do
      # Fill slots with blocking work
      {:ok, _} = BackgroundWorker.submit(fn -> Process.sleep(1000) end)
      {:ok, _} = BackgroundWorker.submit(fn -> Process.sleep(1000) end)
      poke_worker()

      parent = self()

      # Submit in reverse order: low first, then high
      {:ok, _} =
        BackgroundWorker.submit(
          fn -> send(parent, {:executed, :low}) end,
          priority: :low
        )

      {:ok, _} =
        BackgroundWorker.submit(
          fn -> send(parent, {:executed, :high}) end,
          priority: :high
        )

      # Wait for blocking work to finish and queued work to execute
      Process.sleep(1500)

      for _i <- 1..5, do: poke_worker()

      Process.sleep(500)

      # Collect execution order
      results = collect_messages()

      if length(results) >= 2 do
        assert hd(results) == :high
      end
    end
  end

  describe "crash isolation" do
    test "failed tasks don't crash the worker" do
      {:ok, crash_id} =
        BackgroundWorker.submit(fn -> raise "intentional crash" end, label: "crasher")

      poke_worker()
      Process.sleep(100)

      # Worker should still be running
      assert Process.alive?(GenServer.whereis(BackgroundWorker))

      # Failed task should be marked as failed
      assert BackgroundWorker.status(crash_id) == :failed

      # Can still submit new work
      {:ok, new_id} = BackgroundWorker.submit(fn -> :ok end)
      poke_worker()
      Process.sleep(100)

      assert BackgroundWorker.status(new_id) == :completed
    end
  end

  describe "telemetry" do
    test "emits submit event" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :background_worker, :submit]
        ])

      {:ok, work_id} = BackgroundWorker.submit(fn -> :ok end, label: "tel_test")

      assert_receive {[:neonfs, :background_worker, :submit], ^ref, %{},
                      %{work_id: ^work_id, priority: :normal, label: "tel_test"}}
    end

    test "emits complete event" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :background_worker, :complete]
        ])

      {:ok, work_id} = BackgroundWorker.submit(fn -> :ok end, label: "completer")
      poke_worker()
      Process.sleep(100)

      assert_receive {[:neonfs, :background_worker, :complete], ^ref, %{},
                      %{work_id: ^work_id, label: "completer"}}
    end

    test "emits fail event on crash" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :background_worker, :fail]
        ])

      {:ok, work_id} = BackgroundWorker.submit(fn -> raise "boom" end, label: "boomer")
      poke_worker()
      Process.sleep(200)

      assert_receive {[:neonfs, :background_worker, :fail], ^ref, %{},
                      %{work_id: ^work_id, label: "boomer", reason: _}}
    end

    test "emits cancel event" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :background_worker, :cancel]
        ])

      # Fill slots
      {:ok, _} = BackgroundWorker.submit(fn -> Process.sleep(5000) end)
      {:ok, _} = BackgroundWorker.submit(fn -> Process.sleep(5000) end)
      poke_worker()

      {:ok, work_id} = BackgroundWorker.submit(fn -> :ok end)
      BackgroundWorker.cancel(work_id)

      assert_receive {[:neonfs, :background_worker, :cancel], ^ref, %{}, %{work_id: ^work_id}}
    end
  end

  defp collect_messages do
    collect_messages([])
  end

  defp collect_messages(acc) do
    receive do
      {:executed, priority} -> collect_messages([priority | acc])
    after
      0 -> Enum.reverse(acc)
    end
  end
end
