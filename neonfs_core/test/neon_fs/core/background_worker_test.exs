defmodule NeonFS.Core.BackgroundWorkerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.BackgroundWorker

  @start_event [:neonfs, :background_worker, :start]
  @complete_event [:neonfs, :background_worker, :complete]
  @fail_event [:neonfs, :background_worker, :fail]

  setup do
    ensure_task_supervisor()
    ensure_background_worker()
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
    # Always restart fresh to avoid test pollution from reconfigure tests
    case GenServer.whereis(NeonFS.Core.BackgroundWorker) do
      nil -> :ok
      _pid -> stop_supervised(NeonFS.Core.BackgroundWorker)
    end

    # Use max_concurrent: 2 for predictable concurrency tests
    start_supervised!(
      {NeonFS.Core.BackgroundWorker,
       max_concurrent: 2, max_per_minute: 50, default_resource_limit: 1},
      restart: :temporary
    )
  end

  defp wait_for_starts(ref, count, timeout \\ 5_000) do
    for _ <- 1..count do
      assert_receive {@start_event, ^ref, _, _}, timeout
    end
  end

  defp wait_for_completions(ref, count, timeout \\ 5_000) do
    for _ <- 1..count do
      assert_receive {@complete_event, ^ref, _, _}, timeout
    end
  end

  defp blocking_task(tag) do
    test_pid = self()

    fn ->
      send(test_pid, {:task_started, tag, self()})
      receive do: (:continue -> :ok)
    end
  end

  defp counting_blocking_task(counter_ref, max_seen_ref, tag) do
    test_pid = self()

    fn ->
      :counters.add(counter_ref, 1, 1)
      current = :counters.get(counter_ref, 1)
      if current > :counters.get(max_seen_ref, 1), do: :counters.put(max_seen_ref, 1, current)
      send(test_pid, {:task_started, tag, self()})
      receive do: (:continue -> :ok)
      :counters.sub(counter_ref, 1, 1)
    end
  end

  defp wait_for_task_start(tag, timeout \\ 1_000) do
    assert_receive {:task_started, ^tag, pid}, timeout
    pid
  end

  defp release_task(pid), do: send(pid, :continue)

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
      ref = :telemetry_test.attach_event_handlers(self(), [@complete_event])

      {:ok, _id} =
        BackgroundWorker.submit(fn -> :ok end, priority: :high, label: "test_high")

      {:ok, _id} =
        BackgroundWorker.submit(fn -> :ok end, priority: :low, label: "test_low")

      wait_for_completions(ref, 2)
    end

    test "submits with resource declarations" do
      ref = :telemetry_test.attach_event_handlers(self(), [@complete_event])

      {:ok, _id} =
        BackgroundWorker.submit(fn -> :ok end,
          priority: :normal,
          resources: [{:drive, "nvme0"}]
        )

      wait_for_completions(ref, 1)
    end

    test "starts work immediately on submit (demand-driven)" do
      ref = :telemetry_test.attach_event_handlers(self(), [@start_event])

      {:ok, id} = BackgroundWorker.submit(blocking_task(:demand))

      # Work should be running immediately, not waiting for a timer
      wait_for_starts(ref, 1)
      assert BackgroundWorker.status(id) == :running

      pid = wait_for_task_start(:demand)
      release_task(pid)
    end
  end

  describe "status/0" do
    test "returns status map with expected keys" do
      status = BackgroundWorker.status()
      assert Map.has_key?(status, :max_concurrent)
      assert Map.has_key?(status, :max_per_minute)
      assert Map.has_key?(status, :drive_concurrency)
      assert Map.has_key?(status, :queued)
      assert Map.has_key?(status, :running)
      assert Map.has_key?(status, :completed_total)
      assert Map.has_key?(status, :by_priority)
      assert Map.has_key?(status.by_priority, :high)
      assert Map.has_key?(status.by_priority, :normal)
      assert Map.has_key?(status.by_priority, :low)
    end

    test "returns configuration values matching init options" do
      status = BackgroundWorker.status()
      assert status.max_concurrent == 2
      assert status.max_per_minute == 50
      assert status.drive_concurrency == 1
    end
  end

  describe "status/1" do
    test "returns :completed for finished work" do
      ref = :telemetry_test.attach_event_handlers(self(), [@complete_event])

      {:ok, id} = BackgroundWorker.submit(fn -> :done end)
      wait_for_completions(ref, 1)

      assert BackgroundWorker.status(id) == :completed
    end

    test "returns {:error, :not_found} for unknown id" do
      assert BackgroundWorker.status("nonexistent") == {:error, :not_found}
    end

    test "returns :running for in-progress work" do
      ref = :telemetry_test.attach_event_handlers(self(), [@start_event])

      {:ok, id} = BackgroundWorker.submit(blocking_task(:status))
      wait_for_starts(ref, 1)

      assert BackgroundWorker.status(id) == :running

      pid = wait_for_task_start(:status)
      release_task(pid)
    end
  end

  describe "cancel/1" do
    test "cancels queued work" do
      ref = :telemetry_test.attach_event_handlers(self(), [@start_event])

      # Fill concurrency slots
      {:ok, _} = BackgroundWorker.submit(blocking_task(:slot1))
      {:ok, _} = BackgroundWorker.submit(blocking_task(:slot2))
      wait_for_starts(ref, 2)
      slot1_pid = wait_for_task_start(:slot1)
      slot2_pid = wait_for_task_start(:slot2)

      # This should be queued since slots are full
      {:ok, id} = BackgroundWorker.submit(fn -> :ok end)

      assert :ok = BackgroundWorker.cancel(id)
      assert BackgroundWorker.status(id) == :cancelled

      release_task(slot1_pid)
      release_task(slot2_pid)
    end

    test "marks running work as cancelled" do
      ref = :telemetry_test.attach_event_handlers(self(), [@start_event])

      {:ok, id} = BackgroundWorker.submit(blocking_task(:cancel))
      wait_for_starts(ref, 1)
      _pid = wait_for_task_start(:cancel)

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

      for i <- 1..5 do
        BackgroundWorker.submit(counting_blocking_task(counter, max_seen, :"conc_#{i}"))
      end

      # Wait for all tasks to start (max 2 at a time due to max_concurrent: 2)
      # First batch: 2 tasks start
      pid1 = wait_for_task_start(:conc_1)
      pid2 = wait_for_task_start(:conc_2)

      # Release first batch to let remaining tasks start
      release_task(pid1)
      release_task(pid2)

      pid3 = wait_for_task_start(:conc_3)
      pid4 = wait_for_task_start(:conc_4)
      release_task(pid3)
      release_task(pid4)

      pid5 = wait_for_task_start(:conc_5)
      release_task(pid5)

      # max_concurrent is 2 in test setup
      assert :counters.get(max_seen, 1) <= 2
    end
  end

  describe "per-resource concurrency" do
    test "limits concurrent work on the same resource" do
      counter = :counters.new(1, [])
      max_seen = :counters.new(1, [])

      # Submit 3 items all targeting the same drive
      for i <- 1..3 do
        BackgroundWorker.submit(
          counting_blocking_task(counter, max_seen, :"res_#{i}"),
          resources: [{:drive, "nvme0"}]
        )
      end

      # Only 1 should start at a time (default_resource_limit: 1)
      pid1 = wait_for_task_start(:res_1)
      release_task(pid1)

      pid2 = wait_for_task_start(:res_2)
      release_task(pid2)

      pid3 = wait_for_task_start(:res_3)
      release_task(pid3)

      # default_resource_limit is 1, so only 1 at a time on the same drive
      assert :counters.get(max_seen, 1) <= 1
    end

    test "allows concurrent work on different resources" do
      counter = :counters.new(1, [])
      max_seen = :counters.new(1, [])

      # Submit items targeting different drives
      for {drive, i} <- Enum.with_index(["nvme0", "nvme1"]) do
        BackgroundWorker.submit(
          counting_blocking_task(counter, max_seen, :"diff_#{i}"),
          resources: [{:drive, drive}]
        )
      end

      # Both should start concurrently since they're on different resources
      pid0 = wait_for_task_start(:diff_0)
      pid1 = wait_for_task_start(:diff_1)

      # Different resources can run concurrently
      assert :counters.get(max_seen, 1) >= 2

      release_task(pid0)
      release_task(pid1)
    end
  end

  describe "priority ordering" do
    test "high priority tasks dequeued before low priority" do
      ref = :telemetry_test.attach_event_handlers(self(), [@start_event, @complete_event])

      # Fill slots with blocking work
      {:ok, _} = BackgroundWorker.submit(blocking_task(:pri1))
      {:ok, _} = BackgroundWorker.submit(blocking_task(:pri2))
      wait_for_starts(ref, 2)
      pri1_pid = wait_for_task_start(:pri1)
      pri2_pid = wait_for_task_start(:pri2)

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

      # Release blocking tasks to let priority tasks run
      release_task(pri1_pid)
      release_task(pri2_pid)

      # Wait for all work to complete (2 blocking + 2 priority)
      wait_for_completions(ref, 4)

      # Collect execution order
      results = collect_messages()

      if length(results) >= 2 do
        assert hd(results) == :high
      end
    end
  end

  describe "crash isolation" do
    test "failed tasks don't crash the worker" do
      ref = :telemetry_test.attach_event_handlers(self(), [@fail_event, @complete_event])

      {:ok, crash_id} =
        BackgroundWorker.submit(fn -> raise "intentional crash" end, label: "crasher")

      assert_receive {@fail_event, ^ref, _, %{work_id: ^crash_id}}, 5_000

      # Worker should still be running
      assert Process.alive?(GenServer.whereis(BackgroundWorker))

      # Failed task should be marked as failed
      assert BackgroundWorker.status(crash_id) == :failed

      # Can still submit new work
      {:ok, new_id} = BackgroundWorker.submit(fn -> :ok end)
      assert_receive {@complete_event, ^ref, _, %{work_id: ^new_id}}, 5_000

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

      assert_receive {[:neonfs, :background_worker, :complete], ^ref, %{},
                      %{work_id: ^work_id, label: "completer"}},
                     5_000
    end

    test "emits fail event on crash" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :background_worker, :fail]
        ])

      {:ok, work_id} = BackgroundWorker.submit(fn -> raise "boom" end, label: "boomer")

      assert_receive {[:neonfs, :background_worker, :fail], ^ref, %{},
                      %{work_id: ^work_id, label: "boomer", reason: _}},
                     5_000
    end

    test "emits cancel event" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          @start_event,
          [:neonfs, :background_worker, :cancel]
        ])

      # Fill slots
      {:ok, _} = BackgroundWorker.submit(blocking_task(:tel1))
      {:ok, _} = BackgroundWorker.submit(blocking_task(:tel2))
      wait_for_starts(ref, 2)
      tel1_pid = wait_for_task_start(:tel1)
      tel2_pid = wait_for_task_start(:tel2)

      {:ok, work_id} = BackgroundWorker.submit(fn -> :ok end)
      BackgroundWorker.cancel(work_id)

      assert_receive {[:neonfs, :background_worker, :cancel], ^ref, %{}, %{work_id: ^work_id}}

      release_task(tel1_pid)
      release_task(tel2_pid)
    end
  end

  describe "reconfigure/1" do
    test "updates max_concurrent and status reflects new value" do
      assert :ok = BackgroundWorker.reconfigure(max_concurrent: 8)
      status = BackgroundWorker.status()
      assert status.max_concurrent == 8
    end

    test "updates max_per_minute and status reflects new value" do
      assert :ok = BackgroundWorker.reconfigure(max_per_minute: 100)
      status = BackgroundWorker.status()
      assert status.max_per_minute == 100
    end

    test "updates drive_concurrency and status reflects new value" do
      assert :ok = BackgroundWorker.reconfigure(drive_concurrency: 4)
      status = BackgroundWorker.status()
      assert status.drive_concurrency == 4
    end

    test "updates multiple settings at once" do
      assert :ok =
               BackgroundWorker.reconfigure(
                 max_concurrent: 16,
                 max_per_minute: 200,
                 drive_concurrency: 2
               )

      status = BackgroundWorker.status()
      assert status.max_concurrent == 16
      assert status.max_per_minute == 200
      assert status.drive_concurrency == 2
    end

    test "silently ignores unsupported keys" do
      assert :ok = BackgroundWorker.reconfigure(max_concurrent: 6, bogus_key: 999)
      status = BackgroundWorker.status()
      assert status.max_concurrent == 6
    end

    test "updates application env for persistence across restarts" do
      BackgroundWorker.reconfigure(max_concurrent: 12)
      assert Application.get_env(:neonfs_core, :worker_max_concurrent) == 12
    end

    test "new max_concurrent is respected for subsequent work" do
      # Set max_concurrent to 1
      BackgroundWorker.reconfigure(max_concurrent: 1)

      counter = :counters.new(1, [])
      max_seen = :counters.new(1, [])

      for i <- 1..3 do
        BackgroundWorker.submit(counting_blocking_task(counter, max_seen, :"reconf_#{i}"))
      end

      # Only 1 should run at a time (max_concurrent: 1)
      pid1 = wait_for_task_start(:reconf_1)
      release_task(pid1)

      pid2 = wait_for_task_start(:reconf_2)
      release_task(pid2)

      pid3 = wait_for_task_start(:reconf_3)
      release_task(pid3)

      assert :counters.get(max_seen, 1) <= 1
    end

    test "emits reconfigured telemetry event" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :background_worker, :reconfigured]
        ])

      BackgroundWorker.reconfigure(max_concurrent: 10)

      assert_receive {[:neonfs, :background_worker, :reconfigured], ^ref, %{},
                      %{old: %{max_concurrent: _}, new: %{max_concurrent: 10}}}
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
