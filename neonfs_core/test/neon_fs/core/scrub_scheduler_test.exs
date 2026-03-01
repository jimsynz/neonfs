defmodule NeonFS.Core.ScrubSchedulerTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Job
  alias NeonFS.Core.Job.Runners.Scrub
  alias NeonFS.Core.ScrubScheduler

  defmodule MockJobTracker do
    @moduledoc false
    use Agent

    def start_link(opts \\ []) do
      running_jobs = Keyword.get(opts, :running_jobs, [])
      completed_jobs = Keyword.get(opts, :completed_jobs, [])

      Agent.start_link(
        fn -> %{running_jobs: running_jobs, completed_jobs: completed_jobs, created: []} end,
        name: Keyword.get(opts, :name, __MODULE__)
      )
    end

    def list(filters) do
      Agent.get(__MODULE__, fn state ->
        status = Keyword.get(filters, :status)

        jobs =
          case status do
            :running -> state.running_jobs
            :completed -> state.completed_jobs
            _ -> state.running_jobs ++ state.completed_jobs
          end

        jobs
        |> filter_by_type(Keyword.get(filters, :type))
      end)
    end

    def create(type, params) do
      job = %Job{
        id: "mock-scrub-#{System.unique_integer([:positive])}",
        type: type,
        params: params,
        status: :running,
        node: Node.self(),
        progress: %{total: 0, completed: 0, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      Agent.update(__MODULE__, fn state ->
        %{state | created: [job | state.created]}
      end)

      {:ok, job}
    end

    def get_created do
      Agent.get(__MODULE__, fn state -> state.created end)
    end

    def set_running_jobs(jobs) do
      Agent.update(__MODULE__, fn state -> %{state | running_jobs: jobs} end)
    end

    defp filter_by_type(jobs, nil), do: jobs
    defp filter_by_type(jobs, type), do: Enum.filter(jobs, &(&1.type == type))
  end

  defmodule MockVolumeRegistry do
    @moduledoc false
    use Agent

    def start_link(opts \\ []) do
      volumes = Keyword.get(opts, :volumes, [])

      Agent.start_link(
        fn -> %{volumes: volumes} end,
        name: Keyword.get(opts, :name, __MODULE__)
      )
    end

    def list do
      Agent.get(__MODULE__, fn state -> state.volumes end)
    end

    def set_volumes(volumes) do
      Agent.update(__MODULE__, fn state -> %{state | volumes: volumes} end)
    end
  end

  defp make_volume(id, scrub_interval \\ 2_592_000) do
    %{
      id: id,
      name: "vol-#{id}",
      verification: %{on_read: :never, sampling_rate: nil, scrub_interval: scrub_interval}
    }
  end

  # Receives tick events until the job count exceeds `initial_count`, or times out.
  defp wait_for_new_job(ref, initial_count, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_for_new_job(ref, initial_count, deadline)
  end

  defp do_wait_for_new_job(ref, initial_count, deadline) do
    remaining = deadline - System.monotonic_time(:millisecond)

    if remaining <= 0 do
      flunk("Timed out waiting for new scrub job")
    end

    assert_receive {[:neonfs, :scrub_scheduler, :tick], ^ref, %{}, %{}}, remaining

    if length(MockJobTracker.get_created()) > initial_count do
      :ok
    else
      do_wait_for_new_job(ref, initial_count, deadline)
    end
  end

  setup do
    start_supervised!({MockJobTracker, name: MockJobTracker})
    start_supervised!({MockVolumeRegistry, name: MockVolumeRegistry})
    :ok
  end

  describe "start_link/1 and periodic ticks" do
    test "creates a scrub job when interval has elapsed" do
      volume = make_volume("vol-1")
      MockVolumeRegistry.set_volumes([volume])

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :scrub_scheduler, :tick],
          [:neonfs, :scrub_scheduler, :triggered]
        ])

      pid =
        start_supervised!(
          {ScrubScheduler,
           name: :"scrub_sched_#{System.unique_integer([:positive])}",
           check_interval_ms: 50,
           job_tracker_mod: MockJobTracker,
           volume_registry_mod: MockVolumeRegistry}
        )

      assert is_pid(pid)

      # Wait for the first tick to complete
      assert_receive {[:neonfs, :scrub_scheduler, :tick], ^ref, %{}, %{}}, 1_000

      created = MockJobTracker.get_created()
      assert created != []

      first = List.last(created)
      assert first.type == Scrub
      assert first.params == %{volume_id: "vol-1"}

      assert_received {[:neonfs, :scrub_scheduler, :triggered], ^ref, %{},
                       %{job_id: _, volume_id: "vol-1"}}
    end

    test "skips when a scrub job for the volume is already running" do
      volume = make_volume("vol-1")
      MockVolumeRegistry.set_volumes([volume])

      running_job = %Job{
        id: "existing-scrub",
        type: Scrub,
        params: %{volume_id: "vol-1"},
        status: :running,
        node: Node.self(),
        progress: %{total: 100, completed: 50, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      MockJobTracker.set_running_jobs([running_job])

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :scrub_scheduler, :tick],
          [:neonfs, :scrub_scheduler, :skipped]
        ])

      start_supervised!(
        {ScrubScheduler,
         name: :"scrub_sched_skip_#{System.unique_integer([:positive])}",
         check_interval_ms: 50,
         job_tracker_mod: MockJobTracker,
         volume_registry_mod: MockVolumeRegistry}
      )

      # Wait for the first tick to complete
      assert_receive {[:neonfs, :scrub_scheduler, :tick], ^ref, %{}, %{}}, 1_000

      created = MockJobTracker.get_created()
      assert created == []

      assert_received {[:neonfs, :scrub_scheduler, :skipped], ^ref, %{},
                       %{reason: :already_running, volume_id: "vol-1"}}
    end

    test "respects per-volume scrub_interval" do
      # vol-1: 1 second interval (should be due immediately)
      # vol-2: very long interval, last scrub was recent (should NOT be due)
      vol1 = make_volume("vol-1", 1)
      vol2 = make_volume("vol-2", 999_999_999)
      MockVolumeRegistry.set_volumes([vol1, vol2])

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :scrub_scheduler, :tick],
          [:neonfs, :scrub_scheduler, :triggered]
        ])

      name = :"scrub_sched_interval_#{System.unique_integer([:positive])}"

      start_supervised!(
        {ScrubScheduler,
         name: name,
         check_interval_ms: 50,
         job_tracker_mod: MockJobTracker,
         volume_registry_mod: MockVolumeRegistry}
      )

      # First tick will create jobs for both volumes (no last scrub time = always due)
      assert_receive {[:neonfs, :scrub_scheduler, :tick], ^ref, %{}, %{}}, 1_000

      created = MockJobTracker.get_created()

      # Both should have been triggered on first tick (no prior scrub history)
      volume_ids = Enum.map(created, & &1.params.volume_id)
      assert "vol-1" in volume_ids
      assert "vol-2" in volume_ids

      initial_count = length(created)

      # Wait for ticks until vol-1 (1s interval) gets triggered again.
      # vol-2 has a huge interval so it won't be due.
      wait_for_new_job(ref, initial_count, 3_000)

      created_after = MockJobTracker.get_created()
      new_jobs = Enum.drop(created_after, initial_count)

      # Only vol-1 should have triggered again
      new_volume_ids = Enum.map(new_jobs, & &1.params.volume_id)
      assert "vol-1" in new_volume_ids
      refute "vol-2" in new_volume_ids
    end
  end

  describe "trigger_now/1" do
    test "creates a job immediately for specific volume" do
      volume = make_volume("vol-trigger")
      MockVolumeRegistry.set_volumes([volume])

      name = :"scrub_sched_trigger_#{System.unique_integer([:positive])}"

      start_supervised!(
        {ScrubScheduler,
         name: name,
         check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         volume_registry_mod: MockVolumeRegistry}
      )

      assert {:ok, job} = GenServer.call(name, {:trigger_now, "vol-trigger"})
      assert job.type == Scrub
      assert job.params == %{volume_id: "vol-trigger"}

      created = MockJobTracker.get_created()
      assert [_] = created
    end

    test "returns skipped when a scrub job for the volume is already running" do
      running_job = %Job{
        id: "existing-scrub-trigger",
        type: Scrub,
        params: %{volume_id: "vol-running"},
        status: :running,
        node: Node.self(),
        progress: %{total: 0, completed: 0, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      MockJobTracker.set_running_jobs([running_job])

      name = :"scrub_sched_trigger_skip_#{System.unique_integer([:positive])}"

      start_supervised!(
        {ScrubScheduler,
         name: name,
         check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         volume_registry_mod: MockVolumeRegistry}
      )

      assert {:skipped, :already_running} =
               GenServer.call(name, {:trigger_now, "vol-running"})

      created = MockJobTracker.get_created()
      assert created == []
    end
  end

  describe "status/0" do
    test "returns expected fields" do
      name = :"scrub_sched_status_#{System.unique_integer([:positive])}"

      start_supervised!(
        {ScrubScheduler,
         name: name,
         check_interval_ms: 60_000,
         job_tracker_mod: MockJobTracker,
         volume_registry_mod: MockVolumeRegistry}
      )

      status = GenServer.call(name, :status)

      assert status.check_interval_ms == 60_000
      assert status.volume_scrub_times == %{}
    end

    test "includes volume scrub times after a trigger" do
      volume = make_volume("vol-status")
      MockVolumeRegistry.set_volumes([volume])

      name = :"scrub_sched_status_trig_#{System.unique_integer([:positive])}"

      start_supervised!(
        {ScrubScheduler,
         name: name,
         check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         volume_registry_mod: MockVolumeRegistry}
      )

      GenServer.call(name, {:trigger_now, "vol-status"})
      status = GenServer.call(name, :status)

      assert %DateTime{} = status.volume_scrub_times["vol-status"]
    end
  end

  describe "configuration from app env" do
    test "uses check_interval_ms from app env when opts are not provided" do
      Application.put_env(:neonfs_core, :scrub_check_interval_ms, 1_800_000)

      on_exit(fn ->
        Application.delete_env(:neonfs_core, :scrub_check_interval_ms)
      end)

      opts = [
        check_interval_ms: Application.get_env(:neonfs_core, :scrub_check_interval_ms, 3_600_000),
        job_tracker_mod: MockJobTracker,
        volume_registry_mod: MockVolumeRegistry
      ]

      name = :"scrub_sched_appenv_#{System.unique_integer([:positive])}"

      start_supervised!({ScrubScheduler, Keyword.put(opts, :name, name)})

      status = GenServer.call(name, :status)
      assert status.check_interval_ms == 1_800_000
    end

    test "falls back to defaults when app env is not set" do
      Application.delete_env(:neonfs_core, :scrub_check_interval_ms)

      opts = [
        check_interval_ms: Application.get_env(:neonfs_core, :scrub_check_interval_ms, 3_600_000),
        job_tracker_mod: MockJobTracker,
        volume_registry_mod: MockVolumeRegistry
      ]

      name = :"scrub_sched_defaults_#{System.unique_integer([:positive])}"

      start_supervised!({ScrubScheduler, Keyword.put(opts, :name, name)})

      status = GenServer.call(name, :status)
      assert status.check_interval_ms == 3_600_000
    end
  end
end
