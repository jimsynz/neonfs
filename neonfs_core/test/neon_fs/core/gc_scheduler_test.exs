defmodule NeonFS.Core.GCSchedulerTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.GCScheduler
  alias NeonFS.Core.Job

  defmodule MockJobTracker do
    @moduledoc false
    use Agent

    def start_link(opts \\ []) do
      running_jobs = Keyword.get(opts, :running_jobs, [])

      Agent.start_link(
        fn -> %{running_jobs: running_jobs, created: []} end,
        name: Keyword.get(opts, :name, __MODULE__)
      )
    end

    def list(filters) do
      Agent.get(__MODULE__, fn state ->
        jobs = state.running_jobs

        jobs
        |> filter_by_status(Keyword.get(filters, :status))
        |> filter_by_type(Keyword.get(filters, :type))
      end)
    end

    def create(type, params) do
      job = %Job{
        id: "mock-job-#{System.unique_integer([:positive])}",
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

    defp filter_by_status(jobs, nil), do: jobs

    defp filter_by_status(jobs, status) when is_atom(status),
      do: Enum.filter(jobs, &(&1.status == status))

    defp filter_by_status(jobs, statuses) when is_list(statuses),
      do: Enum.filter(jobs, &(&1.status in statuses))

    defp filter_by_type(jobs, nil), do: jobs
    defp filter_by_type(jobs, type), do: Enum.filter(jobs, &(&1.type == type))
  end

  defmodule MockStorageMetrics do
    @moduledoc false
    use Agent

    def start_link(opts \\ []) do
      capacity = Keyword.get(opts, :capacity, default_capacity())

      Agent.start_link(
        fn -> %{capacity: capacity} end,
        name: Keyword.get(opts, :name, __MODULE__)
      )
    end

    def cluster_capacity do
      Agent.get(__MODULE__, fn state -> state.capacity end)
    end

    def set_capacity(capacity) do
      Agent.update(__MODULE__, fn state -> %{state | capacity: capacity} end)
    end

    defp default_capacity do
      %{
        drives: [],
        total_capacity: 1000,
        total_used: 500,
        total_available: 500
      }
    end
  end

  setup do
    start_supervised!({MockJobTracker, name: MockJobTracker})
    start_supervised!({MockStorageMetrics, name: MockStorageMetrics})
    :ok
  end

  describe "start_link/1 and periodic ticks" do
    test "creates a GC job on tick" do
      test_pid = self()

      :telemetry.attach(
        "gc-triggered-test-#{inspect(test_pid)}",
        [:neonfs, :gc_scheduler, :triggered],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:triggered, metadata})
        end,
        nil
      )

      pid =
        start_supervised!(
          {GCScheduler,
           name: :"gc_sched_#{System.unique_integer([:positive])}",
           interval_ms: 50,
           pressure_check_interval_ms: :timer.hours(24),
           job_tracker_mod: MockJobTracker,
           storage_metrics_mod: MockStorageMetrics}
        )

      assert is_pid(pid)

      # Wait for telemetry from the first tick
      assert_receive {:triggered, %{job_id: _}}, 1_000

      created = MockJobTracker.get_created()
      assert created != []

      first = List.last(created)
      assert first.type == NeonFS.Core.Job.Runners.GarbageCollection
      assert first.params == %{}

      :telemetry.detach("gc-triggered-test-#{inspect(test_pid)}")
    end

    test "skips when a GC job is already running" do
      test_pid = self()

      running_job = %Job{
        id: "existing-gc",
        type: NeonFS.Core.Job.Runners.GarbageCollection,
        params: %{},
        status: :running,
        node: Node.self(),
        progress: %{total: 100, completed: 50, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      MockJobTracker.set_running_jobs([running_job])

      :telemetry.attach(
        "gc-skipped-test-#{inspect(test_pid)}",
        [:neonfs, :gc_scheduler, :skipped],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:skipped, metadata})
        end,
        nil
      )

      start_supervised!(
        {GCScheduler,
         name: :"gc_sched_skip_#{System.unique_integer([:positive])}",
         interval_ms: 50,
         pressure_check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      # Wait for telemetry from the skipped tick
      assert_receive {:skipped, %{reason: :already_running}}, 1_000

      # No jobs should have been created
      created = MockJobTracker.get_created()
      assert created == []

      :telemetry.detach("gc-skipped-test-#{inspect(test_pid)}")
    end
  end

  describe "trigger_now/0" do
    test "creates a job immediately" do
      name = :"gc_sched_trigger_#{System.unique_integer([:positive])}"

      start_supervised!(
        {GCScheduler,
         name: name,
         interval_ms: :timer.hours(24),
         pressure_check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      assert {:ok, job} = GenServer.call(name, :trigger_now)
      assert job.type == NeonFS.Core.Job.Runners.GarbageCollection

      created = MockJobTracker.get_created()
      assert [_] = created
    end

    test "returns skipped when a GC job is already running" do
      running_job = %Job{
        id: "existing-gc-trigger",
        type: NeonFS.Core.Job.Runners.GarbageCollection,
        params: %{},
        status: :running,
        node: Node.self(),
        progress: %{total: 0, completed: 0, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      MockJobTracker.set_running_jobs([running_job])

      name = :"gc_sched_trigger_skip_#{System.unique_integer([:positive])}"

      start_supervised!(
        {GCScheduler,
         name: name,
         interval_ms: :timer.hours(24),
         pressure_check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      assert {:skipped, :already_running} = GenServer.call(name, :trigger_now)

      created = MockJobTracker.get_created()
      assert created == []
    end
  end

  describe "status/0" do
    test "returns expected fields including pressure fields" do
      name = :"gc_sched_status_#{System.unique_integer([:positive])}"

      start_supervised!(
        {GCScheduler,
         name: name,
         interval_ms: 60_000,
         pressure_check_interval_ms: 300_000,
         pressure_threshold: 0.90,
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      status = GenServer.call(name, :status)

      assert status.interval_ms == 60_000
      assert is_nil(status.last_triggered_at)
      assert is_nil(status.last_skipped_at)
      assert status.pressure_threshold == 0.90
      assert status.pressure_check_interval_ms == 300_000
      assert is_nil(status.last_pressure_check_at)
      assert is_nil(status.last_pressure_ratio)
    end

    test "updates last_triggered_at after a trigger" do
      name = :"gc_sched_status_trig_#{System.unique_integer([:positive])}"

      start_supervised!(
        {GCScheduler,
         name: name,
         interval_ms: :timer.hours(24),
         pressure_check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      GenServer.call(name, :trigger_now)
      status = GenServer.call(name, :status)

      assert %DateTime{} = status.last_triggered_at
      assert is_nil(status.last_skipped_at)
    end

    test "updates last_skipped_at when skipped" do
      running_job = %Job{
        id: "existing-gc-status",
        type: NeonFS.Core.Job.Runners.GarbageCollection,
        params: %{},
        status: :running,
        node: Node.self(),
        progress: %{total: 0, completed: 0, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      MockJobTracker.set_running_jobs([running_job])

      name = :"gc_sched_status_skip_#{System.unique_integer([:positive])}"

      start_supervised!(
        {GCScheduler,
         name: name,
         interval_ms: :timer.hours(24),
         pressure_check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      GenServer.call(name, :trigger_now)
      status = GenServer.call(name, :status)

      assert is_nil(status.last_triggered_at)
      assert %DateTime{} = status.last_skipped_at
    end
  end

  describe "storage pressure check" do
    test "triggers GC when ratio >= threshold" do
      test_pid = self()

      :telemetry.attach(
        "gc-pressure-trigger-#{inspect(test_pid)}",
        [:neonfs, :gc_scheduler, :pressure_triggered],
        fn _event, _measurements, metadata, _config ->
          send(test_pid, {:pressure_triggered, metadata})
        end,
        nil
      )

      # Set usage at 90% — above 0.85 default threshold
      MockStorageMetrics.set_capacity(%{
        drives: [],
        total_capacity: 1000,
        total_used: 900,
        total_available: 100
      })

      name = :"gc_sched_pressure_trig_#{System.unique_integer([:positive])}"

      start_supervised!(
        {GCScheduler,
         name: name,
         interval_ms: :timer.hours(24),
         pressure_check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      # Manually send pressure_check message
      send(name, :pressure_check)
      :sys.get_state(name)

      created = MockJobTracker.get_created()
      assert [job] = created
      assert job.type == NeonFS.Core.Job.Runners.GarbageCollection

      assert_received {:pressure_triggered, %{ratio: ratio, job_id: _}}
      assert ratio == 0.9

      status = GenServer.call(name, :status)
      assert %DateTime{} = status.last_pressure_check_at
      assert status.last_pressure_ratio == 0.9

      :telemetry.detach("gc-pressure-trigger-#{inspect(test_pid)}")
    end

    test "is a no-op when ratio < threshold" do
      # Set usage at 50% — well below 0.85 threshold
      MockStorageMetrics.set_capacity(%{
        drives: [],
        total_capacity: 1000,
        total_used: 500,
        total_available: 500
      })

      name = :"gc_sched_pressure_noop_#{System.unique_integer([:positive])}"

      start_supervised!(
        {GCScheduler,
         name: name,
         interval_ms: :timer.hours(24),
         pressure_check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      send(name, :pressure_check)
      :sys.get_state(name)

      created = MockJobTracker.get_created()
      assert created == []

      status = GenServer.call(name, :status)
      assert %DateTime{} = status.last_pressure_check_at
      assert status.last_pressure_ratio == 0.5
    end

    test "is a no-op when capacity is unlimited" do
      MockStorageMetrics.set_capacity(%{
        drives: [],
        total_capacity: :unlimited,
        total_used: 999_999,
        total_available: :unlimited
      })

      name = :"gc_sched_pressure_unlimited_#{System.unique_integer([:positive])}"

      start_supervised!(
        {GCScheduler,
         name: name,
         interval_ms: :timer.hours(24),
         pressure_check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      send(name, :pressure_check)
      :sys.get_state(name)

      created = MockJobTracker.get_created()
      assert created == []

      status = GenServer.call(name, :status)
      assert %DateTime{} = status.last_pressure_check_at
      assert is_nil(status.last_pressure_ratio)
    end

    test "skips when GC job already running" do
      # Set usage above threshold
      MockStorageMetrics.set_capacity(%{
        drives: [],
        total_capacity: 1000,
        total_used: 950,
        total_available: 50
      })

      running_job = %Job{
        id: "existing-gc-pressure",
        type: NeonFS.Core.Job.Runners.GarbageCollection,
        params: %{},
        status: :running,
        node: Node.self(),
        progress: %{total: 0, completed: 0, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      MockJobTracker.set_running_jobs([running_job])

      name = :"gc_sched_pressure_skip_#{System.unique_integer([:positive])}"

      start_supervised!(
        {GCScheduler,
         name: name,
         interval_ms: :timer.hours(24),
         pressure_check_interval_ms: :timer.hours(24),
         job_tracker_mod: MockJobTracker,
         storage_metrics_mod: MockStorageMetrics}
      )

      send(name, :pressure_check)
      :sys.get_state(name)

      # No new jobs created (running_jobs are pre-existing, not created by us)
      created = MockJobTracker.get_created()
      assert created == []

      status = GenServer.call(name, :status)
      assert status.last_pressure_ratio == 0.95
    end
  end

  # As in `scrub_scheduler_test.exs`: `init/1` takes these as opts and the
  # application env is the supervisor's business, so writing the env here
  # asserted nothing extra — and `delete_env` in an `async: true` file can
  # clear a key another running test just set.
  describe "interval and threshold configuration" do
    test "uses the values it is given" do
      name =
        start_scheduler(
          interval_ms: 7_200_000,
          pressure_threshold: 0.90,
          pressure_check_interval_ms: 120_000
        )

      status = GenServer.call(name, :status)
      assert status.interval_ms == 7_200_000
      assert status.pressure_threshold == 0.90
      assert status.pressure_check_interval_ms == 120_000
    end

    test "falls back to its own defaults when given none" do
      name = start_scheduler([])

      status = GenServer.call(name, :status)
      assert status.interval_ms == 86_400_000
      assert status.pressure_threshold == 0.85
      assert status.pressure_check_interval_ms == 300_000
    end
  end

  defp start_scheduler(opts) do
    name = :"gc_sched_#{System.unique_integer([:positive])}"

    opts =
      Keyword.merge(opts,
        name: name,
        job_tracker_mod: MockJobTracker,
        storage_metrics_mod: MockStorageMetrics
      )

    start_supervised!({GCScheduler, opts})
    name
  end
end
