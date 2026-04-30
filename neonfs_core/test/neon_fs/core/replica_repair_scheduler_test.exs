defmodule NeonFS.Core.ReplicaRepairSchedulerTest do
  @moduledoc """
  Unit tests for `NeonFS.Core.ReplicaRepairScheduler` (#707). Uses
  the same MockJobTracker / MockVolumeRegistry pattern as
  `ScrubSchedulerTest` so the scheduler is exercised in isolation
  from real `JobTracker` / `VolumeRegistry` state.
  """

  use ExUnit.Case, async: true

  alias NeonFS.Core.Job
  alias NeonFS.Core.Job.Runners.ReplicaRepair, as: Runner
  alias NeonFS.Core.ReplicaRepairScheduler

  defmodule MockJobTracker do
    @moduledoc false
    use Agent

    def start_link(opts \\ []) do
      Agent.start_link(
        fn ->
          %{
            running_jobs: Keyword.get(opts, :running_jobs, []),
            completed_jobs: Keyword.get(opts, :completed_jobs, []),
            created: []
          }
        end,
        name: Keyword.get(opts, :name, __MODULE__)
      )
    end

    def list(filters) do
      Agent.get(__MODULE__, fn state ->
        jobs =
          case Keyword.get(filters, :status) do
            :running -> state.running_jobs
            :completed -> state.completed_jobs
            _ -> state.running_jobs ++ state.completed_jobs
          end

        filter_by_type(jobs, Keyword.get(filters, :type))
      end)
    end

    def create(type, params) do
      job = %Job{
        id: "mock-rr-#{System.unique_integer([:positive])}",
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

    def get_created, do: Agent.get(__MODULE__, & &1.created)

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
      Agent.start_link(
        fn -> %{volumes: Keyword.get(opts, :volumes, [])} end,
        name: Keyword.get(opts, :name, __MODULE__)
      )
    end

    def list, do: Agent.get(__MODULE__, & &1.volumes)
    def set_volumes(vs), do: Agent.update(__MODULE__, fn s -> %{s | volumes: vs} end)
  end

  defp make_volume(id), do: %{id: id, name: "vol-#{id}"}

  defp attach_telemetry(events) do
    ref = make_ref()

    :telemetry.attach_many(
      "rr-sched-test-#{inspect(ref)}",
      events,
      fn event, measurements, metadata, parent ->
        send(parent, {event, ref, measurements, metadata})
      end,
      self()
    )

    ref
  end

  describe "init/1 + periodic tick" do
    test "queues a repair job for every volume on the first tick (no prior history)" do
      MockJobTracker.start_link()
      MockVolumeRegistry.start_link(volumes: [make_volume("v1"), make_volume("v2")])

      ref = attach_telemetry([[:neonfs, :replica_repair, :tick]])

      {:ok, _pid} =
        ReplicaRepairScheduler.start_link(
          check_interval_ms: 50,
          job_tracker_mod: MockJobTracker,
          volume_registry_mod: MockVolumeRegistry,
          name: :"rr_sched_#{System.unique_integer([:positive])}"
        )

      assert_receive {[:neonfs, :replica_repair, :tick], ^ref, %{},
                      %{volumes_checked: 2, jobs_queued: 2}},
                     1_000

      created = MockJobTracker.get_created()
      assert length(created) == 2
      assert Enum.all?(created, &(&1.type == Runner))
    end

    test "skips volumes whose last repair pass is within volume_interval_seconds" do
      now = DateTime.utc_now()
      recent = DateTime.add(now, -100, :second)

      completed_job = %Job{
        id: "completed-1",
        type: Runner,
        params: %{volume_id: "v1"},
        status: :completed,
        node: Node.self(),
        progress: %{total: 0, completed: 0, description: nil},
        state: %{},
        created_at: recent,
        started_at: recent,
        updated_at: recent,
        completed_at: recent
      }

      MockJobTracker.start_link(completed_jobs: [completed_job])
      MockVolumeRegistry.start_link(volumes: [make_volume("v1")])

      ref = attach_telemetry([[:neonfs, :replica_repair, :tick]])

      {:ok, _pid} =
        ReplicaRepairScheduler.start_link(
          check_interval_ms: 50,
          # 1h cutoff — recent job (100s ago) should not trigger
          volume_interval_seconds: 3_600,
          job_tracker_mod: MockJobTracker,
          volume_registry_mod: MockVolumeRegistry,
          name: :"rr_sched_skip_#{System.unique_integer([:positive])}"
        )

      assert_receive {[:neonfs, :replica_repair, :tick], ^ref, %{},
                      %{volumes_checked: 1, jobs_queued: 0}},
                     1_000

      assert MockJobTracker.get_created() == []
    end

    test "dedupes against an already-running job for the same volume" do
      running_job = %Job{
        id: "already-running",
        type: Runner,
        params: %{volume_id: "v1"},
        status: :running,
        node: Node.self(),
        progress: %{total: 0, completed: 0, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      MockJobTracker.start_link(running_jobs: [running_job])
      MockVolumeRegistry.start_link(volumes: [make_volume("v1")])

      ref = attach_telemetry([[:neonfs, :replica_repair_scheduler, :skipped]])

      {:ok, _pid} =
        ReplicaRepairScheduler.start_link(
          check_interval_ms: 50,
          job_tracker_mod: MockJobTracker,
          volume_registry_mod: MockVolumeRegistry,
          name: :"rr_sched_dedupe_#{System.unique_integer([:positive])}"
        )

      assert_receive {[:neonfs, :replica_repair_scheduler, :skipped], ^ref, %{},
                      %{reason: :already_running, volume_id: "v1"}},
                     1_000

      assert MockJobTracker.get_created() == []
    end
  end

  describe "trigger_now/1" do
    test "creates a job immediately when none is running" do
      MockJobTracker.start_link()
      MockVolumeRegistry.start_link(volumes: [make_volume("v1")])

      name = :"rr_sched_trigger_#{System.unique_integer([:positive])}"

      {:ok, _pid} =
        ReplicaRepairScheduler.start_link(
          # disable periodic ticks for this test by using a long interval
          check_interval_ms: 60_000,
          volume_interval_seconds: 60,
          job_tracker_mod: MockJobTracker,
          volume_registry_mod: MockVolumeRegistry,
          name: name
        )

      assert {:ok, job} = GenServer.call(name, {:trigger_now, "v1"})
      assert job.type == Runner
      assert job.params == %{volume_id: "v1"}
    end

    test "skips when a job is already running for the volume" do
      running_job = %Job{
        id: "running",
        type: Runner,
        params: %{volume_id: "v1"},
        status: :running,
        node: Node.self(),
        progress: %{total: 0, completed: 0, description: nil},
        state: %{},
        created_at: DateTime.utc_now(),
        started_at: DateTime.utc_now(),
        updated_at: DateTime.utc_now()
      }

      MockJobTracker.start_link(running_jobs: [running_job])
      MockVolumeRegistry.start_link(volumes: [make_volume("v1")])

      name = :"rr_sched_dedupe2_#{System.unique_integer([:positive])}"

      {:ok, _pid} =
        ReplicaRepairScheduler.start_link(
          check_interval_ms: 60_000,
          job_tracker_mod: MockJobTracker,
          volume_registry_mod: MockVolumeRegistry,
          name: name
        )

      assert {:skipped, :already_running} = GenServer.call(name, {:trigger_now, "v1"})
    end
  end

  describe "status/0" do
    test "returns the configured intervals + per-volume repair times" do
      MockJobTracker.start_link()
      MockVolumeRegistry.start_link()

      name = :"rr_sched_status_#{System.unique_integer([:positive])}"

      {:ok, _pid} =
        ReplicaRepairScheduler.start_link(
          check_interval_ms: 60_000,
          volume_interval_seconds: 1234,
          job_tracker_mod: MockJobTracker,
          volume_registry_mod: MockVolumeRegistry,
          name: name
        )

      status = GenServer.call(name, :status)
      assert status.check_interval_ms == 60_000
      assert status.volume_interval_seconds == 1234
      assert is_map(status.volume_repair_times)
    end
  end
end
