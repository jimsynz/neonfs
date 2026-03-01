defmodule NeonFS.Core.JobTrackerTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.{Job, JobTracker}

  @moduletag :tmp_dir

  defmodule TestRunner do
    @behaviour NeonFS.Core.Job.Runner

    @impl true
    def step(job) do
      # On first step, initialise progress total from params
      total = Map.get(job.params, :total, job.progress.total)

      progress =
        if job.progress.total == 0 and total > 0 do
          %{job.progress | total: total}
        else
          job.progress
        end

      completed = progress.completed + 1

      if completed >= total do
        {:complete, %{job | progress: %{progress | completed: completed}}}
      else
        {:continue, %{job | progress: %{progress | completed: completed}}}
      end
    end

    @impl true
    def label, do: "test-runner"
  end

  defmodule FailingRunner do
    @behaviour NeonFS.Core.Job.Runner

    @impl true
    def step(job) do
      {:error, :intentional_failure, job}
    end

    @impl true
    def label, do: "failing-runner"
  end

  defmodule SlowRunner do
    @behaviour NeonFS.Core.Job.Runner

    @impl true
    def step(job) do
      # Signal that step is running, then wait for release
      case Map.get(job.params, :notify_pid) do
        pid when is_pid(pid) ->
          send(pid, {:slow_runner_step, self()})
          receive do: (:continue -> :ok)

        _ ->
          :ok
      end

      total = Map.get(job.params, :total, job.progress.total)

      progress =
        if job.progress.total == 0 and total > 0 do
          %{job.progress | total: total}
        else
          job.progress
        end

      completed = progress.completed + 1

      if completed >= total do
        {:complete, %{job | progress: %{progress | completed: completed}}}
      else
        {:continue, %{job | progress: %{progress | completed: completed}}}
      end
    end

    @impl true
    def label, do: "slow-runner"
  end

  setup %{tmp_dir: tmp_dir} do
    meta_dir = Path.join(tmp_dir, "job_tracker")
    File.mkdir_p!(meta_dir)

    ensure_task_supervisor()

    pid =
      start_supervised!(
        {JobTracker,
         name: JobTracker, meta_dir: meta_dir, task_supervisor: NeonFS.Core.JobTaskSupervisor}
      )

    on_exit(fn ->
      File.rm_rf!(meta_dir)
    end)

    %{meta_dir: meta_dir, pid: pid}
  end

  defp ensure_task_supervisor do
    case Process.whereis(NeonFS.Core.JobTaskSupervisor) do
      nil ->
        start_supervised!(
          {Task.Supervisor, name: NeonFS.Core.JobTaskSupervisor},
          restart: :temporary
        )

      _pid ->
        :ok
    end
  end

  describe "create/2" do
    test "creates and starts a job" do
      {:ok, job} = JobTracker.create(TestRunner, %{total: 3})

      assert is_binary(job.id)
      assert job.type == TestRunner
      assert job.status == :running
      assert job.node == Node.self()
    end

    test "returns error for invalid runner" do
      assert {:error, {:invalid_runner, String}} = JobTracker.create(String, %{})
    end

    test "job completes after steps" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :job, :completed]
        ])

      {:ok, job} = JobTracker.create(TestRunner, %{total: 3})

      assert_receive {[:neonfs, :job, :completed], ^ref, %{}, %{job_id: job_id}}, 2_000
      assert job_id == job.id

      {:ok, completed} = JobTracker.get(job.id)
      assert completed.status == :completed
      assert completed.progress.completed == 3
    end
  end

  describe "get/1" do
    test "returns existing job" do
      {:ok, job} = JobTracker.create(TestRunner, %{total: 1})
      assert {:ok, _} = JobTracker.get(job.id)
    end

    test "returns error for missing job" do
      assert {:error, :not_found} = JobTracker.get("nonexistent")
    end
  end

  describe "list/1" do
    test "lists all jobs" do
      {:ok, _} = JobTracker.create(TestRunner, %{total: 100})
      {:ok, _} = JobTracker.create(TestRunner, %{total: 100})

      jobs = JobTracker.list()
      assert length(jobs) >= 2
    end

    test "filters by status" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :job, :failed]
        ])

      {:ok, job} = JobTracker.create(FailingRunner, %{})

      assert_receive {[:neonfs, :job, :failed], ^ref, %{}, %{job_id: job_id}}, 2_000
      assert job_id == job.id

      failed = JobTracker.list(status: :failed)
      assert Enum.any?(failed, &(&1.id == job.id))

      running = JobTracker.list(status: :running)
      refute Enum.any?(running, &(&1.id == job.id))
    end

    test "filters by type" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :job, :failed]
        ])

      {:ok, _} = JobTracker.create(TestRunner, %{total: 100})
      {:ok, _} = JobTracker.create(FailingRunner, %{})

      # Wait for FailingRunner to reach terminal state before asserting on filters
      assert_receive {[:neonfs, :job, :failed], ^ref, %{}, %{}}, 2_000

      test_jobs = JobTracker.list(type: TestRunner)
      assert Enum.all?(test_jobs, &(&1.type == TestRunner))
    end
  end

  describe "cancel/1" do
    test "cancels a running job" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :job, :started]
        ])

      {:ok, job} = JobTracker.create(SlowRunner, %{total: 100, notify_pid: self()})

      # Wait for the job to be started before cancelling
      assert_receive {[:neonfs, :job, :started], ^ref, %{}, %{job_id: job_id}}, 2_000
      assert job_id == job.id

      # Wait for step to begin, then cancel before releasing it
      assert_receive {:slow_runner_step, _pid}, 2_000

      assert :ok = JobTracker.cancel(job.id)

      {:ok, cancelled} = JobTracker.get(job.id)
      assert cancelled.status == :cancelled
    end

    test "returns error for missing job" do
      assert {:error, :not_found} = JobTracker.cancel("nonexistent")
    end

    test "returns error for already terminal job" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :job, :failed]
        ])

      {:ok, job} = JobTracker.create(FailingRunner, %{})

      assert_receive {[:neonfs, :job, :failed], ^ref, %{}, %{job_id: job_id}}, 2_000
      assert job_id == job.id

      {:ok, failed_job} = JobTracker.get(job.id)
      assert failed_job.status == :failed

      assert {:error, :already_terminal} = JobTracker.cancel(job.id)
    end
  end

  describe "Job struct" do
    test "new/2 creates a job with defaults" do
      job = Job.new(TestRunner, %{foo: "bar"})

      assert is_binary(job.id)
      assert job.type == TestRunner
      assert job.status == :pending
      assert job.params == %{foo: "bar"}
      assert job.state == %{}
      assert job.error == nil
    end

    test "terminal?/1 returns true for terminal statuses" do
      job = Job.new(TestRunner, %{})

      assert Job.terminal?(%{job | status: :completed})
      assert Job.terminal?(%{job | status: :failed})
      assert Job.terminal?(%{job | status: :cancelled})
      refute Job.terminal?(%{job | status: :running})
      refute Job.terminal?(%{job | status: :pending})
    end

    test "progress_percent/1 calculates correctly" do
      job = Job.new(TestRunner, %{})

      assert "0%" == Job.progress_percent(job)

      job_with_progress = %{job | progress: %{total: 100, completed: 45, description: ""}}
      assert "45%" == Job.progress_percent(job_with_progress)

      job_complete = %{job | progress: %{total: 100, completed: 100, description: ""}}
      assert "100%" == Job.progress_percent(job_complete)
    end
  end

  describe "telemetry" do
    test "emits created event" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :job, :created]
        ])

      {:ok, job} = JobTracker.create(TestRunner, %{total: 1})

      assert_receive {[:neonfs, :job, :created], ^ref, %{}, %{job_id: job_id}}
      assert job_id == job.id
    end

    test "emits completed event" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :job, :completed]
        ])

      {:ok, job} = JobTracker.create(TestRunner, %{total: 1})

      assert_receive {[:neonfs, :job, :completed], ^ref, %{}, %{job_id: job_id}}, 2_000
      assert job_id == job.id
    end

    test "emits failed event" do
      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :job, :failed]
        ])

      {:ok, job} = JobTracker.create(FailingRunner, %{})

      assert_receive {[:neonfs, :job, :failed], ^ref, %{}, %{job_id: job_id}}, 2_000
      assert job_id == job.id
    end
  end
end
