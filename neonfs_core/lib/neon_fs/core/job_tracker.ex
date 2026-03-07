defmodule NeonFS.Core.JobTracker do
  @moduledoc """
  Cluster-visible, persistent job tracker.

  Manages long-running background jobs (key rotation, drive evacuation, etc.)
  with persistence via ETS + DETS snapshots. Jobs survive node restarts and
  are visible across the cluster via `list_cluster/1`.

  Each job runs as a supervised task at `:low` BEAM priority. The step loop
  calls `job.type.step(job)` repeatedly, persisting after each step. On
  restart, incomplete jobs are automatically resumed.

  ## Telemetry Events

    * `[:neonfs, :job, :created]`
    * `[:neonfs, :job, :started]`
    * `[:neonfs, :job, :progress]`
    * `[:neonfs, :job, :completed]`
    * `[:neonfs, :job, :failed]`
    * `[:neonfs, :job, :cancelled]`
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{Job, Persistence, ServiceRegistry}

  @ets_table :neonfs_jobs
  @prune_interval_ms :timer.hours(1)
  @retention_days 7
  @max_terminal_jobs 100

  # Client API

  @doc """
  Starts the JobTracker GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Creates and starts a new job.

  Validates that `type` implements the `Job.Runner` behaviour, builds a
  `%Job{}`, persists it, and spawns a step loop task.
  """
  @spec create(module(), map()) :: {:ok, Job.t()} | {:error, term()}
  def create(type, params \\ %{}) when is_atom(type) and is_map(params) do
    GenServer.call(__MODULE__, {:create, type, params})
  end

  @doc """
  Cancels a running or pending job.

  The step loop checks for cancellation between steps and transitions
  the job to `:cancelled`.
  """
  @spec cancel(String.t()) :: :ok | {:error, :not_found | :already_terminal}
  def cancel(job_id) when is_binary(job_id) do
    GenServer.call(__MODULE__, {:cancel, job_id})
  end

  @doc """
  Gets a job by ID.
  """
  @spec get(String.t()) :: {:ok, Job.t()} | {:error, :not_found}
  def get(job_id) when is_binary(job_id) do
    case :ets.lookup(@ets_table, job_id) do
      [{^job_id, job}] -> {:ok, job}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Lists jobs with optional filters.

  ## Filters

    * `:status` — atom or list of atoms
    * `:type` — module atom
  """
  @spec list(keyword()) :: [Job.t()]
  def list(filters \\ []) do
    @ets_table
    |> :ets.tab2list()
    |> Enum.map(fn {_id, job} -> job end)
    |> apply_filters(filters)
    |> Enum.sort_by(& &1.created_at, {:desc, DateTime})
  end

  @doc """
  Lists jobs across all connected core nodes.
  """
  @spec list_cluster(keyword()) :: [Job.t()]
  def list_cluster(filters \\ []) do
    local = list(filters)

    remote =
      for node <- ServiceRegistry.connected_nodes_by_type(:core), reduce: [] do
        acc ->
          case safe_remote_list(node, filters) do
            jobs when is_list(jobs) -> jobs ++ acc
            _ -> acc
          end
      end

    local ++ remote
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    meta_dir = Keyword.get(opts, :meta_dir, Persistence.meta_dir())
    task_supervisor = Keyword.get(opts, :task_supervisor, NeonFS.Core.JobTaskSupervisor)

    # Create ETS table
    :ets.new(@ets_table, [:named_table, :set, :public, read_concurrency: true])

    dets_path = Path.join(meta_dir, "jobs.dets")

    state = %{
      dets_path: dets_path,
      running_tasks: %{},
      task_refs: %{},
      task_supervisor: task_supervisor
    }

    {:ok, state, {:continue, :restore_and_resume}}
  end

  @impl true
  def handle_continue(:restore_and_resume, state) do
    state = restore_from_dets(state)
    state = resume_incomplete_jobs(state)
    schedule_prune()
    {:noreply, state}
  end

  @impl true
  def handle_call({:create, type, params}, _from, state) do
    case validate_runner(type) do
      :ok ->
        job = Job.new(type, params)
        job = %{job | status: :running, started_at: DateTime.utc_now()}
        persist_job(job)

        emit_telemetry(:created, job)
        emit_telemetry(:started, job)

        state = spawn_step_loop(state, job)
        {:reply, {:ok, job}, state}

      {:error, _} = error ->
        {:reply, error, state}
    end
  end

  def handle_call({:cancel, job_id}, _from, state) do
    case :ets.lookup(@ets_table, job_id) do
      [{^job_id, job}] when job.status in [:completed, :failed, :cancelled] ->
        {:reply, {:error, :already_terminal}, state}

      [{^job_id, job}] ->
        cancelled = %{
          job
          | status: :cancelled,
            updated_at: DateTime.utc_now(),
            completed_at: DateTime.utc_now()
        }

        persist_job(cancelled)
        emit_telemetry(:cancelled, cancelled)
        invoke_on_cancel(cancelled, job_id)

        state = cancel_running_task(state, job_id)
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_info({ref, _result}, state) when is_reference(ref) do
    # Task completed normally
    Process.demonitor(ref, [:flush])
    state = handle_task_done(state, ref)
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    # Task crashed
    state = handle_task_crash(state, ref, reason)
    {:noreply, state}
  end

  def handle_info(:prune_terminal_jobs, state) do
    prune_terminal_jobs()
    schedule_prune()
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    Logger.info("JobTracker shutting down, snapshotting jobs...")
    Persistence.snapshot_table(@ets_table, state.dets_path)
    :ok
  end

  # Private — Cancellation hooks

  defp invoke_on_cancel(job, job_id) do
    if function_exported?(job.type, :on_cancel, 1) do
      try do
        job.type.on_cancel(job)
      rescue
        error ->
          Logger.warning("on_cancel hook failed",
            job_id: job_id,
            error: inspect(error)
          )
      end
    end
  end

  # Private — Job execution

  defp spawn_step_loop(state, job) do
    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        Process.flag(:priority, :low)
        run_job_loop(job)
      end)

    %{
      state
      | running_tasks: Map.put(state.running_tasks, job.id, {task.ref, task.pid}),
        task_refs: Map.put(state.task_refs, task.ref, job.id)
    }
  end

  defp run_job_loop(job) do
    # Check for cancellation before each step
    case :ets.lookup(@ets_table, job.id) do
      [{_, %Job{status: :cancelled}}] ->
        :cancelled

      _ ->
        case job.type.step(job) do
          {:continue, updated} ->
            updated = %{updated | status: :running, updated_at: DateTime.utc_now()}
            persist_job(updated)
            emit_telemetry(:progress, updated)
            run_job_loop(updated)

          {:complete, updated} ->
            completed = %{
              updated
              | status: :completed,
                updated_at: DateTime.utc_now(),
                completed_at: DateTime.utc_now()
            }

            persist_job(completed)
            emit_telemetry(:completed, completed)
            :completed

          {:error, reason, updated} ->
            failed = %{
              updated
              | status: :failed,
                error: reason,
                updated_at: DateTime.utc_now(),
                completed_at: DateTime.utc_now()
            }

            persist_job(failed)
            emit_telemetry(:failed, failed)
            {:error, reason}
        end
    end
  end

  defp handle_task_done(state, ref) do
    case Map.pop(state.task_refs, ref) do
      {nil, _} ->
        state

      {job_id, task_refs} ->
        %{
          state
          | task_refs: task_refs,
            running_tasks: Map.delete(state.running_tasks, job_id)
        }
    end
  end

  defp handle_task_crash(state, ref, reason) do
    case Map.pop(state.task_refs, ref) do
      {nil, _} ->
        state

      {job_id, task_refs} ->
        # Mark job as failed if it wasn't already cancelled
        case :ets.lookup(@ets_table, job_id) do
          [{^job_id, %Job{status: status} = job}]
          when status not in [:cancelled, :completed, :failed] ->
            failed = %{
              job
              | status: :failed,
                error: {:task_crash, reason},
                updated_at: DateTime.utc_now(),
                completed_at: DateTime.utc_now()
            }

            persist_job(failed)
            emit_telemetry(:failed, failed)
            Logger.warning("Job task crashed", job_id: job_id, reason: inspect(reason))

          _ ->
            :ok
        end

        %{
          state
          | task_refs: task_refs,
            running_tasks: Map.delete(state.running_tasks, job_id)
        }
    end
  end

  defp cancel_running_task(state, job_id) do
    case Map.pop(state.running_tasks, job_id) do
      {nil, _} ->
        state

      {{ref, pid}, running_tasks} ->
        Task.Supervisor.terminate_child(state.task_supervisor, pid)
        Process.demonitor(ref, [:flush])

        %{
          state
          | running_tasks: running_tasks,
            task_refs: Map.delete(state.task_refs, ref)
        }
    end
  end

  # Private — Persistence

  defp persist_job(job) do
    :ets.insert(@ets_table, {job.id, job})
  end

  defp restore_from_dets(state) do
    if File.exists?(state.dets_path) do
      case :dets.open_file(@ets_table, type: :set, file: String.to_charlist(state.dets_path)) do
        {:ok, dets_ref} ->
          :dets.to_ets(dets_ref, @ets_table)
          count = :ets.info(@ets_table, :size)
          :dets.close(dets_ref)
          Logger.info("JobTracker restored jobs from DETS", count: count)

        {:error, reason} ->
          Logger.error("JobTracker failed to restore from DETS", reason: inspect(reason))
      end
    end

    state
  end

  defp resume_incomplete_jobs(state) do
    @ets_table
    |> :ets.tab2list()
    |> Enum.filter(fn {_id, job} -> job.status in [:running, :pending] end)
    |> Enum.reduce(state, fn {_id, job}, acc ->
      Logger.info("Resuming job", job_id: job.id, job_label: job.type.label())
      spawn_step_loop(acc, job)
    end)
  end

  # Private — Pruning

  defp prune_terminal_jobs do
    cutoff = DateTime.add(DateTime.utc_now(), -@retention_days, :day)

    terminal_jobs =
      @ets_table
      |> :ets.tab2list()
      |> Enum.filter(fn {_id, job} -> Job.terminal?(job) end)
      |> Enum.sort_by(fn {_id, job} -> job.completed_at || job.updated_at end, {:asc, DateTime})

    # Prune by age
    Enum.each(terminal_jobs, fn {id, job} ->
      completed_at = job.completed_at || job.updated_at

      if DateTime.compare(completed_at, cutoff) == :lt do
        :ets.delete(@ets_table, id)
      end
    end)

    # Prune by count (keep at most @max_terminal_jobs)
    remaining_terminal =
      @ets_table
      |> :ets.tab2list()
      |> Enum.filter(fn {_id, job} -> Job.terminal?(job) end)
      |> Enum.sort_by(fn {_id, job} -> job.completed_at || job.updated_at end, {:asc, DateTime})

    overflow = length(remaining_terminal) - @max_terminal_jobs

    if overflow > 0 do
      remaining_terminal
      |> Enum.take(overflow)
      |> Enum.each(fn {id, _job} -> :ets.delete(@ets_table, id) end)
    end
  end

  defp schedule_prune do
    Process.send_after(self(), :prune_terminal_jobs, @prune_interval_ms)
  end

  # Private — Validation

  defp validate_runner(type) do
    Code.ensure_loaded(type)

    if function_exported?(type, :step, 1) and function_exported?(type, :label, 0) do
      :ok
    else
      {:error, {:invalid_runner, type}}
    end
  end

  # Private — Filtering

  defp apply_filters(jobs, []), do: jobs

  defp apply_filters(jobs, [{:status, status} | rest]) when is_atom(status) do
    jobs
    |> Enum.filter(&(&1.status == status))
    |> apply_filters(rest)
  end

  defp apply_filters(jobs, [{:status, statuses} | rest]) when is_list(statuses) do
    jobs
    |> Enum.filter(&(&1.status in statuses))
    |> apply_filters(rest)
  end

  defp apply_filters(jobs, [{:type, type} | rest]) when is_atom(type) do
    jobs
    |> Enum.filter(&(&1.type == type))
    |> apply_filters(rest)
  end

  defp apply_filters(jobs, [_ | rest]), do: apply_filters(jobs, rest)

  # Private — Cross-node

  defp safe_remote_list(node, filters) do
    :erpc.call(node, __MODULE__, :list, [filters], 5_000)
  catch
    _, _ -> []
  end

  # Private — Telemetry

  defp emit_telemetry(event, job) do
    :telemetry.execute(
      [:neonfs, :job, event],
      %{},
      %{
        job_id: job.id,
        type: job.type,
        status: job.status,
        node: job.node
      }
    )
  end
end
