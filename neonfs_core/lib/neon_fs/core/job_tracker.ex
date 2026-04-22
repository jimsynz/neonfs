defmodule NeonFS.Core.JobTracker do
  @moduledoc """
  Cluster-visible, persistent job tracker.

  Manages long-running background jobs (key rotation, drive evacuation, etc.)
  backed by a node-local DETS table. Jobs survive node restarts and are
  visible across the cluster via `list_cluster/1`.

  Each job runs as a supervised task at `:low` BEAM priority. The step loop
  calls `job.type.step(job)` repeatedly, persisting after each step. On
  restart, incomplete jobs are automatically resumed.

  ## Persistence

  Writes go directly to a DETS file opened by the manager — there is no ETS
  mirror. Public read functions (`get/1`, `list/1`) read DETS without going
  through the GenServer, so reads stay concurrent. Writes are serialised
  through the manager's `handle_call` / `handle_info` callbacks.

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

  @dets_table :neonfs_jobs
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
    case :dets.lookup(@dets_table, job_id) do
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
    @dets_table
    |> all_jobs()
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
    meta_dir = Keyword.get(opts, :meta_dir, Persistence.meta_dir())
    task_supervisor = Keyword.get(opts, :task_supervisor, NeonFS.Core.JobTaskSupervisor)

    File.mkdir_p!(meta_dir)
    dets_path = Path.join(meta_dir, "jobs.dets")

    {:ok, @dets_table} =
      :dets.open_file(@dets_table, type: :set, file: String.to_charlist(dets_path))

    state = %{
      dets_path: dets_path,
      running_tasks: %{},
      task_refs: %{},
      task_supervisor: task_supervisor
    }

    {:ok, state, {:continue, :resume_incomplete_jobs}}
  end

  @impl true
  def handle_continue(:resume_incomplete_jobs, state) do
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
    case :dets.lookup(@dets_table, job_id) do
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
    case :dets.lookup(@dets_table, job.id) do
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
        case :dets.lookup(@dets_table, job_id) do
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
    :dets.insert(@dets_table, {job.id, job})
  end

  defp all_jobs(dets_table) do
    dets_table
    |> :dets.match_object(:_)
    |> Enum.map(fn {_id, job} -> job end)
  end

  defp resume_incomplete_jobs(state) do
    @dets_table
    |> all_jobs()
    |> Enum.filter(fn job -> job.status in [:running, :pending] end)
    |> Enum.reduce(state, fn job, acc ->
      Logger.info("Resuming job", job_id: job.id, job_label: job.type.label())
      spawn_step_loop(acc, job)
    end)
  end

  # Private — Pruning

  defp prune_terminal_jobs do
    cutoff = DateTime.add(DateTime.utc_now(), -@retention_days, :day)

    terminal_jobs =
      @dets_table
      |> all_jobs()
      |> Enum.filter(&Job.terminal?/1)
      |> Enum.sort_by(fn job -> job.completed_at || job.updated_at end, {:asc, DateTime})

    # Prune by age
    Enum.each(terminal_jobs, fn job ->
      completed_at = job.completed_at || job.updated_at

      if DateTime.compare(completed_at, cutoff) == :lt do
        :dets.delete(@dets_table, job.id)
      end
    end)

    # Prune by count (keep at most @max_terminal_jobs)
    remaining_terminal =
      @dets_table
      |> all_jobs()
      |> Enum.filter(&Job.terminal?/1)
      |> Enum.sort_by(fn job -> job.completed_at || job.updated_at end, {:asc, DateTime})

    overflow = length(remaining_terminal) - @max_terminal_jobs

    if overflow > 0 do
      remaining_terminal
      |> Enum.take(overflow)
      |> Enum.each(fn job -> :dets.delete(@dets_table, job.id) end)
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
