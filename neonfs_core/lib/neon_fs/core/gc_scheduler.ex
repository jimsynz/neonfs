defmodule NeonFS.Core.GCScheduler do
  @moduledoc """
  Periodic garbage collection scheduler with storage pressure detection.

  Creates GC jobs via `JobTracker` on a configurable interval. Also monitors
  cluster storage usage and triggers GC when the usage ratio exceeds a
  configurable threshold.

  Skips job creation when a GC job is already running to avoid duplicate
  concurrent runs.

  Each core node runs its own scheduler. Since `JobTracker` stores jobs locally
  and `GarbageCollector.collect/0` only processes local data, each node
  independently schedules its own GC.

  ## Configuration

    * `:interval_ms` — scheduled GC interval (default: 86_400_000 = 24h)
    * `:pressure_check_interval_ms` — pressure check interval (default: 300_000 = 5 min)
    * `:pressure_threshold` — usage ratio that triggers GC (default: 0.85)
    * `:job_tracker_mod` — injectable module for testing (default: `NeonFS.Core.JobTracker`)
    * `:storage_metrics_mod` — injectable module for testing (default: `NeonFS.Core.StorageMetrics`)

  ## Telemetry Events

    * `[:neonfs, :gc_scheduler, :triggered]` — a scheduled GC job was created
    * `[:neonfs, :gc_scheduler, :skipped]` — tick skipped because a GC job is already running
    * `[:neonfs, :gc_scheduler, :pressure_triggered]` — GC triggered by storage pressure
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Job.Runners.GarbageCollection
  alias NeonFS.Core.JobTracker
  alias NeonFS.Core.StorageMetrics

  # Client API

  @doc """
  Starts the GCScheduler GenServer.

  ## Options

    * `:interval_ms` — scheduled GC interval in milliseconds (default: 86_400_000 = 24h)
    * `:pressure_check_interval_ms` — pressure check interval in milliseconds (default: 300_000 = 5 min)
    * `:pressure_threshold` — usage ratio threshold (default: 0.85)
    * `:job_tracker_mod` — module implementing `create/2` and `list/1` (default: `JobTracker`)
    * `:storage_metrics_mod` — module implementing `cluster_capacity/0` (default: `StorageMetrics`)
    * `:name` — GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns the current scheduler status.

  Includes scheduled GC fields (`:interval_ms`, `:last_triggered_at`,
  `:last_skipped_at`) and pressure fields (`:pressure_threshold`,
  `:pressure_check_interval_ms`, `:last_pressure_check_at`,
  `:last_pressure_ratio`).
  """
  @spec status() :: map()
  def status do
    GenServer.call(__MODULE__, :status)
  end

  @doc """
  Triggers an immediate GC job.

  Applies the same duplicate-run check — if a GC job is already running,
  the trigger is skipped.
  """
  @spec trigger_now() :: {:ok, map()} | {:skipped, :already_running}
  def trigger_now do
    GenServer.call(__MODULE__, :trigger_now)
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, 86_400_000),
      pressure_check_interval_ms: Keyword.get(opts, :pressure_check_interval_ms, 300_000),
      pressure_threshold: Keyword.get(opts, :pressure_threshold, 0.85),
      job_tracker_mod: Keyword.get(opts, :job_tracker_mod, JobTracker),
      storage_metrics_mod: Keyword.get(opts, :storage_metrics_mod, StorageMetrics),
      last_triggered_at: nil,
      last_skipped_at: nil,
      last_pressure_check_at: nil,
      last_pressure_ratio: nil
    }

    schedule_tick(state)
    schedule_pressure_check(state)

    Logger.info("GCScheduler started",
      interval_ms: state.interval_ms,
      pressure_check_interval_ms: state.pressure_check_interval_ms,
      pressure_threshold: state.pressure_threshold
    )

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    result = %{
      interval_ms: state.interval_ms,
      last_triggered_at: state.last_triggered_at,
      last_skipped_at: state.last_skipped_at,
      pressure_threshold: state.pressure_threshold,
      pressure_check_interval_ms: state.pressure_check_interval_ms,
      last_pressure_check_at: state.last_pressure_check_at,
      last_pressure_ratio: state.last_pressure_ratio
    }

    {:reply, result, state}
  end

  def handle_call(:trigger_now, _from, state) do
    case maybe_create_job(state) do
      {:triggered, job, state} ->
        {:reply, {:ok, job}, state}

      {:skipped, state} ->
        {:reply, {:skipped, :already_running}, state}
    end
  end

  @impl true
  def handle_info(:tick, state) do
    Logger.metadata(component: :scheduler, scheduler: :gc)

    state =
      case maybe_create_job(state) do
        {:triggered, _job, state} -> state
        {:skipped, state} -> state
      end

    schedule_tick(state)
    {:noreply, state}
  end

  def handle_info(:pressure_check, state) do
    Logger.metadata(component: :scheduler, scheduler: :gc_pressure)
    state = check_storage_pressure(state)
    schedule_pressure_check(state)
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private

  defp gc_job_running?(state) do
    state.job_tracker_mod.list(status: :running, type: GarbageCollection) != []
  end

  defp maybe_create_job(state) do
    if gc_job_running?(state) do
      now = DateTime.utc_now()

      :telemetry.execute(
        [:neonfs, :gc_scheduler, :skipped],
        %{},
        %{reason: :already_running}
      )

      Logger.debug("GCScheduler skipped: GC job already running")
      {:skipped, %{state | last_skipped_at: now}}
    else
      case state.job_tracker_mod.create(GarbageCollection, %{}) do
        {:ok, job} ->
          now = DateTime.utc_now()

          :telemetry.execute(
            [:neonfs, :gc_scheduler, :triggered],
            %{},
            %{job_id: job.id}
          )

          Logger.info("GCScheduler triggered GC job", job_id: job.id)
          {:triggered, job, %{state | last_triggered_at: now}}

        {:error, reason} ->
          Logger.warning("GCScheduler failed to create GC job", reason: inspect(reason))
          {:skipped, state}
      end
    end
  end

  defp check_storage_pressure(state) do
    now = DateTime.utc_now()
    capacity = state.storage_metrics_mod.cluster_capacity()

    case compute_pressure_ratio(capacity) do
      :unlimited ->
        %{state | last_pressure_check_at: now}

      ratio ->
        state = %{state | last_pressure_check_at: now, last_pressure_ratio: ratio}
        maybe_trigger_pressure_gc(state, ratio, now)
    end
  end

  defp compute_pressure_ratio(%{total_capacity: :unlimited}), do: :unlimited
  defp compute_pressure_ratio(%{total_capacity: 0}), do: :unlimited
  defp compute_pressure_ratio(%{total_used: used, total_capacity: cap}), do: used / cap

  defp maybe_trigger_pressure_gc(state, ratio, _now) when ratio < state.pressure_threshold do
    state
  end

  defp maybe_trigger_pressure_gc(state, ratio, now) do
    if gc_job_running?(state) do
      Logger.debug("GCScheduler pressure check above threshold but GC already running",
        ratio: Float.round(ratio, 3),
        pressure_threshold: state.pressure_threshold
      )

      state
    else
      create_pressure_gc_job(state, ratio, now)
    end
  end

  defp create_pressure_gc_job(state, ratio, now) do
    case state.job_tracker_mod.create(GarbageCollection, %{}) do
      {:ok, job} ->
        :telemetry.execute(
          [:neonfs, :gc_scheduler, :pressure_triggered],
          %{},
          %{ratio: ratio, job_id: job.id}
        )

        Logger.info("GCScheduler pressure-triggered GC job",
          job_id: job.id,
          ratio: Float.round(ratio, 3)
        )

        %{state | last_triggered_at: now}

      {:error, reason} ->
        Logger.warning("GCScheduler failed to create pressure-triggered GC job",
          reason: inspect(reason)
        )

        state
    end
  end

  defp schedule_tick(state) do
    Process.send_after(self(), :tick, state.interval_ms)
  end

  defp schedule_pressure_check(state) do
    Process.send_after(self(), :pressure_check, state.pressure_check_interval_ms)
  end
end
