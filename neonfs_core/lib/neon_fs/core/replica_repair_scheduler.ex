defmodule NeonFS.Core.ReplicaRepairScheduler do
  @moduledoc """
  Periodic replica-repair scheduler.

  Walks the volume registry on a configurable interval and creates
  `ReplicaRepair` jobs via `JobTracker` for volumes whose last
  completed repair-pass exceeds the configured volume interval
  (default 24h).

  Mirrors `NeonFS.Core.ScrubScheduler` — same per-volume scheduling,
  same dedupe-against-already-running-job guard, same telemetry
  shape.

  ## Configuration

    * `:check_interval_ms` — how often to check volumes (default
      `3_600_000` = 1h).
    * `:volume_interval_seconds` — how stale a volume's last repair
      must be before a new pass is triggered (default `86_400` =
      24h).
    * `:job_tracker_mod` — injectable module for testing (default
      `NeonFS.Core.JobTracker`).
    * `:volume_registry_mod` — injectable module for testing
      (default `NeonFS.Core.VolumeRegistry`).

  ## Telemetry

    * `[:neonfs, :replica_repair, :tick]` — fired at the end of
      every tick cycle. Metadata: `volumes_checked`, `jobs_queued`.
    * `[:neonfs, :replica_repair_scheduler, :triggered]` — a
      replica-repair job was created. Metadata: `job_id`,
      `volume_id`.
    * `[:neonfs, :replica_repair_scheduler, :skipped]` — tick
      skipped because a repair job is already running for the
      volume. Metadata: `reason`, `volume_id`.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Job.Runners.ReplicaRepair, as: ReplicaRepairRunner
  alias NeonFS.Core.{JobTracker, VolumeRegistry}

  @default_check_interval_ms 3_600_000
  @default_volume_interval_seconds 86_400

  @doc """
  Start the scheduler.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns the current scheduler status — useful for `cluster repair
  status` (sub-issue #709).
  """
  @spec status() :: map()
  def status do
    GenServer.call(__MODULE__, :status)
  end

  @doc """
  Trigger an immediate repair pass for `volume_id`. Used by the
  membership-change auto-trigger (sub-issue #708) and the operator
  CLI (sub-issue #709).

  Idempotent: if a repair job is already running for the volume,
  the trigger is skipped.
  """
  @spec trigger_now(String.t()) :: {:ok, map()} | {:skipped, :already_running}
  def trigger_now(volume_id) do
    GenServer.call(__MODULE__, {:trigger_now, volume_id})
  end

  @impl true
  def init(opts) do
    state = %{
      check_interval_ms: Keyword.get(opts, :check_interval_ms, @default_check_interval_ms),
      volume_interval_seconds:
        Keyword.get(opts, :volume_interval_seconds, @default_volume_interval_seconds),
      job_tracker_mod: Keyword.get(opts, :job_tracker_mod, JobTracker),
      volume_registry_mod: Keyword.get(opts, :volume_registry_mod, VolumeRegistry),
      volume_repair_times: %{}
    }

    state = initialise_repair_times(state)
    schedule_tick(state)

    Logger.info(
      "ReplicaRepairScheduler started: check_interval_ms=#{state.check_interval_ms} " <>
        "volume_interval_seconds=#{state.volume_interval_seconds}"
    )

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    reply = %{
      check_interval_ms: state.check_interval_ms,
      volume_interval_seconds: state.volume_interval_seconds,
      volume_repair_times: state.volume_repair_times
    }

    {:reply, reply, state}
  end

  def handle_call({:trigger_now, volume_id}, _from, state) do
    if running?(state, volume_id) do
      emit_skipped(volume_id)
      {:reply, {:skipped, :already_running}, state}
    else
      case create_job(state, volume_id) do
        {:ok, job, state} -> {:reply, {:ok, job}, state}
        {:error, state} -> {:reply, {:skipped, :already_running}, state}
      end
    end
  end

  @impl true
  def handle_info(:tick, state) do
    Logger.metadata(component: :scheduler, scheduler: :replica_repair)
    {state, summary} = check_all_volumes(state)
    schedule_tick(state)

    :telemetry.execute(
      [:neonfs, :replica_repair, :tick],
      %{},
      %{
        volumes_checked: summary.volumes_checked,
        jobs_queued: summary.jobs_queued
      }
    )

    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Initialisation — seed `volume_repair_times` from already-completed jobs
  defp initialise_repair_times(state) do
    completed = state.job_tracker_mod.list(status: :completed, type: ReplicaRepairRunner)
    times = Enum.reduce(completed, %{}, &update_latest_time/2)
    %{state | volume_repair_times: times}
  end

  defp update_latest_time(job, acc) do
    volume_id = volume_id_from(job)
    if volume_id, do: keep_latest(acc, volume_id, job.completed_at), else: acc
  end

  defp keep_latest(acc, volume_id, completed_at) do
    case Map.get(acc, volume_id) do
      nil ->
        Map.put(acc, volume_id, completed_at)

      existing ->
        if DateTime.compare(completed_at, existing) == :gt do
          Map.put(acc, volume_id, completed_at)
        else
          acc
        end
    end
  end

  ## Per-tick volume walk
  defp check_all_volumes(state) do
    volumes = state.volume_registry_mod.list()
    now = DateTime.utc_now()

    {final_state, queued} =
      Enum.reduce(volumes, {state, 0}, fn volume, {acc, queued_count} ->
        case maybe_repair(acc, volume, now) do
          {:queued, acc2} -> {acc2, queued_count + 1}
          {:skipped, acc2} -> {acc2, queued_count}
        end
      end)

    {final_state, %{volumes_checked: length(volumes), jobs_queued: queued}}
  end

  defp maybe_repair(state, volume, now) do
    cond do
      not due?(state, volume, now) -> {:skipped, state}
      running?(state, volume.id) -> handle_running(state, volume.id)
      true -> handle_create(state, volume.id)
    end
  end

  defp handle_running(state, volume_id) do
    emit_skipped(volume_id)
    {:skipped, state}
  end

  defp handle_create(state, volume_id) do
    case create_job(state, volume_id) do
      {:ok, _job, state} -> {:queued, state}
      {:error, state} -> {:skipped, state}
    end
  end

  defp due?(state, volume, now) do
    case Map.get(state.volume_repair_times, volume.id) do
      nil -> true
      last_at -> DateTime.diff(now, last_at, :second) >= state.volume_interval_seconds
    end
  end

  defp running?(state, volume_id) do
    state.job_tracker_mod.list(status: :running, type: ReplicaRepairRunner)
    |> Enum.any?(&(volume_id_from(&1) == volume_id))
  end

  defp create_job(state, volume_id) do
    case state.job_tracker_mod.create(ReplicaRepairRunner, %{volume_id: volume_id}) do
      {:ok, job} ->
        :telemetry.execute(
          [:neonfs, :replica_repair_scheduler, :triggered],
          %{},
          %{job_id: job.id, volume_id: volume_id}
        )

        Logger.info("ReplicaRepairScheduler triggered repair job",
          job_id: job.id,
          volume_id: volume_id
        )

        updated = Map.put(state.volume_repair_times, volume_id, DateTime.utc_now())
        {:ok, job, %{state | volume_repair_times: updated}}

      {:error, reason} ->
        Logger.warning("ReplicaRepairScheduler failed to create repair job",
          volume_id: volume_id,
          reason: inspect(reason)
        )

        {:error, state}
    end
  end

  defp volume_id_from(job), do: job.params[:volume_id] || job.params["volume_id"]

  defp emit_skipped(volume_id) do
    :telemetry.execute(
      [:neonfs, :replica_repair_scheduler, :skipped],
      %{},
      %{reason: :already_running, volume_id: volume_id}
    )
  end

  defp schedule_tick(state) do
    Process.send_after(self(), :tick, state.check_interval_ms)
  end
end
