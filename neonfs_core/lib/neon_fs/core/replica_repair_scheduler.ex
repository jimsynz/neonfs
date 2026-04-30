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
  @default_membership_rate_limit_ms 60_000

  @telemetry_handler_id_prefix "neonfs-replica-repair-scheduler-membership"

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
  Trigger an immediate repair pass for `scope`.

  `scope` can be:

    * a `volume_id` string — queue one job for that volume.
    * a `[volume_id, ...]` list — queue one job per listed volume.
    * `:all` — queue one job per volume in the registry.

  Idempotent: if a repair job is already running for a target
  volume, that volume is skipped (running-job dedupe). Calls
  arriving within the membership rate-limit window collapse
  to one effective trigger per scope so a flapping membership
  storm doesn't churn repair jobs (see #708).
  """
  @spec trigger_now(String.t() | [String.t()] | :all) ::
          {:ok, [map()]} | {:skipped, :rate_limited | :already_running}
  def trigger_now(scope \\ :all) do
    GenServer.call(__MODULE__, {:trigger_now, scope})
  end

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    name = Keyword.get(opts, :name, __MODULE__)

    state = %{
      name: name,
      check_interval_ms: Keyword.get(opts, :check_interval_ms, @default_check_interval_ms),
      volume_interval_seconds:
        Keyword.get(opts, :volume_interval_seconds, @default_volume_interval_seconds),
      membership_rate_limit_ms:
        Keyword.get(opts, :membership_rate_limit_ms, @default_membership_rate_limit_ms),
      job_tracker_mod: Keyword.get(opts, :job_tracker_mod, JobTracker),
      volume_registry_mod: Keyword.get(opts, :volume_registry_mod, VolumeRegistry),
      volume_repair_times: %{},
      last_membership_trigger_at: nil
    }

    state = initialise_repair_times(state)
    schedule_tick(state)
    attach_membership_telemetry(name)

    Logger.info(
      "ReplicaRepairScheduler started: check_interval_ms=#{state.check_interval_ms} " <>
        "volume_interval_seconds=#{state.volume_interval_seconds} " <>
        "membership_rate_limit_ms=#{state.membership_rate_limit_ms}"
    )

    {:ok, state}
  end

  @impl true
  def terminate(_reason, state) do
    detach_membership_telemetry(state.name)
    :ok
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

  def handle_call({:trigger_now, scope}, _from, state) do
    target_ids = resolve_scope(state, scope)

    {results, state} =
      Enum.reduce(target_ids, {[], state}, fn vid, {acc, st} ->
        process_trigger_volume(st, vid, acc)
      end)

    case Enum.reverse(results) do
      [] -> {:reply, {:skipped, :already_running}, state}
      jobs -> {:reply, {:ok, jobs}, state}
    end
  end

  defp process_trigger_volume(state, volume_id, acc) do
    if running?(state, volume_id) do
      emit_skipped(volume_id)
      {acc, state}
    else
      case create_job(state, volume_id) do
        {:ok, job, st2} -> {[job | acc], st2}
        {:error, st2} -> {acc, st2}
      end
    end
  end

  @impl true
  def handle_cast({:membership_event, _metadata}, state) do
    now_ms = System.monotonic_time(:millisecond)

    if rate_limited?(state, now_ms) do
      :telemetry.execute(
        [:neonfs, :replica_repair_scheduler, :membership_rate_limited],
        %{},
        %{}
      )

      {:noreply, state}
    else
      target_ids = resolve_scope(state, :all)

      state = Enum.reduce(target_ids, state, &process_membership_volume(&2, &1))
      {:noreply, %{state | last_membership_trigger_at: now_ms}}
    end
  end

  defp process_membership_volume(state, volume_id) do
    if running?(state, volume_id) do
      emit_skipped(volume_id)
      state
    else
      case create_job(state, volume_id) do
        {:ok, _job, st2} -> st2
        {:error, st2} -> st2
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

  defp resolve_scope(_state, vid) when is_binary(vid), do: [vid]
  defp resolve_scope(_state, vids) when is_list(vids), do: vids

  defp resolve_scope(state, :all) do
    state.volume_registry_mod.list()
    |> Enum.map(& &1.id)
  end

  defp rate_limited?(%{last_membership_trigger_at: nil}, _now_ms), do: false

  defp rate_limited?(state, now_ms) do
    now_ms - state.last_membership_trigger_at < state.membership_rate_limit_ms
  end

  defp telemetry_handler_id(name) do
    "#{@telemetry_handler_id_prefix}-#{inspect(name)}"
  end

  defp attach_membership_telemetry(name) do
    pid = self()

    :telemetry.attach(
      telemetry_handler_id(name),
      [:neonfs, :service_registry, :service_deregistered],
      fn _event, _measurements, metadata, _config ->
        # Only react to a core service leaving — fuse / nfs / etc.
        # don't change the chunk-replica picture.
        if metadata[:type] in [:core, nil] do
          GenServer.cast(pid, {:membership_event, metadata})
        end
      end,
      nil
    )

    :ok
  end

  defp detach_membership_telemetry(name) do
    :telemetry.detach(telemetry_handler_id(name))
    :ok
  rescue
    _ -> :ok
  end
end
