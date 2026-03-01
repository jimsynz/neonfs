defmodule NeonFS.Core.ScrubScheduler do
  @moduledoc """
  Periodic integrity scrub scheduler.

  Checks all volumes on a configurable interval and creates scrub jobs via
  `JobTracker` for volumes whose `verification.scrub_interval` has elapsed
  since their last completed scrub.

  Unlike `GCScheduler` which runs on a single global interval, ScrubScheduler
  needs per-volume scheduling because each volume has its own `scrub_interval`.
  The check interval (`check_interval_ms`) controls how often the scheduler
  evaluates whether any volume is due — this should be shorter than any
  individual volume's scrub interval (default 1 hour check vs default 7 day
  scrub interval).

  Skips job creation when a scrub job for the same volume is already running
  to avoid duplicate concurrent runs.

  ## Configuration

    * `:check_interval_ms` — how often to check volumes (default: 3_600_000 = 1h)
    * `:job_tracker_mod` — injectable module for testing (default: `NeonFS.Core.JobTracker`)
    * `:volume_registry_mod` — injectable module for testing (default: `NeonFS.Core.VolumeRegistry`)

  ## Telemetry Events

    * `[:neonfs, :scrub_scheduler, :tick]` — emitted at the end of every tick cycle
    * `[:neonfs, :scrub_scheduler, :triggered]` — a scrub job was created
    * `[:neonfs, :scrub_scheduler, :skipped]` — tick skipped because a scrub job is already running
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Job.Runners.Scrub
  alias NeonFS.Core.JobTracker
  alias NeonFS.Core.VolumeRegistry

  # Client API

  @doc """
  Starts the ScrubScheduler GenServer.

  ## Options

    * `:check_interval_ms` — interval between volume checks in milliseconds (default: 3_600_000 = 1h)
    * `:job_tracker_mod` — module implementing `create/2` and `list/1` (default: `JobTracker`)
    * `:volume_registry_mod` — module implementing `list/0` (default: `VolumeRegistry`)
    * `:name` — GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Returns the current scheduler status.

  Includes `:check_interval_ms` and per-volume last scrub info in
  `:volume_scrub_times`.
  """
  @spec status() :: map()
  def status do
    GenServer.call(__MODULE__, :status)
  end

  @doc """
  Triggers an immediate scrub for a specific volume.

  Applies the same duplicate-run check — if a scrub job for that volume is
  already running, the trigger is skipped.
  """
  @spec trigger_now(String.t()) :: {:ok, map()} | {:skipped, :already_running}
  def trigger_now(volume_id) do
    GenServer.call(__MODULE__, {:trigger_now, volume_id})
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    state = %{
      check_interval_ms: Keyword.get(opts, :check_interval_ms, 3_600_000),
      job_tracker_mod: Keyword.get(opts, :job_tracker_mod, JobTracker),
      volume_registry_mod: Keyword.get(opts, :volume_registry_mod, VolumeRegistry),
      volume_scrub_times: %{}
    }

    state = initialise_scrub_times(state)
    schedule_tick(state)

    Logger.info("ScrubScheduler started", check_interval_ms: state.check_interval_ms)

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    result = %{
      check_interval_ms: state.check_interval_ms,
      volume_scrub_times: state.volume_scrub_times
    }

    {:reply, result, state}
  end

  def handle_call({:trigger_now, volume_id}, _from, state) do
    if scrub_running_for_volume?(state, volume_id) do
      :telemetry.execute(
        [:neonfs, :scrub_scheduler, :skipped],
        %{},
        %{reason: :already_running, volume_id: volume_id}
      )

      {:reply, {:skipped, :already_running}, state}
    else
      case create_scrub_job(state, volume_id) do
        {:ok, job, state} ->
          {:reply, {:ok, job}, state}

        {:error, state} ->
          {:reply, {:skipped, :already_running}, state}
      end
    end
  end

  @impl true
  def handle_info(:tick, state) do
    Logger.metadata(component: :scheduler, scheduler: :scrub)
    state = check_all_volumes(state)
    schedule_tick(state)
    :telemetry.execute([:neonfs, :scrub_scheduler, :tick], %{}, %{})
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  # Private — Initialisation

  defp initialise_scrub_times(state) do
    completed_scrubs =
      state.job_tracker_mod.list(status: :completed, type: Scrub)

    times =
      Enum.reduce(completed_scrubs, %{}, fn job, acc ->
        update_latest_scrub_time(acc, job)
      end)

    %{state | volume_scrub_times: times}
  end

  defp update_latest_scrub_time(acc, job) do
    volume_id = job.params[:volume_id] || job.params["volume_id"]

    if volume_id do
      existing = Map.get(acc, volume_id)
      keep_latest_time(acc, volume_id, job.completed_at, existing)
    else
      acc
    end
  end

  defp keep_latest_time(acc, volume_id, completed_at, nil) do
    Map.put(acc, volume_id, completed_at)
  end

  defp keep_latest_time(acc, volume_id, completed_at, existing) do
    if DateTime.compare(completed_at, existing) == :gt do
      Map.put(acc, volume_id, completed_at)
    else
      acc
    end
  end

  # Private — Volume checking

  defp check_all_volumes(state) do
    volumes = state.volume_registry_mod.list()
    now = DateTime.utc_now()

    Enum.reduce(volumes, state, fn volume, acc ->
      maybe_scrub_volume(acc, volume, now)
    end)
  end

  defp maybe_scrub_volume(state, volume, now) do
    scrub_interval_seconds = get_scrub_interval(volume)
    last_scrub = Map.get(state.volume_scrub_times, volume.id)

    if scrub_due?(last_scrub, scrub_interval_seconds, now) do
      attempt_scrub(state, volume.id)
    else
      state
    end
  end

  defp scrub_due?(nil, _interval_seconds, _now), do: true

  defp scrub_due?(last_scrub, interval_seconds, now) do
    next_due = DateTime.add(last_scrub, interval_seconds, :second)
    DateTime.compare(now, next_due) != :lt
  end

  defp get_scrub_interval(%{verification: %{scrub_interval: interval}})
       when is_integer(interval) and interval > 0 do
    interval
  end

  defp get_scrub_interval(_volume), do: 2_592_000

  defp attempt_scrub(state, volume_id) do
    if scrub_running_for_volume?(state, volume_id) do
      :telemetry.execute(
        [:neonfs, :scrub_scheduler, :skipped],
        %{},
        %{reason: :already_running, volume_id: volume_id}
      )

      Logger.debug("ScrubScheduler skipped volume, scrub already running",
        volume_id: volume_id
      )

      state
    else
      case create_scrub_job(state, volume_id) do
        {:ok, _job, state} -> state
        {:error, state} -> state
      end
    end
  end

  defp create_scrub_job(state, volume_id) do
    case state.job_tracker_mod.create(Scrub, %{volume_id: volume_id}) do
      {:ok, job} ->
        now = DateTime.utc_now()

        :telemetry.execute(
          [:neonfs, :scrub_scheduler, :triggered],
          %{},
          %{job_id: job.id, volume_id: volume_id}
        )

        Logger.info("ScrubScheduler triggered scrub job",
          job_id: job.id,
          volume_id: volume_id
        )

        updated_times = Map.put(state.volume_scrub_times, volume_id, now)
        {:ok, job, %{state | volume_scrub_times: updated_times}}

      {:error, reason} ->
        Logger.warning("ScrubScheduler failed to create scrub job",
          volume_id: volume_id,
          reason: inspect(reason)
        )

        {:error, state}
    end
  end

  defp scrub_running_for_volume?(state, volume_id) do
    state.job_tracker_mod.list(status: :running, type: Scrub)
    |> Enum.any?(fn job ->
      job_volume_id = job.params[:volume_id] || job.params["volume_id"]
      job_volume_id == volume_id
    end)
  end

  defp schedule_tick(state) do
    Process.send_after(self(), :tick, state.check_interval_ms)
  end
end
