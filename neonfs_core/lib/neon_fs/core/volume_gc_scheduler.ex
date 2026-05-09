defmodule NeonFS.Core.VolumeGCScheduler do
  @moduledoc """
  Per-volume GC scheduler. Replaces the global `GCScheduler` tick.

  On every tick the scheduler walks `VolumeRegistry.list/0`, reads
  each volume's `RootSegment.schedules.gc.{interval_ms, last_run}`
  via `Volume.MetadataReader`, and dispatches a `GarbageCollection`
  JobTracker job (scoped to that `volume_id`) for any volume whose
  cadence has elapsed.

  Successful dispatch updates `last_run` in the volume's root segment
  via `Volume.MetadataWriter.update_schedule/4`. The CAS retry inside
  the writer handles the (rare) race against another core node
  dispatching for the same volume — losing the race is fine, the
  next tick re-evaluates.

  ## Pressure check

  A separate timer runs `StorageMetrics.cluster_capacity/0`; when the
  used/cap ratio exceeds `:pressure_threshold`, every volume gets a
  GC job regardless of its `last_run`. Mirrors the global
  `GCScheduler` pressure path.

  ## Configuration

    * `:tick_interval_ms` — how often the scheduler re-evaluates every
      volume's schedule (default: 60_000 = 1 min). Per-volume cadence
      lives in the volume's root segment.
    * `:pressure_check_interval_ms` — pressure check cadence (default:
      300_000 = 5 min).
    * `:pressure_threshold` — usage ratio that triggers GC fan-out
      (default: 0.85).
    * `:job_tracker_mod` — injectable for testing
      (default: `NeonFS.Core.JobTracker`).
    * `:storage_metrics_mod` — injectable for testing
      (default: `NeonFS.Core.StorageMetrics`).
    * `:volume_registry_mod` — injectable for testing
      (default: `NeonFS.Core.VolumeRegistry`).
    * `:metadata_reader_mod` / `:metadata_writer_mod` — injectable
      for testing (default: `NeonFS.Core.Volume.MetadataReader` /
      `NeonFS.Core.Volume.MetadataWriter`).
    * `:metadata_reader_opts` / `:metadata_writer_opts` — keyword
      lists threaded into reader/writer calls.

  ## Telemetry

    * `[:neonfs, :volume_gc_scheduler, :triggered]` —
      `%{}, %{volume_id, job_id, reason: :scheduled | :pressure}`
    * `[:neonfs, :volume_gc_scheduler, :skipped]` —
      `%{}, %{volume_id, reason: :already_running | :not_due | :read_failed | :write_failed}`
    * `[:neonfs, :volume_gc_scheduler, :pressure_check]` —
      `%{ratio}, %{}` (also emits one `:triggered` per dispatched volume)
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Job.Runners.GarbageCollection
  alias NeonFS.Core.{JobTracker, StorageMetrics, VolumeRegistry}
  alias NeonFS.Core.Volume.{MetadataReader, MetadataWriter}

  @default_tick_ms 60_000
  @default_pressure_check_ms 300_000
  @default_pressure_threshold 0.85

  ## Client API

  @doc """
  Starts the scheduler. See moduledoc for options.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Triggers an immediate GC job for `volume_id`. Skips if a
  `GarbageCollection` job for that volume is already running.
  """
  @spec trigger_now(binary(), GenServer.server()) ::
          {:ok, map()} | {:skipped, :already_running} | {:error, term()}
  def trigger_now(volume_id, server \\ __MODULE__) when is_binary(volume_id) do
    GenServer.call(server, {:trigger_now, volume_id})
  end

  @doc """
  Returns scheduler status for observability.
  """
  @spec status(GenServer.server()) :: map()
  def status(server \\ __MODULE__) do
    GenServer.call(server, :status)
  end

  ## GenServer callbacks

  @impl true
  def init(opts) do
    state = %{
      tick_interval_ms: Keyword.get(opts, :tick_interval_ms, @default_tick_ms),
      pressure_check_interval_ms:
        Keyword.get(opts, :pressure_check_interval_ms, @default_pressure_check_ms),
      pressure_threshold: Keyword.get(opts, :pressure_threshold, @default_pressure_threshold),
      job_tracker_mod: Keyword.get(opts, :job_tracker_mod, JobTracker),
      storage_metrics_mod: Keyword.get(opts, :storage_metrics_mod, StorageMetrics),
      volume_registry_mod: Keyword.get(opts, :volume_registry_mod, VolumeRegistry),
      metadata_reader_mod: Keyword.get(opts, :metadata_reader_mod, MetadataReader),
      metadata_writer_mod: Keyword.get(opts, :metadata_writer_mod, MetadataWriter),
      metadata_reader_opts: Keyword.get(opts, :metadata_reader_opts, []),
      metadata_writer_opts: Keyword.get(opts, :metadata_writer_opts, []),
      last_tick_at: nil,
      last_pressure_check_at: nil,
      last_pressure_ratio: nil
    }

    schedule_tick(state)
    schedule_pressure_check(state)

    Logger.info(
      "VolumeGCScheduler started: tick=#{state.tick_interval_ms}ms " <>
        "pressure_check=#{state.pressure_check_interval_ms}ms " <>
        "pressure_threshold=#{state.pressure_threshold}"
    )

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    fields = [
      :tick_interval_ms,
      :pressure_threshold,
      :pressure_check_interval_ms,
      :last_tick_at,
      :last_pressure_check_at,
      :last_pressure_ratio
    ]

    {:reply, Map.take(state, fields), state}
  end

  def handle_call({:trigger_now, volume_id}, _from, state) do
    {:reply, dispatch_one(state, volume_id, :manual), state}
  end

  @impl true
  def handle_info(:tick, state) do
    Logger.metadata(component: :scheduler, scheduler: :volume_gc)
    state = run_scheduled_tick(state)
    schedule_tick(state)
    {:noreply, state}
  end

  def handle_info(:pressure_check, state) do
    Logger.metadata(component: :scheduler, scheduler: :volume_gc_pressure)
    state = check_storage_pressure(state)
    schedule_pressure_check(state)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Internal — Scheduled tick

  defp run_scheduled_tick(state) do
    state.volume_registry_mod.list()
    |> Enum.each(fn volume -> consider_volume(state, volume) end)

    %{state | last_tick_at: DateTime.utc_now()}
  end

  defp consider_volume(state, %{id: volume_id}) do
    with {:ok, segment, _root_entry} <- read_segment(state, volume_id),
         schedule = Map.get(segment.schedules, :gc),
         true <- due?(schedule) do
      case dispatch_one(state, volume_id, :scheduled) do
        {:ok, _job} -> :ok
        _ -> :ok
      end
    else
      _ -> :ok
    end
  end

  defp due?(%{interval_ms: interval, last_run: nil}) when is_integer(interval) and interval > 0,
    do: true

  defp due?(%{interval_ms: interval, last_run: %DateTime{} = last})
       when is_integer(interval) and interval > 0 do
    DateTime.diff(DateTime.utc_now(), last, :millisecond) >= interval
  end

  defp due?(_), do: false

  ## Internal — Pressure check

  defp check_storage_pressure(state) do
    now = DateTime.utc_now()
    capacity = state.storage_metrics_mod.cluster_capacity()

    case compute_ratio(capacity) do
      :unlimited ->
        %{state | last_pressure_check_at: now}

      ratio ->
        :telemetry.execute([:neonfs, :volume_gc_scheduler, :pressure_check], %{ratio: ratio}, %{})

        state = %{state | last_pressure_check_at: now, last_pressure_ratio: ratio}

        if ratio >= state.pressure_threshold do
          fan_out_pressure(state)
        end

        state
    end
  end

  defp fan_out_pressure(state) do
    Enum.each(state.volume_registry_mod.list(), fn %{id: volume_id} ->
      dispatch_one(state, volume_id, :pressure)
    end)
  end

  defp compute_ratio(%{total_capacity: :unlimited}), do: :unlimited
  defp compute_ratio(%{total_capacity: 0}), do: :unlimited
  defp compute_ratio(%{total_used: used, total_capacity: cap}), do: used / cap

  ## Internal — Dispatch

  defp dispatch_one(state, volume_id, reason) do
    if gc_job_running?(state, volume_id) do
      :telemetry.execute([:neonfs, :volume_gc_scheduler, :skipped], %{}, %{
        volume_id: volume_id,
        reason: :already_running
      })

      {:skipped, :already_running}
    else
      do_dispatch(state, volume_id, reason)
    end
  end

  defp do_dispatch(state, volume_id, reason) do
    case state.job_tracker_mod.create(GarbageCollection, %{volume_id: volume_id}) do
      {:ok, job} ->
        :telemetry.execute([:neonfs, :volume_gc_scheduler, :triggered], %{}, %{
          volume_id: volume_id,
          job_id: job.id,
          reason: reason
        })

        _ = touch_last_run(state, volume_id)
        {:ok, job}

      {:error, dispatch_reason} = err ->
        Logger.warning("VolumeGCScheduler failed to create GC job",
          volume_id: volume_id,
          reason: inspect(dispatch_reason)
        )

        err
    end
  end

  defp gc_job_running?(state, volume_id) do
    state.job_tracker_mod.list(status: :running, type: GarbageCollection)
    |> Enum.any?(fn job -> Map.get(job.params || %{}, :volume_id) == volume_id end)
  end

  defp touch_last_run(state, volume_id) do
    schedule = %{interval_ms: current_interval(state, volume_id), last_run: DateTime.utc_now()}

    case state.metadata_writer_mod.update_schedule(
           volume_id,
           :gc,
           schedule,
           state.metadata_writer_opts
         ) do
      {:ok, _root} ->
        :ok

      err ->
        :telemetry.execute([:neonfs, :volume_gc_scheduler, :skipped], %{}, %{
          volume_id: volume_id,
          reason: :write_failed
        })

        Logger.warning("VolumeGCScheduler last_run update failed",
          volume_id: volume_id,
          reason: inspect(err)
        )

        err
    end
  end

  defp current_interval(state, volume_id) do
    case read_segment(state, volume_id) do
      {:ok, segment, _root_entry} ->
        Map.get(segment.schedules, :gc, %{}) |> Map.get(:interval_ms, 86_400_000)

      _ ->
        86_400_000
    end
  end

  defp read_segment(state, volume_id) do
    state.metadata_reader_mod.resolve_segment_for_write(volume_id, state.metadata_reader_opts)
  end

  ## Internal — Timers

  defp schedule_tick(%{tick_interval_ms: ms}) do
    Process.send_after(self(), :tick, ms)
  end

  defp schedule_pressure_check(%{pressure_check_interval_ms: ms}) do
    Process.send_after(self(), :pressure_check, ms)
  end
end
