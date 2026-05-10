defmodule NeonFS.Core.VolumeAntiEntropyScheduler do
  @moduledoc """
  Per-volume anti-entropy scheduler (#921). Mirrors
  `NeonFS.Core.VolumeScrubScheduler` / `NeonFS.Core.VolumeGCScheduler`.

  On every tick the scheduler walks `VolumeRegistry.list/0`, reads
  each volume's `RootSegment.schedules.anti_entropy.{interval_ms,
  last_run}` via `Volume.MetadataReader`, and dispatches a per-volume
  `Job.Runners.VolumeAntiEntropy` job for any volume whose cadence
  has elapsed.

  After a successful dispatch the scheduler persists the new
  `last_run` into the volume's root segment via
  `Volume.MetadataWriter.update_schedule/4`. CAS retries inside the
  writer absorb the (rare) race against another core node firing
  for the same volume — losing the race is fine, the next tick
  re-checks.

  ## Configuration

    * `:tick_interval_ms` — how often the scheduler re-evaluates
      every volume's schedule (default: 3_600_000 = 1h). Per-volume
      cadence lives in the volume's root segment.
    * `:job_tracker_mod` — injectable for testing
      (default: `NeonFS.Core.JobTracker`).
    * `:volume_registry_mod` — injectable for testing
      (default: `NeonFS.Core.VolumeRegistry`).
    * `:metadata_reader_mod` / `:metadata_writer_mod` — injectable
      for testing.
    * `:metadata_reader_opts` / `:metadata_writer_opts` — keyword
      lists threaded into reader/writer calls.

  ## Telemetry

    * `[:neonfs, :volume_anti_entropy_scheduler, :triggered]` —
      `%{}, %{volume_id, job_id, reason: :scheduled | :manual}`
    * `[:neonfs, :volume_anti_entropy_scheduler, :skipped]` —
      `%{}, %{volume_id, reason: :already_running | :write_failed}`
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Job.Runners.VolumeAntiEntropy
  alias NeonFS.Core.{JobTracker, VolumeRegistry}
  alias NeonFS.Core.Volume.{MetadataReader, MetadataWriter}

  @default_tick_ms 3_600_000
  @default_interval_ms 3_600_000

  ## Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Triggers an immediate anti-entropy job for `volume_id`. Skips if a
  `VolumeAntiEntropy` job for that volume is already running.
  """
  @spec trigger_now(binary(), GenServer.server()) ::
          {:ok, map()} | {:skipped, :already_running} | {:error, term()}
  def trigger_now(volume_id, server \\ __MODULE__) when is_binary(volume_id) do
    GenServer.call(server, {:trigger_now, volume_id})
  end

  @spec status(GenServer.server()) :: map()
  def status(server \\ __MODULE__) do
    GenServer.call(server, :status)
  end

  ## GenServer callbacks

  @impl true
  def init(opts) do
    state = %{
      tick_interval_ms: Keyword.get(opts, :tick_interval_ms, @default_tick_ms),
      job_tracker_mod: Keyword.get(opts, :job_tracker_mod, JobTracker),
      volume_registry_mod: Keyword.get(opts, :volume_registry_mod, VolumeRegistry),
      metadata_reader_mod: Keyword.get(opts, :metadata_reader_mod, MetadataReader),
      metadata_writer_mod: Keyword.get(opts, :metadata_writer_mod, MetadataWriter),
      metadata_reader_opts: Keyword.get(opts, :metadata_reader_opts, []),
      metadata_writer_opts: Keyword.get(opts, :metadata_writer_opts, []),
      last_tick_at: nil
    }

    schedule_tick(state)

    Logger.info("VolumeAntiEntropyScheduler started: tick=#{state.tick_interval_ms}ms")

    {:ok, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    {:reply, Map.take(state, [:tick_interval_ms, :last_tick_at]), state}
  end

  def handle_call({:trigger_now, volume_id}, _from, state) do
    {:reply, dispatch_one(state, volume_id, :manual), state}
  end

  @impl true
  def handle_info(:tick, state) do
    Logger.metadata(component: :scheduler, scheduler: :volume_anti_entropy)
    state = run_scheduled_tick(state)
    schedule_tick(state)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Internal — Scheduled tick

  defp run_scheduled_tick(state) do
    Enum.each(state.volume_registry_mod.list(), fn volume -> consider_volume(state, volume) end)
    %{state | last_tick_at: DateTime.utc_now()}
  end

  defp consider_volume(state, %{id: volume_id}) do
    with {:ok, segment, _root_entry} <- read_segment(state, volume_id),
         schedule = Map.get(segment.schedules, :anti_entropy),
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

  ## Internal — Dispatch

  defp dispatch_one(state, volume_id, reason) do
    if anti_entropy_running?(state, volume_id) do
      :telemetry.execute([:neonfs, :volume_anti_entropy_scheduler, :skipped], %{}, %{
        volume_id: volume_id,
        reason: :already_running
      })

      {:skipped, :already_running}
    else
      do_dispatch(state, volume_id, reason)
    end
  end

  defp do_dispatch(state, volume_id, reason) do
    case state.job_tracker_mod.create(VolumeAntiEntropy, %{volume_id: volume_id}) do
      {:ok, job} ->
        :telemetry.execute([:neonfs, :volume_anti_entropy_scheduler, :triggered], %{}, %{
          volume_id: volume_id,
          job_id: job.id,
          reason: reason
        })

        _ = touch_last_run(state, volume_id)
        {:ok, job}

      {:error, dispatch_reason} = err ->
        Logger.warning("VolumeAntiEntropyScheduler failed to create job",
          volume_id: volume_id,
          reason: inspect(dispatch_reason)
        )

        err
    end
  end

  defp anti_entropy_running?(state, volume_id) do
    state.job_tracker_mod.list(status: :running, type: VolumeAntiEntropy)
    |> Enum.any?(fn job -> Map.get(job.params || %{}, :volume_id) == volume_id end)
  end

  defp touch_last_run(state, volume_id) do
    schedule = %{interval_ms: current_interval(state, volume_id), last_run: DateTime.utc_now()}

    case state.metadata_writer_mod.update_schedule(
           volume_id,
           :anti_entropy,
           schedule,
           state.metadata_writer_opts
         ) do
      {:ok, _root} ->
        :ok

      err ->
        :telemetry.execute([:neonfs, :volume_anti_entropy_scheduler, :skipped], %{}, %{
          volume_id: volume_id,
          reason: :write_failed
        })

        Logger.warning("VolumeAntiEntropyScheduler last_run update failed",
          volume_id: volume_id,
          reason: inspect(err)
        )

        err
    end
  end

  defp current_interval(state, volume_id) do
    case read_segment(state, volume_id) do
      {:ok, segment, _root_entry} ->
        Map.get(segment.schedules, :anti_entropy, %{})
        |> Map.get(:interval_ms, @default_interval_ms)

      _ ->
        @default_interval_ms
    end
  end

  defp read_segment(state, volume_id) do
    state.metadata_reader_mod.resolve_segment_for_write(volume_id, state.metadata_reader_opts)
  end

  ## Internal — Timers

  defp schedule_tick(%{tick_interval_ms: ms}) do
    Process.send_after(self(), :tick, ms)
  end
end
