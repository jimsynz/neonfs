defmodule NeonFS.Containerd.WriteReaper do
  @moduledoc """
  Periodically reclaims stalled or abandoned `WriteSession`s.

  A `WriteSession` deliberately outlives any single bidi-stream
  connection so containerd can resume a partial write with the same
  `ref`. The cost is that an ingest the client abandons — a cancelled
  or retried pull whose stream simply stops — leaves its session (and
  the `ChunkWriter` task it owns) running indefinitely. Nothing else
  calls `WriteSession.abort_stale/1`, so without this reaper those
  sessions leak (#1354).

  The reaper sweeps on a fixed interval, aborting every session whose
  `updated_at` is older than the configured max age. Active sessions —
  anything that has fed data within the window — are left alone, so a
  slow-but-live pull is never interrupted.

  ## Configuration (`:neonfs_containerd` app env)

    * `:write_reaper_interval_ms` — sweep cadence (default 5 minutes).
    * `:write_session_max_age_seconds` — a session idle longer than this
      is reclaimed (default 24 hours).

  Both are internal safety-net defaults, not per-deployment tuning
  knobs; the cleanup is correctness, not policy.
  """

  use GenServer

  require Logger

  alias NeonFS.Containerd.WriteSession

  @default_interval_ms :timer.minutes(5)
  @default_max_age_seconds 86_400

  @doc """
  Start the reaper. Accepts `:interval_ms` and `:max_age_seconds`
  overrides (tests use tight values); both otherwise fall back to app
  env and then the module defaults.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: Keyword.get(opts, :name, __MODULE__))
  end

  @impl true
  def init(opts) do
    state = %{
      interval_ms: Keyword.get(opts, :interval_ms, config_interval_ms()),
      max_age_seconds: Keyword.get(opts, :max_age_seconds, config_max_age_seconds())
    }

    {:ok, schedule(state)}
  end

  @impl true
  def handle_info(:sweep, state) do
    reaped = WriteSession.abort_stale(state.max_age_seconds)

    if reaped != [] do
      Logger.info(
        "reclaimed #{length(reaped)} stale containerd write session(s): #{inspect(reaped)}"
      )
    end

    :telemetry.execute(
      [:neonfs, :containerd, :write_reaper, :sweep],
      %{reaped_count: length(reaped)},
      %{refs: reaped}
    )

    {:noreply, schedule(state)}
  end

  defp schedule(state) do
    Process.send_after(self(), :sweep, state.interval_ms)
    state
  end

  defp config_interval_ms do
    Application.get_env(:neonfs_containerd, :write_reaper_interval_ms, @default_interval_ms)
  end

  defp config_max_age_seconds do
    Application.get_env(
      :neonfs_containerd,
      :write_session_max_age_seconds,
      @default_max_age_seconds
    )
  end
end
