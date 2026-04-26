defmodule NeonFS.Core.DRSnapshotScheduler do
  @moduledoc """
  Periodic scheduler for DR snapshots — sub-issue #323 of #247.

  Wraps `NeonFS.Core.DRSnapshot.create/1` in a GenServer that fires
  on a configurable cadence (default daily) and applies a
  grandfather-father-son retention policy after each snapshot.

  ## Leader-only execution

  In a multi-node Ra cluster every member runs the scheduler GenServer
  (because the supervisor sits in every core node). Only the current
  Ra leader actually creates the snapshot; followers no-op the tick
  and reschedule. Without this check, a 3-node cluster would write
  three snapshots per interval, all racing each other on the
  `_system` volume.

  ## Retention policy

  The default policy keeps:

    * **7** daily snapshots — the newest per UTC day in the last
      7 days (rolling window).
    * **4** weekly snapshots — the newest per ISO week in the last
      4 weeks, taken from snapshots that fall outside the daily
      window.
    * **12** monthly snapshots — the newest per calendar month in
      the last 12 months, taken from snapshots that fall outside
      the weekly window.

  Every other snapshot is pruned. The classification is computed by
  `compute_retention/2`, a pure function unit-tested in
  `dr_snapshot_scheduler_test.exs`.

  ## Configuration

      config :neonfs_core, NeonFS.Core.DRSnapshotScheduler,
        interval_ms: 86_400_000,                # 24 h
        daily: 7, weekly: 4, monthly: 12,
        enabled: true

  Set `enabled: false` to skip scheduling entirely (useful in
  release configs where a different node should own the timer).

  ## Out of scope

  Cron-style "exactly 02:00 local" scheduling — the current
  interval timer is fine for production where the first run picks a
  random offset; opening that as a follow-up if operators ask for
  it. Same for manual / labelled snapshots that should override
  retention — DRSnapshot's manifest doesn't carry a class yet
  (#324 covers operator-issued snapshots).
  """

  use GenServer

  alias NeonFS.Core.{DRSnapshot, FileIndex, RaSupervisor, VolumeRegistry}

  require Logger

  @default_interval_ms 86_400_000
  @default_daily 7
  @default_weekly 4
  @default_monthly 12
  @snapshot_root "/dr"

  @typedoc "Bucket a snapshot is kept under, or `:prune` for deletion."
  @type retention_class :: :daily | :weekly | :monthly | :prune

  ## Client API

  @doc """
  Starts the scheduler. The supervisor that mounts it should pass
  `enabled: false` (via app env) when this node should not own the
  timer — typically all but one core node in a release.

  ## Options (also read from `Application.get_env/2`)

    * `:interval_ms` — milliseconds between ticks (default
      `#{@default_interval_ms}` = 24 h).
    * `:daily`, `:weekly`, `:monthly` — retention bucket sizes
      (defaults `#{@default_daily}` / `#{@default_weekly}` /
      `#{@default_monthly}`).
    * `:enabled` — when `false`, the scheduler starts but doesn't
      schedule any ticks (default `true`).
    * `:name` — GenServer name (default `__MODULE__`).
    * `:leader_check_fn` — `(-> boolean())` test seam; defaults to
      `&local_is_leader?/0`.
    * `:create_fn` — test seam; defaults to `&DRSnapshot.create/1`.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Triggers a tick synchronously and returns the result. Test-only —
  in production the timer drives ticks.
  """
  @spec run_now(GenServer.server()) ::
          {:ok, %{snapshot: term() | :skipped, pruned: [String.t()]}}
          | {:error, term()}
  def run_now(server \\ __MODULE__) do
    GenServer.call(server, :run_now, 60_000)
  end

  ## Pure retention math (public for testability)

  @doc """
  Classifies a list of snapshots into retention buckets given a
  reference time `now`. Returns a map of `id => retention_class`.

  Each snapshot is `%{id: String.t(), created_at: DateTime.t()}`.

  Algorithm: snapshots are sorted newest-first, then for each bucket
  in `[:daily, :weekly, :monthly]` we walk the list and keep the
  *first* snapshot we see whose calendar key (UTC day / ISO week /
  calendar month) hasn't been claimed yet, up to the bucket's size.
  Anything not picked by any bucket is `:prune`.

  This means a single snapshot can satisfy multiple buckets (the
  "father" of a week is also the "son" of its day) but is only ever
  *labelled* by the smallest bucket it qualifies for, so pruning
  reasons are deterministic.
  """
  @spec compute_retention([%{id: String.t(), created_at: DateTime.t()}], keyword()) ::
          %{String.t() => retention_class()}
  def compute_retention(snapshots, opts \\ []) do
    now = Keyword.get_lazy(opts, :now, &DateTime.utc_now/0)
    daily_n = Keyword.get(opts, :daily, @default_daily)
    weekly_n = Keyword.get(opts, :weekly, @default_weekly)
    monthly_n = Keyword.get(opts, :monthly, @default_monthly)

    sorted = Enum.sort_by(snapshots, & &1.created_at, {:desc, DateTime})

    {kept_dailies, sorted} = pick_per_key(sorted, &day_key/1, daily_n, now, :within_days, 7)
    {kept_weeklies, sorted} = pick_per_key(sorted, &week_key/1, weekly_n, now, :within_weeks, 4)

    {kept_monthlies, _sorted} =
      pick_per_key(sorted, &month_key/1, monthly_n, now, :within_months, 12)

    Map.new(snapshots, fn %{id: id} ->
      cond do
        id in kept_dailies -> {id, :daily}
        id in kept_weeklies -> {id, :weekly}
        id in kept_monthlies -> {id, :monthly}
        true -> {id, :prune}
      end
    end)
  end

  ## GenServer callbacks

  @impl true
  def init(opts) do
    cfg = read_config(opts)

    state = %{
      cfg: cfg,
      timer_ref: nil
    }

    state =
      if cfg.enabled do
        schedule_tick(state, cfg.interval_ms)
      else
        state
      end

    {:ok, state}
  end

  @impl true
  def handle_call(:run_now, _from, state) do
    {result, state} = do_tick(state)
    {:reply, {:ok, result}, state}
  end

  @impl true
  def handle_info(:tick, state) do
    {_result, state} = do_tick(state)
    state = schedule_tick(state, state.cfg.interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Internal — tick + retention + pruning

  defp do_tick(state) do
    if state.cfg.leader_check_fn.() do
      run_leader_tick(state)
    else
      Logger.debug("DRSnapshotScheduler tick skipped: not the Ra leader")
      {%{snapshot: :skipped, pruned: []}, state}
    end
  end

  defp run_leader_tick(state) do
    case state.cfg.create_fn.([]) do
      {:ok, snapshot} ->
        pruned = prune_old_snapshots(state.cfg)

        :telemetry.execute(
          [:neonfs, :dr_snapshot_scheduler, :tick],
          %{pruned_count: length(pruned)},
          %{snapshot_path: snapshot.path}
        )

        {%{snapshot: snapshot, pruned: pruned}, state}

      {:error, reason} ->
        Logger.warning("DRSnapshotScheduler.create failed", reason: inspect(reason))
        {%{snapshot: {:error, reason}, pruned: []}, state}
    end
  end

  defp prune_old_snapshots(cfg) do
    case list_snapshots() do
      {:ok, snapshots} ->
        opts = [daily: cfg.daily, weekly: cfg.weekly, monthly: cfg.monthly]
        classes = compute_retention(snapshots, opts)

        prune_ids =
          snapshots
          |> Enum.filter(fn s -> Map.get(classes, s.id) == :prune end)
          |> Enum.map(& &1.id)

        Enum.each(prune_ids, &delete_snapshot/1)
        prune_ids

      {:error, reason} ->
        Logger.warning("DRSnapshotScheduler could not list snapshots",
          reason: inspect(reason)
        )

        []
    end
  end

  defp list_snapshots do
    with {:ok, volume} <- VolumeRegistry.get_by_name("_system"),
         {:ok, children} <- FileIndex.list_dir(volume.id, @snapshot_root) do
      {:ok, snapshots_from_children(children)}
    end
  rescue
    # `VolumeRegistry.get_by_name/1` raises if its ETS table isn't
    # initialised (no Ra cluster running, e.g. unit tests). Treat as
    # "no snapshots known yet" so the scheduler doesn't crash on an
    # empty cold start.
    _ -> {:error, :unavailable}
  catch
    :exit, _ -> {:error, :unavailable}
  end

  defp snapshots_from_children(children) do
    children
    |> Enum.filter(fn {_name, %{type: type}} -> type == :dir end)
    |> Enum.flat_map(&snapshot_from_child/1)
  end

  defp snapshot_from_child({name, _info}) do
    case parse_timestamp(name) do
      {:ok, dt} -> [%{id: name, created_at: dt}]
      :error -> []
    end
  end

  defp delete_snapshot(id) do
    with {:ok, volume} <- VolumeRegistry.get_by_name("_system") do
      dir = Path.join(@snapshot_root, id)

      case FileIndex.list_dir(volume.id, dir) do
        {:ok, children} -> delete_dir_children(volume.id, dir, children)
        _ -> :ok
      end
    end
  end

  defp delete_dir_children(volume_id, dir, children) do
    Enum.each(children, fn {name, _info} ->
      file_path = Path.join(dir, name)

      with {:ok, file} <- FileIndex.get_by_path(volume_id, file_path) do
        FileIndex.delete(file.id)
      end
    end)
  end

  ## Internal — retention helpers

  defp pick_per_key(snapshots, key_fn, max_kept, now, window_kind, window_size) do
    ctx = %{
      key_fn: key_fn,
      max_kept: max_kept,
      now: now,
      window_kind: window_kind,
      window_size: window_size
    }

    {kept, remaining, _} =
      Enum.reduce(snapshots, {MapSet.new(), [], MapSet.new()}, &apply_pick_step(&1, &2, ctx))

    {kept, Enum.reverse(remaining)}
  end

  defp apply_pick_step(snap, {kept, leftover, claimed}, ctx) do
    cond do
      MapSet.size(kept) >= ctx.max_kept ->
        {kept, [snap | leftover], claimed}

      not in_window?(snap.created_at, ctx.now, ctx.window_kind, ctx.window_size) ->
        {kept, [snap | leftover], claimed}

      true ->
        claim_or_skip(snap, kept, leftover, claimed, ctx.key_fn)
    end
  end

  defp claim_or_skip(snap, kept, leftover, claimed, key_fn) do
    key = key_fn.(snap.created_at)

    if MapSet.member?(claimed, key) do
      {kept, [snap | leftover], claimed}
    else
      {MapSet.put(kept, snap.id), leftover, MapSet.put(claimed, key)}
    end
  end

  defp in_window?(created_at, now, :within_days, n) do
    DateTime.diff(now, created_at, :day) < n
  end

  defp in_window?(created_at, now, :within_weeks, n) do
    DateTime.diff(now, created_at, :day) < n * 7
  end

  defp in_window?(created_at, now, :within_months, n) do
    # Approximate: 31 days / month upper bound is fine for retention.
    DateTime.diff(now, created_at, :day) < n * 31
  end

  defp day_key(%DateTime{year: y, month: m, day: d}), do: {y, m, d}

  defp week_key(%DateTime{} = dt) do
    case Date.from_iso8601(DateTime.to_date(dt) |> Date.to_iso8601()) do
      {:ok, date} ->
        {year, week} = :calendar.iso_week_number({date.year, date.month, date.day})
        {year, week}

      _ ->
        {dt.year, dt.month}
    end
  end

  defp month_key(%DateTime{year: y, month: m}), do: {y, m}

  ## Internal — config + scheduling

  defp read_config(opts) do
    env = Application.get_env(:neonfs_core, __MODULE__, [])
    merged = Keyword.merge(env, opts)

    %{
      enabled: Keyword.get(merged, :enabled, true),
      interval_ms: Keyword.get(merged, :interval_ms, @default_interval_ms),
      daily: Keyword.get(merged, :daily, @default_daily),
      weekly: Keyword.get(merged, :weekly, @default_weekly),
      monthly: Keyword.get(merged, :monthly, @default_monthly),
      leader_check_fn: Keyword.get(merged, :leader_check_fn, &local_is_leader?/0),
      create_fn: Keyword.get(merged, :create_fn, &DRSnapshot.create/1)
    }
  end

  defp schedule_tick(state, interval_ms) when is_integer(interval_ms) and interval_ms > 0 do
    if state.timer_ref, do: Process.cancel_timer(state.timer_ref)
    %{state | timer_ref: Process.send_after(self(), :tick, interval_ms)}
  end

  defp schedule_tick(state, _), do: state

  defp local_is_leader? do
    case :ra.members(RaSupervisor.server_id(), 5_000) do
      {:ok, _members, {_, leader_node}} -> leader_node == Node.self()
      _ -> false
    end
  catch
    :exit, _ -> false
  end

  # Snapshot ids are produced by `DRSnapshot.default_timestamp/0` as
  # the basic-format ISO 8601 (e.g. `20260425T235959Z`); reverse the
  # transform to recover the `DateTime` for retention math.
  defp parse_timestamp(name) do
    with [_, y, mo, d, h, mi, s] <-
           Regex.run(~r/^(\d{4})(\d{2})(\d{2})T(\d{2})(\d{2})(\d{2})Z$/, name),
         {:ok, dt, 0} <- DateTime.from_iso8601("#{y}-#{mo}-#{d}T#{h}:#{mi}:#{s}Z") do
      {:ok, dt}
    else
      _ -> :error
    end
  end
end
