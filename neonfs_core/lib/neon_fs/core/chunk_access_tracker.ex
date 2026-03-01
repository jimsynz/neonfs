defmodule NeonFS.Core.ChunkAccessTracker do
  @moduledoc """
  Tracks chunk access frequency using ETS with sliding time windows.

  Records access counts in 1-hour and 24-hour windows to feed the TieringManager's
  promotion/demotion decisions.

  ## Performance

  `record_access/1` uses direct ETS operations (`ets.update_counter/3`) to avoid
  making the GenServer a bottleneck. The GenServer's role is limited to periodic
  maintenance (decay, cleanup, DETS persistence).

  ## ETS Table

  The `:chunk_access_tracker` table stores tuples of:
  `{chunk_hash, hourly_count, daily_count, last_accessed_unix, staleness_count}`

  ## Persistence

  Stats are periodically synced to DETS on each decay timer tick and on
  graceful shutdown. On startup, existing stats are restored from DETS.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Persistence

  @ets_table :chunk_access_tracker
  @decay_interval_ms 3_600_000
  @telemetry_sample_rate 100
  @default_max_chunks 1_000_000
  @staleness_prune_threshold 3

  ## Client API

  @doc """
  Starts the ChunkAccessTracker GenServer.

  ## Options

    * `:decay_interval_ms` - Interval for hourly decay (default: 3_600_000 = 1 hour)
    * `:dets_path` - Path to DETS persistence file (default: `meta_dir/chunk_access_tracker.dets`)
    * `:max_chunks` - Maximum tracked chunks before pruning (default: 1_000_000)
    * `:name` - GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Records a chunk access. Fast path via direct ETS update.

  This is called from the read path on every chunk fetch.
  Uses `ets.update_counter/3` for lock-free concurrent updates.
  """
  @spec record_access(binary()) :: :ok
  def record_access(chunk_hash) when is_binary(chunk_hash) do
    now = System.system_time(:second)

    # Try to update existing entry; insert new if not found
    try do
      :ets.update_counter(@ets_table, chunk_hash, [{2, 1}, {3, 1}])
      :ets.update_element(@ets_table, chunk_hash, [{4, now}, {5, 0}])
    rescue
      ArgumentError ->
        # Entry doesn't exist yet — insert new
        :ets.insert(@ets_table, {chunk_hash, 1, 1, now, 0})
    end

    maybe_trigger_prune()

    # Emit telemetry on a sampled basis (1 in N)
    if :rand.uniform(@telemetry_sample_rate) == 1 do
      :telemetry.execute(
        [:neonfs, :chunk_access_tracker, :access],
        %{count: 1},
        %{hash: chunk_hash}
      )
    end

    :ok
  end

  @doc """
  Returns access stats for a chunk.

  ## Returns

    * `%{hourly: count, daily: count, last_accessed: DateTime}` if tracked
    * `%{hourly: 0, daily: 0, last_accessed: nil}` if not tracked
  """
  @spec get_stats(binary()) :: %{
          hourly: non_neg_integer(),
          daily: non_neg_integer(),
          last_accessed: DateTime.t() | nil
        }
  def get_stats(chunk_hash) when is_binary(chunk_hash) do
    case :ets.lookup(@ets_table, chunk_hash) do
      [{_hash, hourly, daily, last_unix, _staleness}] ->
        %{
          hourly: hourly,
          daily: daily,
          last_accessed: DateTime.from_unix!(last_unix)
        }

      [] ->
        %{hourly: 0, daily: 0, last_accessed: nil}
    end
  end

  @doc """
  Returns chunks exceeding a given access threshold in the hourly window.

  Returns a list of `{chunk_hash, hourly_count}` tuples sorted by count descending.
  """
  @spec list_hot_chunks(non_neg_integer(), non_neg_integer()) :: [{binary(), non_neg_integer()}]
  def list_hot_chunks(threshold, limit \\ 100) do
    @ets_table
    |> :ets.tab2list()
    |> Enum.filter(fn {_hash, hourly, _daily, _ts, _stale} -> hourly >= threshold end)
    |> Enum.map(fn {hash, hourly, _daily, _ts, _stale} -> {hash, hourly} end)
    |> Enum.sort_by(fn {_hash, count} -> count end, :desc)
    |> Enum.take(limit)
  end

  @doc """
  Returns chunks with zero accesses in the last N hours.

  A chunk is "cold" if its `last_accessed` timestamp is older than `hours_ago` hours
  from now.
  """
  @spec list_cold_chunks(non_neg_integer(), non_neg_integer()) :: [binary()]
  def list_cold_chunks(hours_ago, limit \\ 100) do
    cutoff = System.system_time(:second) - hours_ago * 3600

    @ets_table
    |> :ets.tab2list()
    |> Enum.filter(fn {_hash, _hourly, _daily, last_ts, _stale} -> last_ts < cutoff end)
    |> Enum.map(fn {hash, _hourly, _daily, _ts, _stale} -> hash end)
    |> Enum.take(limit)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    table =
      :ets.new(@ets_table, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    decay_interval = Keyword.get(opts, :decay_interval_ms, @decay_interval_ms)
    dets_path = Keyword.get(opts, :dets_path)
    max_chunks = Keyword.get(opts, :max_chunks, @default_max_chunks)

    :persistent_term.put({__MODULE__, :max_chunks}, max_chunks)

    restore_from_dets(dets_path)

    if decay_interval > 0 do
      Process.send_after(self(), :decay, decay_interval)
    end

    Logger.info("ChunkAccessTracker started")

    {:ok,
     %{table: table, decay_interval: decay_interval, dets_path: dets_path, max_chunks: max_chunks}}
  end

  @impl true
  def terminate(_reason, state) do
    try do
      sync_to_dets(state.dets_path)
    rescue
      e ->
        Logger.warning("ChunkAccessTracker DETS sync on terminate failed",
          error: inspect(e)
        )
    catch
      :exit, reason ->
        Logger.warning("ChunkAccessTracker DETS sync on terminate exited",
          reason: inspect(reason)
        )
    end

    :ok
  end

  @impl true
  def handle_info(:decay, state) do
    perform_decay(state.table)
    sync_to_dets(state.dets_path)

    :telemetry.execute([:neonfs, :chunk_access_tracker, :decay_complete], %{}, %{})

    if state.decay_interval > 0 do
      Process.send_after(self(), :decay, state.decay_interval)
    end

    {:noreply, state}
  end

  def handle_info(:prune, state) do
    prune_if_over_limit(state.table, state.max_chunks)
    :telemetry.execute([:neonfs, :chunk_access_tracker, :prune_complete], %{}, %{})
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## Private Functions

  defp perform_decay(table) do
    entries = :ets.tab2list(table)

    {decayed, cleaned} =
      Enum.reduce(entries, {0, 0}, fn {hash, hourly, daily, last_ts, staleness}, {dec, clean} ->
        decay_amount = div(daily, 24)
        new_daily = max(0, daily - decay_amount + hourly)
        active? = new_daily > 0 or hourly > 0

        cond do
          not active? and staleness + 1 >= @staleness_prune_threshold ->
            :ets.delete(table, hash)
            {dec, clean + 1}

          not active? ->
            :ets.insert(table, {hash, 0, new_daily, last_ts, staleness + 1})
            {dec + 1, clean}

          true ->
            :ets.insert(table, {hash, 0, new_daily, last_ts, 0})
            {dec + 1, clean}
        end
      end)

    if decayed > 0 or cleaned > 0 do
      Logger.debug("ChunkAccessTracker decay complete", decayed: decayed, cleaned: cleaned)
    end
  end

  defp maybe_trigger_prune do
    max = :persistent_term.get({__MODULE__, :max_chunks}, @default_max_chunks)

    if :ets.info(@ets_table, :size) > max do
      case GenServer.whereis(__MODULE__) do
        nil -> :ok
        pid -> send(pid, :prune)
      end
    end
  end

  defp prune_if_over_limit(table, max_chunks) do
    size = :ets.info(table, :size)

    if size > max_chunks do
      prune_inactive_entries(table, size - max_chunks)
      prune_oldest_entries(table, max_chunks)

      Logger.debug("ChunkAccessTracker pruned",
        previous_size: size,
        current_size: :ets.info(table, :size)
      )
    end
  end

  defp prune_inactive_entries(table, to_remove) do
    table
    |> :ets.tab2list()
    |> Enum.filter(fn {_hash, hourly, daily, _ts, _stale} -> hourly == 0 and daily == 0 end)
    |> Enum.sort_by(fn {_hash, _h, _d, ts, staleness} -> {-staleness, ts} end)
    |> Enum.take(to_remove)
    |> Enum.each(fn {hash, _h, _d, _ts, _stale} -> :ets.delete(table, hash) end)
  end

  defp prune_oldest_entries(table, max_chunks) do
    remaining = :ets.info(table, :size)

    if remaining > max_chunks do
      table
      |> :ets.tab2list()
      |> Enum.sort_by(fn {_hash, _h, _d, ts, _stale} -> ts end)
      |> Enum.take(remaining - max_chunks)
      |> Enum.each(fn {hash, _h, _d, _ts, _stale} -> :ets.delete(table, hash) end)
    end
  end

  defp resolve_dets_path(nil) do
    Path.join(Persistence.meta_dir(), "chunk_access_tracker.dets")
  end

  defp resolve_dets_path(path) when is_binary(path), do: path

  defp restore_from_dets(dets_path_opt) do
    path = resolve_dets_path(dets_path_opt)

    if File.exists?(path) do
      do_restore_from_dets(path)
    end
  end

  defp do_restore_from_dets(path) do
    case :dets.open_file(:chunk_access_tracker_dets, type: :set, file: String.to_charlist(path)) do
      {:ok, dets_ref} ->
        count =
          :dets.foldl(
            fn entry, acc ->
              :ets.insert(@ets_table, normalise_entry(entry))
              acc + 1
            end,
            0,
            dets_ref
          )

        :dets.close(dets_ref)

        Logger.info("ChunkAccessTracker restored entries from DETS",
          count: count,
          path: path
        )

      {:error, reason} ->
        Logger.warning("Failed to open DETS file",
          path: path,
          reason: inspect(reason)
        )
    end
  end

  defp sync_to_dets(dets_path_opt) do
    path = resolve_dets_path(dets_path_opt)
    Path.dirname(path) |> File.mkdir_p!()
    temp_path = "#{path}.tmp"

    File.rm(temp_path)

    case :dets.open_file(:chunk_access_tracker_dets,
           type: :set,
           file: String.to_charlist(temp_path)
         ) do
      {:ok, dets_ref} ->
        :ets.to_dets(@ets_table, dets_ref)
        :dets.sync(dets_ref)
        :dets.close(dets_ref)

        case File.rename(temp_path, path) do
          :ok ->
            :ok

          {:error, reason} ->
            Logger.error("Failed to rename DETS temp file",
              temp_path: temp_path,
              path: path,
              reason: inspect(reason)
            )

            File.rm(temp_path)
        end

      {:error, reason} ->
        Logger.error("Failed to open DETS for sync", reason: inspect(reason))
    end
  rescue
    e ->
      Logger.error("ChunkAccessTracker DETS sync failed", error: inspect(e))
  end

  # Handle legacy 4-element tuples from old DETS files
  defp normalise_entry({hash, hourly, daily, last_ts}),
    do: {hash, hourly, daily, last_ts, 0}

  defp normalise_entry({_hash, _hourly, _daily, _last_ts, _staleness} = entry),
    do: entry
end
