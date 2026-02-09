defmodule NeonFS.Core.ChunkAccessTracker do
  @moduledoc """
  Tracks chunk access frequency using ETS with sliding time windows.

  Records access counts in 1-hour and 24-hour windows to feed the TieringManager's
  promotion/demotion decisions.

  ## Performance

  `record_access/1` uses direct ETS operations (`ets.update_counter/3`) to avoid
  making the GenServer a bottleneck. The GenServer's role is limited to periodic
  maintenance (decay, cleanup).

  ## ETS Table

  The `:chunk_access_tracker` table stores tuples of:
  `{chunk_hash, hourly_count, daily_count, last_accessed_unix}`
  """

  use GenServer
  require Logger

  @ets_table :chunk_access_tracker
  @decay_interval_ms 3_600_000
  @telemetry_sample_rate 100

  ## Client API

  @doc """
  Starts the ChunkAccessTracker GenServer.

  ## Options

    * `:decay_interval_ms` - Interval for hourly decay (default: 3_600_000 = 1 hour)
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
      :ets.update_element(@ets_table, chunk_hash, {4, now})
    rescue
      ArgumentError ->
        # Entry doesn't exist yet — insert new
        :ets.insert(@ets_table, {chunk_hash, 1, 1, now})
    end

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
      [{_hash, hourly, daily, last_unix}] ->
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
    |> Enum.filter(fn {_hash, hourly, _daily, _ts} -> hourly >= threshold end)
    |> Enum.map(fn {hash, hourly, _daily, _ts} -> {hash, hourly} end)
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
    |> Enum.filter(fn {_hash, _hourly, _daily, last_ts} -> last_ts < cutoff end)
    |> Enum.map(fn {hash, _hourly, _daily, _ts} -> hash end)
    |> Enum.take(limit)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    table =
      :ets.new(@ets_table, [
        :named_table,
        :set,
        :public,
        read_concurrency: true,
        write_concurrency: true
      ])

    decay_interval = Keyword.get(opts, :decay_interval_ms, @decay_interval_ms)

    if decay_interval > 0 do
      Process.send_after(self(), :decay, decay_interval)
    end

    Logger.info("ChunkAccessTracker started")

    {:ok, %{table: table, decay_interval: decay_interval}}
  end

  @impl true
  def handle_info(:decay, state) do
    perform_decay(state.table)

    if state.decay_interval > 0 do
      Process.send_after(self(), :decay, state.decay_interval)
    end

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## Private Functions

  defp perform_decay(table) do
    # Roll hourly counts into daily aggregate, reset hourly counters.
    # Daily count keeps the hourly contribution from each window,
    # and we subtract the previous hourly (approximation of 24h sliding window).
    entries = :ets.tab2list(table)
    decayed = 0
    cleaned = 0

    {decayed, cleaned} =
      Enum.reduce(entries, {decayed, cleaned}, fn {hash, hourly, daily, last_ts}, {dec, clean} ->
        # New daily = daily - hourly/24 (approximate decay of oldest hour contribution)
        # This is a simplification: we subtract ~1/24 of daily to simulate window slide
        decay_amount = div(daily, 24)
        new_daily = max(0, daily - decay_amount + hourly)

        if new_daily == 0 and hourly == 0 do
          # Clean up entries with no activity
          :ets.delete(table, hash)
          {dec, clean + 1}
        else
          # Reset hourly, update daily
          :ets.insert(table, {hash, 0, new_daily, last_ts})
          {dec + 1, clean}
        end
      end)

    if decayed > 0 or cleaned > 0 do
      Logger.debug("ChunkAccessTracker decay: #{decayed} decayed, #{cleaned} cleaned")
    end
  end
end
