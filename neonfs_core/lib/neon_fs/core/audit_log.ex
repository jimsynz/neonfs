defmodule NeonFS.Core.AuditLog do
  @moduledoc """
  Operational audit logging for security-relevant events.

  Stores events in a bounded ETS table with DETS persistence. Events are
  recorded asynchronously via `GenServer.cast/2` to avoid slowing down
  operations. Periodic pruning enforces both count and time-based retention.

  These are operational logs — not tamper-proof forensic records. For
  tamper-resistant audit trails, ship logs to an external system (syslog, SIEM).

  ## Storage

  Events are stored in a local ordered_set ETS table keyed by
  `{timestamp_unix_us, id}` for natural chronological ordering. DETS
  persistence is handled by the Persistence module.

  ## Retention

  - **Max events**: Default 100,000. Oldest events pruned when limit reached.
  - **Max age**: Default 90 days. Expired events pruned periodically.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{AuditEvent, Persistence}

  @ets_table :audit_log
  @default_max_events 100_000
  @default_max_age_days 90
  @default_prune_interval_ms 60_000

  # Client API

  @doc """
  Starts the audit log GenServer.

  ## Options

  - `:max_events` - Maximum number of events to retain (default: #{@default_max_events})
  - `:max_age_days` - Maximum age of events in days (default: #{@default_max_age_days})
  - `:prune_interval_ms` - Prune check interval in milliseconds (default: #{@default_prune_interval_ms})
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Records an audit event asynchronously.

  Uses `GenServer.cast/2` — never blocks the caller. If the audit log
  process is overloaded or unavailable, events may be silently dropped.
  """
  @spec log(AuditEvent.t()) :: :ok
  def log(%AuditEvent{} = event) do
    GenServer.cast(__MODULE__, {:log, event})
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  @doc false
  @spec flush() :: :ok
  def flush, do: GenServer.call(__MODULE__, :flush)

  @doc """
  Convenience function to create and log an event in one call.
  """
  @spec log_event(keyword()) :: :ok
  def log_event(attrs) do
    attrs
    |> AuditEvent.new()
    |> log()
  end

  @doc """
  Returns the N most recent events from the ETS cache.
  """
  @spec recent(pos_integer()) :: [AuditEvent.t()]
  def recent(limit \\ 50) when is_integer(limit) and limit > 0 do
    case :ets.whereis(@ets_table) do
      :undefined ->
        []

      _ref ->
        @ets_table
        |> ets_reverse_fold(limit)
        |> Enum.map(fn {_key, event} -> event end)
    end
  end

  @doc """
  Queries events matching the given criteria.

  ## Filter Options

  - `:event_type` - Filter by event type (atom or list of atoms)
  - `:actor_uid` - Filter by actor UID (integer)
  - `:resource` - Filter by resource (binary, prefix match)
  - `:since` - Filter events after this DateTime
  - `:until` - Filter events before this DateTime
  - `:limit` - Maximum number of results (default: 100)
  """
  @spec query(keyword()) :: [AuditEvent.t()]
  def query(filters \\ []) do
    case :ets.whereis(@ets_table) do
      :undefined ->
        []

      _ref ->
        limit = Keyword.get(filters, :limit, 100)

        @ets_table
        |> ets_fold_all()
        |> Enum.map(fn {_key, event} -> event end)
        |> apply_filters(filters)
        |> Enum.take(limit)
    end
  end

  @doc """
  Returns the current event count.
  """
  @spec count() :: non_neg_integer()
  def count do
    case :ets.whereis(@ets_table) do
      :undefined -> 0
      _ref -> :ets.info(@ets_table, :size)
    end
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    :ets.new(@ets_table, [:ordered_set, :named_table, :public, read_concurrency: true])

    state = %{
      max_events: Keyword.get(opts, :max_events, @default_max_events),
      max_age_days: Keyword.get(opts, :max_age_days, @default_max_age_days),
      prune_interval_ms: Keyword.get(opts, :prune_interval_ms, @default_prune_interval_ms)
    }

    schedule_prune(state.prune_interval_ms)

    {:ok, state}
  end

  @impl true
  def handle_call(:flush, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_cast({:log, %AuditEvent{} = event}, state) do
    key = event_key(event)
    :ets.insert(@ets_table, {key, event})

    :telemetry.execute(
      [:neonfs, :audit, :logged],
      %{count: 1},
      %{event_type: event.event_type}
    )

    # Inline count-based pruning if over limit
    current_size = :ets.info(@ets_table, :size)

    if current_size > state.max_events do
      prune_oldest(current_size - state.max_events)
    end

    {:noreply, state}
  end

  @impl true
  def handle_info(:prune, state) do
    prune_expired(state.max_age_days)
    prune_by_count(state.max_events)
    :telemetry.execute([:neonfs, :audit, :pruned], %{}, %{})
    schedule_prune(state.prune_interval_ms)
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    meta_dir = Persistence.meta_dir()
    dets_path = Path.join(meta_dir, "audit_log.dets")
    Persistence.snapshot_table(@ets_table, dets_path)
    Logger.info("AuditLog table saved")
    :ok
  rescue
    _ -> :ok
  end

  # Private

  defp event_key(%AuditEvent{timestamp: ts, id: id}) do
    unix_us = DateTime.to_unix(ts, :microsecond)
    {unix_us, id}
  end

  defp schedule_prune(interval_ms) when interval_ms > 0 do
    Process.send_after(self(), :prune, interval_ms)
  end

  defp schedule_prune(_), do: :ok

  defp prune_expired(max_age_days) do
    cutoff =
      DateTime.utc_now() |> DateTime.add(-max_age_days, :day) |> DateTime.to_unix(:microsecond)

    prune_before_key(cutoff)
  end

  defp prune_before_key(cutoff_us) do
    case :ets.first(@ets_table) do
      :"$end_of_table" ->
        :ok

      {ts_us, _id} = key when ts_us < cutoff_us ->
        :ets.delete(@ets_table, key)
        prune_before_key(cutoff_us)

      _ ->
        :ok
    end
  end

  defp prune_by_count(max_events) do
    current_size = :ets.info(@ets_table, :size)

    if current_size > max_events do
      prune_oldest(current_size - max_events)
    end
  end

  defp prune_oldest(0), do: :ok

  defp prune_oldest(count) do
    case :ets.first(@ets_table) do
      :"$end_of_table" ->
        :ok

      key ->
        :ets.delete(@ets_table, key)
        prune_oldest(count - 1)
    end
  end

  defp ets_reverse_fold(table, limit) do
    do_reverse_fold(table, :ets.last(table), limit, [])
  end

  defp do_reverse_fold(_table, :"$end_of_table", _limit, acc), do: acc
  defp do_reverse_fold(_table, _key, 0, acc), do: acc

  defp do_reverse_fold(table, key, remaining, acc) do
    case :ets.lookup(table, key) do
      [entry] ->
        do_reverse_fold(table, :ets.prev(table, key), remaining - 1, [entry | acc])

      [] ->
        acc
    end
  end

  defp ets_fold_all(table) do
    do_forward_fold(table, :ets.first(table), [])
    |> Enum.reverse()
  end

  defp do_forward_fold(_table, :"$end_of_table", acc), do: acc

  defp do_forward_fold(table, key, acc) do
    case :ets.lookup(table, key) do
      [entry] ->
        do_forward_fold(table, :ets.next(table, key), [entry | acc])

      [] ->
        acc
    end
  end

  defp apply_filters(events, filters) do
    events
    |> filter_by_event_type(Keyword.get(filters, :event_type))
    |> filter_by_actor_uid(Keyword.get(filters, :actor_uid))
    |> filter_by_resource(Keyword.get(filters, :resource))
    |> filter_by_since(Keyword.get(filters, :since))
    |> filter_by_until(Keyword.get(filters, :until))
  end

  defp filter_by_event_type(events, nil), do: events

  defp filter_by_event_type(events, types) when is_list(types) do
    type_set = MapSet.new(types)
    Enum.filter(events, &MapSet.member?(type_set, &1.event_type))
  end

  defp filter_by_event_type(events, type) when is_atom(type) do
    Enum.filter(events, &(&1.event_type == type))
  end

  defp filter_by_actor_uid(events, nil), do: events

  defp filter_by_actor_uid(events, uid) do
    Enum.filter(events, &(&1.actor_uid == uid))
  end

  defp filter_by_resource(events, nil), do: events

  defp filter_by_resource(events, resource) do
    Enum.filter(events, &(&1.resource != nil and String.starts_with?(&1.resource, resource)))
  end

  defp filter_by_since(events, nil), do: events

  defp filter_by_since(events, since) do
    Enum.filter(events, &(DateTime.compare(&1.timestamp, since) in [:gt, :eq]))
  end

  defp filter_by_until(events, nil), do: events

  defp filter_by_until(events, until_dt) do
    Enum.filter(events, &(DateTime.compare(&1.timestamp, until_dt) in [:lt, :eq]))
  end
end
