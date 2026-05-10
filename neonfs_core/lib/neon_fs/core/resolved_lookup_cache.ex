defmodule NeonFS.Core.ResolvedLookupCache do
  @moduledoc """
  ETS-backed cache of fully resolved file→chunk mappings.

  Caches the composed result of a complete file resolution — the chunk list,
  stripe info, and POSIX attrs needed to serve reads. This is the primary
  mechanism for metadata availability during partial outages: once a client has
  resolved a file's metadata, it can continue serving reads even if the metadata
  quorum is temporarily unreachable.

  Since data chunks are immutable and content-addressed, stale metadata points
  to older but still valid file versions. The cache is evicted on writes (to
  prevent serving stale data after an update) and expires after a configurable
  TTL (to bound staleness).

  Caches the final composed result of a multi-step lookup (directory
  entry → file meta → chunk list), fronting the per-volume index
  trees that `MetadataReader` walks. Pre-#792 there was also a
  per-segment cache on the (now-deleted) `MetadataStore`; this one
  lives on as the cluster-wide resolution cache.

  ## Telemetry Events

    * `[:neonfs, :resolved_cache, :hit]` — cache hit
    * `[:neonfs, :resolved_cache, :miss]` — cache miss
    * `[:neonfs, :resolved_cache, :evict]` — explicit eviction
  """

  use GenServer
  require Logger

  @ets_table :resolved_lookup_cache
  @default_ttl_ms 300_000
  @default_max_entries 100_000
  @default_cleanup_interval_ms 60_000

  @type file_id :: String.t()
  @type resolved_metadata :: %{
          optional(:file_meta) => term(),
          optional(:chunks) => [term()],
          optional(:stripes) => [term()]
        }

  ## Client API

  @doc """
  Starts the ResolvedLookupCache GenServer.

  ## Options

    * `:ttl_ms` — time-to-live for cached entries in milliseconds (default: #{@default_ttl_ms})
    * `:max_entries` — maximum number of cached entries (default: #{@default_max_entries})
    * `:cleanup_interval_ms` — how often to sweep expired entries (default: #{@default_cleanup_interval_ms})
    * `:now_fn` — zero-arity function returning current time in milliseconds (default: `System.monotonic_time(:millisecond)`)
    * `:name` — GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Caches a fully resolved file metadata entry.

  Entries are stored with a timestamp for TTL expiry and LRU tracking.
  If the cache is full (exceeds max_entries), the oldest entry is evicted.

  ## Parameters

    * `file_id` — the file identifier
    * `resolved_metadata` — the composed resolution result
    * `opts` — optional keyword list:
      * `:ttl_ms` — override the default TTL for this entry
  """
  @spec put(file_id(), resolved_metadata(), keyword()) :: :ok
  def put(file_id, resolved_metadata, opts \\ []) do
    GenServer.call(__MODULE__, {:put, file_id, resolved_metadata, opts})
  end

  @doc """
  Retrieves a cached resolution for the given file ID.

  Returns `{:ok, resolved_metadata}` on cache hit, or `:miss` if not found
  or expired.
  """
  @spec get(file_id()) :: {:ok, resolved_metadata()} | :miss
  def get(file_id) do
    now_fn = :persistent_term.get({__MODULE__, :now_fn}, &default_now/0)
    now = now_fn.()

    case :ets.lookup(@ets_table, file_id) do
      [{^file_id, metadata, expires_at, _inserted_at}] when expires_at > now ->
        :ets.update_element(@ets_table, file_id, {4, now})

        :telemetry.execute(
          [:neonfs, :resolved_cache, :hit],
          %{},
          %{file_id: file_id}
        )

        {:ok, metadata}

      [{^file_id, _metadata, _expires_at, _inserted_at}] ->
        :ets.delete(@ets_table, file_id)

        :telemetry.execute(
          [:neonfs, :resolved_cache, :miss],
          %{},
          %{file_id: file_id, reason: :expired}
        )

        :miss

      [] ->
        :telemetry.execute(
          [:neonfs, :resolved_cache, :miss],
          %{},
          %{file_id: file_id, reason: :not_cached}
        )

        :miss
    end
  end

  @doc """
  Evicts a cached entry for the given file ID.

  Called on writes and pub/sub invalidation to prevent serving stale data.
  """
  @spec evict(file_id()) :: :ok
  def evict(file_id) do
    :ets.delete(@ets_table, file_id)

    :telemetry.execute(
      [:neonfs, :resolved_cache, :evict],
      %{},
      %{file_id: file_id}
    )

    :ok
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    :ets.new(@ets_table, [
      :named_table,
      :set,
      :public,
      read_concurrency: true,
      write_concurrency: true
    ])

    ttl_ms = Keyword.get(opts, :ttl_ms, @default_ttl_ms)
    max_entries = Keyword.get(opts, :max_entries, @default_max_entries)
    cleanup_interval_ms = Keyword.get(opts, :cleanup_interval_ms, @default_cleanup_interval_ms)
    now_fn = Keyword.get(opts, :now_fn, &default_now/0)

    :persistent_term.put({__MODULE__, :now_fn}, now_fn)

    if cleanup_interval_ms > 0 do
      Process.send_after(self(), :cleanup_expired, cleanup_interval_ms)
    end

    {:ok,
     %{
       ttl_ms: ttl_ms,
       max_entries: max_entries,
       cleanup_interval_ms: cleanup_interval_ms,
       now_fn: now_fn
     }}
  end

  @impl true
  def handle_call({:put, file_id, resolved_metadata, opts}, _from, state) do
    now = state.now_fn.()
    ttl_ms = Keyword.get(opts, :ttl_ms, state.ttl_ms)
    expires_at = now + ttl_ms

    maybe_evict_lru(state)
    :ets.insert(@ets_table, {file_id, resolved_metadata, expires_at, now})

    {:reply, :ok, state}
  end

  @impl true
  def handle_info(:cleanup_expired, state) do
    now = state.now_fn.()

    expired =
      :ets.select(@ets_table, [
        {{:"$1", :_, :"$2", :_}, [{:<, :"$2", now}], [:"$1"]}
      ])

    Enum.each(expired, &:ets.delete(@ets_table, &1))

    if state.cleanup_interval_ms > 0 do
      Process.send_after(self(), :cleanup_expired, state.cleanup_interval_ms)
    end

    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## Private

  defp maybe_evict_lru(state) do
    current_size = :ets.info(@ets_table, :size)

    if current_size >= state.max_entries do
      evict_oldest()
    end
  end

  defp evict_oldest do
    case :ets.first(@ets_table) do
      :"$end_of_table" -> :ok
      _key -> delete_lru_entry()
    end
  end

  defp delete_lru_entry do
    now_fn = :persistent_term.get({__MODULE__, :now_fn}, &default_now/0)

    {oldest_key, _oldest_time} =
      :ets.foldl(
        fn {key, _meta, _exp, accessed_at}, {_ok, oldest_t} = acc ->
          if accessed_at < oldest_t, do: {key, accessed_at}, else: acc
        end,
        {nil, now_fn.()},
        @ets_table
      )

    if oldest_key do
      :ets.delete(@ets_table, oldest_key)
    end
  end

  defp default_now, do: System.monotonic_time(:millisecond)
end
