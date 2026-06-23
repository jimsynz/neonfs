defmodule NeonFS.Client.ChunkCache do
  @moduledoc """
  Per-node, byte-bounded LRU cache of whole chunk bytes for the
  `ChunkReader` data-plane read path.

  `ChunkReader` fetches a whole chunk over the TLS data plane and slices
  the requested window out of it. Repeated or overlapping reads — the
  pathological case being containerd image unpack, which issues thousands
  of small advancing `ReaderAt` reads inside the same chunk — otherwise
  re-fetch the whole chunk every time (#1350). This cache holds the
  verified whole-chunk bytes so those reads are served from memory.

  ## Shape

    * **Per-node:** each node owns its own cache; there is no cross-node
      coherence. One instance per BEAM node, started by
      `NeonFS.Client.Application`.
    * **ETS-backed reads:** `get/1` reads directly from a `:public`,
      `read_concurrency: true` ETS table — no GenServer round-trip on the
      hot path. The owning process is the only writer, so the byte budget
      and LRU ordering never race.
    * **Byte-bounded LRU:** the cache is bounded by total cached **bytes**,
      not entry count. Inserting an entry that would exceed the cap evicts
      least-recently-used entries until it fits. The working set never
      scales with file size — each entry is one chunk, bounded by chunk
      size — so this honours the "No Whole-File Buffering" rule.
    * **Key `{volume, chunk_hash}`:** chunks are content-addressed
      (SHA-256), so the bytes for a hash are immutable — there is no
      invalidation; eviction is purely for memory pressure. The volume
      component namespaces entries per volume.

  Only verified bytes are cached: `ChunkReader` populates the cache after
  its content-hash check passes, so a corrupt replica is never stored.

  ## Configuration

  `:neonfs_client` `:chunk_cache` `:max_bytes` (default 128 MiB), wired
  from `NEONFS_CHUNK_CACHE_MAX_BYTES` by each release's `runtime.exs`.

  When the cache process is not running (the library default in tests, or
  any node that disabled client children), `get/1` reports a miss and
  `put/2` is a no-op — callers need no conditional.
  """

  use GenServer

  @table :neonfs_client_chunk_cache
  @default_max_bytes 128 * 1024 * 1024

  @type key :: {volume :: String.t(), chunk_hash :: binary()}

  ## Public API

  @doc "Start the cache. `:max_bytes` overrides the app-env / default cap."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Look up cached bytes for `key`. Returns `{:ok, bytes}` on a hit and
  `:miss` otherwise (including when the cache isn't running). A hit bumps
  the entry to most-recently-used.
  """
  @spec get(key()) :: {:ok, binary()} | :miss
  def get(key) do
    case :ets.lookup(@table, key) do
      [{^key, bytes}] ->
        GenServer.cast(__MODULE__, {:touch, key})
        :telemetry.execute([:neonfs, :client, :chunk_cache, :hit], %{size: byte_size(bytes)}, %{})
        {:ok, bytes}

      [] ->
        :telemetry.execute([:neonfs, :client, :chunk_cache, :miss], %{}, %{})
        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc """
  Cache `bytes` under `key`, evicting least-recently-used entries if the
  insert would exceed the byte cap. A no-op when the cache isn't running
  or when a single chunk is larger than the whole cap.
  """
  @spec put(key(), binary()) :: :ok
  def put(key, bytes) when is_binary(bytes) do
    case Process.whereis(__MODULE__) do
      nil -> :ok
      pid -> GenServer.call(pid, {:put, key, bytes})
    end
  catch
    :exit, _ -> :ok
  end

  @doc "Current cache occupancy in bytes and entries (diagnostics / tests)."
  @spec stats() :: %{
          bytes: non_neg_integer(),
          entries: non_neg_integer(),
          max_bytes: pos_integer()
        }
  def stats, do: GenServer.call(__MODULE__, :stats)

  ## GenServer callbacks

  @impl true
  def init(opts) do
    :ets.new(@table, [:named_table, :public, :set, read_concurrency: true])

    state = %{
      max_bytes: Keyword.get(opts, :max_bytes, configured_max_bytes()),
      bytes: 0,
      sizes: %{},
      key_seq: %{},
      lru: :gb_trees.empty(),
      seq: 0
    }

    {:ok, state}
  end

  @impl true
  def handle_cast({:touch, key}, state) do
    case Map.fetch(state.key_seq, key) do
      {:ok, old_seq} ->
        seq = state.seq + 1
        lru = :gb_trees.insert(seq, key, :gb_trees.delete(old_seq, state.lru))
        {:noreply, %{state | seq: seq, lru: lru, key_seq: Map.put(state.key_seq, key, seq)}}

      :error ->
        # Evicted between the ETS hit and this cast — nothing to bump.
        {:noreply, state}
    end
  end

  @impl true
  def handle_call({:put, key, bytes}, _from, state) do
    size = byte_size(bytes)

    cond do
      Map.has_key?(state.sizes, key) ->
        {:reply, :ok, state}

      size > state.max_bytes ->
        {:reply, :ok, state}

      true ->
        {:reply, :ok, insert(evict_until_fits(state, size), key, bytes, size)}
    end
  end

  def handle_call(:stats, _from, state) do
    {:reply, %{bytes: state.bytes, entries: map_size(state.sizes), max_bytes: state.max_bytes},
     state}
  end

  ## Internal

  defp insert(state, key, bytes, size) do
    seq = state.seq + 1
    :ets.insert(@table, {key, bytes})

    %{
      state
      | seq: seq,
        bytes: state.bytes + size,
        sizes: Map.put(state.sizes, key, size),
        key_seq: Map.put(state.key_seq, key, seq),
        lru: :gb_trees.insert(seq, key, state.lru)
    }
  end

  defp evict_until_fits(state, incoming_size) do
    if state.bytes + incoming_size <= state.max_bytes or :gb_trees.is_empty(state.lru) do
      state
    else
      {_seq, key, lru} = :gb_trees.take_smallest(state.lru)
      {size, sizes} = Map.pop(state.sizes, key)
      :ets.delete(@table, key)

      :telemetry.execute([:neonfs, :client, :chunk_cache, :eviction], %{size: size}, %{})

      evict_until_fits(
        %{
          state
          | bytes: state.bytes - size,
            sizes: sizes,
            key_seq: Map.delete(state.key_seq, key),
            lru: lru
        },
        incoming_size
      )
    end
  end

  defp configured_max_bytes do
    :neonfs_client
    |> Application.get_env(:chunk_cache, [])
    |> Keyword.get(:max_bytes, @default_max_bytes)
  end
end
