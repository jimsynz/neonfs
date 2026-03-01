defmodule NeonFS.Core.ChunkCache do
  @moduledoc """
  LRU cache for decompressed/decrypted chunks backed by ETS.

  Avoids redundant decompression on repeated reads. A global memory limit is
  enforced using `:chunk_cache_max_memory`. Uses two ETS tables:

    * `:chunk_cache_data` — `{{volume_id, chunk_hash}, data, byte_size, timestamp}`
    * `:chunk_cache_lru` — `{{timestamp, volume_id, chunk_hash}}` (ordered_set for eviction)

  On access, the LRU entry is updated to the current timestamp. When memory
  exceeds the limit, oldest entries (by timestamp) are evicted first.

  ## Telemetry Events

    * `[:neonfs, :chunk_cache, :hit]` — cache hit
    * `[:neonfs, :chunk_cache, :miss]` — cache miss
    * `[:neonfs, :chunk_cache, :eviction]` — entry evicted
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{Volume, VolumeRegistry}

  @data_table :chunk_cache_data
  @lru_table :chunk_cache_lru
  @default_max_memory 268_435_456

  @type chunk_type :: :local | :transformed | :reconstructed | :remote

  ## Client API

  @doc "Starts the ChunkCache GenServer."
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, _opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Looks up a cached chunk.

  Returns `{:ok, data}` on cache hit, `:miss` on cache miss.
  Updates the LRU timestamp on hit.
  """
  @spec get(String.t(), binary()) :: {:ok, binary()} | :miss
  def get(volume_id, chunk_hash) do
    case :ets.lookup(@data_table, {volume_id, chunk_hash}) do
      [{key, data, _size, old_ts}] ->
        new_ts = now_us()
        update_lru(key, old_ts, new_ts)
        :ets.update_element(@data_table, key, {4, new_ts})
        bump_stat(:hits)

        :telemetry.execute(
          [:neonfs, :chunk_cache, :hit],
          %{bytes: byte_size(data)},
          %{volume_id: volume_id}
        )

        {:ok, data}

      [] ->
        bump_stat(:misses)

        :telemetry.execute(
          [:neonfs, :chunk_cache, :miss],
          %{},
          %{volume_id: volume_id}
        )

        :miss
    end
  rescue
    ArgumentError -> :miss
  end

  @doc """
  Stores a chunk in the cache using the configured memory limit.

  Evicts oldest entries if the cache exceeds the configured `max_memory` bytes.
  """
  @spec put(String.t(), binary(), binary()) :: :ok
  def put(volume_id, chunk_hash, data) do
    GenServer.cast(__MODULE__, {:put, volume_id, chunk_hash, data, []})
  end

  @doc """
  Stores a chunk in the cache with additional options.

  ## Options

    * `:chunk_type` - `:local`, `:transformed`, `:reconstructed`, or `:remote`
  """
  @spec put(String.t(), binary(), binary(), keyword()) :: :ok
  def put(volume_id, chunk_hash, data, opts) when is_list(opts) do
    GenServer.cast(__MODULE__, {:put, volume_id, chunk_hash, data, opts})
  end

  @doc false
  @spec flush() :: :ok
  def flush, do: GenServer.call(__MODULE__, :flush)

  @doc """
  Removes a chunk from all volumes in the cache.
  """
  @spec invalidate(binary()) :: :ok
  def invalidate(chunk_hash) do
    GenServer.cast(__MODULE__, {:invalidate_all, chunk_hash})
  end

  @doc """
  Removes a chunk from a specific volume in the cache.
  """
  @spec invalidate(String.t(), binary()) :: :ok
  def invalidate(volume_id, chunk_hash) do
    GenServer.cast(__MODULE__, {:invalidate, volume_id, chunk_hash})
  end

  @doc """
  Returns cache statistics.
  """
  @spec stats() :: %{
          hits: non_neg_integer(),
          misses: non_neg_integer(),
          evictions: non_neg_integer(),
          memory_used: non_neg_integer()
        }
  def stats do
    hits = get_stat(:hits)
    misses = get_stat(:misses)
    evictions = get_stat(:evictions)
    memory_used = calculate_memory_used()
    %{hits: hits, misses: misses, evictions: evictions, memory_used: memory_used}
  rescue
    ArgumentError -> %{hits: 0, misses: 0, evictions: 0, memory_used: 0}
  end

  @doc "Returns current cache entry count."
  @spec entry_count() :: non_neg_integer()
  def entry_count do
    case :ets.info(@data_table, :size) do
      :undefined -> 0
      count when is_integer(count) and count >= 0 -> count
    end
  rescue
    ArgumentError -> 0
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    :ets.new(@data_table, [:named_table, :set, :public, read_concurrency: true])
    :ets.new(@lru_table, [:named_table, :ordered_set, :public])
    :ets.new(:chunk_cache_stats, [:named_table, :set, :public])
    :ets.insert(:chunk_cache_stats, [{:hits, 0}, {:misses, 0}, {:evictions, 0}])

    max_memory =
      Application.get_env(:neonfs_core, :chunk_cache_max_memory, @default_max_memory)

    Logger.info("ChunkCache started", max_memory_bytes: max_memory)
    {:ok, %{max_memory: max_memory}}
  end

  @impl true
  def handle_call(:flush, _from, state), do: {:reply, :ok, state}

  @impl true
  def handle_cast({:put, volume_id, chunk_hash, data, opts}, state) do
    chunk_types = normalise_chunk_types(opts)

    if cache_allowed?(volume_id, chunk_types) do
      key = {volume_id, chunk_hash}
      size = byte_size(data)
      ts = now_us()

      # Remove existing entry if present (to update)
      remove_entry(key)

      # Insert new entry
      :ets.insert(@data_table, {key, data, size, ts})
      :ets.insert(@lru_table, {{ts, volume_id, chunk_hash}})

      # Evict if over global memory limit
      evict_if_needed(state.max_memory)
    end

    {:noreply, state}
  end

  def handle_cast({:invalidate_all, chunk_hash}, state) do
    # Find all entries with this chunk_hash across all volumes
    entries =
      :ets.tab2list(@data_table)
      |> Enum.filter(fn {{_vol, hash}, _data, _size, _ts} -> hash == chunk_hash end)

    Enum.each(entries, fn {{vol_id, _hash} = key, _data, _size, ts} ->
      :ets.delete(@data_table, key)
      :ets.delete(@lru_table, {ts, vol_id, chunk_hash})
    end)

    {:noreply, state}
  end

  def handle_cast({:invalidate, volume_id, chunk_hash}, state) do
    remove_entry({volume_id, chunk_hash})
    {:noreply, state}
  end

  ## Private

  defp remove_entry(key) do
    case :ets.lookup(@data_table, key) do
      [{_key, _data, _size, ts}] ->
        {vol_id, hash} = key
        :ets.delete(@data_table, key)
        :ets.delete(@lru_table, {ts, vol_id, hash})

      [] ->
        :ok
    end
  end

  defp update_lru({volume_id, chunk_hash}, old_ts, new_ts) do
    :ets.delete(@lru_table, {old_ts, volume_id, chunk_hash})
    :ets.insert(@lru_table, {{new_ts, volume_id, chunk_hash}})
  end

  defp evict_if_needed(max_memory) do
    used = calculate_memory_used()

    if used > max_memory do
      evict_oldest(used - max_memory)
    end
  end

  defp evict_oldest(bytes_to_free) when bytes_to_free <= 0, do: :ok

  defp evict_oldest(bytes_to_free) do
    case find_oldest_entry() do
      nil ->
        :ok

      {ts, volume_id, chunk_hash, size} ->
        :ets.delete(@data_table, {volume_id, chunk_hash})
        :ets.delete(@lru_table, {ts, volume_id, chunk_hash})
        bump_stat(:evictions)

        :telemetry.execute(
          [:neonfs, :chunk_cache, :eviction],
          %{bytes: size},
          %{volume_id: volume_id}
        )

        evict_oldest(bytes_to_free - size)
    end
  end

  defp find_oldest_entry do
    do_find_oldest(:ets.first(@lru_table))
  end

  defp do_find_oldest(:"$end_of_table"), do: nil

  defp do_find_oldest({ts, volume_id, chunk_hash} = key) do
    case :ets.lookup(@data_table, {volume_id, chunk_hash}) do
      [{_key, _data, size, _ts}] ->
        {ts, volume_id, chunk_hash, size}

      [] ->
        :ets.delete(@lru_table, key)
        do_find_oldest(:ets.next(@lru_table, key))
    end
  end

  defp calculate_memory_used do
    :ets.tab2list(@data_table)
    |> Enum.reduce(0, fn {_key, _data, size, _ts}, acc -> acc + size end)
  end

  defp normalise_chunk_types(opts) do
    chunk_type = Keyword.get(opts, :chunk_type, :local)

    chunk_type
    |> List.wrap()
    |> Enum.reject(&is_nil/1)
    |> Enum.uniq()
    |> then(fn
      [] -> [:local]
      types -> types
    end)
  end

  defp cache_allowed?(_volume_id, [:local]), do: true

  defp cache_allowed?(volume_id, chunk_types) do
    caching = lookup_caching_config(volume_id)

    Enum.all?(chunk_types, fn
      :local -> true
      :transformed -> caching.transformed_chunks
      :reconstructed -> caching.reconstructed_stripes
      :remote -> caching.remote_chunks
      _ -> false
    end)
  end

  defp lookup_caching_config(volume_id) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} -> volume.caching
      _ -> Volume.default_caching()
    end
  rescue
    _ -> Volume.default_caching()
  catch
    :exit, _ -> Volume.default_caching()
  end

  defp bump_stat(key) do
    :ets.update_counter(:chunk_cache_stats, key, {2, 1})
  rescue
    ArgumentError -> :ok
  end

  defp get_stat(key) do
    case :ets.lookup(:chunk_cache_stats, key) do
      [{^key, val}] -> val
      [] -> 0
    end
  rescue
    ArgumentError -> 0
  end

  defp now_us do
    System.monotonic_time(:microsecond)
  end
end
