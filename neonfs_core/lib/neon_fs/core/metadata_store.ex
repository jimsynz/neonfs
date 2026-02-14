defmodule NeonFS.Core.MetadataStore do
  @moduledoc """
  Metadata storage layer for leaderless quorum-replicated metadata.

  Wraps the BlobStore metadata namespace with ETS caching, HLC timestamps,
  MessagePack serialisation (via msgpax), and tombstone support. Each metadata
  record contains a value, an HLC timestamp, and a tombstone flag.

  ## Record Format

  Records are serialised as MessagePack binaries via `NeonFS.Core.MetadataCodec`:

      %{"v" => value, "ts" => [wall_ms, counter, node_string], "t" => tombstone}

  ## Serialisation Transformations

  - Atom values become strings (`:hot` → `"hot"`)
  - Atom map keys become string keys (`%{name: "x"}` → `%{"name" => "x"}`)
  - Tuples, MapSets, Ranges, DateTimes are preserved via MessagePack Ext types

  ## ETS Caching

  Per-segment ETS tables are created on demand. Entries are `{key_hash, record}`.
  Capped at `max_cache_size` per table (insertion skipped when full — proper LRU
  deferred to a future task).

  ## Key Hashing

  Callers provide string keys like `"chunk:" <> chunk_hash_hex`. These are
  SHA-256 hashed to produce the 32-byte key used for BlobStore addressing.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.DriveRegistry
  alias NeonFS.Core.HLC
  alias NeonFS.Core.MetadataCodec

  @default_max_cache_size 10_000

  @type segment_id :: binary()
  @type key :: String.t()
  @type record :: %{
          value: term(),
          hlc_timestamp: HLC.timestamp(),
          tombstone: boolean()
        }

  ## Client API

  @doc """
  Starts the MetadataStore GenServer.

  ## Options

    * `:max_cache_size` - Maximum entries per segment cache table (default: #{@default_max_cache_size})
    * `:drive_id` - Specific drive to use (default: auto-select hot-tier drive)
    * `:blob_store_server` - BlobStore GenServer name (default: `NeonFS.Core.BlobStore`)
    * `:name` - GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Writes a metadata value for the given segment and key.

  Generates an HLC timestamp, serialises the record, writes to BlobStore,
  and updates the ETS cache.

  ## Parameters

    * `segment_id` - 32-byte binary segment identifier
    * `key` - String key (e.g. `"chunk:" <> hash_hex`)
    * `value` - Any Erlang term to store
    * `opts` - Optional keyword list:
      * `:server` - GenServer name, defaults to `__MODULE__`
  """
  @spec write(segment_id(), key(), term(), keyword()) :: :ok | {:error, term()}
  def write(segment_id, key, value, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)

    case Keyword.get(opts, :caller_timestamp) do
      nil ->
        GenServer.call(server, {:write, segment_id, key, value})

      timestamp ->
        GenServer.call(server, {:write_with_timestamp, segment_id, key, value, timestamp})
    end
  end

  @doc """
  Generates a new HLC timestamp for use as a coordinator-level write timestamp.

  Call this on the coordinating node BEFORE dispatching quorum writes, then pass
  the returned timestamp via `caller_timestamp: ts` to `write/4` on all replicas.
  This ensures all replicas record the same logical timestamp for the same write,
  preventing late-arriving writes from overwriting later logical writes.
  """
  @spec generate_timestamp(keyword()) :: HLC.timestamp()
  def generate_timestamp(opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, :generate_timestamp)
  end

  @doc """
  Reads a metadata value for the given segment and key.

  Checks the ETS cache first, then falls back to BlobStore. Tombstoned
  records are treated as not found.

  ## Returns

    * `{:ok, value, timestamp}` - The stored value and its HLC timestamp
    * `{:error, :not_found}` - If the key doesn't exist or is tombstoned
  """
  @spec read(segment_id(), key(), keyword()) ::
          {:ok, term(), HLC.timestamp()} | {:error, :not_found}
  def read(segment_id, key, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:read, segment_id, key})
  end

  @doc """
  Deletes a metadata key by writing a tombstone marker.

  The key is not physically removed — a tombstone record is written so that
  the deletion propagates through quorum replication.
  """
  @spec delete(segment_id(), key(), keyword()) :: :ok | {:error, term()}
  def delete(segment_id, key, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:delete, segment_id, key})
  end

  @doc """
  Lists all live (non-tombstoned) metadata entries in a segment.

  ## Returns

    * `{:ok, entries}` - List of `{key_hash, value, timestamp}` tuples
    * `{:error, reason}` - On failure
  """
  @spec list_segment(segment_id(), keyword()) ::
          {:ok, [{binary(), term(), HLC.timestamp()}]} | {:error, term()}
  def list_segment(segment_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_segment, segment_id})
  end

  @doc """
  Lists all metadata entries in a segment, including tombstones.

  Used by anti-entropy to compare full segment state between replicas.

  ## Returns

    * `{:ok, entries}` - List of `{key_hash, record}` tuples where record
      contains `:value`, `:hlc_timestamp`, and `:tombstone` fields
    * `{:error, reason}` - On failure
  """
  @spec list_segment_all(segment_id(), keyword()) ::
          {:ok, [{binary(), record()}]} | {:error, term()}
  def list_segment_all(segment_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:list_segment_all, segment_id})
  end

  @doc """
  Computes a Merkle tree hash for a segment.

  Hashes each key-value-timestamp record, sorts by key_hash, then computes
  a root hash from the concatenation of all record hashes. Two replicas with
  identical data will produce the same root hash.

  ## Returns

    * `{:ok, root_hash, record_count}` - The root hash and number of records
    * `{:error, reason}` - On failure
  """
  @spec merkle_tree(segment_id(), keyword()) ::
          {:ok, binary(), non_neg_integer()} | {:error, term()}
  def merkle_tree(segment_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:merkle_tree, segment_id})
  end

  @doc """
  Physically removes a tombstoned key from storage and cache.

  Used by anti-entropy after confirming all replicas agree on the tombstone.
  Unlike `delete/3` which writes a tombstone marker, this permanently removes
  the record from BlobStore.
  """
  @spec purge_tombstone(segment_id(), key(), keyword()) :: :ok | {:error, term()}
  def purge_tombstone(segment_id, key, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:purge_tombstone, segment_id, key})
  end

  @doc """
  Loads all records from a segment on disk into the ETS cache.

  Useful for warming the cache after startup or segment migration.
  """
  @spec load_segment(segment_id(), keyword()) :: :ok | {:error, term()}
  def load_segment(segment_id, opts \\ []) do
    server = Keyword.get(opts, :server, __MODULE__)
    GenServer.call(server, {:load_segment, segment_id})
  end

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    max_cache_size = Keyword.get(opts, :max_cache_size, @default_max_cache_size)
    blob_store_server = Keyword.get(opts, :blob_store_server, BlobStore)
    drive_id = Keyword.get(opts, :drive_id) || select_drive()

    hlc = HLC.new(Node.self())

    {:ok,
     %{
       hlc: hlc,
       drive_id: drive_id,
       segment_tables: %{},
       max_cache_size: max_cache_size,
       blob_store_server: blob_store_server
     }}
  end

  @impl true
  def handle_call(:generate_timestamp, _from, state) do
    {timestamp, new_hlc} = HLC.now(state.hlc)
    {:reply, timestamp, %{state | hlc: new_hlc}}
  end

  @impl true
  def handle_call({:write, segment_id, key, value}, _from, state) do
    key_hash = compute_key_hash(key)
    {timestamp, new_hlc} = HLC.now(state.hlc)
    do_write(segment_id, key_hash, value, timestamp, %{state | hlc: new_hlc})
  end

  @impl true
  def handle_call(
        {:write_with_timestamp, segment_id, key, value, caller_timestamp},
        _from,
        state
      ) do
    key_hash = compute_key_hash(key)

    # Advance local HLC to stay monotonic with the caller's timestamp.
    # This ensures future locally-generated timestamps are >= the caller's.
    new_hlc =
      case HLC.receive_timestamp(state.hlc, caller_timestamp) do
        {:ok, _merged_ts, advanced_hlc} -> advanced_hlc
        {:error, :clock_skew_detected, _skew} -> state.hlc
      end

    do_write(segment_id, key_hash, value, caller_timestamp, %{state | hlc: new_hlc})
  end

  @impl true
  def handle_call({:read, segment_id, key}, _from, state) do
    key_hash = compute_key_hash(key)

    case cache_get(state, segment_id, key_hash) do
      {:ok, record} ->
        {:reply, record_to_read_result(record), state}

      :miss ->
        {reply, new_state} = read_from_blob_store(state, segment_id, key_hash)
        {:reply, reply, new_state}
    end
  end

  @impl true
  def handle_call({:delete, segment_id, key}, _from, state) do
    key_hash = compute_key_hash(key)
    {timestamp, new_hlc} = HLC.now(state.hlc)

    record = %{value: nil, hlc_timestamp: timestamp, tombstone: true}

    case MetadataCodec.encode_record(record) do
      {:ok, serialised} ->
        case BlobStore.write_metadata(segment_id, key_hash, serialised, state.drive_id,
               server: state.blob_store_server
             ) do
          {:ok, {}} ->
            {:ok, normalised} = MetadataCodec.decode_record(serialised)
            new_state = %{state | hlc: new_hlc}
            new_state = cache_put(new_state, segment_id, key_hash, normalised)
            {:reply, :ok, new_state}

          {:error, reason} ->
            {:reply, {:error, reason}, %{state | hlc: new_hlc}}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, %{state | hlc: new_hlc}}
    end
  end

  @impl true
  def handle_call({:list_segment, segment_id}, _from, state) do
    case BlobStore.list_metadata_segment(segment_id, state.drive_id,
           server: state.blob_store_server
         ) do
      {:ok, key_hashes} ->
        entries = collect_live_entries(key_hashes, segment_id, state)
        {:reply, {:ok, entries}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:list_segment_all, segment_id}, _from, state) do
    case BlobStore.list_metadata_segment(segment_id, state.drive_id,
           server: state.blob_store_server
         ) do
      {:ok, key_hashes} ->
        entries = collect_all_entries(key_hashes, segment_id, state)
        {:reply, {:ok, entries}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:merkle_tree, segment_id}, _from, state) do
    case BlobStore.list_metadata_segment(segment_id, state.drive_id,
           server: state.blob_store_server
         ) do
      {:ok, key_hashes} ->
        {root_hash, count} = compute_merkle_tree(key_hashes, segment_id, state)
        {:reply, {:ok, root_hash, count}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:purge_tombstone, segment_id, key}, _from, state) do
    key_hash = compute_key_hash(key)

    case BlobStore.delete_metadata(segment_id, key_hash, state.drive_id,
           server: state.blob_store_server
         ) do
      result when result in [:ok, {:ok, {}}] ->
        cache_remove(state, segment_id, key_hash)
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:load_segment, segment_id}, _from, state) do
    case BlobStore.list_metadata_segment(segment_id, state.drive_id,
           server: state.blob_store_server
         ) do
      {:ok, key_hashes} ->
        new_state = load_records_into_cache(key_hashes, segment_id, state)
        {:reply, :ok, new_state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  ## Private: Write helper

  defp do_write(segment_id, key_hash, value, timestamp, state) do
    record = %{value: value, hlc_timestamp: timestamp, tombstone: false}

    case MetadataCodec.encode_record(record) do
      {:ok, serialised} ->
        case BlobStore.write_metadata(segment_id, key_hash, serialised, state.drive_id,
               server: state.blob_store_server
             ) do
          {:ok, {}} ->
            # Decode the serialised record to normalise values for cache consistency
            # (atom keys → string keys, atom values → strings)
            {:ok, normalised} = MetadataCodec.decode_record(serialised)
            new_state = cache_put(state, segment_id, key_hash, normalised)
            {:reply, :ok, new_state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  ## Private: Record reading helpers

  defp record_to_read_result(%{tombstone: true}), do: {:error, :not_found}
  defp record_to_read_result(record), do: {:ok, record.value, record.hlc_timestamp}

  defp read_from_blob_store(state, segment_id, key_hash) do
    case BlobStore.read_metadata(segment_id, key_hash, state.drive_id,
           server: state.blob_store_server
         ) do
      {:ok, data} ->
        record = deserialise_record(data)
        new_state = cache_put(state, segment_id, key_hash, record)
        {record_to_read_result(record), new_state}

      {:error, _reason} ->
        {{:error, :not_found}, state}
    end
  end

  defp collect_live_entries(key_hashes, segment_id, state) do
    key_hashes
    |> Enum.reduce([], fn key_hash, acc ->
      case read_record_from_blob_store(segment_id, key_hash, state) do
        %{tombstone: false} = record ->
          [{key_hash, record.value, record.hlc_timestamp} | acc]

        _tombstone_or_error ->
          acc
      end
    end)
    |> Enum.reverse()
  end

  defp load_records_into_cache(key_hashes, segment_id, state) do
    Enum.reduce(key_hashes, state, fn key_hash, acc ->
      case read_record_from_blob_store(segment_id, key_hash, acc) do
        nil -> acc
        record -> cache_put(acc, segment_id, key_hash, record)
      end
    end)
  end

  defp read_record_from_blob_store(segment_id, key_hash, state) do
    case BlobStore.read_metadata(segment_id, key_hash, state.drive_id,
           server: state.blob_store_server
         ) do
      {:ok, data} -> deserialise_record(data)
      {:error, _} -> nil
    end
  end

  defp collect_all_entries(key_hashes, segment_id, state) do
    key_hashes
    |> Enum.reduce([], fn key_hash, acc ->
      case read_record_from_blob_store(segment_id, key_hash, state) do
        nil -> acc
        record -> [{key_hash, record} | acc]
      end
    end)
    |> Enum.reverse()
  end

  defp compute_merkle_tree(key_hashes, segment_id, state) do
    record_hashes =
      key_hashes
      |> Enum.sort()
      |> Enum.reduce([], fn key_hash, acc ->
        case read_record_from_blob_store(segment_id, key_hash, state) do
          nil ->
            acc

          record ->
            hash = hash_record(key_hash, record)
            [hash | acc]
        end
      end)
      |> Enum.reverse()

    count = length(record_hashes)

    root_hash =
      if count == 0 do
        :crypto.hash(:sha256, <<>>)
      else
        record_hashes
        |> Enum.reduce(<<>>, &(&2 <> &1))
        |> then(&:crypto.hash(:sha256, &1))
      end

    {root_hash, count}
  end

  defp hash_record(key_hash, record) do
    tombstone_byte = if record.tombstone, do: <<1>>, else: <<0>>

    :crypto.hash(
      :sha256,
      key_hash <>
        :erlang.term_to_binary(record.value) <>
        :erlang.term_to_binary(record.hlc_timestamp) <>
        tombstone_byte
    )
  end

  defp cache_remove(state, segment_id, key_hash) do
    case Map.fetch(state.segment_tables, segment_id) do
      {:ok, table} -> :ets.delete(table, key_hash)
      :error -> true
    end
  end

  ## Private: Key hashing

  defp compute_key_hash(key) when is_binary(key) do
    :crypto.hash(:sha256, key)
  end

  ## Private: Record serialisation

  defp deserialise_record(data) do
    case MetadataCodec.decode_record(data) do
      {:ok, record} -> record
      {:error, reason} -> raise "Failed to decode metadata record: #{inspect(reason)}"
    end
  end

  ## Private: ETS cache

  defp ensure_segment_table(state, segment_id) do
    case Map.fetch(state.segment_tables, segment_id) do
      {:ok, table} ->
        {table, state}

      :error ->
        table = :ets.new(:metadata_cache, [:set, :public])
        new_tables = Map.put(state.segment_tables, segment_id, table)
        {table, %{state | segment_tables: new_tables}}
    end
  end

  defp cache_get(state, segment_id, key_hash) do
    case Map.fetch(state.segment_tables, segment_id) do
      {:ok, table} ->
        case :ets.lookup(table, key_hash) do
          [{^key_hash, record}] -> {:ok, record}
          [] -> :miss
        end

      :error ->
        :miss
    end
  end

  defp cache_put(state, segment_id, key_hash, record) do
    {table, new_state} = ensure_segment_table(state, segment_id)

    # Check if entry already exists (update is always allowed)
    case :ets.lookup(table, key_hash) do
      [{^key_hash, _}] ->
        :ets.insert(table, {key_hash, record})
        new_state

      [] ->
        if :ets.info(table, :size) < new_state.max_cache_size do
          :ets.insert(table, {key_hash, record})
        end

        new_state
    end
  end

  ## Private: Drive selection

  defp select_drive do
    with {:error, _} <- DriveRegistry.select_drive(:hot),
         {:error, _} <- DriveRegistry.select_drive(:warm),
         {:error, _} <- DriveRegistry.select_drive(:cold) do
      "default"
    else
      {:ok, drive} -> drive.id
    end
  end
end
