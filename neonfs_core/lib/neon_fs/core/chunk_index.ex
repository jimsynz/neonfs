defmodule NeonFS.Core.ChunkIndex do
  @moduledoc """
  GenServer managing chunk metadata against the per-volume metadata path
  (#785). Provides fast point lookups by `(volume_id, hash)` plus list /
  scan operations served from a local ETS write-through cache.

  ## Per-volume metadata path

  Reads delegate to `Volume.MetadataReader.get_chunk_meta/3` and writes
  to `Volume.MetadataWriter.put/5` — both walk the bootstrap layer →
  root segment → index tree, keyed by the volume the chunk belongs to.
  The `:metadata_reader_opts` and `:metadata_writer_opts` start_link
  opts inject the underlying cluster-state / Ra / NIF dependencies so
  unit tests can stub them. Production callers leave both empty and pick
  up real defaults.

  ## Local-Only State

  `active_write_refs` is per-node ephemeral state and is intentionally
  excluded from the persisted `ChunkMeta` payload (see
  `chunk_to_storable_map/1`). Add / remove ref operations mutate the
  local ETS entry directly and never round-trip through MetadataWriter.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{
    ChunkCrypto,
    ChunkMeta,
    FileIndex
  }

  alias NeonFS.Core.Volume.{MetadataReader, MetadataValue, MetadataWriter}

  @type location :: ChunkMeta.location()

  @chunk_key_prefix "chunk:"

  # Client API

  @doc """
  Starts the ChunkIndex GenServer.

  ## Options

    * `:metadata_reader_opts` — keyword list forwarded to
      `Volume.MetadataReader.get_chunk_meta/3`. Production callers
      leave this empty; tests inject a stub backend.
    * `:metadata_writer_opts` — keyword list forwarded to
      `Volume.MetadataWriter.put/5` and `delete/4`. Production callers
      leave this empty; tests inject a stub backend.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores or updates chunk metadata.

  Returns `:ok` on success, or `{:error, reason}` if the operation fails.
  """
  @spec put(ChunkMeta.t()) :: :ok | {:error, term()}
  def put(%ChunkMeta{} = chunk_meta) do
    GenServer.call(__MODULE__, {:put, chunk_meta}, 10_000)
  end

  @doc """
  Retrieves chunk metadata by `volume_id` and `hash`.

  Resolves through `Volume.MetadataReader.get_chunk_meta/3`. The local
  ETS table is a write-through materialisation for list operations on
  this node — serving point reads from it would return stale values
  for keys written or deleted elsewhere in the cluster (#342).
  """
  @spec get(binary(), binary()) :: {:ok, ChunkMeta.t()} | {:error, :not_found}
  def get(volume_id, hash) when is_binary(volume_id) and is_binary(hash) do
    get_from_metadata_reader(volume_id, hash)
  end

  @doc """
  Deletes chunk metadata by hash.
  """
  @spec delete(binary()) :: :ok | {:error, term()}
  def delete(hash) when is_binary(hash) do
    GenServer.call(__MODULE__, {:delete, hash}, 10_000)
  end

  @doc """
  Checks whether chunk metadata exists for the given `volume_id` /
  `hash`.

  Resolves through `Volume.MetadataReader.get_chunk_meta/3` — see
  `get/2` for rationale.
  """
  @spec exists?(binary(), binary()) :: boolean()
  def exists?(volume_id, hash) when is_binary(volume_id) and is_binary(hash) do
    match?({:ok, _}, get_from_metadata_reader(volume_id, hash))
  end

  @doc """
  Returns all chunk metadata stored in the local ETS cache.
  """
  @spec list_all() :: [ChunkMeta.t()]
  def list_all do
    :ets.foldl(
      fn
        {_hash, %ChunkMeta{} = chunk_meta}, acc -> [chunk_meta | acc]
        _, acc -> acc
      end,
      [],
      :chunk_index
    )
  end

  @doc """
  Returns all chunks associated with a volume.

  Resolves chunks through FileIndex: gets all files for the volume, collects
  their chunk hashes, and looks up each from the local ETS cache.
  """
  @spec get_chunks_for_volume(binary()) :: [ChunkMeta.t()]
  def get_chunks_for_volume(volume_id) when is_binary(volume_id) do
    chunk_hashes =
      FileIndex.list_volume(volume_id)
      |> Enum.flat_map(fn file_meta -> file_meta.chunks end)
      |> Enum.uniq()

    Enum.reduce(chunk_hashes, [], fn hash, acc ->
      case :ets.lookup(:chunk_index, hash) do
        [{^hash, %ChunkMeta{} = chunk_meta}] -> [chunk_meta | acc]
        _ -> acc
      end
    end)
  end

  @doc """
  Lists all chunks at a specific location (node + drive_id + tier).
  """
  @spec list_by_location(location()) :: [ChunkMeta.t()]
  def list_by_location(%{node: node, drive_id: drive_id, tier: tier}) do
    :ets.foldl(
      fn
        {_hash, %ChunkMeta{} = chunk_meta}, acc ->
          if has_location?(chunk_meta, node, drive_id, tier) do
            [chunk_meta | acc]
          else
            acc
          end

        _, acc ->
          acc
      end,
      [],
      :chunk_index
    )
  end

  @doc """
  Lists all chunks at a specific node (any drive or tier).
  """
  @spec list_by_node(atom()) :: [ChunkMeta.t()]
  def list_by_node(node) when is_atom(node) do
    :ets.foldl(
      fn
        {_hash, %ChunkMeta{} = chunk_meta}, acc ->
          if has_node?(chunk_meta, node) do
            [chunk_meta | acc]
          else
            acc
          end

        _, acc ->
          acc
      end,
      [],
      :chunk_index
    )
  end

  @doc """
  Lists all chunks with a location on a given node and drive (any tier).
  """
  @spec list_by_drive(node(), String.t()) :: [ChunkMeta.t()]
  def list_by_drive(node, drive_id) when is_atom(node) and is_binary(drive_id) do
    :ets.foldl(
      fn
        {_hash, %ChunkMeta{} = chunk_meta}, acc ->
          if has_drive?(chunk_meta, node, drive_id) do
            [chunk_meta | acc]
          else
            acc
          end

        _, acc ->
          acc
      end,
      [],
      :chunk_index
    )
  end

  @doc """
  Lists all uncommitted chunks.
  """
  @spec list_uncommitted() :: [ChunkMeta.t()]
  def list_uncommitted do
    :ets.foldl(
      fn
        {_hash, %ChunkMeta{commit_state: :uncommitted} = chunk_meta}, acc ->
          [chunk_meta | acc]

        _, acc ->
          acc
      end,
      [],
      :chunk_index
    )
  end

  @doc """
  Adds a write reference to a chunk.

  Write references are local-only (ephemeral, single-node) and are never replicated.
  """
  @spec add_write_ref(binary(), ChunkMeta.write_id()) :: :ok | {:error, term()}
  def add_write_ref(hash, write_id) do
    GenServer.call(__MODULE__, {:add_write_ref, hash, write_id}, 10_000)
  end

  @doc """
  Removes a write reference from a chunk.

  Write references are local-only (ephemeral, single-node) and are never replicated.
  """
  @spec remove_write_ref(binary(), ChunkMeta.write_id()) :: :ok | {:error, term()}
  def remove_write_ref(hash, write_id) do
    GenServer.call(__MODULE__, {:remove_write_ref, hash, write_id}, 10_000)
  end

  @doc """
  Commits a chunk, transitioning from uncommitted to committed.
  Only succeeds if there are no active write refs.
  """
  @spec commit(binary()) :: :ok | {:error, term()}
  def commit(hash) do
    GenServer.call(__MODULE__, {:commit, hash}, 10_000)
  end

  @doc """
  Updates the locations for a chunk (used during replication).
  """
  @spec update_locations(binary(), [location()]) :: :ok | {:error, term()}
  def update_locations(hash, locations) when is_binary(hash) and is_list(locations) do
    GenServer.call(__MODULE__, {:update_locations, hash, locations}, 10_000)
  end

  # Server Callbacks

  @impl true
  def init(opts) do
    :ets.new(:chunk_index, [:set, :named_table, :public, read_concurrency: true])

    metadata_reader_opts =
      Keyword.get(opts, :metadata_reader_opts) ||
        :persistent_term.get({__MODULE__, :metadata_reader_opts}, [])

    :persistent_term.put({__MODULE__, :metadata_reader_opts}, metadata_reader_opts)

    metadata_writer_opts =
      Keyword.get(opts, :metadata_writer_opts) ||
        :persistent_term.get({__MODULE__, :metadata_writer_opts}, [])

    :persistent_term.put({__MODULE__, :metadata_writer_opts}, metadata_writer_opts)

    {:ok, %{}}
  end

  @impl true
  def handle_call({:put, chunk_meta}, _from, state) do
    case write_chunk(chunk_meta) do
      :ok ->
        :ets.insert(:chunk_index, {chunk_meta.hash, chunk_meta})
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:delete, hash}, _from, state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, %ChunkMeta{volume_id: volume_id} = _meta}] when is_binary(volume_id) ->
        case delete_chunk_meta(volume_id, hash) do
          :ok ->
            :ets.delete(:chunk_index, hash)
            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      _ ->
        # No local entry, nothing to delete in the index tree either —
        # the put would have populated both. Treat as idempotent :ok.
        :ets.delete(:chunk_index, hash)
        {:reply, :ok, state}
    end
  end

  @impl true
  def handle_call({:add_write_ref, hash, write_id}, _from, state) do
    # `active_write_refs` are local-only (ephemeral, per-node) — never
    # persisted to the volume index tree. ETS-only mutation.
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        updated_meta = ChunkMeta.add_write_ref(chunk_meta, write_id)
        :ets.insert(:chunk_index, {hash, updated_meta})
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:remove_write_ref, hash, write_id}, _from, state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        updated_meta = ChunkMeta.remove_write_ref(chunk_meta, write_id)
        :ets.insert(:chunk_index, {hash, updated_meta})
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:commit, hash}, _from, state) do
    reply = do_commit_chunk(hash)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:update_locations, hash, locations}, _from, state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        updated_meta = %{chunk_meta | locations: locations}

        case write_chunk(updated_meta) do
          :ok ->
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  # Only erase persistent_term on clean shutdown, not on crash. On crash
  # restart, the surviving persistent_term value (set by rebuild_quorum_ring)
  # prevents the child from overwriting it with a stale child_spec ring.
  @impl true
  def terminate(reason, _state) when reason in [:normal, :shutdown] do
    safe_erase_persistent_terms()
  end

  def terminate({:shutdown, _}, _state) do
    safe_erase_persistent_terms()
  end

  def terminate(_reason, _state), do: :ok

  defp safe_erase_persistent_terms do
    for key <- [:metadata_reader_opts, :metadata_writer_opts] do
      try do
        :persistent_term.erase({__MODULE__, key})
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end

  defp metadata_reader_opts do
    :persistent_term.get({__MODULE__, :metadata_reader_opts}, [])
  end

  defp metadata_writer_opts do
    :persistent_term.get({__MODULE__, :metadata_writer_opts}, [])
  end

  # Private — MetadataWriter operations

  defp write_chunk(%ChunkMeta{} = chunk_meta) do
    if is_binary(chunk_meta.volume_id) do
      key = chunk_key(chunk_meta.hash)
      encoded = MetadataValue.encode(chunk_to_storable_map(chunk_meta))

      case MetadataWriter.put(
             chunk_meta.volume_id,
             :chunk_index,
             key,
             encoded,
             metadata_writer_opts()
           ) do
        {:ok, _root} -> :ok
        {:error, _, _} = err -> err
        {:error, _reason} = err -> err
      end
    else
      {:error, :missing_volume_id}
    end
  end

  defp delete_chunk_meta(volume_id, hash) when is_binary(volume_id) do
    key = chunk_key(hash)

    case MetadataWriter.delete(volume_id, :chunk_index, key, metadata_writer_opts()) do
      {:ok, _root} -> :ok
      {:error, _, _} = err -> err
      {:error, _reason} = err -> err
    end
  end

  defp get_from_metadata_reader(volume_id, hash) do
    key = chunk_key(hash)

    case MetadataReader.get_chunk_meta(volume_id, key, metadata_reader_opts()) do
      {:ok, value} -> finalise_metadata_read(hash, value)
      {:error, :not_found} -> {:error, :not_found}
      {:error, _reason} -> {:error, :not_found}
    end
  rescue
    _ -> {:error, :not_found}
  end

  defp finalise_metadata_read(hash, value) do
    chunk_meta =
      value
      |> storable_map_to_chunk()
      |> merge_local_only_fields(hash)

    :ets.insert(:chunk_index, {hash, chunk_meta})
    {:ok, chunk_meta}
  end

  # active_write_refs are local-only (ephemeral, per-node) and never
  # replicated, so they are absent from the quorum payload. Preserve
  # them from the local ETS entry when present.
  defp merge_local_only_fields(%ChunkMeta{} = chunk_meta, hash) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, %ChunkMeta{active_write_refs: refs}}] when refs != nil ->
        %{chunk_meta | active_write_refs: refs}

      _ ->
        chunk_meta
    end
  end

  # Private — Commit

  defp do_commit_chunk(hash) do
    with [{^hash, chunk_meta}] <- :ets.lookup(:chunk_index, hash),
         {:ok, committed_meta} <- ChunkMeta.commit(chunk_meta),
         :ok <- write_chunk(committed_meta) do
      :ets.insert(:chunk_index, {committed_meta.hash, committed_meta})
      :ok
    else
      [] -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  # Private — ETS query helpers

  defp has_location?(chunk_meta, node, drive_id, tier) do
    Enum.any?(chunk_meta.locations, fn location ->
      location.node == node and location.drive_id == drive_id and location.tier == tier
    end)
  end

  defp has_node?(chunk_meta, node) do
    Enum.any?(chunk_meta.locations, fn location ->
      location.node == node
    end)
  end

  defp has_drive?(chunk_meta, node, drive_id) do
    Enum.any?(chunk_meta.locations, fn location ->
      location.node == node and location.drive_id == drive_id
    end)
  end

  # Private — Key format

  defp chunk_key(hash) when is_binary(hash) do
    @chunk_key_prefix <> Base.encode16(hash)
  end

  # Private — Serialisation

  defp chunk_to_storable_map(%ChunkMeta{} = chunk_meta) do
    %{
      volume_id: chunk_meta.volume_id,
      hash: chunk_meta.hash,
      original_size: chunk_meta.original_size,
      stored_size: chunk_meta.stored_size,
      compression: chunk_meta.compression,
      crypto: encode_crypto(chunk_meta.crypto),
      locations: chunk_meta.locations,
      target_replicas: chunk_meta.target_replicas,
      commit_state: chunk_meta.commit_state,
      stripe_id: chunk_meta.stripe_id,
      stripe_index: chunk_meta.stripe_index,
      created_at: chunk_meta.created_at,
      last_verified: chunk_meta.last_verified
    }
  end

  defp storable_map_to_chunk(map) when is_map(map) do
    %ChunkMeta{
      volume_id: get_field(map, :volume_id),
      hash: get_field(map, :hash),
      original_size: get_field(map, :original_size),
      stored_size: get_field(map, :stored_size),
      compression: to_atom(get_field(map, :compression)),
      crypto: decode_crypto(get_field(map, :crypto)),
      locations: decode_locations(get_field(map, :locations, [])),
      target_replicas: get_field(map, :target_replicas, 1),
      commit_state: to_atom(get_field(map, :commit_state, :uncommitted)),
      active_write_refs: MapSet.new(),
      stripe_id: get_field(map, :stripe_id),
      stripe_index: get_field(map, :stripe_index),
      created_at: decode_datetime(get_field(map, :created_at)),
      last_verified: decode_datetime(get_field(map, :last_verified))
    }
  end

  defp get_field(map, key, default \\ nil) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || default
  end

  defp decode_locations(locations) when is_list(locations) do
    Enum.map(locations, fn loc ->
      %{
        node: to_atom(get_field(loc, :node)),
        drive_id: get_field(loc, :drive_id),
        tier: to_atom(get_field(loc, :tier))
      }
    end)
  end

  defp decode_locations(_), do: []

  defp to_atom(value) when is_atom(value), do: value
  defp to_atom(value) when is_binary(value), do: String.to_existing_atom(value)
  defp to_atom(nil), do: nil

  defp decode_datetime(%DateTime{} = dt), do: dt
  defp decode_datetime(nil), do: nil

  defp decode_datetime(str) when is_binary(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _offset} -> dt
      _ -> nil
    end
  end

  defp decode_datetime(_), do: nil

  defp encode_crypto(nil), do: nil

  defp encode_crypto(%ChunkCrypto{} = crypto) do
    %{algorithm: crypto.algorithm, nonce: crypto.nonce, key_version: crypto.key_version}
  end

  defp decode_crypto(nil), do: nil

  defp decode_crypto(map) when is_map(map) do
    %ChunkCrypto{
      algorithm: to_atom(get_field(map, :algorithm, :aes_256_gcm)),
      nonce: get_field(map, :nonce),
      key_version: get_field(map, :key_version)
    }
  end
end
