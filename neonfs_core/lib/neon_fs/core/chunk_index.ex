defmodule NeonFS.Core.ChunkIndex do
  @moduledoc """
  GenServer managing chunk metadata with quorum-backed distributed storage.

  Provides fast lookups by hash and queries by location or commit state.
  Uses QuorumCoordinator for distributed writes/reads and maintains a local
  ETS cache for fast reads.

  ## Quorum Mode

  When started with `:quorum_opts`, writes go through `QuorumCoordinator.quorum_write/3`
  and cache misses fall back to `QuorumCoordinator.quorum_read/2`. When quorum_opts
  is nil, falls back to Ra consensus for backward compatibility with the existing
  supervision tree (until the quorum infrastructure is wired into the application).

  ## Local-Only State

  `active_write_refs` are ephemeral per-node state and are never replicated.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{
    ChunkCrypto,
    ChunkMeta,
    FileIndex,
    MetadataCodec,
    QuorumCoordinator,
    RaServer,
    RaSupervisor
  }

  @type location :: ChunkMeta.location()

  @chunk_key_prefix "chunk:"

  # Client API

  @doc """
  Starts the ChunkIndex GenServer.

  ## Options

    * `:quorum_opts` — keyword list passed to QuorumCoordinator (must include `:ring`).
      When nil, operates in local-only ETS mode.
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
  Retrieves chunk metadata by hash.

  Always resolves through `QuorumCoordinator.quorum_read/2` (or the Ra
  fallback when `quorum_opts` is nil). The local ETS table is a
  write-through materialisation for list operations on this node —
  serving point reads from it would return stale values for keys
  written or deleted elsewhere in the cluster (#342).
  """
  @spec get(binary()) :: {:ok, ChunkMeta.t()} | {:error, :not_found}
  def get(hash) when is_binary(hash) do
    get_from_quorum(hash)
  end

  @doc """
  Deletes chunk metadata by hash.
  """
  @spec delete(binary()) :: :ok | {:error, term()}
  def delete(hash) when is_binary(hash) do
    GenServer.call(__MODULE__, {:delete, hash}, 10_000)
  end

  @doc """
  Checks whether chunk metadata exists for the given hash.

  Always resolves through `QuorumCoordinator.quorum_read/2` — see
  `get/1` for rationale.
  """
  @spec exists?(binary()) :: boolean()
  def exists?(hash) when is_binary(hash) do
    match?({:ok, _}, get_from_quorum(hash))
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

    # Use explicit opts first (unit tests pass quorum_opts directly).
    # Fall back to persistent_term (set by Supervisor or rebuild_quorum_ring).
    # On crash restart, child_spec has no quorum_opts, so persistent_term
    # (preserved by crash-safe terminate) provides the authoritative ring.
    quorum_opts =
      Keyword.get(opts, :quorum_opts) ||
        :persistent_term.get({__MODULE__, :quorum_opts}, nil)

    :persistent_term.put({__MODULE__, :quorum_opts}, quorum_opts)

    if quorum_opts do
      case load_from_local_store() do
        {:ok, count} ->
          Logger.info("ChunkIndex started, loaded chunks from local store", count: count)

        {:error, reason} ->
          Logger.debug("ChunkIndex started, local store not available", reason: reason)
      end
    else
      case restore_from_ra() do
        {:ok, count} ->
          Logger.info("ChunkIndex started, restored chunks from Ra", count: count)

        {:error, reason} ->
          Logger.debug("ChunkIndex started but Ra not ready yet", reason: reason)
      end
    end

    {:ok, %{quorum_opts: quorum_opts}}
  end

  @impl true
  def handle_call({:put, chunk_meta}, _from, state) do
    case do_quorum_write(chunk_meta, quorum_opts()) do
      :ok ->
        :ets.insert(:chunk_index, {chunk_meta.hash, chunk_meta})
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:delete, hash}, _from, state) do
    case do_quorum_delete(hash, quorum_opts()) do
      :ok ->
        :ets.delete(:chunk_index, hash)
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:add_write_ref, hash, write_id}, _from, %{quorum_opts: nil} = state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        case maybe_ra_command({:add_write_ref, hash, write_id}) do
          {:ok, :ok} ->
            updated_meta = ChunkMeta.add_write_ref(chunk_meta, write_id)
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:ok, {:error, :not_found}} ->
            {:reply, {:error, :not_found}, state}

          {:error, :ra_not_available} ->
            updated_meta = ChunkMeta.add_write_ref(chunk_meta, write_id)
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:add_write_ref, hash, write_id}, _from, state) do
    # Quorum mode: write refs are local-only (ephemeral, not replicated)
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
  def handle_call({:remove_write_ref, hash, write_id}, _from, %{quorum_opts: nil} = state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        case maybe_ra_command({:remove_write_ref, hash, write_id}) do
          {:ok, :ok} ->
            updated_meta = ChunkMeta.remove_write_ref(chunk_meta, write_id)
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:ok, {:error, :not_found}} ->
            {:reply, {:error, :not_found}, state}

          {:error, :ra_not_available} ->
            updated_meta = ChunkMeta.remove_write_ref(chunk_meta, write_id)
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:remove_write_ref, hash, write_id}, _from, state) do
    # Quorum mode: write refs are local-only (ephemeral, not replicated)
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
    reply = do_commit_chunk(hash, quorum_opts())
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:update_locations, hash, locations}, _from, %{quorum_opts: nil} = state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        updated_meta = %{chunk_meta | locations: locations}

        case maybe_ra_command({:update_chunk_locations, hash, locations}) do
          {:ok, :ok} ->
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:ok, {:error, reason}} ->
            {:reply, {:error, reason}, state}

          {:error, :ra_not_available} ->
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:update_locations, hash, locations}, _from, state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        updated_meta = %{chunk_meta | locations: locations}

        case do_quorum_write(updated_meta, quorum_opts()) do
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
    safe_erase_quorum_opts()
  end

  def terminate({:shutdown, _}, _state) do
    safe_erase_quorum_opts()
  end

  def terminate(_reason, _state), do: :ok

  defp safe_erase_quorum_opts do
    :persistent_term.erase({__MODULE__, :quorum_opts})
    :ok
  rescue
    ArgumentError -> :ok
  end

  # Private — Current quorum opts (always read from persistent_term to get
  # the latest ring after rebuild_quorum_ring updates it)

  defp quorum_opts do
    :persistent_term.get({__MODULE__, :quorum_opts}, nil)
  end

  # Private — Quorum operations

  defp do_quorum_write(chunk_meta, nil) do
    case maybe_ra_command({:put_chunk, chunk_to_ra_map(chunk_meta)}) do
      {:ok, :ok} ->
        :ok

      {:error, :ra_not_available} ->
        :ets.insert(:chunk_index, {chunk_meta.hash, chunk_meta})
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp do_quorum_write(chunk_meta, quorum_opts) do
    key = chunk_key(chunk_meta.hash)
    storable = chunk_to_storable_map(chunk_meta)

    case QuorumCoordinator.quorum_write(key, storable, quorum_opts) do
      {:ok, :written} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_quorum_delete(hash, nil) do
    case maybe_ra_command({:delete_chunk, hash}) do
      {:ok, :ok} -> :ok
      {:error, :ra_not_available} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_quorum_delete(hash, quorum_opts) do
    key = chunk_key(hash)

    case QuorumCoordinator.quorum_delete(key, quorum_opts) do
      {:ok, :written} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp get_from_quorum(hash) do
    case quorum_opts() do
      nil ->
        get_from_ra(hash)

      opts ->
        key = chunk_key(hash)

        case QuorumCoordinator.quorum_read(key, opts) do
          {:ok, value} -> finalise_quorum_read(hash, value)
          {:ok, value, :possibly_stale} -> finalise_quorum_read(hash, value)
          {:error, :not_found} -> {:error, :not_found}
          {:error, _reason} -> {:error, :not_found}
        end
    end
  rescue
    _ -> {:error, :not_found}
  end

  defp finalise_quorum_read(hash, value) do
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

  defp do_commit_chunk(hash, quorum_opts) do
    with [{^hash, chunk_meta}] <- :ets.lookup(:chunk_index, hash),
         {:ok, committed_meta} <- ChunkMeta.commit(chunk_meta),
         :ok <- persist_committed_chunk(committed_meta, quorum_opts) do
      :ok
    else
      [] -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp persist_committed_chunk(committed_meta, nil) do
    case maybe_ra_command({:commit_chunk, committed_meta.hash}) do
      {:ok, :ok} ->
        :ets.insert(:chunk_index, {committed_meta.hash, committed_meta})
        :ok

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:error, :ra_not_available} ->
        :ets.insert(:chunk_index, {committed_meta.hash, committed_meta})
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp persist_committed_chunk(committed_meta, quorum_opts) do
    case do_quorum_write(committed_meta, quorum_opts) do
      :ok ->
        :ets.insert(:chunk_index, {committed_meta.hash, committed_meta})
        :ok

      {:error, reason} ->
        {:error, reason}
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

  # Private — Local store loading

  defp load_from_local_store do
    drives = Application.get_env(:neonfs_core, :drives) || default_drives()

    count =
      Enum.reduce(drives, 0, fn drive, total ->
        drive_path = drive_path(drive)
        meta_dir = Path.join(drive_path, "meta")

        case File.ls(meta_dir) do
          {:ok, segment_dirs} ->
            total + load_segments_from_disk(meta_dir, segment_dirs)

          {:error, _} ->
            total
        end
      end)

    {:ok, count}
  rescue
    _ -> {:error, :not_available}
  end

  defp load_segments_from_disk(meta_dir, segment_dirs) do
    Enum.reduce(segment_dirs, 0, fn segment_hex, count ->
      segment_dir = Path.join(meta_dir, segment_hex)
      key_files = walk_metadata_files(segment_dir)
      count + load_chunk_files(key_files)
    end)
  end

  defp load_chunk_files(file_paths) do
    Enum.reduce(file_paths, 0, fn file_path, count ->
      case load_chunk_file(file_path) do
        :ok -> count + 1
        :skip -> count
      end
    end)
  end

  defp load_chunk_file(file_path) do
    with {:ok, data} <- File.read(file_path),
         {:ok, chunk_meta} <- decode_chunk_record(data) do
      :ets.insert(:chunk_index, {chunk_meta.hash, chunk_meta})
      :ok
    else
      _ -> :skip
    end
  end

  defp walk_metadata_files(dir) do
    case File.ls(dir) do
      {:ok, entries} ->
        Enum.flat_map(entries, &collect_metadata_entry(dir, &1))

      {:error, _} ->
        []
    end
  end

  defp collect_metadata_entry(dir, entry) do
    path = Path.join(dir, entry)

    cond do
      File.dir?(path) -> walk_metadata_files(path)
      String.contains?(entry, ".tmp") -> []
      true -> [path]
    end
  end

  defp decode_chunk_record(data) do
    case MetadataCodec.decode_record(data) do
      {:ok, %{tombstone: true}} ->
        :skip

      {:ok, %{value: value}} when is_map(value) ->
        if chunk_metadata?(value) do
          {:ok, storable_map_to_chunk(value)}
        else
          :skip
        end

      _ ->
        :skip
    end
  end

  defp chunk_metadata?(map) do
    (Map.has_key?(map, :hash) or Map.has_key?(map, "hash")) and
      (Map.has_key?(map, :original_size) or Map.has_key?(map, "original_size")) and
      (Map.has_key?(map, :stored_size) or Map.has_key?(map, "stored_size"))
  end

  defp default_drives do
    base_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs/blobs")
    [%{id: "default", path: base_dir, tier: :hot, capacity: 0}]
  end

  defp drive_path(%{path: path}), do: path
  defp drive_path(drive) when is_map(drive), do: Map.get(drive, :path, Map.get(drive, "path", ""))

  # Private — Ra fallback (nil quorum_opts mode)

  defp get_from_ra(hash) do
    case RaSupervisor.query(fn state ->
           chunks = Map.get(state, :chunks, %{})
           Map.get(chunks, hash)
         end) do
      {:ok, nil} ->
        {:error, :not_found}

      {:ok, chunk_map} ->
        chunk_meta = ra_map_to_chunk(chunk_map)
        :ets.insert(:chunk_index, {hash, chunk_meta})
        {:ok, chunk_meta}

      {:error, :noproc} ->
        {:error, :not_found}

      {:error, _reason} ->
        {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :not_found}
  end

  defp maybe_ra_command(cmd) do
    initialized = RaServer.initialized?()

    try do
      case RaSupervisor.command(cmd) do
        {:ok, {:error, :unknown_command}, _leader} ->
          {:error, :ra_not_available}

        {:ok, result, _leader} ->
          {:ok, result}

        {:error, :noproc} ->
          ra_noproc_error(initialized)

        {:error, reason} ->
          {:error, reason}

        {:timeout, _node} ->
          {:error, :timeout}
      end
    catch
      :exit, {:noproc, _} ->
        ra_noproc_error(initialized)

      kind, reason ->
        Logger.debug("Ra command error", kind: kind, reason: reason)

        if initialized,
          do: {:error, {:ra_error, {kind, reason}}},
          else: {:error, :ra_not_available}
    end
  end

  defp ra_noproc_error(true), do: {:error, :ra_unavailable}
  defp ra_noproc_error(false), do: {:error, :ra_not_available}

  defp restore_from_ra do
    case RaSupervisor.query(fn state -> Map.get(state, :chunks, %{}) end) do
      {:ok, chunks} when is_map(chunks) ->
        count =
          Enum.reduce(chunks, 0, fn {hash, chunk_map}, acc ->
            chunk_meta = ra_map_to_chunk(chunk_map)
            :ets.insert(:chunk_index, {hash, chunk_meta})
            acc + 1
          end)

        {:ok, count}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp chunk_to_ra_map(%ChunkMeta{} = chunk_meta) do
    %{
      hash: chunk_meta.hash,
      original_size: chunk_meta.original_size,
      stored_size: chunk_meta.stored_size,
      compression: chunk_meta.compression,
      crypto: encode_crypto(chunk_meta.crypto),
      locations: chunk_meta.locations,
      target_replicas: chunk_meta.target_replicas,
      commit_state: chunk_meta.commit_state,
      active_write_refs: chunk_meta.active_write_refs,
      stripe_id: chunk_meta.stripe_id,
      stripe_index: chunk_meta.stripe_index,
      created_at: chunk_meta.created_at,
      last_verified: chunk_meta.last_verified
    }
  end

  defp ra_map_to_chunk(chunk_map) when is_map(chunk_map) do
    %ChunkMeta{
      hash: chunk_map.hash,
      original_size: chunk_map.original_size,
      stored_size: chunk_map.stored_size,
      compression: chunk_map.compression,
      crypto: decode_crypto(chunk_map[:crypto]),
      locations: chunk_map.locations,
      target_replicas: chunk_map.target_replicas,
      commit_state: chunk_map.commit_state,
      active_write_refs: chunk_map.active_write_refs,
      stripe_id: chunk_map[:stripe_id],
      stripe_index: chunk_map[:stripe_index],
      created_at: chunk_map.created_at,
      last_verified: chunk_map[:last_verified]
    }
  end
end
