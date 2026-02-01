defmodule NeonFS.Core.ChunkIndex do
  @moduledoc """
  GenServer managing chunk metadata with Ra-backed distributed storage.

  Provides fast lookups by hash and queries by location or commit state.
  Uses Ra consensus for writes and maintains a local ETS cache for fast reads.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{ChunkMeta, RaSupervisor}

  @type location :: ChunkMeta.location()

  # Client API

  @doc """
  Starts the ChunkIndex GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Stores or updates chunk metadata.
  """
  @spec put(ChunkMeta.t()) :: :ok
  def put(%ChunkMeta{} = chunk_meta) do
    GenServer.call(__MODULE__, {:put, chunk_meta})
  end

  @doc """
  Retrieves chunk metadata by hash.
  """
  @spec get(binary()) :: {:ok, ChunkMeta.t()} | {:error, :not_found}
  def get(hash) when is_binary(hash) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] -> {:ok, chunk_meta}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Deletes chunk metadata by hash.
  """
  @spec delete(binary()) :: :ok
  def delete(hash) when is_binary(hash) do
    GenServer.call(__MODULE__, {:delete, hash})
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

        # Skip non-ChunkMeta entries (can happen with stale DETS data)
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

        # Skip non-ChunkMeta entries (can happen with stale DETS data)
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

        # Skip committed chunks or non-ChunkMeta entries
        _, acc ->
          acc
      end,
      [],
      :chunk_index
    )
  end

  @doc """
  Adds a write reference to a chunk.
  """
  @spec add_write_ref(binary(), ChunkMeta.write_id()) :: :ok | {:error, :not_found}
  def add_write_ref(hash, write_id) do
    GenServer.call(__MODULE__, {:add_write_ref, hash, write_id})
  end

  @doc """
  Removes a write reference from a chunk.
  """
  @spec remove_write_ref(binary(), ChunkMeta.write_id()) :: :ok | {:error, :not_found}
  def remove_write_ref(hash, write_id) do
    GenServer.call(__MODULE__, {:remove_write_ref, hash, write_id})
  end

  @doc """
  Commits a chunk, transitioning from uncommitted to committed.
  Only succeeds if there are no active write refs.
  """
  @spec commit(binary()) :: :ok | {:error, :not_found | :has_active_writes}
  def commit(hash) do
    GenServer.call(__MODULE__, {:commit, hash})
  end

  @doc """
  Updates the locations for a chunk (used during replication).
  """
  @spec update_locations(binary(), [location()]) :: :ok | {:error, :not_found}
  def update_locations(hash, locations) when is_binary(hash) and is_list(locations) do
    GenServer.call(__MODULE__, {:update_locations, hash, locations})
  end

  # Server Callbacks

  @impl true
  def init(_opts) do
    # Create ETS table for chunk metadata
    # :set - one entry per hash
    # :named_table - accessible by name :chunk_index
    # :public - readable from any process
    # read_concurrency: true - optimize for concurrent reads
    :ets.new(:chunk_index, [:set, :named_table, :public, read_concurrency: true])

    # Try to restore chunks from Ra state into ETS
    # If Ra is not ready yet (e.g., during startup), that's okay - the index
    # will be populated as operations occur
    case restore_from_ra() do
      {:ok, count} ->
        Logger.info(
          "ChunkIndex started with ETS table :chunk_index, restored #{count} chunks from Ra"
        )

      {:error, reason} ->
        Logger.debug("ChunkIndex started but Ra not ready yet: #{inspect(reason)}")
    end

    {:ok, %{}}
  end

  @impl true
  def handle_call({:put, chunk_meta}, _from, state) do
    # Try to write through Ra if available, otherwise write directly to ETS
    case maybe_ra_command({:put_chunk, struct_to_map(chunk_meta)}) do
      {:ok, :ok} ->
        # Update local ETS cache
        :ets.insert(:chunk_index, {chunk_meta.hash, chunk_meta})
        {:reply, :ok, state}

      {:error, :ra_not_available} ->
        # Ra not available, write directly to ETS (Phase 1 mode)
        :ets.insert(:chunk_index, {chunk_meta.hash, chunk_meta})
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:delete, hash}, _from, state) do
    # Try to write through Ra if available
    case maybe_ra_command({:delete_chunk, hash}) do
      {:ok, :ok} ->
        # Update local ETS cache
        :ets.delete(:chunk_index, hash)
        {:reply, :ok, state}

      {:error, :ra_not_available} ->
        # Ra not available, write directly to ETS
        :ets.delete(:chunk_index, hash)
        {:reply, :ok, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:add_write_ref, hash, write_id}, _from, state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        # Try to write through Ra if available
        case maybe_ra_command({:add_write_ref, hash, write_id}) do
          {:ok, :ok} ->
            updated_meta = ChunkMeta.add_write_ref(chunk_meta, write_id)
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:ok, {:error, :not_found}} ->
            {:reply, {:error, :not_found}, state}

          {:error, :ra_not_available} ->
            # Ra not available, write directly to ETS
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
  def handle_call({:remove_write_ref, hash, write_id}, _from, state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        # Try to write through Ra if available
        case maybe_ra_command({:remove_write_ref, hash, write_id}) do
          {:ok, :ok} ->
            updated_meta = ChunkMeta.remove_write_ref(chunk_meta, write_id)
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:ok, {:error, :not_found}} ->
            {:reply, {:error, :not_found}, state}

          {:error, :ra_not_available} ->
            # Ra not available, write directly to ETS
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
  def handle_call({:commit, hash}, _from, state) do
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        case ChunkMeta.commit(chunk_meta) do
          {:ok, committed_meta} ->
            # Try to write through Ra if available
            case maybe_ra_command({:commit_chunk, hash}) do
              {:ok, :ok} ->
                :ets.insert(:chunk_index, {hash, committed_meta})
                {:reply, :ok, state}

              {:ok, {:error, reason}} ->
                {:reply, {:error, reason}, state}

              {:error, :ra_not_available} ->
                # Ra not available, write directly to ETS
                :ets.insert(:chunk_index, {hash, committed_meta})
                {:reply, :ok, state}

              {:error, reason} ->
                {:reply, {:error, reason}, state}
            end

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

        # Try to write through Ra if available
        case maybe_ra_command({:update_chunk_locations, hash, locations}) do
          {:ok, :ok} ->
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:ok, {:error, reason}} ->
            {:reply, {:error, reason}, state}

          {:error, :ra_not_available} ->
            # Ra not available, write directly to ETS
            :ets.insert(:chunk_index, {hash, updated_meta})
            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  # Private Helpers

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

  # Try to execute a Ra command, but gracefully handle Ra not being available
  # Returns {:ok, result} | {:error, :ra_not_available} | {:error, reason}
  defp maybe_ra_command(cmd) do
    try do
      case RaSupervisor.command(cmd) do
        {:ok, result, _leader} ->
          {:ok, result}

        {:error, :noproc} ->
          # Ra not available
          {:error, :ra_not_available}

        {:error, reason} ->
          {:error, reason}

        {:timeout, _node} ->
          {:error, :timeout}
      end
    catch
      kind, reason ->
        Logger.debug("Ra not available: #{inspect({kind, reason})}")
        {:error, :ra_not_available}
    end
  end

  # Restore chunks from Ra state into ETS
  defp restore_from_ra do
    case RaSupervisor.query(fn state -> state.chunks end) do
      {:ok, chunks} when is_map(chunks) ->
        count =
          Enum.reduce(chunks, 0, fn {hash, chunk_map}, acc ->
            chunk_meta = map_to_struct(chunk_map)
            :ets.insert(:chunk_index, {hash, chunk_meta})
            acc + 1
          end)

        {:ok, count}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Convert ChunkMeta struct to map for Ra storage
  defp struct_to_map(%ChunkMeta{} = chunk_meta) do
    %{
      hash: chunk_meta.hash,
      original_size: chunk_meta.original_size,
      stored_size: chunk_meta.stored_size,
      compression: chunk_meta.compression,
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

  # Convert map from Ra storage to ChunkMeta struct
  defp map_to_struct(chunk_map) when is_map(chunk_map) do
    %ChunkMeta{
      hash: chunk_map.hash,
      original_size: chunk_map.original_size,
      stored_size: chunk_map.stored_size,
      compression: chunk_map.compression,
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
