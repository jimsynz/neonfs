defmodule NeonFS.Core.ChunkIndex do
  @moduledoc """
  GenServer managing chunk metadata with ETS-backed storage.

  Provides fast lookups by hash and queries by location or commit state.
  For Phase 1, this is in-memory only. Phase 2 will add Ra consensus backing.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.ChunkMeta

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

  # Server Callbacks

  @impl true
  def init(_opts) do
    # Create ETS table for chunk metadata
    # :set - one entry per hash
    # :named_table - accessible by name :chunk_index
    # :public - readable from any process
    # read_concurrency: true - optimize for concurrent reads
    :ets.new(:chunk_index, [:set, :named_table, :public, read_concurrency: true])

    Logger.info("ChunkIndex started with ETS table :chunk_index")
    {:ok, %{}}
  end

  @impl true
  def handle_call({:put, chunk_meta}, _from, state) do
    :ets.insert(:chunk_index, {chunk_meta.hash, chunk_meta})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:delete, hash}, _from, state) do
    :ets.delete(:chunk_index, hash)
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:add_write_ref, hash, write_id}, _from, state) do
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
    case :ets.lookup(:chunk_index, hash) do
      [{^hash, chunk_meta}] ->
        case ChunkMeta.commit(chunk_meta) do
          {:ok, committed_meta} ->
            :ets.insert(:chunk_index, {hash, committed_meta})
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
end
