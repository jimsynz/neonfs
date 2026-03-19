defmodule NeonFS.NFS.InodeTable do
  @moduledoc """
  Manages bidirectional mapping between NFS inodes and filesystem paths.

  NFS uses file handles containing inode numbers to reference files and directories.
  This module maintains the mapping between these inodes and the actual file paths
  within volumes.

  ## Deterministic Inode Allocation

  Inodes are derived from `SHA-256(volume_name + "\\0" + path)` truncated to 64 bits.
  This makes inode numbers deterministic — the same (volume, path) always produces
  the same inode regardless of allocation order or which NFS node computes it.
  This enables portable NFS file handles across cluster nodes behind a load balancer.

  The birthday bound for 64-bit hashes means collision probability is negligible
  at realistic file counts (< 1 in 10^7 at 3.6M files). Collisions are detected
  and fall back to sequential allocation (losing portability for that single entry).

  Uses ETS tables for fast concurrent reads:
  - `:nfs_inode_to_path` - Maps inode -> {volume_name, path}
  - `:nfs_path_to_inode` - Maps {volume_name, path} -> inode

  ## Lifecycle

  - Started as a GenServer in the supervision tree
  - Inodes are allocated on demand (lookup, create, mkdir)
  - Inodes are released when files are deleted (remove)
  - Root inode (1) is pre-allocated for the virtual root and never released
  """

  use GenServer
  require Logger

  @root_inode 1
  @reserved_inodes [0, 1]

  ## Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Allocate an inode for a path within a volume.

  If the path already has an inode, returns the existing one.
  Otherwise allocates a new inode and creates the mapping.
  """
  @spec allocate_inode(String.t(), String.t()) :: {:ok, non_neg_integer()}
  def allocate_inode(volume_name, path) do
    GenServer.call(__MODULE__, {:allocate_inode, volume_name, path})
  end

  @doc """
  Get the path for an inode.

  Returns `{:ok, {volume_name, path}}` if the inode exists,
  `{:error, :not_found}` otherwise.
  """
  @spec get_path(non_neg_integer()) ::
          {:ok, {String.t() | nil, String.t()}} | {:error, :not_found}
  def get_path(inode) do
    case :ets.lookup(:nfs_inode_to_path, inode) do
      [{^inode, volume_name, path}] -> {:ok, {volume_name, path}}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Get the inode for a path within a volume.
  """
  @spec get_inode(String.t(), String.t()) :: {:ok, non_neg_integer()} | {:error, :not_found}
  def get_inode(volume_name, path) do
    case :ets.lookup(:nfs_path_to_inode, {volume_name, path}) do
      [{{^volume_name, ^path}, inode}] -> {:ok, inode}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Release (delete) an inode mapping.

  The root inode (1) cannot be released.
  """
  @spec release_inode(non_neg_integer()) :: :ok | {:error, :cannot_release_root}
  def release_inode(@root_inode), do: {:error, :cannot_release_root}

  def release_inode(inode) do
    GenServer.call(__MODULE__, {:release_inode, inode})
  end

  @doc """
  Get the root inode number (always 1).
  """
  @spec root_inode() :: non_neg_integer()
  def root_inode, do: @root_inode

  @doc """
  Clear all inode mappings except root. Primarily for testing.
  """
  @spec clear() :: :ok
  def clear do
    GenServer.call(__MODULE__, :clear)
  end

  ## GenServer Callbacks

  @impl true
  def init(_opts) do
    :ets.new(:nfs_inode_to_path, [:named_table, :public, :set, read_concurrency: true])
    :ets.new(:nfs_path_to_inode, [:named_table, :public, :set, read_concurrency: true])

    :ets.insert(:nfs_inode_to_path, {@root_inode, nil, "/"})
    :ets.insert(:nfs_path_to_inode, {{nil, "/"}, @root_inode})

    Logger.info("NFS InodeTable started", root_inode: @root_inode)

    {:ok, %{fallback_counter: 2}}
  end

  @impl true
  def handle_call({:allocate_inode, volume_name, path}, _from, state) do
    case :ets.lookup(:nfs_path_to_inode, {volume_name, path}) do
      [{{^volume_name, ^path}, existing_inode}] ->
        {:reply, {:ok, existing_inode}, state}

      [] ->
        inode = compute_inode(volume_name, path)

        case :ets.lookup(:nfs_inode_to_path, inode) do
          [] ->
            insert_mapping(inode, volume_name, path)
            {:reply, {:ok, inode}, state}

          [{^inode, ^volume_name, ^path}] ->
            {:reply, {:ok, inode}, state}

          [{^inode, other_volume, other_path}] ->
            Logger.warning(
              "Inode hash collision: #{volume_name}:#{path} collides with #{other_volume}:#{other_path}"
            )

            {fallback_inode, new_state} = allocate_fallback(volume_name, path, state)
            {:reply, {:ok, fallback_inode}, new_state}
        end
    end
  end

  @impl true
  def handle_call({:release_inode, inode}, _from, state) do
    case :ets.lookup(:nfs_inode_to_path, inode) do
      [{^inode, volume_name, path}] ->
        :ets.delete(:nfs_inode_to_path, inode)
        :ets.delete(:nfs_path_to_inode, {volume_name, path})
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:clear, _from, _state) do
    :ets.delete_all_objects(:nfs_inode_to_path)
    :ets.delete_all_objects(:nfs_path_to_inode)

    :ets.insert(:nfs_inode_to_path, {@root_inode, nil, "/"})
    :ets.insert(:nfs_path_to_inode, {{nil, "/"}, @root_inode})

    {:reply, :ok, %{fallback_counter: 2}}
  end

  ## Private: Inode Computation

  defp compute_inode(volume_name, path) do
    key = (volume_name || "") <> "\0" <> path
    <<inode::unsigned-little-64, _::binary>> = :crypto.hash(:sha256, key)
    if inode in @reserved_inodes, do: inode + 2, else: inode
  end

  defp insert_mapping(inode, volume_name, path) do
    :ets.insert(:nfs_inode_to_path, {inode, volume_name, path})
    :ets.insert(:nfs_path_to_inode, {{volume_name, path}, inode})
  end

  defp allocate_fallback(volume_name, path, state) do
    inode = find_free_inode(state.fallback_counter)
    insert_mapping(inode, volume_name, path)
    {inode, %{state | fallback_counter: inode + 1}}
  end

  defp find_free_inode(candidate) when candidate in @reserved_inodes do
    find_free_inode(candidate + 1)
  end

  defp find_free_inode(candidate) do
    case :ets.lookup(:nfs_inode_to_path, candidate) do
      [] -> candidate
      _ -> find_free_inode(candidate + 1)
    end
  end
end
