defmodule NeonFS.NFS.InodeTable do
  @moduledoc """
  Manages bidirectional mapping between NFS inodes and filesystem paths.

  NFS uses file handles containing inode numbers to reference files and directories.
  This module maintains the mapping between these inodes and the actual file paths
  within volumes. Unlike the FUSE inode table, the NFS table must be persistent
  across server restarts (persistence is added in a later phase).

  ## Implementation

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
  @next_inode_key :next_inode

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

    # Pre-allocate root inode (inode 1 = virtual root directory)
    :ets.insert(:nfs_inode_to_path, {@root_inode, nil, "/"})
    :ets.insert(:nfs_path_to_inode, {{nil, "/"}, @root_inode})

    # Track next available inode (start at 2, since 1 is root)
    :ets.insert(:nfs_inode_to_path, {@next_inode_key, @root_inode + 1})

    Logger.info("NFS InodeTable started", root_inode: @root_inode)

    {:ok, %{}}
  end

  @impl true
  def handle_call({:allocate_inode, volume_name, path}, _from, state) do
    case :ets.lookup(:nfs_path_to_inode, {volume_name, path}) do
      [{{^volume_name, ^path}, existing_inode}] ->
        {:reply, {:ok, existing_inode}, state}

      [] ->
        [{@next_inode_key, next_inode}] = :ets.lookup(:nfs_inode_to_path, @next_inode_key)
        inode = next_inode

        :ets.insert(:nfs_inode_to_path, {@next_inode_key, next_inode + 1})
        :ets.insert(:nfs_inode_to_path, {inode, volume_name, path})
        :ets.insert(:nfs_path_to_inode, {{volume_name, path}, inode})

        {:reply, {:ok, inode}, state}
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
  def handle_call(:clear, _from, state) do
    :ets.delete_all_objects(:nfs_inode_to_path)
    :ets.delete_all_objects(:nfs_path_to_inode)

    :ets.insert(:nfs_inode_to_path, {@root_inode, nil, "/"})
    :ets.insert(:nfs_path_to_inode, {{nil, "/"}, @root_inode})
    :ets.insert(:nfs_inode_to_path, {@next_inode_key, @root_inode + 1})

    {:reply, :ok, state}
  end
end
