defmodule NeonFS.FUSE.InodeTable do
  @moduledoc """
  Manages bidirectional mapping between FUSE inodes and filesystem paths.

  FUSE uses inodes (unsigned 64-bit integers) to reference files and directories.
  Inode 1 is reserved for the root directory. This module maintains the mapping
  between these inodes and the actual file paths within volumes.

  ## Implementation

  Uses ETS tables for fast concurrent reads:
  - `:inode_to_path` - Maps inode -> {volume_id, path}
  - `:path_to_inode` - Maps {volume_id, path} -> inode

  ## Lifecycle

  - Started as a GenServer in the supervision tree
  - Inodes are allocated on demand (lookup, create, mkdir)
  - Inodes are released when files are deleted (unlink, rmdir)
  - Root inode (1) is pre-allocated and never released
  """

  use GenServer
  require Logger

  @root_inode 1
  @next_inode_key :next_inode

  ## Client API

  @doc """
  Start the inode table GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Allocate an inode for a path.

  If the path already has an inode, returns the existing one.
  Otherwise allocates a new inode and creates the mapping.

  ## Examples

      iex> InodeTable.allocate_inode("vol1", "/test.txt")
      {:ok, 2}

      iex> InodeTable.allocate_inode("vol1", "/test.txt")
      {:ok, 2}  # Same inode for same path
  """
  @spec allocate_inode(String.t(), String.t()) :: {:ok, non_neg_integer()}
  def allocate_inode(volume_id, path) do
    GenServer.call(__MODULE__, {:allocate_inode, volume_id, path})
  end

  @doc """
  Get the path for an inode.

  Returns `{:ok, {volume_id, path}}` if the inode exists, `{:error, :not_found}` otherwise.

  ## Examples

      iex> InodeTable.get_path(1)
      {:ok, {nil, "/"}}  # Root inode

      iex> InodeTable.get_path(999)
      {:error, :not_found}
  """
  @spec get_path(non_neg_integer()) ::
          {:ok, {String.t() | nil, String.t()}} | {:error, :not_found}
  def get_path(inode) do
    case :ets.lookup(:inode_to_path, inode) do
      [{^inode, volume_id, path}] -> {:ok, {volume_id, path}}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Get the inode for a path.

  Returns `{:ok, inode}` if the path exists, `{:error, :not_found}` otherwise.

  ## Examples

      iex> InodeTable.get_inode("vol1", "/test.txt")
      {:ok, 2}

      iex> InodeTable.get_inode("vol1", "/nonexistent")
      {:error, :not_found}
  """
  @spec get_inode(String.t(), String.t()) :: {:ok, non_neg_integer()} | {:error, :not_found}
  def get_inode(volume_id, path) do
    case :ets.lookup(:path_to_inode, {volume_id, path}) do
      [{{^volume_id, ^path}, inode}] -> {:ok, inode}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Release (delete) an inode mapping.

  This should be called when a file or directory is deleted.
  The root inode (1) cannot be released.

  ## Examples

      iex> InodeTable.release_inode(2)
      :ok

      iex> InodeTable.release_inode(1)
      {:error, :cannot_release_root}
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
  Clear all inode mappings except root.

  This is primarily for testing purposes.
  """
  @spec clear() :: :ok
  def clear do
    GenServer.call(__MODULE__, :clear)
  end

  ## GenServer Callbacks

  @impl true
  def init(_opts) do
    # Create ETS tables for bidirectional lookup
    :ets.new(:inode_to_path, [:named_table, :public, :set, read_concurrency: true])
    :ets.new(:path_to_inode, [:named_table, :public, :set, read_concurrency: true])

    # Pre-allocate root inode (inode 1 = root directory)
    :ets.insert(:inode_to_path, {@root_inode, nil, "/"})
    :ets.insert(:path_to_inode, {{nil, "/"}, @root_inode})

    # Track next available inode (start at 2, since 1 is root)
    :ets.insert(:inode_to_path, {@next_inode_key, @root_inode + 1})

    Logger.info("InodeTable started with root inode #{@root_inode}")

    {:ok, %{}}
  end

  @impl true
  def handle_call({:allocate_inode, volume_id, path}, _from, state) do
    # Check if path already has an inode
    case :ets.lookup(:path_to_inode, {volume_id, path}) do
      [{{^volume_id, ^path}, existing_inode}] ->
        {:reply, {:ok, existing_inode}, state}

      [] ->
        # Allocate new inode
        [{@next_inode_key, next_inode}] = :ets.lookup(:inode_to_path, @next_inode_key)
        inode = next_inode

        # Update next inode counter
        :ets.insert(:inode_to_path, {@next_inode_key, next_inode + 1})

        # Create bidirectional mapping
        :ets.insert(:inode_to_path, {inode, volume_id, path})
        :ets.insert(:path_to_inode, {{volume_id, path}, inode})

        {:reply, {:ok, inode}, state}
    end
  end

  @impl true
  def handle_call({:release_inode, inode}, _from, state) do
    case :ets.lookup(:inode_to_path, inode) do
      [{^inode, volume_id, path}] ->
        :ets.delete(:inode_to_path, inode)
        :ets.delete(:path_to_inode, {volume_id, path})
        {:reply, :ok, state}

      [] ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call(:clear, _from, state) do
    # Clear all except root and next_inode counter
    :ets.delete_all_objects(:inode_to_path)
    :ets.delete_all_objects(:path_to_inode)

    # Restore root and counter
    :ets.insert(:inode_to_path, {@root_inode, nil, "/"})
    :ets.insert(:path_to_inode, {{nil, "/"}, @root_inode})
    :ets.insert(:inode_to_path, {@next_inode_key, @root_inode + 1})

    {:reply, :ok, state}
  end
end
