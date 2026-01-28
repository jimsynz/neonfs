defmodule NeonFS.Core.FileIndex do
  @moduledoc """
  GenServer for managing file metadata storage and lookups.

  Uses ETS tables for efficient concurrent reads:
  - `:file_index_by_id` - lookup files by ID
  - `:file_index_by_path` - lookup files by {volume_id, path}

  The GenServer serializes write operations while allowing concurrent reads
  from any process via the public ETS tables.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.FileMeta

  @type file_id :: String.t()
  @type volume_id :: String.t()
  @type path :: String.t()

  ## Client API

  @doc """
  Starts the FileIndex GenServer.
  """
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates a new file metadata entry.

  Returns the created FileMeta struct.

  ## Parameters
  - `file`: A FileMeta struct to store

  ## Examples
      iex> file = FileMeta.new("vol1", "/test.txt")
      iex> FileIndex.create(file)
      {:ok, %FileMeta{...}}
  """
  @spec create(FileMeta.t()) :: {:ok, FileMeta.t()} | {:error, :already_exists}
  def create(%FileMeta{} = file) do
    GenServer.call(__MODULE__, {:create, file})
  end

  @doc """
  Retrieves a file by its ID.

  Returns `{:ok, file}` if found, `{:error, :not_found}` otherwise.
  """
  @spec get(file_id()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get(file_id) do
    case :ets.lookup(:file_index_by_id, file_id) do
      [{^file_id, file}] -> {:ok, file}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Retrieves a file by volume ID and path.

  Returns `{:ok, file}` if found, `{:error, :not_found}` otherwise.
  """
  @spec get_by_path(volume_id(), path()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get_by_path(volume_id, path) do
    normalized_path = FileMeta.normalize_path(path)

    case :ets.lookup(:file_index_by_path, {volume_id, normalized_path}) do
      [{_, file}] -> {:ok, file}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Updates an existing file metadata entry.

  Returns `{:ok, updated_file}` if successful, `{:error, :not_found}` if file doesn't exist.

  ## Parameters
  - `file_id`: The ID of the file to update
  - `updates`: Keyword list of fields to update

  ## Examples
      iex> FileIndex.update("file-id", size: 2048, mode: 0o755)
      {:ok, %FileMeta{size: 2048, mode: 0o755, version: 2}}
  """
  @spec update(file_id(), keyword()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def update(file_id, updates) do
    GenServer.call(__MODULE__, {:update, file_id, updates})
  end

  @doc """
  Deletes a file metadata entry.

  This performs a soft delete by setting a deleted flag or can be a hard delete
  depending on implementation needs. For Phase 1, this performs a hard delete.

  Returns `:ok` if successful, `{:error, :not_found}` if file doesn't exist.
  """
  @spec delete(file_id()) :: :ok | {:error, :not_found}
  def delete(file_id) do
    GenServer.call(__MODULE__, {:delete, file_id})
  end

  @doc """
  Lists all files in a directory.

  Returns a list of FileMeta structs for files whose paths start with the given directory path.

  ## Parameters
  - `volume_id`: The volume ID
  - `dir_path`: The directory path (e.g., "/documents")

  ## Examples
      iex> FileIndex.list_dir("vol1", "/documents")
      [%FileMeta{path: "/documents/file1.txt"}, %FileMeta{path: "/documents/file2.pdf"}]
  """
  @spec list_dir(volume_id(), path()) :: [FileMeta.t()]
  def list_dir(volume_id, dir_path) do
    normalized_path = FileMeta.normalize_path(dir_path)

    # Get all files in the volume
    list_volume(volume_id)
    |> Enum.filter(fn file ->
      # Check if file path starts with directory path
      if normalized_path == "/" do
        # Root directory - include all files
        true
      else
        # Subdirectory - check if path starts with dir_path/
        String.starts_with?(file.path, normalized_path <> "/")
      end
    end)
  end

  @doc """
  Lists all files in a volume.

  Returns a list of all FileMeta structs for the given volume.
  """
  @spec list_volume(volume_id()) :: [FileMeta.t()]
  def list_volume(volume_id) do
    :ets.select(:file_index_by_path, [
      {{{volume_id, :_}, :"$1"}, [], [:"$1"]}
    ])
  end

  @doc """
  Lists all files across all volumes.

  Primarily useful for testing and debugging.
  """
  @spec list_all() :: [FileMeta.t()]
  def list_all do
    :ets.select(:file_index_by_id, [{{:_, :"$1"}, [], [:"$1"]}])
  end

  ## Server Callbacks

  @impl true
  def init(_opts) do
    # Create ETS tables for file lookups
    :ets.new(:file_index_by_id, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    :ets.new(:file_index_by_path, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    Logger.info("FileIndex started with ETS tables")
    {:ok, %{}}
  end

  @impl true
  def handle_call({:create, file}, _from, state) do
    # Check if file already exists by ID or path
    id_exists? = :ets.member(:file_index_by_id, file.id)
    path_key = {file.volume_id, file.path}
    path_exists? = :ets.member(:file_index_by_path, path_key)

    if id_exists? or path_exists? do
      {:reply, {:error, :already_exists}, state}
    else
      # Validate path before creating
      case FileMeta.validate_path(file.path) do
        :ok ->
          :ets.insert(:file_index_by_id, {file.id, file})
          :ets.insert(:file_index_by_path, {path_key, file})
          {:reply, {:ok, file}, state}

        {:error, reason} ->
          {:reply, {:error, reason}, state}
      end
    end
  end

  @impl true
  def handle_call({:update, file_id, updates}, _from, state) do
    case :ets.lookup(:file_index_by_id, file_id) do
      [] ->
        {:reply, {:error, :not_found}, state}

      [{^file_id, old_file}] ->
        do_update(file_id, old_file, updates, state)
    end
  end

  @impl true
  def handle_call({:delete, file_id}, _from, state) do
    case :ets.lookup(:file_index_by_id, file_id) do
      [] ->
        {:reply, {:error, :not_found}, state}

      [{^file_id, file}] ->
        # Delete from both tables
        :ets.delete(:file_index_by_id, file_id)
        path_key = {file.volume_id, file.path}
        :ets.delete(:file_index_by_path, path_key)

        {:reply, :ok, state}
    end
  end

  ## Private helpers

  # Handle path updates separately to reduce nesting
  defp do_update(file_id, old_file, updates, state) do
    updated_file = FileMeta.update(old_file, updates)

    if Keyword.has_key?(updates, :path) do
      update_with_path_change(file_id, old_file, updated_file, state)
    else
      update_without_path_change(file_id, updated_file, state)
    end
  end

  defp update_with_path_change(file_id, old_file, updated_file, state) do
    new_path = FileMeta.normalize_path(updated_file.path)

    case FileMeta.validate_path(new_path) do
      :ok ->
        # Remove old path entry
        old_path_key = {old_file.volume_id, old_file.path}
        :ets.delete(:file_index_by_path, old_path_key)

        # Insert new path entry
        new_path_key = {updated_file.volume_id, updated_file.path}
        :ets.insert(:file_index_by_path, {new_path_key, updated_file})
        :ets.insert(:file_index_by_id, {file_id, updated_file})

        {:reply, {:ok, updated_file}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp update_without_path_change(file_id, updated_file, state) do
    path_key = {updated_file.volume_id, updated_file.path}
    :ets.insert(:file_index_by_id, {file_id, updated_file})
    :ets.insert(:file_index_by_path, {path_key, updated_file})

    {:reply, {:ok, updated_file}, state}
  end
end
