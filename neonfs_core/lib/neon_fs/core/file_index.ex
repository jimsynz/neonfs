defmodule NeonFS.Core.FileIndex do
  @moduledoc """
  GenServer for managing file metadata storage and lookups.

  Uses ETS tables for efficient concurrent reads:
  - `:file_index_by_id` - lookup files by ID
  - `:file_index_by_path` - lookup files by {volume_id, path}

  The GenServer serializes write operations while allowing concurrent reads
  from any process via the public ETS tables.

  ## Ra Integration

  When Ra is available (Phase 2+), file metadata is replicated across the cluster
  via the Ra consensus protocol. All mutations (create, update, delete) are first
  written to Ra, then the local ETS cache is updated. On startup, the ETS cache
  is restored from Ra state.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{FileMeta, RaServer, RaSupervisor}

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
  @spec create(FileMeta.t()) :: {:ok, FileMeta.t()} | {:error, term()}
  def create(%FileMeta{} = file) do
    GenServer.call(__MODULE__, {:create, file}, 10_000)
  end

  @doc """
  Retrieves a file by its ID.

  First checks local ETS cache, then falls back to Ra if not found locally.
  Returns `{:ok, file}` if found, `{:error, :not_found}` otherwise.
  """
  @spec get(file_id()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get(file_id) do
    case :ets.lookup(:file_index_by_id, file_id) do
      [{^file_id, file}] ->
        {:ok, file}

      [] ->
        # Not in local cache, try Ra
        get_from_ra(file_id)
    end
  end

  @doc """
  Retrieves a file by volume ID and path.

  First checks local ETS cache, then falls back to Ra if not found locally.
  Returns `{:ok, file}` if found, `{:error, :not_found}` otherwise.
  """
  @spec get_by_path(volume_id(), path()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get_by_path(volume_id, path) do
    normalized_path = FileMeta.normalize_path(path)

    case :ets.lookup(:file_index_by_path, {volume_id, normalized_path}) do
      [{_, file}] ->
        {:ok, file}

      [] ->
        # Not in local cache, try Ra
        get_by_path_from_ra(volume_id, normalized_path)
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
  @spec update(file_id(), keyword()) :: {:ok, FileMeta.t()} | {:error, term()}
  def update(file_id, updates) do
    GenServer.call(__MODULE__, {:update, file_id, updates}, 10_000)
  end

  @doc """
  Deletes a file metadata entry.

  This performs a soft delete by setting a deleted flag or can be a hard delete
  depending on implementation needs. For Phase 1, this performs a hard delete.

  Returns `:ok` if successful, `{:error, :not_found}` if file doesn't exist.
  """
  @spec delete(file_id()) :: :ok | {:error, term()}
  def delete(file_id) do
    GenServer.call(__MODULE__, {:delete, file_id}, 10_000)
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

    # Try to restore state from Ra if available
    case restore_from_ra() do
      {:ok, count} when count > 0 ->
        Logger.info("FileIndex restored #{count} files from Ra")

      {:ok, 0} ->
        Logger.debug("FileIndex: no files to restore from Ra")

      {:error, :ra_not_available} ->
        Logger.debug("FileIndex started but Ra not ready yet: :noproc")

      {:error, reason} ->
        Logger.warning("FileIndex failed to restore from Ra: #{inspect(reason)}")
    end

    {:ok, %{}}
  end

  @impl true
  def handle_call({:create, file}, _from, state) do
    reply = do_create_file(file)
    {:reply, reply, state}
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
        # Try to write through Ra if available
        case maybe_ra_command({:delete_file, file_id}) do
          {:ok, :ok} ->
            # Update local ETS cache
            :ets.delete(:file_index_by_id, file_id)
            path_key = {file.volume_id, file.path}
            :ets.delete(:file_index_by_path, path_key)
            {:reply, :ok, state}

          {:error, :ra_not_available} ->
            # Ra not available, delete directly from ETS
            :ets.delete(:file_index_by_id, file_id)
            path_key = {file.volume_id, file.path}
            :ets.delete(:file_index_by_path, path_key)
            {:reply, :ok, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end
    end
  end

  ## Private helpers

  defp do_create_file(file) do
    with :ok <- check_file_not_exists(file),
         :ok <- FileMeta.validate_path(file.path),
         :ok <- persist_file(file) do
      {:ok, file}
    end
  end

  defp check_file_not_exists(file) do
    id_exists? = :ets.member(:file_index_by_id, file.id)
    path_key = {file.volume_id, file.path}
    path_exists? = :ets.member(:file_index_by_path, path_key)

    if id_exists? or path_exists? do
      {:error, :already_exists}
    else
      :ok
    end
  end

  defp persist_file(file) do
    path_key = {file.volume_id, file.path}

    case maybe_ra_command({:put_file, struct_to_map(file)}) do
      {:ok, :ok} ->
        insert_file_to_ets(file, path_key)
        :ok

      {:error, :ra_not_available} ->
        insert_file_to_ets(file, path_key)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp insert_file_to_ets(file, path_key) do
    :ets.insert(:file_index_by_id, {file.id, file})
    :ets.insert(:file_index_by_path, {path_key, file})
  end

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
        # Build the updates map for Ra
        updates = struct_to_map(updated_file)

        # Try to write through Ra if available
        case maybe_ra_command({:update_file, file_id, updates}) do
          {:ok, :ok} ->
            # Update local ETS cache
            old_path_key = {old_file.volume_id, old_file.path}
            :ets.delete(:file_index_by_path, old_path_key)
            new_path_key = {updated_file.volume_id, updated_file.path}
            :ets.insert(:file_index_by_path, {new_path_key, updated_file})
            :ets.insert(:file_index_by_id, {file_id, updated_file})
            {:reply, {:ok, updated_file}, state}

          {:error, :ra_not_available} ->
            # Ra not available, update directly in ETS
            old_path_key = {old_file.volume_id, old_file.path}
            :ets.delete(:file_index_by_path, old_path_key)
            new_path_key = {updated_file.volume_id, updated_file.path}
            :ets.insert(:file_index_by_path, {new_path_key, updated_file})
            :ets.insert(:file_index_by_id, {file_id, updated_file})
            {:reply, {:ok, updated_file}, state}

          {:error, reason} ->
            {:reply, {:error, reason}, state}
        end

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  defp update_without_path_change(file_id, updated_file, state) do
    # Build the updates map for Ra
    updates = struct_to_map(updated_file)

    # Try to write through Ra if available
    case maybe_ra_command({:update_file, file_id, updates}) do
      {:ok, :ok} ->
        # Update local ETS cache
        path_key = {updated_file.volume_id, updated_file.path}
        :ets.insert(:file_index_by_id, {file_id, updated_file})
        :ets.insert(:file_index_by_path, {path_key, updated_file})
        {:reply, {:ok, updated_file}, state}

      {:error, :ra_not_available} ->
        # Ra not available, update directly in ETS
        path_key = {updated_file.volume_id, updated_file.path}
        :ets.insert(:file_index_by_id, {file_id, updated_file})
        :ets.insert(:file_index_by_path, {path_key, updated_file})
        {:reply, {:ok, updated_file}, state}

      {:error, reason} ->
        {:reply, {:error, reason}, state}
    end
  end

  # Ra integration helpers

  # Query Ra for a file by ID, caching the result locally if found
  defp get_from_ra(file_id) do
    query_fn = fn state ->
      state
      |> Map.get(:files, %{})
      |> Map.get(file_id)
    end

    case RaSupervisor.query(query_fn) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, file_map} -> cache_and_return_file(file_map)
      {:error, _} -> {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :not_found}
  end

  # Query Ra for a file by volume_id and path, caching the result locally if found
  defp get_by_path_from_ra(volume_id, path) do
    query_fn = fn state ->
      state
      |> Map.get(:files, %{})
      |> find_file_by_path(volume_id, path)
    end

    case RaSupervisor.query(query_fn) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, file_map} -> cache_and_return_file(file_map)
      {:error, _} -> {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :not_found}
  end

  defp find_file_by_path(files, volume_id, path) do
    Enum.find_value(files, fn {_id, file_map} ->
      if file_map[:volume_id] == volume_id and file_map[:path] == path, do: file_map
    end)
  end

  defp cache_and_return_file(file_map) do
    file = map_to_struct(file_map)
    path_key = {file.volume_id, file.path}
    :ets.insert(:file_index_by_id, {file.id, file})
    :ets.insert(:file_index_by_path, {path_key, file})
    {:ok, file}
  end

  # Try to execute a Ra command, but gracefully handle Ra not being available
  # Returns {:ok, result} | {:error, :ra_not_available} | {:error, reason}
  #
  # IMPORTANT: Only returns :ra_not_available when Ra has not been initialized yet
  # (Phase 1 single-node mode). Once Ra is initialized, errors are propagated
  # so that quorum loss is properly detected.
  defp maybe_ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, result, _leader} ->
        {:ok, result}

      {:error, :noproc} ->
        # Ra server not running - check if it was ever initialized
        if RaServer.initialized?() do
          {:error, :ra_unavailable}
        else
          {:error, :ra_not_available}
        end

      {:error, reason} ->
        {:error, reason}

      {:timeout, _} ->
        {:error, :timeout}
    end
  catch
    :exit, {:noproc, _} ->
      if RaServer.initialized?() do
        {:error, :ra_unavailable}
      else
        {:error, :ra_not_available}
      end

    :exit, reason ->
      if RaServer.initialized?() do
        {:error, {:ra_exit, reason}}
      else
        {:error, :ra_not_available}
      end
  end

  # Restore files from Ra state into ETS
  defp restore_from_ra do
    case RaSupervisor.query(fn state -> Map.get(state, :files, %{}) end) do
      {:ok, files} when is_map(files) ->
        count =
          Enum.reduce(files, 0, fn {_id, file_map}, acc ->
            file_meta = map_to_struct(file_map)
            :ets.insert(:file_index_by_id, {file_meta.id, file_meta})
            path_key = {file_meta.volume_id, file_meta.path}
            :ets.insert(:file_index_by_path, {path_key, file_meta})
            acc + 1
          end)

        {:ok, count}

      {:error, :noproc} ->
        {:error, :ra_not_available}

      {:error, reason} ->
        {:error, reason}
    end
  catch
    :exit, {:noproc, _} ->
      {:error, :ra_not_available}

    :exit, reason ->
      {:error, {:ra_exit, reason}}
  end

  # Convert a FileMeta struct to a map for Ra storage
  defp struct_to_map(%FileMeta{} = file) do
    Map.from_struct(file)
  end

  # Convert a map from Ra storage back to a FileMeta struct
  defp map_to_struct(file_map) when is_map(file_map) do
    struct(FileMeta, file_map)
  end
end
