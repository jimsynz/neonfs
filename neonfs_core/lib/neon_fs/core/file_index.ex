defmodule NeonFS.Core.FileIndex do
  @moduledoc """
  GenServer managing file metadata with quorum-backed distributed storage
  and DirectoryEntry-based path lookups.

  Files are stored with key `"file:<file_id>"` and sharded by `hash(file_id)`.
  Directory entries are stored with key `"dir:<volume_id>:<parent_path>"` and
  sharded by `hash(parent_path)`.

  ## Quorum Mode

  Writes go through `QuorumCoordinator.quorum_write/3` and cache misses fall back
  to `QuorumCoordinator.quorum_read/2`. Requires `:quorum_opts` at startup.

  ## Cross-Segment Operations

  File creation and deletion span two segments (FileMeta + DirectoryEntry) and
  use the IntentLog for crash-safe atomicity. File moves across directories also
  use IntentLog to coordinate two DirectoryEntry updates.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{
    DirectoryEntry,
    FileMeta,
    Intent,
    IntentLog,
    MetadataCodec,
    QuorumCoordinator
  }

  @type file_id :: String.t()
  @type volume_id :: String.t()
  @type path :: String.t()

  @file_key_prefix "file:"
  @dir_key_prefix "dir:"

  ## Client API

  @doc """
  Starts the FileIndex GenServer.

  ## Options

    * `:quorum_opts` — keyword list passed to QuorumCoordinator (must include `:ring`).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates a new file metadata entry.

  Uses IntentLog for cross-segment atomicity (FileMeta + DirectoryEntry).
  The parent directory must exist — use `mkdir/3` to create directories first,
  or this function auto-creates the root directory for the volume.
  """
  @spec create(FileMeta.t()) :: {:ok, FileMeta.t()} | {:error, term()}
  def create(%FileMeta{} = file) do
    GenServer.call(__MODULE__, {:create, file}, 15_000)
  end

  @doc """
  Retrieves a file by its ID.

  Checks local ETS cache first, falls back to quorum read.
  """
  @spec get(file_id()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get(file_id) do
    case :ets.lookup(:file_index_by_id, file_id) do
      [{^file_id, file}] ->
        {:ok, file}

      [] ->
        get_from_quorum(file_id)
    end
  end

  @doc """
  Retrieves a file by volume ID and path.

  Parses the path into parent_path + name, reads the DirectoryEntry to find
  the file_id, then reads the FileMeta via quorum.
  """
  @spec get_by_path(volume_id(), path()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get_by_path(volume_id, path) do
    normalized = FileMeta.normalize_path(path)
    {parent, name} = split_path(normalized)

    with {:ok, dir_entry} <- read_dir_entry(volume_id, parent),
         {:ok, child} <- DirectoryEntry.get_child(dir_entry, name),
         {:ok, file} <- get(child.id) do
      {:ok, file}
    else
      {:error, _} -> {:error, :not_found}
    end
  end

  @doc """
  Updates an existing file metadata entry.

  Quorum-writes the updated FileMeta. Does not modify DirectoryEntry
  (path changes should use `rename/4` or `move/5` instead).
  """
  @spec update(file_id(), keyword()) :: {:ok, FileMeta.t()} | {:error, term()}
  def update(file_id, updates) do
    GenServer.call(__MODULE__, {:update, file_id, updates}, 10_000)
  end

  @doc """
  Deletes a file metadata entry.

  Uses IntentLog for cross-segment atomicity to remove both the FileMeta
  and the DirectoryEntry child reference.
  """
  @spec delete(file_id()) :: :ok | {:error, term()}
  def delete(file_id) do
    GenServer.call(__MODULE__, {:delete, file_id}, 15_000)
  end

  @doc """
  Lists directory contents.

  Quorum reads the DirectoryEntry for the given path and returns
  the children map: `%{name => %{type: :file | :dir, id: binary()}}`.
  """
  @spec list_dir(volume_id(), path()) ::
          {:ok, %{String.t() => DirectoryEntry.child_info()}} | {:error, term()}
  def list_dir(volume_id, dir_path) do
    normalized = FileMeta.normalize_path(dir_path)

    case read_dir_entry(volume_id, normalized) do
      {:ok, dir_entry} -> {:ok, dir_entry.children}
      {:error, :not_found} -> {:ok, %{}}
      {:error, reason} -> {:error, reason}
    end
  end

  @doc """
  Creates a directory.

  Creates a DirectoryEntry for the new directory and adds a child entry
  to the parent DirectoryEntry.
  """
  @spec mkdir(volume_id(), path(), keyword()) :: {:ok, DirectoryEntry.t()} | {:error, term()}
  def mkdir(volume_id, path, opts \\ []) do
    GenServer.call(__MODULE__, {:mkdir, volume_id, path, opts}, 10_000)
  end

  @doc """
  Renames a file or directory within the same parent directory.

  Single DirectoryEntry quorum write — no IntentLog needed since it's
  a single-segment operation.
  """
  @spec rename(volume_id(), path(), String.t(), String.t()) :: :ok | {:error, term()}
  def rename(volume_id, parent_path, old_name, new_name) do
    GenServer.call(__MODULE__, {:rename, volume_id, parent_path, old_name, new_name}, 10_000)
  end

  @doc """
  Moves a file or directory across directories.

  Uses IntentLog for cross-segment atomicity (two DirectoryEntry writes).
  """
  @spec move(volume_id(), path(), path(), String.t()) :: :ok | {:error, term()}
  def move(volume_id, source_dir, dest_dir, name) do
    GenServer.call(__MODULE__, {:move, volume_id, source_dir, dest_dir, name}, 15_000)
  end

  @doc """
  Ensures a root directory entry exists for the given volume.
  """
  @spec ensure_root_dir(volume_id()) :: :ok | {:error, term()}
  def ensure_root_dir(volume_id) do
    GenServer.call(__MODULE__, {:ensure_root_dir, volume_id}, 10_000)
  end

  @doc """
  Lists all files across all volumes from the local ETS cache.

  Primarily useful for garbage collection and debugging.
  """
  @spec list_all() :: [FileMeta.t()]
  def list_all do
    :ets.foldl(
      fn
        {_id, %FileMeta{} = file}, acc -> [file | acc]
        _, acc -> acc
      end,
      [],
      :file_index_by_id
    )
  end

  @doc """
  Lists all files in a volume from the local ETS cache.
  """
  @spec list_volume(volume_id()) :: [FileMeta.t()]
  def list_volume(volume_id) do
    :ets.foldl(
      fn
        {_id, %FileMeta{volume_id: ^volume_id} = file}, acc -> [file | acc]
        _, acc -> acc
      end,
      [],
      :file_index_by_id
    )
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    :ets.new(:file_index_by_id, [:set, :named_table, :public, read_concurrency: true])

    quorum_opts = Keyword.get(opts, :quorum_opts)
    :persistent_term.put({__MODULE__, :quorum_opts}, quorum_opts)

    if quorum_opts do
      case load_from_local_store() do
        {:ok, count} ->
          Logger.info("FileIndex started in quorum mode, loaded #{count} files from local store")

        {:error, reason} ->
          Logger.debug(
            "FileIndex started in quorum mode, local store not available: #{inspect(reason)}"
          )
      end
    else
      Logger.warning("FileIndex started without quorum_opts — writes will fail")
    end

    {:ok, %{quorum_opts: quorum_opts}}
  end

  @impl true
  def handle_call({:create, file}, _from, state) do
    reply = do_create(file, quorum_opts())
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:update, file_id, updates}, _from, state) do
    reply = do_update(file_id, updates, quorum_opts())
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, file_id}, _from, state) do
    reply = do_delete(file_id, quorum_opts())
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:mkdir, volume_id, path, opts}, _from, state) do
    reply = do_mkdir(volume_id, path, opts, quorum_opts())
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:rename, volume_id, parent_path, old_name, new_name}, _from, state) do
    reply = do_rename(volume_id, parent_path, old_name, new_name, quorum_opts())
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:move, volume_id, source_dir, dest_dir, name}, _from, state) do
    reply = do_move(volume_id, source_dir, dest_dir, name, quorum_opts())
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:ensure_root_dir, volume_id}, _from, state) do
    reply = do_ensure_root_dir(volume_id, quorum_opts())
    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    :persistent_term.erase({__MODULE__, :quorum_opts})
    :ok
  rescue
    ArgumentError -> :ok
  end

  ## Private — Create

  defp do_create(_file, nil), do: {:error, :no_quorum}

  defp do_create(file, quorum_opts) do
    with :ok <- FileMeta.validate_path(file.path) do
      {parent_path, name} = split_path(file.path)

      intent =
        Intent.new(
          id: UUIDv7.generate(),
          operation: :file_create,
          conflict_key: {:create, file.volume_id, parent_path, name},
          params: %{volume_id: file.volume_id, path: file.path, file_id: file.id}
        )

      with {:ok, intent_id} <- try_acquire_intent(intent),
           :ok <- do_ensure_root_dir(file.volume_id, quorum_opts),
           :ok <- ensure_parent_dirs(file.volume_id, parent_path, quorum_opts),
           :ok <- quorum_write_file(file, quorum_opts),
           :ok <-
             quorum_add_dir_child(file.volume_id, parent_path, name, :file, file.id, quorum_opts) do
        complete_intent(intent_id)
        :ets.insert(:file_index_by_id, {file.id, file})
        {:ok, file}
      else
        {:error, :conflict, _existing} ->
          {:error, :already_exists}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  ## Private — Update

  defp do_update(_file_id, _updates, nil), do: {:error, :no_quorum}

  defp do_update(file_id, updates, quorum_opts) do
    case fetch_file(file_id, quorum_opts) do
      {:ok, old_file} ->
        updated_file = FileMeta.update(old_file, updates)

        case quorum_write_file(updated_file, quorum_opts) do
          :ok ->
            :ets.insert(:file_index_by_id, {file_id, updated_file})
            {:ok, updated_file}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private — Delete

  defp do_delete(_file_id, nil), do: {:error, :no_quorum}

  defp do_delete(file_id, quorum_opts) do
    case fetch_file(file_id, quorum_opts) do
      {:ok, file} ->
        {parent_path, name} = split_path(file.path)

        intent =
          Intent.new(
            id: UUIDv7.generate(),
            operation: :file_delete,
            conflict_key: {:file, file_id},
            params: %{file_id: file_id, volume_id: file.volume_id, path: file.path}
          )

        with {:ok, intent_id} <- try_acquire_intent(intent),
             :ok <- quorum_delete_file(file_id, quorum_opts),
             :ok <-
               quorum_remove_dir_child(file.volume_id, parent_path, name, quorum_opts) do
          complete_intent(intent_id)
          :ets.delete(:file_index_by_id, file_id)
          :ok
        else
          {:error, reason} -> {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private — Mkdir

  defp do_mkdir(_volume_id, _path, _opts, nil), do: {:error, :quorum_required}

  defp do_mkdir(volume_id, path, opts, quorum_opts) do
    normalized = FileMeta.normalize_path(path)
    {parent_path, name} = split_path(normalized)

    dir_id = UUIDv7.generate()
    new_dir = DirectoryEntry.new(volume_id, normalized, opts)

    with :ok <- do_ensure_root_dir(volume_id, quorum_opts),
         :ok <- ensure_parent_dirs(volume_id, parent_path, quorum_opts),
         :ok <- quorum_write_dir_entry(new_dir, quorum_opts),
         :ok <- quorum_add_dir_child(volume_id, parent_path, name, :dir, dir_id, quorum_opts) do
      {:ok, new_dir}
    end
  end

  ## Private — Rename (within same directory)

  defp do_rename(_volume_id, _parent_path, _old_name, _new_name, nil),
    do: {:error, :quorum_required}

  defp do_rename(volume_id, parent_path, old_name, new_name, quorum_opts) do
    normalized = FileMeta.normalize_path(parent_path)

    case read_dir_entry(volume_id, normalized) do
      {:ok, dir_entry} ->
        case DirectoryEntry.rename_child(dir_entry, old_name, new_name) do
          {:ok, updated_entry} ->
            quorum_write_dir_entry(updated_entry, quorum_opts)

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private — Move (across directories)

  defp do_move(_volume_id, _source_dir, _dest_dir, _name, nil), do: {:error, :quorum_required}

  defp do_move(volume_id, source_dir, dest_dir, name, quorum_opts) do
    source_normalized = FileMeta.normalize_path(source_dir)
    dest_normalized = FileMeta.normalize_path(dest_dir)

    intent =
      Intent.new(
        id: UUIDv7.generate(),
        operation: :file_move,
        conflict_key: {:dir, volume_id, source_normalized},
        params: %{
          volume_id: volume_id,
          source_dir: source_normalized,
          dest_dir: dest_normalized,
          name: name
        }
      )

    with {:ok, source_entry} <- read_dir_entry(volume_id, source_normalized),
         {:ok, child} <- DirectoryEntry.get_child(source_entry, name),
         {:ok, intent_id} <- try_acquire_intent(intent),
         :ok <-
           quorum_remove_dir_child(volume_id, source_normalized, name, quorum_opts),
         :ok <-
           quorum_add_dir_child(
             volume_id,
             dest_normalized,
             name,
             child.type,
             child.id,
             quorum_opts
           ) do
      complete_intent(intent_id)
      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  ## Private — Root directory

  defp do_ensure_root_dir(_volume_id, nil), do: {:error, :no_quorum}

  defp do_ensure_root_dir(volume_id, quorum_opts) do
    dir_key = dir_key(volume_id, "/")

    case QuorumCoordinator.quorum_read(dir_key, quorum_opts) do
      {:ok, _value} ->
        :ok

      {:ok, _value, :possibly_stale} ->
        :ok

      {:error, :not_found} ->
        root = DirectoryEntry.new(volume_id, "/")
        quorum_write_dir_entry(root, quorum_opts)

      {:error, _reason} ->
        root = DirectoryEntry.new(volume_id, "/")
        quorum_write_dir_entry(root, quorum_opts)
    end
  end

  ## Private — Parent directory creation

  defp ensure_parent_dirs(_volume_id, "/", _quorum_opts), do: :ok

  defp ensure_parent_dirs(volume_id, path, quorum_opts) do
    parts = path_parts(path)

    Enum.reduce_while(parts, :ok, fn dir_path, :ok ->
      case ensure_single_parent_dir(volume_id, dir_path, quorum_opts) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp ensure_single_parent_dir(volume_id, dir_path, quorum_opts) do
    dir_key = dir_key(volume_id, dir_path)

    case QuorumCoordinator.quorum_read(dir_key, quorum_opts) do
      {:ok, _} ->
        :ok

      {:ok, _, :possibly_stale} ->
        :ok

      {:error, :not_found} ->
        create_parent_dir(volume_id, dir_path, quorum_opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp create_parent_dir(volume_id, dir_path, quorum_opts) do
    dir = DirectoryEntry.new(volume_id, dir_path)
    {parent, name} = split_path(dir_path)

    with :ok <- quorum_write_dir_entry(dir, quorum_opts) do
      quorum_add_dir_child(volume_id, parent, name, :dir, UUIDv7.generate(), quorum_opts)
    end
  end

  ## Private — Quorum operations for files

  defp quorum_write_file(file, quorum_opts) do
    key = file_key(file.id)
    storable = file_to_storable_map(file)

    case QuorumCoordinator.quorum_write(key, storable, quorum_opts) do
      {:ok, :written} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp quorum_delete_file(file_id, quorum_opts) do
    key = file_key(file_id)

    case QuorumCoordinator.quorum_delete(key, quorum_opts) do
      {:ok, :written} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  ## Private — Quorum operations for directory entries

  defp quorum_write_dir_entry(%DirectoryEntry{} = entry, quorum_opts) do
    key = dir_key(entry.volume_id, entry.parent_path)
    storable = DirectoryEntry.to_storable_map(entry)

    case QuorumCoordinator.quorum_write(key, storable, quorum_opts) do
      {:ok, :written} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp quorum_add_dir_child(volume_id, parent_path, name, type, id, quorum_opts) do
    case read_dir_entry(volume_id, parent_path) do
      {:ok, dir_entry} ->
        updated = DirectoryEntry.add_child(dir_entry, name, type, id)
        quorum_write_dir_entry(updated, quorum_opts)

      {:error, :not_found} ->
        # Parent doesn't exist yet — create it with the child
        new_dir =
          DirectoryEntry.new(volume_id, parent_path)
          |> DirectoryEntry.add_child(name, type, id)

        quorum_write_dir_entry(new_dir, quorum_opts)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp quorum_remove_dir_child(volume_id, parent_path, name, quorum_opts) do
    case read_dir_entry(volume_id, parent_path) do
      {:ok, dir_entry} ->
        updated = DirectoryEntry.remove_child(dir_entry, name)
        quorum_write_dir_entry(updated, quorum_opts)

      {:error, :not_found} ->
        # Parent doesn't exist — nothing to remove
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private — Read helpers

  defp read_dir_entry(volume_id, path) do
    case quorum_opts() do
      nil -> {:error, :not_found}
      opts -> read_dir_entry_quorum(volume_id, path, opts)
    end
  end

  defp read_dir_entry_quorum(volume_id, path, quorum_opts) do
    key = dir_key(volume_id, path)

    case QuorumCoordinator.quorum_read(key, quorum_opts) do
      {:ok, value} ->
        {:ok, DirectoryEntry.from_storable_map(value)}

      {:ok, value, :possibly_stale} ->
        {:ok, DirectoryEntry.from_storable_map(value)}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp fetch_file(file_id, quorum_opts) do
    case :ets.lookup(:file_index_by_id, file_id) do
      [{^file_id, file}] -> {:ok, file}
      [] -> get_file_from_quorum(file_id, quorum_opts)
    end
  end

  defp get_from_quorum(file_id) do
    case quorum_opts() do
      nil -> {:error, :not_found}
      opts -> get_file_from_quorum(file_id, opts)
    end
  end

  defp get_file_from_quorum(file_id, quorum_opts) do
    key = file_key(file_id)

    case QuorumCoordinator.quorum_read(key, quorum_opts) do
      {:ok, value} ->
        file = storable_map_to_file(value)
        :ets.insert(:file_index_by_id, {file_id, file})
        {:ok, file}

      {:ok, value, :possibly_stale} ->
        file = storable_map_to_file(value)
        :ets.insert(:file_index_by_id, {file_id, file})
        {:ok, file}

      {:error, :not_found} ->
        {:error, :not_found}

      {:error, _reason} ->
        {:error, :not_found}
    end
  rescue
    _ -> {:error, :not_found}
  end

  ## Private — IntentLog helpers

  defp try_acquire_intent(intent) do
    case IntentLog.try_acquire(intent) do
      {:ok, intent_id} -> {:ok, intent_id}
      {:error, :conflict, existing} -> {:error, :conflict, existing}
      {:error, :ra_not_available} -> {:ok, intent.id}
      {:error, :ra_unavailable} -> {:ok, intent.id}
      {:error, reason} -> {:error, reason}
    end
  rescue
    _ -> {:ok, intent.id}
  catch
    :exit, _ -> {:ok, intent.id}
  end

  defp complete_intent(intent_id) do
    case IntentLog.complete(intent_id) do
      :ok -> :ok
      {:error, _} -> :ok
    end
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  ## Private — Key format

  defp file_key(file_id), do: @file_key_prefix <> file_id
  defp dir_key(volume_id, path), do: @dir_key_prefix <> volume_id <> ":" <> path

  ## Private — Serialisation

  defp file_to_storable_map(%FileMeta{} = file) do
    %{
      id: file.id,
      volume_id: file.volume_id,
      path: file.path,
      chunks: file.chunks,
      stripes: file.stripes,
      size: file.size,
      mode: file.mode,
      uid: file.uid,
      gid: file.gid,
      created_at: file.created_at,
      modified_at: file.modified_at,
      accessed_at: file.accessed_at,
      version: file.version,
      previous_version_id: file.previous_version_id,
      hlc_timestamp: file.hlc_timestamp
    }
  end

  defp storable_map_to_file(map) when is_map(map) do
    %FileMeta{
      id: get_field(map, :id),
      volume_id: get_field(map, :volume_id),
      path: get_field(map, :path),
      chunks: get_field(map, :chunks, []),
      stripes: decode_stripes(get_field(map, :stripes)),
      size: get_field(map, :size, 0),
      mode: get_field(map, :mode, 0o644),
      uid: get_field(map, :uid, 0),
      gid: get_field(map, :gid, 0),
      created_at: decode_datetime(get_field(map, :created_at)),
      modified_at: decode_datetime(get_field(map, :modified_at)),
      accessed_at: decode_datetime(get_field(map, :accessed_at)),
      version: get_field(map, :version, 1),
      previous_version_id: get_field(map, :previous_version_id),
      hlc_timestamp: get_field(map, :hlc_timestamp)
    }
  end

  defp get_field(map, key, default \\ nil) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || default
  end

  defp decode_stripes(nil), do: nil

  defp decode_stripes(stripes) when is_list(stripes) do
    Enum.map(stripes, &decode_stripe_entry/1)
  end

  defp decode_stripes(_), do: nil

  defp decode_stripe_entry(%{stripe_id: _, byte_range: _} = entry), do: entry

  defp decode_stripe_entry(entry) when is_map(entry) do
    %{
      stripe_id: get_field(entry, :stripe_id),
      byte_range: decode_byte_range(get_field(entry, :byte_range))
    }
  end

  defp decode_byte_range({s, e}), do: {s, e}
  defp decode_byte_range([s, e]) when is_integer(s) and is_integer(e), do: {s, e}
  defp decode_byte_range(other), do: other

  defp decode_datetime(%DateTime{} = dt), do: dt
  defp decode_datetime(nil), do: nil

  defp decode_datetime(str) when is_binary(str) do
    case DateTime.from_iso8601(str) do
      {:ok, dt, _offset} -> dt
      _ -> nil
    end
  end

  defp decode_datetime(_), do: nil

  ## Private — Local store loading

  defp load_from_local_store do
    drives = Application.get_env(:neonfs_core, :drives) || default_drives()

    count =
      Enum.reduce(drives, 0, fn drive, total ->
        path = drive_path(drive)
        meta_dir = Path.join(path, "meta")

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
      file_paths = walk_metadata_files(segment_dir)
      count + load_file_records(file_paths)
    end)
  end

  defp load_file_records(file_paths) do
    Enum.reduce(file_paths, 0, fn file_path, count ->
      case load_file_record(file_path) do
        :ok -> count + 1
        :skip -> count
      end
    end)
  end

  defp load_file_record(file_path) do
    with {:ok, data} <- File.read(file_path),
         {:ok, %{tombstone: false, value: value}} <- MetadataCodec.decode_record(data),
         true <- file_metadata?(value) do
      file = storable_map_to_file(value)
      :ets.insert(:file_index_by_id, {file.id, file})
      :ok
    else
      _ -> :skip
    end
  end

  defp file_metadata?(map) when is_map(map) do
    has_field?(map, :id) and has_field?(map, :volume_id) and has_field?(map, :path)
  end

  defp file_metadata?(_), do: false

  defp has_field?(map, key) do
    Map.has_key?(map, key) or Map.has_key?(map, Atom.to_string(key))
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

  defp default_drives do
    base_dir = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs/blobs")
    [%{id: "default", path: base_dir, tier: :hot, capacity: 0}]
  end

  defp drive_path(%{path: path}), do: path
  defp drive_path(drive) when is_map(drive), do: Map.get(drive, :path, Map.get(drive, "path", ""))

  ## Private — Path helpers

  defp split_path("/"), do: {"/", ""}

  defp split_path(path) do
    parts = String.split(path, "/", trim: true)
    name = List.last(parts)
    parent_parts = Enum.drop(parts, -1)
    parent = "/" <> Enum.join(parent_parts, "/")
    parent = if parent == "/", do: "/", else: parent
    {parent, name}
  end

  defp path_parts(path) do
    parts = String.split(path, "/", trim: true)

    parts
    |> Enum.scan([], fn part, acc -> acc ++ [part] end)
    |> Enum.map(fn parts -> "/" <> Enum.join(parts, "/") end)
  end

  defp quorum_opts do
    :persistent_term.get({__MODULE__, :quorum_opts}, nil)
  end
end
