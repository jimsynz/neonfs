defmodule NeonFS.Core.FileIndex do
  @moduledoc """
  GenServer managing file metadata with quorum-backed distributed storage
  and DirectoryEntry-based path lookups.

  Files are stored with key `"file:<file_id>"` and sharded by `hash(file_id)`.
  Directory entries are stored with key `"dir:<volume_id>:<parent_path>"` and
  sharded by `hash(parent_path)`.

  ## Per-volume metadata path

  Reads delegate to `Volume.MetadataReader` and writes to `Volume.MetadataWriter` —
  both walk the bootstrap layer → root segment → index tree. The
  `:metadata_reader_opts` and `:metadata_writer_opts` start_link opts inject the
  underlying cluster-state / Ra / NIF dependencies so unit tests can stub them.
  Production callers leave both empty and pick up real defaults.

  ## Cross-Segment Operations

  File creation and deletion span two segments (FileMeta + DirectoryEntry) and
  use the IntentLog for crash-safe atomicity. File moves across directories also
  use IntentLog to coordinate two DirectoryEntry updates.
  """

  use GenServer
  require Logger
  import Bitwise, only: [|||: 2]

  alias NeonFS.Core.{
    ChunkIndex,
    DirectoryEntry,
    FileMeta,
    Intent,
    IntentLog
  }

  alias NeonFS.Core.Volume.{MetadataReader, MetadataValue, MetadataWriter}

  alias NeonFS.Events.Broadcaster

  alias NeonFS.Events.{
    DirCreated,
    DirRenamed,
    FileContentUpdated,
    FileCreated,
    FileDeleted,
    FileRenamed
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
  Retrieves a file by `volume_id` and `file_id`.

  Resolves through `Volume.MetadataReader.get_file_meta/3`. The local
  ETS table is a write-through materialisation for list operations on
  this node — serving point reads from it would return stale values
  for files written or deleted elsewhere in the cluster (#342).
  """
  @spec get(volume_id(), file_id()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get(volume_id, file_id) do
    get_from_metadata_reader(volume_id, file_id)
  end

  @doc """
  Retrieves a file by volume ID and path.

  Parses the path into parent_path + name, reads the DirectoryEntry to
  find the file_id, then reads the FileMeta via `Volume.MetadataReader`.
  """
  @spec get_by_path(volume_id(), path()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get_by_path(volume_id, path) do
    normalized = FileMeta.normalize_path(path)
    {parent, name} = split_path(normalized)

    with {:ok, dir_entry} <- read_dir_entry(volume_id, parent),
         {:ok, child} <- DirectoryEntry.get_child(dir_entry, name) do
      resolve_child(volume_id, normalized, child)
    else
      {:error, _} -> {:error, :not_found}
    end
  end

  defp resolve_child(volume_id, path, %{type: :dir} = child) do
    {:ok, synthesise_dir_file_meta(volume_id, path, child)}
  end

  defp resolve_child(volume_id, _path, child) do
    case get(volume_id, child.id) do
      {:ok, file} -> {:ok, file}
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
  Truncates a file to the given size, trimming chunks and stripes as needed.

  When `new_size` is smaller than the current file size, walks the chunk list
  to determine which chunks to keep based on accumulated byte offsets from
  `ChunkIndex`. Chunks that start at or beyond the new size are dropped.
  Stripe references are similarly trimmed for erasure-coded files.

  When `new_size` is equal to or larger than the current size, only the `size`
  field is updated (sparse file semantics — no zero-filled chunks allocated).

  Any additional setattr updates (mode, uid, gid, timestamps) can be passed
  via `additional_updates` and will be applied in the same quorum write.
  """
  @spec truncate(file_id(), non_neg_integer(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, term()}
  def truncate(file_id, new_size, additional_updates \\ []) do
    GenServer.call(__MODULE__, {:truncate, file_id, new_size, additional_updates}, 10_000)
  end

  @doc """
  Updates only the `accessed_at` timestamp on a file (atime touch).

  Uses `FileMeta.touch/1` — no version bump, no `changed_at` or `modified_at` update.
  Intended for `relatime`-style atime updates on read.
  """
  @spec touch(file_id()) :: {:ok, FileMeta.t()} | {:error, term()}
  def touch(file_id) do
    GenServer.call(__MODULE__, {:touch, file_id}, 10_000)
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
  Marks a file as detached and removes its directory entry, while
  keeping the FileMeta reachable by `file_id`.

  Used by `Core.delete_file/2` when the path has live `:pinned`
  namespace claims (open file handles). The FileMeta moves into a
  tombstone state — `detached: true`, `pinned_claim_ids: [...]` —
  that survives until the last pin releases. The matching directory
  entry is removed in the same IntentLog transaction so subsequent
  `LOOKUP` returns `:not_found`.

  Idempotent: a second call on an already-detached FileMeta returns
  the existing record without changes (#643 sub-issue of #638).
  """
  @spec mark_detached(file_id(), [String.t()]) :: {:ok, FileMeta.t()} | {:error, term()}
  def mark_detached(file_id, pinned_claim_ids) when is_list(pinned_claim_ids) do
    GenServer.call(__MODULE__, {:mark_detached, file_id, pinned_claim_ids}, 15_000)
  end

  @doc """
  Removes a single `claim_id` from a detached file's `pinned_claim_ids`
  list. When the resulting list is empty the file is purged via
  `purge_detached/1` so chunk GC can reclaim its blobs.

  Idempotent in two directions: passing a `claim_id` not in the list
  is a no-op, and passing a `file_id` for a non-detached or
  already-purged file returns `:ok`. The unlink-while-open GC handler
  (#644 of #638) calls this once per pin-release telemetry event;
  duplicate or stale notifications must not error.
  """
  @spec decrement_pin(file_id(), String.t()) :: :ok | {:error, term()}
  def decrement_pin(file_id, claim_id) when is_binary(file_id) and is_binary(claim_id) do
    GenServer.call(__MODULE__, {:decrement_pin, file_id, claim_id}, 15_000)
  end

  @doc """
  Final purge of a detached FileMeta. Quorum-deletes the record so
  the orphaned-chunk GC can reclaim its blobs on the next sweep.
  Refuses (`{:error, :not_detached}`) if the file is *not* in the
  detached state — the caller is expected to mark it detached first
  via `mark_detached/2`. Idempotent for files that are already gone
  (returns `:ok`).
  """
  @spec purge_detached(file_id()) :: :ok | {:error, term()}
  def purge_detached(file_id) when is_binary(file_id) do
    GenServer.call(__MODULE__, {:purge_detached, file_id}, 15_000)
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
  Lists directory children with full file metadata.

  Like `list_dir/2`, but resolves each child's FileMeta (for files) so that
  timestamps, size, mode, uid, and gid are all present. Directory children
  receive a synthetic attributes map with current timestamps.

  Returns `{:ok, [{name, child_path, attrs}]}` where attrs is either a
  `FileMeta` struct or a map with synthetic directory attributes.
  """
  @spec list_dir_full(volume_id(), path()) ::
          {:ok, [{String.t(), String.t(), map()}]} | {:error, term()}
  def list_dir_full(volume_id, dir_path) do
    normalized = FileMeta.normalize_path(dir_path)

    case read_dir_entry(volume_id, normalized) do
      {:ok, dir_entry} ->
        entries =
          Enum.map(dir_entry.children, fn {name, child_info} ->
            child_path = Path.join(normalized, name)
            attrs = resolve_child_attrs(child_info, volume_id, child_path)
            {name, child_path, attrs}
          end)

        {:ok, entries}

      {:error, :not_found} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp resolve_child_attrs(%{type: :dir} = child_info, volume_id, child_path) do
    synthesise_dir_file_meta(volume_id, child_path, child_info)
  end

  defp resolve_child_attrs(child_info, volume_id, child_path) do
    file_id = Map.get(child_info, :id) || Map.get(child_info, "id")

    file_result =
      if file_id do
        get(volume_id, file_id)
      else
        get_by_path(volume_id, child_path)
      end

    case file_result do
      {:ok, file} ->
        file

      {:error, _} ->
        now = DateTime.utc_now()

        %{
          size: 0,
          mode: 0o100644,
          uid: 0,
          gid: 0,
          accessed_at: now,
          modified_at: now,
          changed_at: now
        }
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
  def handle_call({:create, file}, _from, state) do
    reply = do_create(file)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:update, file_id, updates}, _from, state) do
    reply = do_update(file_id, updates)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:truncate, file_id, new_size, additional_updates}, _from, state) do
    reply = do_truncate(file_id, new_size, additional_updates)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:touch, file_id}, _from, state) do
    reply = do_touch(file_id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, file_id}, _from, state) do
    reply = do_delete(file_id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:mark_detached, file_id, pinned_claim_ids}, _from, state) do
    reply = do_mark_detached(file_id, pinned_claim_ids)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:decrement_pin, file_id, claim_id}, _from, state) do
    reply = do_decrement_pin(file_id, claim_id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:purge_detached, file_id}, _from, state) do
    reply = do_purge_detached(file_id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:mkdir, volume_id, path, opts}, _from, state) do
    reply = do_mkdir(volume_id, path, opts)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:rename, volume_id, parent_path, old_name, new_name}, _from, state) do
    reply = do_rename(volume_id, parent_path, old_name, new_name)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:move, volume_id, source_dir, dest_dir, name}, _from, state) do
    reply = do_move(volume_id, source_dir, dest_dir, name)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:ensure_root_dir, volume_id}, _from, state) do
    reply = do_ensure_root_dir(volume_id)
    {:reply, reply, state}
  end

  # Only erase persistent_term on clean shutdown, not on crash. On crash
  # restart, the surviving values prevent the child from overwriting them
  # with a stale child_spec.
  @impl true
  def terminate(reason, _state) when reason in [:normal, :shutdown] do
    safe_erase_metadata_opts()
  end

  def terminate({:shutdown, _}, _state) do
    safe_erase_metadata_opts()
  end

  def terminate(_reason, _state), do: :ok

  defp safe_erase_metadata_opts do
    for key <- [:metadata_reader_opts, :metadata_writer_opts] do
      try do
        :persistent_term.erase({__MODULE__, key})
      rescue
        ArgumentError -> :ok
      end
    end

    :ok
  end

  ## Private — Create

  defp do_create(file) do
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
           :ok <- do_ensure_root_dir(file.volume_id),
           :ok <- ensure_parent_dirs(file.volume_id, parent_path),
           :ok <- write_file(file),
           :ok <-
             add_dir_child(file.volume_id, parent_path, name, :file, file.id) do
        complete_intent(intent_id)
        :ets.insert(:file_index_by_id, {file.id, file})

        safe_broadcast(file.volume_id, %FileCreated{
          volume_id: file.volume_id,
          file_id: file.id,
          path: file.path
        })

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

  defp do_update(file_id, updates) do
    case fetch_file(file_id) do
      {:ok, old_file} ->
        updated_file = FileMeta.update(old_file, updates)

        case write_file(updated_file) do
          :ok ->
            :ets.insert(:file_index_by_id, {file_id, updated_file})

            safe_broadcast(updated_file.volume_id, %FileContentUpdated{
              volume_id: updated_file.volume_id,
              file_id: file_id,
              path: updated_file.path
            })

            {:ok, updated_file}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private — Truncate

  defp do_truncate(file_id, new_size, additional_updates) do
    case fetch_file(file_id) do
      {:ok, file} ->
        truncation_updates = truncation_updates_for(file, new_size)
        all_updates = Keyword.merge(additional_updates, truncation_updates)
        do_update(file_id, all_updates)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp truncation_updates_for(_file, 0) do
    [size: 0, chunks: [], stripes: nil]
  end

  defp truncation_updates_for(file, new_size) when new_size >= file.size do
    [size: new_size]
  end

  defp truncation_updates_for(file, new_size) do
    trimmed_chunks = trim_chunks_to_size(file.volume_id, file.chunks, new_size)
    trimmed_stripes = trim_stripes_to_size(file.stripes, new_size)
    [size: new_size, chunks: trimmed_chunks, stripes: trimmed_stripes]
  end

  defp trim_chunks_to_size(volume_id, chunks, target_size) do
    do_trim_chunks(volume_id, chunks, target_size, 0, [])
  end

  defp do_trim_chunks(_volume_id, [], _target, _offset, acc), do: Enum.reverse(acc)

  defp do_trim_chunks(volume_id, [hash | rest], target, offset, acc) do
    case ChunkIndex.get(volume_id, hash) do
      {:ok, chunk_meta} ->
        if offset >= target do
          # This chunk starts at or beyond the target — drop it and all remaining
          Enum.reverse(acc)
        else
          # This chunk covers bytes before the target — keep it
          do_trim_chunks(volume_id, rest, target, offset + chunk_meta.original_size, [
            hash | acc
          ])
        end

      {:error, :not_found} ->
        # Can't determine size — keep the chunk to be safe
        Logger.warning("Chunk size unknown during truncation, keeping chunk")
        do_trim_chunks(volume_id, rest, target, offset, [hash | acc])
    end
  end

  defp trim_stripes_to_size(nil, _target), do: nil
  defp trim_stripes_to_size([], _target), do: nil

  defp trim_stripes_to_size(stripes, target_size) do
    stripes
    |> Enum.filter(fn stripe_ref ->
      {start, _end_byte} = byte_range_bounds(stripe_ref.byte_range)
      start < target_size
    end)
    |> Enum.map(fn stripe_ref ->
      {start, end_byte} = byte_range_bounds(stripe_ref.byte_range)

      if end_byte > target_size do
        %{stripe_ref | byte_range: {start, target_size}}
      else
        stripe_ref
      end
    end)
    |> case do
      [] -> nil
      trimmed -> trimmed
    end
  end

  defp byte_range_bounds({s, e}), do: {s, e}
  defp byte_range_bounds(s..e//_), do: {s, e}

  ## Private — Touch (atime-only update, no version bump)

  defp do_touch(file_id) do
    case fetch_file(file_id) do
      {:ok, old_file} ->
        touched_file = FileMeta.touch(old_file)

        case write_file(touched_file) do
          :ok ->
            :ets.insert(:file_index_by_id, {file_id, touched_file})
            {:ok, touched_file}

          {:error, reason} ->
            {:error, reason}
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private — Delete

  defp do_delete(file_id) do
    case fetch_file(file_id) do
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
             :ok <- delete_file_meta(file),
             :ok <-
               remove_dir_child(file.volume_id, parent_path, name) do
          complete_intent(intent_id)
          delete_file_from_ets(file_id, file)
          :ok
        else
          {:error, reason} ->
            # When quorum writes fail (e.g. ENOSPC), still delete from the local
            # ETS cache so GC can identify orphaned chunks. The tombstone will be
            # missing from the quorum store — anti-entropy or the next successful
            # delete will reconcile. Without this fallback, a full disk creates a
            # deadlock: file delete needs disk space for tombstone, GC needs file
            # delete to identify orphans, disk space needs GC to free blobs.
            Logger.warning(
              "FileIndex delete quorum write failed (#{inspect(reason)}), " <>
                "proceeding with local-only delete for file #{file_id}"
            )

            delete_file_from_ets(file_id, file)
            :ok
        end

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private — Mark detached (POSIX unlink-while-open, #643 of #638)

  defp do_mark_detached(file_id, pinned_claim_ids) do
    case fetch_file(file_id) do
      {:ok, %FileMeta{detached: true} = existing} ->
        {:ok, existing}

      {:ok, file} ->
        detach_file(file, pinned_claim_ids)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp detach_file(file, pinned_claim_ids) do
    {parent_path, name} = split_path(file.path)
    detached_file = FileMeta.update(file, detached: true, pinned_claim_ids: pinned_claim_ids)

    intent =
      Intent.new(
        id: UUIDv7.generate(),
        operation: :file_detach,
        conflict_key: {:file, file.id},
        params: %{file_id: file.id, volume_id: file.volume_id, path: file.path}
      )

    with {:ok, intent_id} <- try_acquire_intent(intent),
         :ok <- write_file(detached_file),
         :ok <- remove_dir_child(file.volume_id, parent_path, name) do
      complete_intent(intent_id)
      :ets.insert(:file_index_by_id, {file.id, detached_file})

      safe_broadcast(file.volume_id, %FileDeleted{
        volume_id: file.volume_id,
        file_id: file.id,
        path: file.path
      })

      {:ok, detached_file}
    end
  end

  ## Private — Decrement pin (POSIX unlink-while-open, #644 of #638)

  defp do_decrement_pin(file_id, claim_id) do
    case fetch_file(file_id) do
      {:ok, %FileMeta{detached: true, pinned_claim_ids: ids} = file} ->
        decrement_pinned_claim_ids(file, claim_id, ids)

      {:ok, _file} ->
        # Not detached — treat as no-op so duplicate / stale telemetry
        # events don't surface as errors to the GC handler.
        :ok

      {:error, :not_found} ->
        :ok
    end
  end

  defp decrement_pinned_claim_ids(file, claim_id, ids) do
    case List.delete(ids, claim_id) do
      ^ids ->
        :ok

      [] ->
        purge_detached_record(file)

      remaining ->
        rewrite_pinned_claim_ids(file, remaining)
    end
  end

  defp rewrite_pinned_claim_ids(file, remaining) do
    updated = FileMeta.update(file, pinned_claim_ids: remaining)

    case write_file(updated) do
      :ok ->
        :ets.insert(:file_index_by_id, {file.id, updated})
        :ok

      {:error, _} = err ->
        err
    end
  end

  ## Private — Purge detached (POSIX unlink-while-open, #644 of #638)

  defp do_purge_detached(file_id) do
    case fetch_file(file_id) do
      {:ok, %FileMeta{detached: true} = file} ->
        purge_detached_record(file)

      {:ok, _file} ->
        {:error, :not_detached}

      {:error, :not_found} ->
        :ok
    end
  end

  defp purge_detached_record(file) do
    case delete_file_meta(file) do
      :ok ->
        delete_file_from_ets(file.id, file)
        :ok

      {:error, _} = err ->
        err
    end
  end

  defp delete_file_from_ets(file_id, file) do
    :ets.delete(:file_index_by_id, file_id)

    safe_broadcast(file.volume_id, %FileDeleted{
      volume_id: file.volume_id,
      file_id: file_id,
      path: file.path
    })
  end

  ## Private — Mkdir

  defp do_mkdir(volume_id, path, opts) do
    normalized = FileMeta.normalize_path(path)
    {parent_path, name} = split_path(normalized)

    dir_id = UUIDv7.generate()
    new_dir = DirectoryEntry.new(volume_id, normalized, opts)

    with :ok <- do_ensure_root_dir(volume_id),
         :ok <- ensure_parent_dirs(volume_id, parent_path),
         :ok <- write_dir_entry(new_dir),
         :ok <- add_dir_child(volume_id, parent_path, name, :dir, dir_id) do
      safe_broadcast(volume_id, %DirCreated{volume_id: volume_id, path: normalized})
      {:ok, new_dir}
    end
  end

  ## Private — Rename (within same directory)

  defp do_rename(volume_id, parent_path, old_name, new_name) do
    normalized = FileMeta.normalize_path(parent_path)

    with {:ok, dir_entry} <- read_dir_entry(volume_id, normalized),
         child_result = DirectoryEntry.get_child(dir_entry, old_name),
         {:ok, updated_entry} <- DirectoryEntry.rename_child(dir_entry, old_name, new_name),
         :ok <- write_dir_entry(updated_entry) do
      broadcast_rename_event(volume_id, child_result, normalized, old_name, new_name)
      :ok
    end
  end

  ## Private — Move (across directories)

  defp do_move(volume_id, source_dir, dest_dir, name) do
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
           remove_dir_child(volume_id, source_normalized, name),
         :ok <-
           add_dir_child(volume_id, dest_normalized, name, child.type, child.id) do
      complete_intent(intent_id)

      broadcast_move_event(volume_id, child, source_normalized, dest_normalized, name)

      :ok
    else
      {:error, reason} -> {:error, reason}
    end
  end

  ## Private — Root directory

  defp do_ensure_root_dir(volume_id) do
    case read_dir_entry(volume_id, "/") do
      {:ok, _entry} -> :ok
      {:error, _} -> write_dir_entry(DirectoryEntry.new(volume_id, "/"))
    end
  end

  ## Private — Parent directory creation

  defp ensure_parent_dirs(_volume_id, "/"), do: :ok

  defp ensure_parent_dirs(volume_id, path) do
    parts = path_parts(path)

    Enum.reduce_while(parts, :ok, fn dir_path, :ok ->
      case ensure_single_parent_dir(volume_id, dir_path) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
  end

  defp ensure_single_parent_dir(volume_id, dir_path) do
    case read_dir_entry(volume_id, dir_path) do
      {:ok, _entry} -> :ok
      {:error, :not_found} -> create_parent_dir(volume_id, dir_path)
      {:error, reason} -> {:error, reason}
    end
  end

  defp create_parent_dir(volume_id, dir_path) do
    dir = DirectoryEntry.new(volume_id, dir_path)
    {parent, name} = split_path(dir_path)

    with :ok <- write_dir_entry(dir) do
      add_dir_child(volume_id, parent, name, :dir, UUIDv7.generate())
    end
  end

  ## Private — MetadataWriter operations for files

  defp write_file(%FileMeta{} = file) do
    key = file_key(file.id)
    storable = file_to_storable_map(file)
    encoded = MetadataValue.encode(storable)

    file.volume_id
    |> MetadataWriter.put(:file_index, key, encoded, metadata_writer_opts())
    |> normalise_writer_result()
  end

  defp delete_file_meta(%FileMeta{} = file) do
    key = file_key(file.id)

    file.volume_id
    |> MetadataWriter.delete(:file_index, key, metadata_writer_opts())
    |> normalise_writer_result()
  end

  ## Private — MetadataWriter operations for directory entries

  defp write_dir_entry(%DirectoryEntry{} = entry) do
    key = dir_key(entry.volume_id, entry.parent_path)
    storable = DirectoryEntry.to_storable_map(entry)
    encoded = MetadataValue.encode(storable)

    entry.volume_id
    |> MetadataWriter.put(:file_index, key, encoded, metadata_writer_opts())
    |> normalise_writer_result()
  end

  # `MetadataWriter.put/5` and `delete/4` can return either
  # `{:error, reason}` or `{:error, reason, info}` (the latter for
  # things like `:insufficient_replicas`). FileIndex's `do_*` chains
  # plus every external caller (`Core`, `WriteOperation`, ACL manager)
  # only pattern-match the 2-tuple, so a 3-tuple bubbling up crashes
  # the GenServer with `MatchError` (#908). Collapse the 3-tuple at
  # the writer boundary — `info` is debug detail; the reason is what
  # callers branch on.
  defp normalise_writer_result({:ok, _root}), do: :ok
  defp normalise_writer_result({:error, reason}), do: {:error, reason}
  defp normalise_writer_result({:error, reason, _info}), do: {:error, reason}

  defp add_dir_child(volume_id, parent_path, name, type, id) do
    case read_dir_entry(volume_id, parent_path) do
      {:ok, dir_entry} ->
        updated = DirectoryEntry.add_child(dir_entry, name, type, id)
        write_dir_entry(updated)

      {:error, :not_found} ->
        # Parent doesn't exist yet — create it with the child
        new_dir =
          DirectoryEntry.new(volume_id, parent_path)
          |> DirectoryEntry.add_child(name, type, id)

        write_dir_entry(new_dir)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp remove_dir_child(volume_id, parent_path, name) do
    case read_dir_entry(volume_id, parent_path) do
      {:ok, dir_entry} ->
        updated = DirectoryEntry.remove_child(dir_entry, name)
        write_dir_entry(updated)

      {:error, :not_found} ->
        # Parent doesn't exist — nothing to remove
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  ## Private — Directory metadata synthesis

  defp synthesise_dir_file_meta(volume_id, path, child_info) do
    {mode, uid, gid, hlc_timestamp} =
      case read_dir_entry(volume_id, path) do
        {:ok, dir_entry} ->
          {0o040000 ||| dir_entry.mode, dir_entry.uid, dir_entry.gid, dir_entry.hlc_timestamp}

        {:error, _} ->
          {0o040755, Map.get(child_info, :uid, 0), Map.get(child_info, :gid, 0), nil}
      end

    now = DateTime.utc_now()

    %FileMeta{
      id: Map.get(child_info, :id),
      volume_id: volume_id,
      path: path,
      chunks: [],
      stripes: nil,
      size: 0,
      content_type: "inode/directory",
      mode: mode,
      uid: uid,
      gid: gid,
      acl_entries: [],
      default_acl: nil,
      created_at: now,
      modified_at: now,
      accessed_at: now,
      changed_at: now,
      version: 1,
      previous_version_id: nil,
      hlc_timestamp: hlc_timestamp
    }
  end

  ## Private — Read helpers

  defp read_dir_entry(volume_id, path) do
    key = dir_key(volume_id, path)

    case MetadataReader.get_file_meta(volume_id, key, metadata_reader_opts()) do
      {:ok, value} -> {:ok, DirectoryEntry.from_storable_map(value)}
      {:error, :not_found} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp fetch_file(file_id) do
    case :ets.lookup(:file_index_by_id, file_id) do
      [{^file_id, file}] -> {:ok, file}
      [] -> {:error, :not_found}
    end
  end

  defp get_from_metadata_reader(volume_id, file_id) do
    key = file_key(file_id)

    case MetadataReader.get_file_meta(volume_id, key, metadata_reader_opts()) do
      {:ok, value} ->
        file = storable_map_to_file(value)
        :ets.insert(:file_index_by_id, {file_id, file})
        {:ok, file}

      {:error, _} ->
        {:error, :not_found}
    end
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

  ## Private — Event broadcasting

  defp safe_broadcast(volume_id, event) do
    Broadcaster.broadcast(volume_id, event)
  rescue
    _ ->
      Logger.warning("Event broadcast failed", event_type: inspect(event.__struct__))
      :ok
  catch
    :exit, _ ->
      Logger.warning("Event broadcast failed", event_type: inspect(event.__struct__))
      :ok
  end

  defp broadcast_rename_event(volume_id, {:ok, child}, parent_path, old_name, new_name) do
    old_path = join_path(parent_path, old_name)
    new_path = join_path(parent_path, new_name)

    case child.type do
      :file ->
        safe_broadcast(volume_id, %FileRenamed{
          volume_id: volume_id,
          file_id: child.id,
          old_path: old_path,
          new_path: new_path
        })

      :dir ->
        safe_broadcast(volume_id, %DirRenamed{
          volume_id: volume_id,
          old_path: old_path,
          new_path: new_path
        })
    end
  end

  defp broadcast_rename_event(_volume_id, {:error, _}, _parent, _old, _new), do: :ok

  defp broadcast_move_event(volume_id, child, source_dir, dest_dir, name) do
    old_path = join_path(source_dir, name)
    new_path = join_path(dest_dir, name)

    case child.type do
      :file ->
        safe_broadcast(volume_id, %FileRenamed{
          volume_id: volume_id,
          file_id: child.id,
          old_path: old_path,
          new_path: new_path
        })

      :dir ->
        safe_broadcast(volume_id, %DirRenamed{
          volume_id: volume_id,
          old_path: old_path,
          new_path: new_path
        })
    end
  end

  defp join_path("/", name), do: "/" <> name
  defp join_path(parent, name), do: parent <> "/" <> name

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
      content_type: file.content_type,
      mode: file.mode,
      uid: file.uid,
      gid: file.gid,
      acl_entries: file.acl_entries,
      default_acl: file.default_acl,
      xattrs: file.xattrs,
      created_at: file.created_at,
      modified_at: file.modified_at,
      accessed_at: file.accessed_at,
      changed_at: file.changed_at,
      version: file.version,
      previous_version_id: file.previous_version_id,
      hlc_timestamp: file.hlc_timestamp,
      detached: file.detached,
      pinned_claim_ids: file.pinned_claim_ids
    }
  end

  defp storable_map_to_file(map) when is_map(map) do
    path = get_field(map, :path)

    %FileMeta{
      id: get_field(map, :id),
      volume_id: get_field(map, :volume_id),
      path: path,
      chunks: get_field(map, :chunks, []),
      stripes: decode_stripes(get_field(map, :stripes)),
      size: get_field(map, :size, 0),
      content_type: get_field(map, :content_type) || MIME.from_path(path || ""),
      mode: get_field(map, :mode, 0o644),
      uid: get_field(map, :uid, 0),
      gid: get_field(map, :gid, 0),
      acl_entries: decode_acl_entries(get_field(map, :acl_entries, [])),
      default_acl: decode_default_acl(get_field(map, :default_acl)),
      xattrs: decode_xattrs(get_field(map, :xattrs, %{})),
      created_at: decode_datetime(get_field(map, :created_at)),
      modified_at: decode_datetime(get_field(map, :modified_at)),
      accessed_at: decode_datetime(get_field(map, :accessed_at)),
      changed_at: decode_datetime(get_field(map, :changed_at)),
      version: get_field(map, :version, 1),
      previous_version_id: get_field(map, :previous_version_id),
      hlc_timestamp: get_field(map, :hlc_timestamp),
      detached: get_field(map, :detached, false),
      pinned_claim_ids: get_field(map, :pinned_claim_ids, [])
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

  defp decode_acl_entries(nil), do: []

  defp decode_acl_entries(entries) when is_list(entries),
    do: Enum.map(entries, &decode_acl_entry/1)

  defp decode_xattrs(nil), do: %{}

  defp decode_xattrs(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {to_binary(k), to_binary(v)} end)
  end

  defp decode_xattrs(_), do: %{}

  defp to_binary(b) when is_binary(b), do: b
  defp to_binary(other), do: :erlang.iolist_to_binary([other])

  defp decode_default_acl(nil), do: nil

  defp decode_default_acl(entries) when is_list(entries),
    do: Enum.map(entries, &decode_acl_entry/1)

  defp decode_acl_entry(entry) when is_map(entry) do
    %{
      type: decode_acl_tag(get_field(entry, :type)),
      id: get_field(entry, :id),
      permissions: decode_permissions(get_field(entry, :permissions, MapSet.new()))
    }
  end

  defp decode_acl_tag(tag) when is_atom(tag), do: tag
  defp decode_acl_tag(tag) when is_binary(tag), do: String.to_existing_atom(tag)

  defp decode_permissions(%MapSet{} = ms), do: MapSet.new(ms, &decode_permission/1)
  defp decode_permissions(list) when is_list(list), do: MapSet.new(list, &decode_permission/1)
  defp decode_permissions(nil), do: MapSet.new()

  defp decode_permission(p) when is_atom(p), do: p
  defp decode_permission(p) when is_binary(p), do: String.to_existing_atom(p)

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

  defp metadata_writer_opts do
    :persistent_term.get({__MODULE__, :metadata_writer_opts}, [])
  end

  defp metadata_reader_opts do
    :persistent_term.get({__MODULE__, :metadata_reader_opts}, [])
  end
end
