defmodule NeonFS.Core.FileIndex do
  @moduledoc """
  GenServer managing file metadata with quorum-backed distributed storage
  and per-entry directory (`dirent:`) path lookups.

  Files are stored with key `"file:<file_id>"` and sharded by `hash(file_id)`.
  Each directory has a small metadata record at `"dir:<volume_id>:<parent_path>"`
  (mode/uid/gid/hlc only). Each child is its own keyed entry at
  `"dirent:<volume_id>:<dir_path>\\0<name>"` → `%{type, id}`, so adding or
  removing a child is a single small keyed write rather than a
  read-modify-write of a growing child-list blob. Directory listing is a
  prefix range scan over the volume's `:file_index` tree.

  ## Per-volume metadata path

  Reads delegate to `Volume.MetadataReader` and writes to `Volume.MetadataWriter` —
  both walk the bootstrap layer → root segment → index tree. The
  `:metadata_reader_opts` and `:metadata_writer_opts` start_link opts inject the
  underlying cluster-state / Ra / NIF dependencies so unit tests can stub them.
  Production callers leave both empty and pick up real defaults.

  ## Cross-Segment Operations

  File creation and deletion span two writes (the FileMeta and the child's
  `dirent:` entry) and use the IntentLog for crash-safe atomicity. Renames
  and moves also span two dirent writes (drop old, add new) and take the
  directory intent for cross-node mutual exclusion.
  """

  use GenServer
  require Logger
  import Bitwise, only: [|||: 2]

  alias NeonFS.Core.{
    ChunkIndex,
    DirectoryEntry,
    FileMeta,
    Intent,
    IntentLog,
    ShardCommitter
  }

  alias NeonFS.Core.Volume.{MetadataReader, MetadataValue, Shard}

  alias NeonFS.Error.{AlreadyExists, Conflict}

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
  @type child_info :: %{type: :file | :dir, id: binary()}

  @file_key_prefix "file:"
  @dir_key_prefix "dir:"
  @dirent_key_prefix "dirent:"

  # Transaction batching (#1295): operations stage their metadata
  # mutations and a windowed committer flushes them as one root-CAS per
  # volume. The window flushes early when the mailbox drains, so a
  # sequential caller never waits — only a concurrent burst accumulates.
  @default_max_batch_size 128
  @default_max_batch_delay_ms 5

  ## Client API

  @doc """
  Starts the FileIndex GenServer.

  ## Options

  None currently. Per-volume metadata reader/writer opts are read
  from `:persistent_term` at call time (post-#792 — see
  `NeonFS.Core.Volume.MetadataReader` / `Volume.MetadataWriter`).
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates a new file metadata entry.

  Uses IntentLog for cross-segment atomicity (FileMeta + child dirent).
  The parent directory must exist — use `mkdir/3` to create directories first,
  or this function auto-creates the root directory for the volume.
  """
  @spec create(FileMeta.t()) :: {:ok, FileMeta.t()} | {:error, term()}
  def create(%FileMeta{} = file) do
    GenServer.call(__MODULE__, {:create, file}, 15_000)
  end

  @doc """
  Creates a file and commits its content chunks in the same batched root
  flip (#1304).

  Folds `ChunkIndex.commit_mutations/2` for `chunk_hashes` into the same
  staged transaction as the file-meta + dirent create, so a content
  write's chunk-meta commit and its file create share one shard-CAS
  instead of N+1 separate flips, and concurrent content writes coalesce
  in the windowed committer. On commit, `write_id`'s write refs are
  dropped from the chunks via `ChunkIndex.finalize_commit/2`.

  `opts` may carry additional mutations to fold into the same batch
  (#1320 — the erasure path folds its `:stripe_index` puts here):

    * `:extra_mutations` — extra `MetadataWriter` mutations to stage.
    * `:on_commit` — a 0-arity fun run once the batch is durable (e.g.
      materialising the stripe ETS entries).
  """
  @spec create_committing_chunks(FileMeta.t(), binary(), [binary()], keyword()) ::
          {:ok, FileMeta.t()} | {:error, term()}
  def create_committing_chunks(%FileMeta{} = file, write_id, chunk_hashes, opts \\ [])
      when is_binary(write_id) and is_list(chunk_hashes) and is_list(opts) do
    GenServer.call(
      __MODULE__,
      {:create_committing_chunks, file, write_id, chunk_hashes, opts},
      15_000
    )
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

  Parses the path into parent_path + name, reads the child's `dirent:`
  entry to find the file_id, then reads the FileMeta via
  `Volume.MetadataReader`.
  """
  @spec get_by_path(volume_id(), path()) :: {:ok, FileMeta.t()} | {:error, :not_found}
  def get_by_path(volume_id, path) do
    normalized = FileMeta.normalize_path(path)
    {parent, name} = split_path(normalized)

    case read_dirent(volume_id, parent, name) do
      {:ok, child} -> resolve_child(volume_id, normalized, child)
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

  Quorum-writes the updated FileMeta. Does not modify any dirent
  (path changes should use `rename/4` or `move/5` instead).
  """
  @spec update(file_id(), keyword()) :: {:ok, FileMeta.t()} | {:error, term()}
  def update(file_id, updates) do
    GenServer.call(__MODULE__, {:update, file_id, updates}, 10_000)
  end

  @doc """
  Updates a file and commits its newly-written content chunks in the same
  batched root flip (#1304) — the `update/2` counterpart of
  `create_committing_chunks/3`, used by the append / partial-write commit
  path.
  """
  @spec update_committing_chunks(file_id(), keyword(), binary(), [binary()]) ::
          {:ok, FileMeta.t()} | {:error, term()}
  def update_committing_chunks(file_id, updates, write_id, chunk_hashes)
      when is_binary(write_id) and is_list(chunk_hashes) do
    GenServer.call(
      __MODULE__,
      {:update_committing_chunks, file_id, updates, write_id, chunk_hashes},
      15_000
    )
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
  and the child's dirent.
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

  Range-scans the directory's `dirent:` entries and returns the
  children map: `%{name => %{type: :file | :dir, id: binary()}}`.
  """
  @spec list_dir(volume_id(), path()) ::
          {:ok, %{String.t() => child_info()}} | {:error, term()}
  def list_dir(volume_id, dir_path) do
    normalized = FileMeta.normalize_path(dir_path)
    list_dirents(volume_id, normalized)
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

    case list_dirents(volume_id, normalized) do
      {:ok, children} ->
        entries =
          Enum.map(children, fn {name, child_info} ->
            child_path = Path.join(normalized, name)
            attrs = resolve_child_attrs(child_info, volume_id, child_path)
            {name, child_path, attrs}
          end)

        {:ok, entries}

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

  @doc """
  Lists every file in a volume from the **authoritative** metadata tree
  (a `MetadataReader` range scan over the `file:` keyspace), not the
  local ETS cache.

  `list_volume/1`'s ETS cache only reflects writes whose `on_commit`
  ran on the local node, so an interface (e.g. S3 `ListObjects`) routed
  to a node that didn't perform the write — or reading a file written
  through a different interface — sees a stale listing (#1034). This
  reads the same source `get_by_path/2` and WebDAV's `list_dir/2`
  already use, so a file is listable as soon as it's committed.
  """
  @spec list_volume_authoritative(volume_id()) :: {:ok, [FileMeta.t()]} | {:error, term()}
  def list_volume_authoritative(volume_id) do
    {start_key, end_key} = file_key_range()

    case MetadataReader.range(volume_id, :file_index, start_key, end_key, metadata_reader_opts()) do
      {:ok, raw_entries} -> {:ok, decode_file_entries(raw_entries, volume_id)}
      {:error, _} = err -> err
    end
  end

  # The metadata tree is per-volume, so the range is already scoped — but
  # match `list_volume/1`'s contract and filter on `volume_id` defensively
  # (the `file:<id>` key carries no volume).
  defp decode_file_entries(raw_entries, volume_id) do
    for {_key, bytes} <- raw_entries,
        {:ok, value} <- [MetadataValue.decode(bytes)],
        %FileMeta{volume_id: ^volume_id} = file <- [storable_map_to_file(value)],
        do: file
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

    state = %{
      pending: %{},
      pending_count: 0,
      pending_files: %{},
      timer: nil,
      max_batch_size:
        Keyword.get(opts, :max_batch_size) ||
          Application.get_env(:neonfs_core, :metadata_max_batch_size, @default_max_batch_size),
      max_batch_delay_ms:
        Keyword.get(opts, :max_batch_delay_ms) ||
          Application.get_env(
            :neonfs_core,
            :metadata_max_batch_delay_ms,
            @default_max_batch_delay_ms
          )
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:create, file}, from, state) do
    stage_or_reply(plan_create(file), from, state)
  end

  @impl true
  def handle_call({:create_committing_chunks, file, write_id, chunk_hashes, opts}, from, state) do
    plan =
      plan_create(file)
      |> with_chunk_commit(write_id, chunk_hashes)
      |> with_extra_mutations(opts)

    stage_or_reply(plan, from, state)
  end

  @impl true
  def handle_call({:update, file_id, updates}, from, state) do
    stage_or_reply(plan_update(file_id, updates, state.pending_files), from, state)
  end

  @impl true
  def handle_call(
        {:update_committing_chunks, file_id, updates, write_id, chunk_hashes},
        from,
        state
      ) do
    plan = plan_update(file_id, updates, state.pending_files)
    stage_or_reply(with_chunk_commit(plan, write_id, chunk_hashes), from, state)
  end

  @impl true
  def handle_call({:truncate, file_id, new_size, additional_updates}, from, state) do
    stage_or_reply(
      plan_truncate(file_id, new_size, additional_updates, state.pending_files),
      from,
      state
    )
  end

  @impl true
  def handle_call({:touch, file_id}, from, state) do
    stage_or_reply(plan_touch(file_id, state.pending_files), from, state)
  end

  @impl true
  def handle_call({:delete, file_id}, from, state) do
    stage_or_reply(plan_delete(file_id, state.pending_files), from, state)
  end

  @impl true
  def handle_call({:mark_detached, file_id, pinned_claim_ids}, from, state) do
    stage_or_reply(
      plan_mark_detached(file_id, pinned_claim_ids, state.pending_files),
      from,
      state
    )
  end

  @impl true
  def handle_call({:decrement_pin, file_id, claim_id}, from, state) do
    stage_or_reply(plan_decrement_pin(file_id, claim_id, state.pending_files), from, state)
  end

  @impl true
  def handle_call({:purge_detached, file_id}, from, state) do
    stage_or_reply(plan_purge_detached(file_id, state.pending_files), from, state)
  end

  @impl true
  def handle_call({:mkdir, volume_id, path, opts}, from, state) do
    stage_or_reply(plan_mkdir(volume_id, path, opts), from, state)
  end

  @impl true
  def handle_call({:rename, volume_id, parent_path, old_name, new_name}, from, state) do
    stage_or_reply(plan_rename(volume_id, parent_path, old_name, new_name), from, state)
  end

  @impl true
  def handle_call({:move, volume_id, source_dir, dest_dir, name}, from, state) do
    stage_or_reply(plan_move(volume_id, source_dir, dest_dir, name), from, state)
  end

  @impl true
  def handle_call({:ensure_root_dir, volume_id}, from, state) do
    stage_or_reply(plan_ensure_root_dir(volume_id), from, state)
  end

  @impl true
  def handle_info(:flush_batch, state) do
    {:noreply, flush(%{state | timer: nil})}
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

  ## Private — Operation planners
  #
  # Each planner runs synchronously inside `handle_call` (reads,
  # validation, intent acquire), then returns either `{:now, reply}` —
  # answered immediately, no metadata write — or `{:stage, volume_id,
  # mutations, on_commit, on_abort, reply}` to join the next batch.
  # `mutations` are pure `MetadataWriter` ops; `on_commit` runs the
  # post-durable side effects (ETS, broadcasts, intent complete) once
  # the batch commits, `on_abort` handles a failed batch and returns
  # that caller's reply. `file_effect` (an optional 7th element) feeds
  # the in-batch `pending_files` overlay so a later same-file read in
  # the same batch sees this op's result.

  defp plan_create(file) do
    case FileMeta.validate_path(file.path) do
      :ok -> plan_validated_create(file)
      {:error, reason} -> {:now, {:error, reason}}
    end
  end

  defp plan_validated_create(file) do
    {parent_path, name} = split_path(file.path)

    intent =
      Intent.new(
        id: UUIDv7.generate(),
        operation: :file_create,
        conflict_key: {:create, file.volume_id, parent_path, name},
        params: %{volume_id: file.volume_id, path: file.path, file_id: file.id}
      )

    with {:ok, intent_id} <- try_acquire_intent(intent),
         {:ok, ancestor_muts} <- ancestor_mutations(file.volume_id, parent_path) do
      mutations =
        ancestor_muts ++
          [
            file_put_mutation(file),
            dirent_put_mutation(file.volume_id, parent_path, name, :file, file.id)
          ]

      on_commit = fn ->
        complete_intent(intent_id)
        :ets.insert(:file_index_by_id, {file.id, file})

        safe_broadcast(file.volume_id, %FileCreated{
          volume_id: file.volume_id,
          file_id: file.id,
          path: file.path
        })
      end

      {:stage, file.volume_id, mutations, on_commit, default_on_abort(), {:ok, file},
       {:put, file}}
    else
      {:error, %Conflict{}} -> {:now, {:error, AlreadyExists.from_reason(:already_exists)}}
      {:error, reason} -> {:now, {:error, reason}}
    end
  end

  defp plan_update(file_id, updates, overlay) do
    case fetch_file(file_id, overlay) do
      {:ok, old_file} ->
        updated_file = FileMeta.update(old_file, updates)

        on_commit = fn ->
          :ets.insert(:file_index_by_id, {file_id, updated_file})

          safe_broadcast(updated_file.volume_id, %FileContentUpdated{
            volume_id: updated_file.volume_id,
            file_id: file_id,
            path: updated_file.path
          })
        end

        {:stage, updated_file.volume_id, [file_put_mutation(updated_file)], on_commit,
         default_on_abort(), {:ok, updated_file}, {:put, updated_file}}

      {:error, reason} ->
        {:now, {:error, reason}}
    end
  end

  defp plan_truncate(file_id, new_size, additional_updates, overlay) do
    case fetch_file(file_id, overlay) do
      {:ok, file} ->
        truncation_updates = truncation_updates_for(file, new_size)
        all_updates = Keyword.merge(additional_updates, truncation_updates)
        plan_update(file_id, all_updates, overlay)

      {:error, reason} ->
        {:now, {:error, reason}}
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

  defp plan_touch(file_id, overlay) do
    case fetch_file(file_id, overlay) do
      {:ok, old_file} ->
        touched_file = FileMeta.touch(old_file)
        on_commit = fn -> :ets.insert(:file_index_by_id, {file_id, touched_file}) end

        {:stage, touched_file.volume_id, [file_put_mutation(touched_file)], on_commit,
         default_on_abort(), {:ok, touched_file}, {:put, touched_file}}

      {:error, reason} ->
        {:now, {:error, reason}}
    end
  end

  ## Private — Delete

  defp plan_delete(file_id, overlay) do
    case fetch_file(file_id, overlay) do
      {:ok, file} -> plan_delete_file(file_id, file)
      {:error, reason} -> {:now, {:error, reason}}
    end
  end

  defp plan_delete_file(file_id, file) do
    {parent_path, name} = split_path(file.path)

    intent =
      Intent.new(
        id: UUIDv7.generate(),
        operation: :file_delete,
        conflict_key: {:file, file_id},
        params: %{file_id: file_id, volume_id: file.volume_id, path: file.path}
      )

    case try_acquire_intent(intent) do
      {:ok, intent_id} ->
        mutations = [
          file_delete_mutation(file),
          dirent_delete_mutation(file.volume_id, parent_path, name)
        ]

        on_commit = fn ->
          complete_intent(intent_id)
          delete_file_from_ets(file_id, file)
        end

        {:stage, file.volume_id, mutations, on_commit, delete_on_abort(file_id, file), :ok,
         {:delete, file_id}}

      {:error, reason} ->
        # When the intent can't be acquired (e.g. quorum down), still delete
        # from the local ETS cache so GC can identify orphaned chunks — see
        # `delete_on_abort/2` for why the deadlock matters.
        local_only_delete(file_id, file, reason)
        {:now, :ok}
    end
  end

  # When the batch commit fails (e.g. ENOSPC), still delete from the local
  # ETS cache so GC can identify orphaned chunks. The tombstone will be
  # missing from the quorum store — anti-entropy or the next successful
  # delete reconciles. Without this fallback a full disk deadlocks: file
  # delete needs disk for the tombstone, GC needs the delete to identify
  # orphans, freeing disk needs GC.
  defp delete_on_abort(file_id, file) do
    fn reason ->
      local_only_delete(file_id, file, reason)
      :ok
    end
  end

  defp local_only_delete(file_id, file, reason) do
    Logger.warning(
      "FileIndex delete quorum write failed (#{inspect(reason)}), " <>
        "proceeding with local-only delete for file #{file_id}"
    )

    delete_file_from_ets(file_id, file)
  end

  ## Private — Mark detached (POSIX unlink-while-open, #643 of #638)

  defp plan_mark_detached(file_id, pinned_claim_ids, overlay) do
    case fetch_file(file_id, overlay) do
      {:ok, %FileMeta{detached: true} = existing} ->
        {:now, {:ok, existing}}

      {:ok, file} ->
        plan_detach(file, pinned_claim_ids)

      {:error, reason} ->
        {:now, {:error, reason}}
    end
  end

  defp plan_detach(file, pinned_claim_ids) do
    {parent_path, name} = split_path(file.path)
    detached_file = FileMeta.update(file, detached: true, pinned_claim_ids: pinned_claim_ids)

    intent =
      Intent.new(
        id: UUIDv7.generate(),
        operation: :file_detach,
        conflict_key: {:file, file.id},
        params: %{file_id: file.id, volume_id: file.volume_id, path: file.path}
      )

    case try_acquire_intent(intent) do
      {:ok, intent_id} ->
        mutations = [
          file_put_mutation(detached_file),
          dirent_delete_mutation(file.volume_id, parent_path, name)
        ]

        on_commit = fn ->
          complete_intent(intent_id)
          :ets.insert(:file_index_by_id, {file.id, detached_file})

          safe_broadcast(file.volume_id, %FileDeleted{
            volume_id: file.volume_id,
            file_id: file.id,
            path: file.path
          })
        end

        {:stage, file.volume_id, mutations, on_commit, default_on_abort(), {:ok, detached_file},
         {:put, detached_file}}

      {:error, reason} ->
        {:now, {:error, reason}}
    end
  end

  ## Private — Decrement pin (POSIX unlink-while-open, #644 of #638)

  defp plan_decrement_pin(file_id, claim_id, overlay) do
    case fetch_file(file_id, overlay) do
      {:ok, %FileMeta{detached: true, pinned_claim_ids: ids} = file} ->
        plan_decrement(file, claim_id, ids)

      {:ok, _file} ->
        # Not detached — treat as no-op so duplicate / stale telemetry
        # events don't surface as errors to the GC handler.
        {:now, :ok}

      {:error, :not_found} ->
        {:now, :ok}
    end
  end

  defp plan_decrement(file, claim_id, ids) do
    case List.delete(ids, claim_id) do
      ^ids ->
        {:now, :ok}

      [] ->
        plan_purge_record(file, :ok)

      remaining ->
        updated = FileMeta.update(file, pinned_claim_ids: remaining)
        on_commit = fn -> :ets.insert(:file_index_by_id, {file.id, updated}) end

        {:stage, updated.volume_id, [file_put_mutation(updated)], on_commit, default_on_abort(),
         :ok, {:put, updated}}
    end
  end

  ## Private — Purge detached (POSIX unlink-while-open, #644 of #638)

  defp plan_purge_detached(file_id, overlay) do
    case fetch_file(file_id, overlay) do
      {:ok, %FileMeta{detached: true} = file} -> plan_purge_record(file, :ok)
      {:ok, _file} -> {:now, {:error, :not_detached}}
      {:error, :not_found} -> {:now, :ok}
    end
  end

  defp plan_purge_record(file, reply) do
    on_commit = fn -> delete_file_from_ets(file.id, file) end

    {:stage, file.volume_id, [file_delete_mutation(file)], on_commit, default_on_abort(), reply,
     {:delete, file.id}}
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

  defp plan_mkdir(volume_id, path, opts) do
    normalized = FileMeta.normalize_path(path)
    {parent_path, name} = split_path(normalized)

    # `validate_path/1` is the storage-layer gate for the leading-slash
    # invariant — the persisted `/` root entry is only consistent if every
    # dir path starts with `/`. `FileMeta.normalize_path/1` only trims
    # trailing slashes; it does not prepend a leading one (#1210).
    with :ok <- FileMeta.validate_path(normalized),
         {:ok, ancestor_muts} <- ancestor_mutations(volume_id, parent_path) do
      new_dir = DirectoryEntry.new(volume_id, normalized, opts)
      dir_id = UUIDv7.generate()

      mutations =
        ancestor_muts ++
          [
            dir_record_mutation(new_dir),
            dirent_put_mutation(volume_id, parent_path, name, :dir, dir_id)
          ]

      on_commit = fn ->
        safe_broadcast(volume_id, %DirCreated{volume_id: volume_id, path: normalized})
      end

      {:stage, volume_id, mutations, on_commit, default_on_abort(), {:ok, new_dir}}
    else
      {:error, reason} -> {:now, {:error, reason}}
    end
  end

  ## Private — Rename (within same directory)

  # A dirent is per-child, so a rename is a delete-old + put-new pair.
  # Both writes land in one batch commit (atomic root flip); the
  # directory intent gives cross-node mutual exclusion (same conflict
  # key as `move`).
  defp plan_rename(volume_id, parent_path, old_name, new_name) do
    normalized = FileMeta.normalize_path(parent_path)

    with {:ok, child} <- read_dirent(volume_id, normalized, old_name),
         :ok <- check_rename_target(volume_id, normalized, old_name, new_name),
         {:ok, intent_id} <- try_acquire_intent(rename_intent(volume_id, normalized)) do
      mutations =
        [
          dirent_put_mutation(volume_id, normalized, new_name, child.type, child.id)
          | rename_delete_mutations(volume_id, normalized, old_name, new_name)
        ]

      on_commit = fn ->
        complete_intent(intent_id)
        broadcast_rename_event(volume_id, child, normalized, old_name, new_name)
      end

      {:stage, volume_id, mutations, on_commit, default_on_abort(), :ok}
    else
      {:error, reason} -> {:now, {:error, reason}}
    end
  end

  defp check_rename_target(_volume_id, _dir, name, name), do: :ok

  defp check_rename_target(volume_id, dir, _old_name, new_name) do
    case read_dirent(volume_id, dir, new_name) do
      {:ok, _} -> {:error, AlreadyExists.from_reason(:already_exists, new_name)}
      {:error, :not_found} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp rename_delete_mutations(_volume_id, _dir, name, name), do: []

  defp rename_delete_mutations(volume_id, dir, old_name, _new_name),
    do: [dirent_delete_mutation(volume_id, dir, old_name)]

  defp rename_intent(volume_id, dir) do
    Intent.new(
      id: UUIDv7.generate(),
      operation: :file_rename,
      conflict_key: {:dir, volume_id, dir},
      params: %{volume_id: volume_id, dir: dir}
    )
  end

  ## Private — Move (across directories)

  defp plan_move(volume_id, source_dir, dest_dir, name) do
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

    with {:ok, child} <- read_dirent(volume_id, source_normalized, name),
         {:ok, intent_id} <- try_acquire_intent(intent) do
      mutations = [
        dirent_put_mutation(volume_id, dest_normalized, name, child.type, child.id),
        dirent_delete_mutation(volume_id, source_normalized, name)
      ]

      on_commit = fn ->
        complete_intent(intent_id)
        broadcast_move_event(volume_id, child, source_normalized, dest_normalized, name)
      end

      {:stage, volume_id, mutations, on_commit, default_on_abort(), :ok}
    else
      {:error, reason} -> {:now, {:error, reason}}
    end
  end

  ## Private — Root directory

  defp plan_ensure_root_dir(volume_id) do
    case root_dir_mutations(volume_id) do
      {:ok, []} ->
        {:now, :ok}

      {:ok, mutations} ->
        {:stage, volume_id, mutations, noop_on_commit(), default_on_abort(), :ok}
    end
  end

  ## Private — Mutation builders
  #
  # Pure `MetadataWriter` ops; reads (`read_dir_entry`) hit the committed
  # root, so two ops in one batch that both need a missing ancestor each
  # emit the same idempotent create — last-write-wins on the same key.

  defp file_put_mutation(%FileMeta{} = file) do
    {:put, :file_index, file_key(file.id), MetadataValue.encode(file_to_storable_map(file))}
  end

  defp file_delete_mutation(%FileMeta{} = file) do
    {:delete, :file_index, file_key(file.id)}
  end

  defp dir_record_mutation(%DirectoryEntry{} = entry) do
    {:put, :file_index, dir_key(entry.volume_id, entry.parent_path),
     MetadataValue.encode(DirectoryEntry.to_storable_map(entry))}
  end

  defp dirent_put_mutation(volume_id, dir_path, name, type, id) do
    {:put, :file_index, dirent_key(volume_id, dir_path, name),
     MetadataValue.encode(%{type: type, id: id})}
  end

  defp dirent_delete_mutation(volume_id, dir_path, name) do
    {:delete, :file_index, dirent_key(volume_id, dir_path, name)}
  end

  defp ancestor_mutations(volume_id, parent_path) do
    with {:ok, root_muts} <- root_dir_mutations(volume_id),
         {:ok, parent_muts} <- parent_dir_mutations(volume_id, parent_path) do
      {:ok, root_muts ++ parent_muts}
    end
  end

  defp root_dir_mutations(volume_id) do
    case read_dir_entry(volume_id, "/") do
      {:ok, _entry} -> {:ok, []}
      {:error, _} -> {:ok, [dir_record_mutation(DirectoryEntry.new(volume_id, "/"))]}
    end
  end

  defp parent_dir_mutations(_volume_id, "/"), do: {:ok, []}

  defp parent_dir_mutations(volume_id, path) do
    path
    |> path_parts()
    |> Enum.reduce_while({:ok, []}, fn dir_path, {:ok, acc} ->
      case read_dir_entry(volume_id, dir_path) do
        {:ok, _entry} ->
          {:cont, {:ok, acc}}

        {:error, :not_found} ->
          {parent, name} = split_path(dir_path)

          muts = [
            dir_record_mutation(DirectoryEntry.new(volume_id, dir_path)),
            dirent_put_mutation(volume_id, parent, name, :dir, UUIDv7.generate())
          ]

          {:cont, {:ok, acc ++ muts}}

        {:error, reason} ->
          {:halt, {:error, reason}}
      end
    end)
  end

  defp default_on_abort, do: fn reason -> {:error, reason} end
  defp noop_on_commit, do: fn -> :ok end

  ## Private — Chunk-commit folding (#1304)
  #
  # Augments a staged file operation so the content chunks' `:committed`
  # metadata persists in the *same* shard-grouped batch as the file
  # create/update — collapsing a content write's N chunk commits + the
  # file flip into one CAS per shard. A pre-stage failure (`{:now, _}`)
  # passes straight through so the write path still aborts the chunks.

  defp with_chunk_commit({:now, _} = now, _write_id, _chunk_hashes), do: now

  # `plan_create/1` and `plan_update/3` — the only planners routed here —
  # always stage a 7-tuple (they carry a `file_effect`), so that's the
  # only `{:stage, …}` shape this needs to handle.
  defp with_chunk_commit(
         {:stage, volume_id, mutations, on_commit, on_abort, reply, file_effect},
         write_id,
         hashes
       ) do
    chunk_mutations = ChunkIndex.commit_mutations(volume_id, hashes)

    folded_on_commit = fn ->
      ChunkIndex.finalize_commit(write_id, hashes)
      on_commit.()
    end

    {:stage, volume_id, mutations ++ chunk_mutations, folded_on_commit, on_abort, reply,
     file_effect}
  end

  # Folds caller-supplied extra mutations (#1320 — the erasure path's
  # `:stripe_index` puts) and an extra post-commit hook into the staged
  # transaction, so they share the same shard-CAS as the file + chunks.
  defp with_extra_mutations({:now, _} = now, _opts), do: now

  defp with_extra_mutations(
         {:stage, volume_id, mutations, on_commit, on_abort, reply, file_effect},
         opts
       ) do
    extra_mutations = Keyword.get(opts, :extra_mutations, [])
    extra_on_commit = Keyword.get(opts, :on_commit, fn -> :ok end)

    folded_on_commit = fn ->
      on_commit.()
      extra_on_commit.()
    end

    {:stage, volume_id, mutations ++ extra_mutations, folded_on_commit, on_abort, reply,
     file_effect}
  end

  ## Private — Batch committer
  #
  # Operations stage their mutations; a windowed flush commits each
  # volume's pending mutations as one `MetadataWriter.apply_batch` (a
  # single root-CAS). The window flushes early when the mailbox drains,
  # so a lone caller commits immediately while a concurrent burst
  # coalesces — `max_batch_size` ops or `max_batch_delay_ms` cap it.

  defp stage_or_reply({:now, reply}, _from, state), do: {:reply, reply, state}

  defp stage_or_reply({:stage, volume_id, mutations, on_commit, on_abort, reply}, from, state) do
    stage_or_reply({:stage, volume_id, mutations, on_commit, on_abort, reply, nil}, from, state)
  end

  defp stage_or_reply(
         {:stage, volume_id, mutations, on_commit, on_abort, reply, file_effect},
         from,
         state
       ) do
    txn = %{
      from: from,
      mutations: mutations,
      on_commit: on_commit,
      on_abort: on_abort,
      reply: reply
    }

    state
    |> stage_txn(volume_id, txn, file_effect)
    |> after_stage()
  end

  defp stage_txn(state, volume_id, txn, file_effect) do
    pending = Map.update(state.pending, volume_id, [txn], &[txn | &1])

    %{
      state
      | pending: pending,
        pending_count: state.pending_count + 1,
        pending_files: apply_file_effect(state.pending_files, file_effect)
    }
  end

  defp apply_file_effect(overlay, nil), do: overlay
  defp apply_file_effect(overlay, {:put, %FileMeta{} = file}), do: Map.put(overlay, file.id, file)
  defp apply_file_effect(overlay, {:delete, file_id}), do: Map.put(overlay, file_id, :deleted)

  defp after_stage(state) do
    cond do
      state.pending_count >= state.max_batch_size -> {:noreply, flush(cancel_timer(state))}
      mailbox_empty?() -> {:noreply, flush(cancel_timer(state))}
      true -> {:noreply, arm_timer(state)}
    end
  end

  defp flush(%{pending: pending} = state) when map_size(pending) == 0, do: state

  defp flush(state) do
    Enum.each(state.pending, fn {volume_id, txns} ->
      commit_volume_batch(volume_id, Enum.reverse(txns))
    end)

    %{state | pending: %{}, pending_count: 0, pending_files: %{}}
  end

  # Group the volume batch's mutations by shard and commit each shard
  # through its `ShardCommitter` worker concurrently (#1308): distinct
  # shards commit in parallel, the same shard always on one writer. Then
  # reply each op by whether every shard it touched committed.
  defp commit_volume_batch(volume_id, txns) do
    writer_opts = metadata_writer_opts()

    by_shard =
      txns
      |> Enum.flat_map(& &1.mutations)
      |> Enum.group_by(&Shard.for_key(mutation_key(&1)))

    shard_results = commit_shards(volume_id, by_shard, writer_opts)

    Enum.each(txns, &reply_after_commit(&1, shard_results))
  end

  defp commit_shards(volume_id, by_shard, writer_opts) do
    by_shard
    |> Task.async_stream(
      fn {shard, mutations} ->
        {shard, ShardCommitter.commit(volume_id, shard, mutations, writer_opts)}
      end,
      max_concurrency: max(map_size(by_shard), 1),
      timeout: :infinity,
      ordered: false
    )
    |> Enum.reduce(%{}, fn {:ok, {shard, result}}, acc -> Map.put(acc, shard, result) end)
  end

  defp reply_after_commit(txn, shard_results) do
    shards = txn.mutations |> Enum.map(&Shard.for_key(mutation_key(&1))) |> Enum.uniq()

    case first_shard_error(shards, shard_results) do
      nil ->
        txn.on_commit.()
        GenServer.reply(txn.from, txn.reply)

      {:error, reason} ->
        GenServer.reply(txn.from, txn.on_abort.(reason))
    end
  end

  defp first_shard_error(shards, shard_results) do
    Enum.find_value(shards, fn shard ->
      case Map.get(shard_results, shard) do
        {:error, _} = err -> err
        _ -> nil
      end
    end)
  end

  defp mutation_key({:put, _kind, key, _value}), do: key
  defp mutation_key({:delete, _kind, key}), do: key
  defp mutation_key({:merge, _kind, key, _fields}), do: key

  defp arm_timer(%{timer: nil} = state) do
    %{state | timer: Process.send_after(self(), :flush_batch, state.max_batch_delay_ms)}
  end

  defp arm_timer(state), do: state

  defp cancel_timer(%{timer: nil} = state), do: state

  defp cancel_timer(%{timer: ref} = state) do
    Process.cancel_timer(ref)
    %{state | timer: nil}
  end

  defp mailbox_empty? do
    case Process.info(self(), :message_queue_len) do
      {:message_queue_len, 0} -> true
      _ -> false
    end
  end

  defp read_dirent(volume_id, dir_path, name) do
    key = dirent_key(volume_id, dir_path, name)

    case MetadataReader.get_file_meta(volume_id, key, metadata_reader_opts()) do
      {:ok, value} -> {:ok, decode_dirent(value)}
      {:error, :not_found} -> {:error, :not_found}
      {:error, reason} -> {:error, reason}
    end
  end

  defp list_dirents(volume_id, dir_path) do
    {start_key, end_key} = dirent_range(volume_id, dir_path)

    case MetadataReader.range(volume_id, :file_index, start_key, end_key, metadata_reader_opts()) do
      {:ok, raw_entries} -> decode_dirent_entries(raw_entries)
      {:error, reason} -> {:error, reason}
    end
  end

  defp decode_dirent_entries(raw_entries) do
    Enum.reduce_while(raw_entries, {:ok, %{}}, fn {key, bytes}, {:ok, acc} ->
      case MetadataValue.decode(bytes) do
        {:ok, value} ->
          {:cont, {:ok, Map.put(acc, dirent_name_from_key(key), decode_dirent(value))}}

        {:error, _} = err ->
          {:halt, err}
      end
    end)
  end

  defp decode_dirent(value) do
    %{type: decode_child_type(get_field(value, :type)), id: get_field(value, :id)}
  end

  defp decode_child_type(type) when type in [:file, "file"], do: :file
  defp decode_child_type(type) when type in [:dir, "dir"], do: :dir
  defp decode_child_type(_), do: :file

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

  # Reads the file record, consulting the in-batch overlay first so an
  # operation sees a same-file mutation staged earlier in the same batch
  # (otherwise concurrent same-file updates would read the committed
  # value and lose each other — the #1260 invariant). Falls through to
  # the local ETS materialisation for anything not touched this batch.
  defp fetch_file(file_id, overlay) do
    case Map.get(overlay, file_id) do
      :deleted -> {:error, :not_found}
      %FileMeta{} = file -> {:ok, file}
      nil -> fetch_file_from_ets(file_id)
    end
  end

  defp fetch_file_from_ets(file_id) do
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
      {:error, %{class: :unavailable}} -> {:ok, intent.id}
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

  defp broadcast_rename_event(volume_id, child, parent_path, old_name, new_name) do
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

  # Half-open range `[prefix, prefix⁺)` covering every `file:<id>` key.
  # The id follows the prefix directly, so the exclusive upper bound
  # increments the prefix's final byte.
  defp file_key_range do
    size = byte_size(@file_key_prefix) - 1
    <<head::binary-size(^size), last>> = @file_key_prefix
    {@file_key_prefix, head <> <<last + 1>>}
  end

  defp dir_key(volume_id, path), do: @dir_key_prefix <> volume_id <> ":" <> path

  # `<<0>>` separates the directory path from the child name. A NUL can
  # never appear in a path component, so a single-level listing is the
  # range `[prefix<<0>>, prefix<<1>>)` — grandchildren (whose keys carry
  # a `/` after the path, sorting above `<<1>>`) are excluded.
  defp dirent_key(volume_id, dir_path, name),
    do: dirent_prefix(volume_id, dir_path) <> name

  defp dirent_prefix(volume_id, dir_path),
    do: @dirent_key_prefix <> volume_id <> ":" <> dir_path <> <<0>>

  defp dirent_range(volume_id, dir_path) do
    base = @dirent_key_prefix <> volume_id <> ":" <> dir_path
    {base <> <<0>>, base <> <<1>>}
  end

  defp dirent_name_from_key(key) do
    case :binary.split(key, <<0>>) do
      [_prefix, name] -> name
      _ -> key
    end
  end

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
      metadata: file.metadata,
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
      metadata: decode_metadata(get_field(map, :metadata, %{})),
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

  # FileMeta.metadata is a generic string-keyed map (S3 user metadata, the S3
  # content-MD5 ETag, etc.). Normalise keys to binary on decode but leave values
  # untouched — unlike xattrs, values are not necessarily binary.
  defp decode_metadata(nil), do: %{}

  defp decode_metadata(map) when is_map(map) do
    Map.new(map, fn {k, v} -> {to_binary(k), v} end)
  end

  defp decode_metadata(_), do: %{}

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
