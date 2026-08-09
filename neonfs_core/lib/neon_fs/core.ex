defmodule NeonFS.Core do
  @moduledoc """
  Public RPC facade for NeonFS core operations.

  Non-core nodes (S3, WebDAV, FUSE, NFS) call functions in this module via
  `NeonFS.Client.Router`, which dispatches `:rpc.call/5` to a core node.
  Each function resolves volume names to IDs and delegates to the
  appropriate internal module.
  """

  alias NeonFS.Core.Authorise
  alias NeonFS.Core.ClusterMode
  alias NeonFS.Core.CommitChunks
  alias NeonFS.Core.CredentialManager
  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.NamespaceCoordinator
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.Core.ReadOperation
  alias NeonFS.Core.StripeIndex
  alias NeonFS.Core.SyncOperation
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Core.WriteOperation
  alias NeonFS.Error.{Conflict, FileNotFound, Invalid, NotFound, Unavailable, VolumeNotFound}

  import Bitwise, only: [&&&: 2]

  # --- Credential operations ---

  @doc """
  Looks up a credential by access key ID.

  Called by the S3 backend during SigV4 authentication and by the
  WebDAV backend during HTTP Basic authentication.
  """
  @spec lookup_credential(String.t()) :: {:ok, map()} | {:error, NotFound.t()}
  def lookup_credential(access_key_id) do
    case CredentialManager.lookup(access_key_id) do
      {:ok, credential} ->
        {:ok, %{secret_access_key: credential.secret_access_key, identity: credential.identity}}

      {:error, :not_found} ->
        {:error, NotFound.exception(message: "Credential not found")}
    end
  end

  # --- Volume operations ---

  @doc """
  Lists all volumes.
  """
  @spec list_volumes() :: {:ok, [NeonFS.Core.Volume.t()]}
  def list_volumes do
    {:ok, VolumeRegistry.list()}
  end

  @doc """
  Gets a volume by name.
  """
  @spec get_volume(String.t()) :: {:ok, NeonFS.Core.Volume.t()} | {:error, VolumeNotFound.t()}
  def get_volume(name) do
    resolve_volume(name)
  end

  @doc """
  Whether the cluster is currently `:frozen` — a coordinated
  maintenance freeze during which new client writes are rejected so
  in-flight writes can settle before a planned power-down.

  Exposed on the RPC facade so interface nodes can surface a "read-only /
  temporarily unavailable" response to their clients.
  """
  @spec cluster_frozen?() :: boolean()
  def cluster_frozen? do
    ClusterMode.frozen?()
  end

  @doc """
  Returns the distinct core nodes that hold a replica of the volume's
  root metadata segment.

  Interface nodes use this (via `NeonFS.Client.RootPlacement`) to route
  metadata writes to a node that can perform them locally, avoiding the
  per-write remote re-dispatch the `MetadataWriter` fallback otherwise
  pays. Reads the authoritative `root_entry.drive_locations`
  from the Ra-backed bootstrap layer — no untracked copies.
  """
  @spec volume_root_nodes(String.t()) :: {:ok, [node()]} | {:error, term()}
  def volume_root_nodes(volume_name) when is_binary(volume_name) do
    with {:ok, %{id: volume_id}} <- resolve_volume(volume_name) do
      volume_root_nodes_by_id(volume_id)
    end
  end

  @doc """
  Like `volume_root_nodes/1` but keyed by the volume's UUID id.

  The Ra `volume_root` bootstrap entry is already keyed by `volume_id`, so this
  skips the name→id resolution `volume_root_nodes/1` does — for callers (e.g.
  FUSE) that hold the id and issue writes through id-keyed APIs.
  """
  @spec volume_root_nodes_by_id(String.t()) :: {:ok, [node()]} | {:error, term()}
  def volume_root_nodes_by_id(volume_id) when is_binary(volume_id) do
    with {:ok, entry} <- fetch_volume_root(volume_id) do
      {:ok, entry.drive_locations |> Enum.map(& &1.node) |> Enum.uniq()}
    end
  end

  # The replica nodes are the same across a volume's shards at provision
  # time; shard 0 always exists, so it answers "which nodes hold this
  # volume's metadata".
  defp fetch_volume_root(volume_id) do
    case RaSupervisor.local_query(&MetadataStateMachine.get_volume_root(&1, volume_id, 0)) do
      {:ok, nil} -> {:error, VolumeNotFound.exception(volume_id: volume_id)}
      {:ok, entry} -> {:ok, entry}
      {:error, _} = error -> error
    end
  end

  @doc """
  Gets a volume by its UUID id.

  Counterpart to `get_volume/1` for callers (e.g. interface nodes
  resolving a filehandle that embeds the volume's UUID rather than
  its name) that hold a stable id but not the current name.
  """
  @spec get_volume_by_id(String.t()) ::
          {:ok, NeonFS.Core.Volume.t()} | {:error, VolumeNotFound.t()}
  def get_volume_by_id(id) do
    case VolumeRegistry.get(id) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, VolumeNotFound.exception(volume_id: id)}
    end
  end

  @doc """
  Creates a volume with the given name.
  """
  @spec create_volume(String.t()) :: {:ok, NeonFS.Core.Volume.t()} | {:error, term()}
  def create_volume(name) do
    VolumeRegistry.create(name)
  end

  @doc """
  Creates a volume with the given name and options.
  """
  @spec create_volume(String.t(), keyword()) :: {:ok, NeonFS.Core.Volume.t()} | {:error, term()}
  def create_volume(name, opts) do
    VolumeRegistry.create(name, opts)
  end

  @doc """
  Deletes a volume by name.
  """
  @spec delete_volume(String.t()) :: :ok | {:error, term()}
  def delete_volume(name) do
    with {:ok, volume} <- resolve_volume(name) do
      VolumeRegistry.delete(volume.id)
    end
  end

  @doc """
  Checks whether a volume with the given name exists.
  """
  @spec volume_exists?(String.t()) :: boolean()
  def volume_exists?(name) do
    match?({:ok, _}, VolumeRegistry.get_by_name(name))
  end

  # --- File operations ---

  @doc """
  Reads a file's content from a volume.

  Supports partial reads via offset and length, avoiding full-file
  materialisation for range requests.

  ## Options

    * `:offset` - Byte offset to start reading from (default: 0)
    * `:length` - Number of bytes to read (default: `:all` for entire file)

  """
  @spec read_file(String.t(), String.t(), keyword()) :: {:ok, binary()} | {:error, term()}
  def read_file(volume_name, path, opts \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      ReadOperation.read_file(volume.id, normalize_path(path), opts)
    end
  end

  @doc """
  Returns a lazy stream of chunk data for a file's byte range.

  Performs authorisation and metadata resolution eagerly. Chunk data is
  fetched lazily so at most one chunk is held in memory at a time.

  Streams cannot be serialised across Erlang distribution. This API is
  for local consumption on core nodes only.

  ## Options

    * `:offset` - Byte offset to start streaming from (default: 0)
    * `:length` - Number of bytes to stream (default: `:all` for entire file)

  """
  @spec read_file_stream(String.t(), String.t(), keyword()) ::
          {:ok, %{stream: Enumerable.t(), file_size: non_neg_integer()}} | {:error, term()}
  def read_file_stream(volume_name, path, opts \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      ReadOperation.read_file_stream(volume.id, normalize_path(path), opts)
    end
  end

  @doc """
  Returns chunk references for a file's byte range without fetching data.

  Interface nodes (FUSE, NFS, S3, WebDAV) call this to get metadata then
  fetch chunks directly over the TLS data plane, keeping bulk data off
  Erlang distribution. See `NeonFS.Client.ChunkReader` for a ready-made
  consumer.

  Replicated volumes return refs for each relevant file chunk. Erasure-coded
  volumes return refs for the data chunks of each overlapping stripe when
  all data chunks are available; if any data chunk is missing (requiring
  parity-based reconstruction), `{:error, :stripe_refs_unsupported}` is
  returned so the caller can fall back to `read_file/3`.

  ## Options

    * `:offset` - Byte offset to start from (default: 0)
    * `:length` - Number of bytes to include (default: `:all`)

  """
  @spec read_file_refs(String.t(), String.t(), keyword()) ::
          {:ok, %{file_size: non_neg_integer(), chunks: [map()]}} | {:error, term()}
  def read_file_refs(volume_name, path, opts \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      ReadOperation.read_file_refs(volume.id, normalize_path(path), opts)
    end
  end

  @doc """
  Streams content to a file, chunking and storing each chunk as it
  arrives instead of buffering the whole file in memory.

  Accepts an `Enumerable.t()` of binary segments. The peak working set
  is bounded by the strategy's maximum chunk size, so multi-gigabyte
  files complete without OOMing the core node.

  Currently supports replicated volumes only; erasure-coded volumes
  return `{:error, :streaming_writes_not_supported_for_erasure}` — for
  erasure-coded whole-file writes, use `write_file_at/5` with offset 0.
  """
  @spec write_file_streamed(String.t(), String.t(), Enumerable.t(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, term()}
  def write_file_streamed(volume_name, path, stream, opts \\ []) do
    with :ok <- ensure_writable(),
         {:ok, volume} <- resolve_volume(volume_name) do
      WriteOperation.write_file_streamed(volume.id, normalize_path(path), stream, opts)
    end
  end

  @doc """
  Writes `data` to a file at `offset`, creating the file if it does not exist.

  For a new file at offset 0 this replaces the file contents. For an
  existing file, only the chunks / stripes overlapping the write range are
  rewritten. This is the whole-binary counterpart to `write_file_streamed/4`
  and the only supported entry point for whole-file writes on erasure-coded
  volumes until streaming erasure encoding lands.
  """
  @spec write_file_at(String.t(), String.t(), non_neg_integer(), binary(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, term()}
  def write_file_at(volume_name, path, offset, data, opts \\ []) do
    with :ok <- ensure_writable(),
         {:ok, volume} <- resolve_volume(volume_name) do
      WriteOperation.write_file_at(volume.id, normalize_path(path), offset, data, opts)
    end
  end

  @doc """
  Reads a file's content by `file_id` rather than path.

  Counterpart to `read_file/3` for callers holding a long-lived
  handle (FUSE / NFSv4 fd) that may have been resolved before an
  unlink. Works against `:detached` FileMetas — the unlink-while-open
  story keeps chunks reachable by `file_id` until the
  last `:pinned` claim releases.
  """
  @spec read_file_by_id(String.t(), binary(), keyword()) :: {:ok, binary()} | {:error, term()}
  def read_file_by_id(volume_name, file_id, opts \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      ReadOperation.read_file_by_id(volume.id, file_id, opts)
    end
  end

  @doc """
  Lazy-stream counterpart to `read_file_by_id/3`. Same caveats as
  `read_file_stream/3` apply (no distribution-safe serialisation).
  """
  @spec read_file_stream_by_id(String.t(), binary(), keyword()) ::
          {:ok, %{stream: Enumerable.t(), file_size: non_neg_integer()}} | {:error, term()}
  def read_file_stream_by_id(volume_name, file_id, opts \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      ReadOperation.read_file_stream_by_id(volume.id, file_id, opts)
    end
  end

  @doc """
  Refs counterpart to `read_file_by_id/3` — `file_id`-keyed metadata-
  only fetch for interface nodes that pull bulk data over the TLS
  data plane.
  """
  @spec read_file_refs_by_id(String.t(), binary(), keyword()) ::
          {:ok, %{file_size: non_neg_integer(), chunks: [map()]}} | {:error, term()}
  def read_file_refs_by_id(volume_name, file_id, opts \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      ReadOperation.read_file_refs_by_id(volume.id, file_id, opts)
    end
  end

  @doc """
  Counterpart to `write_file_at/5` keyed by `file_id`. Targets an
  already-existing file resolved by id rather than path — used by
  FUSE / NFSv4 fd holders writing through a cached handle to a file
  that may have been detached by another peer. Does not
  support `:create_only`.
  """
  @spec write_file_at_by_id(String.t(), binary(), non_neg_integer(), binary(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, term()}
  def write_file_at_by_id(volume_name, file_id, offset, data, opts \\ []) do
    with :ok <- ensure_writable(),
         {:ok, volume} <- resolve_volume(volume_name) do
      WriteOperation.write_file_at_by_id(volume.id, file_id, offset, data, opts)
    end
  end

  @doc """
  Zero-fills `length` bytes at `offset` of the file with `file_id` in a
  single metadata commit, whatever the length.

  Chunks the range covers entirely are replaced by a stored zero chunk of
  the same size rather than rewritten, so the cost tracks the chunks whose
  content actually changes; only the partial chunks at either end are
  read-modify-written. The peak working set is one chunk. The range must
  lie within the file. Replicated volumes only.
  """
  @spec write_zeroes_by_id(
          String.t(),
          binary(),
          non_neg_integer(),
          non_neg_integer(),
          keyword()
        ) :: {:ok, NeonFS.Core.FileMeta.t()} | {:error, term()}
  def write_zeroes_by_id(volume_name, file_id, offset, length, opts \\ []) do
    with :ok <- ensure_writable(),
         {:ok, volume} <- resolve_volume(volume_name) do
      WriteOperation.write_zeroes_at_by_id(volume.id, file_id, offset, length, opts)
    end
  end

  @doc """
  Durability barrier for `path` — blocks until every chunk of the file
  has at least the volume's `min_copies` durable replicas, driving
  synchronous replication for any shortfall.

  This is the core mechanism behind `fsync`/`sync`/COMMIT across the
  interface layer. On a `write_ack: :local` volume the
  extra replicas are placed by a fire-and-forget background task after
  the write acks; this barrier forces them to complete so a read — or a
  whole-cluster restart — immediately after the sync sees durable data.

  Erasure-coded volumes return `:ok` immediately: their shards are
  written synchronously on the write path.

  Returns `:ok` once every chunk meets `min_copies`, or
  `{:error, {:under_replicated, have, want}}` for the first chunk that
  cannot reach it.
  """
  @spec sync_file(String.t(), String.t()) :: :ok | {:error, term()}
  def sync_file(volume_name, path) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      SyncOperation.sync_file(volume.id, normalize_path(path))
    end
  end

  @doc """
  `file_id`-keyed counterpart to `sync_file/2`. FUSE / NFSv4 fd holders
  sync through a cached handle whose file may have been detached by
  another peer; resolving by id keeps a `:detached` file syncable.
  """
  @spec sync_file_by_id(String.t(), binary()) :: :ok | {:error, term()}
  def sync_file_by_id(volume_name, file_id) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      SyncOperation.sync_file_by_id(volume.id, file_id)
    end
  end

  @doc """
  Commits a file whose chunk bytes have already been written to their
  replicas — the write-side counterpart to `read_file_refs/3`.

  Interface nodes that chunked a stream locally (via
  `NeonFS.Client.ChunkWriter`) and pushed each chunk to replicas through
  `Router.data_call(:put_chunk, …)` call this RPC to lay down the
  `FileIndex` entry and finalise the commit.

  `chunk_hashes` is the ordered list of chunk hashes the writer produced.
  `opts` must carry `:total_size` (the file's byte length) and
  `:locations` (map `%{hash => [%{node, drive_id, tier}]}`) so each chunk
  can be validated and have its `ChunkIndex` entry populated. Optional
  fields (`:uid`, `:gids`, `:client_ref`, `:mode`, `:content_type`,
  `:metadata`) mirror `write_file/4`.

  Errors:

    * `{:error, {:missing_chunk, hash}}` — no reported location answered
      `has_chunk` for that hash.
    * `{:error, {:unknown_chunk_location, hash}}` — a hash in
      `chunk_hashes` has no entry in `opts[:locations]`.
    * Any other error from the lock / authorisation / index layer.
  """
  @spec commit_chunks(String.t(), String.t(), [binary()], keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, term()}
  def commit_chunks(volume_name, path, chunk_hashes, opts \\ []) do
    with :ok <- ensure_writable(),
         {:ok, volume} <- resolve_volume(volume_name) do
      CommitChunks.commit(volume.id, normalize_path(path), chunk_hashes, opts)
    end
  end

  @doc """
  Pins a file by identity so an open handle survives rename and unlink


  Resolves `path` to a `file_id` and takes a `:pinned` namespace claim
  keyed by `{volume_id, file_id}` rather than by path, so:

    * renaming the file does not strand the pin on the old name, and
    * unlinking the file — under whichever name it currently has —
      sees the pin and tombstones the `FileMeta` instead of hard-
      deleting its chunks.

  `holder` is the pid whose death releases the pin (the coordinator's
  holder-DOWN bulk release is the crash safety net). Interface nodes
  calling over RPC must pass a long-lived pid on their own node — the
  RPC handler process dies the moment the call returns.

  Returns the resolved `file_id` alongside the claim id; callers hold
  both for the life of the handle and pass the claim id to
  `unpin_file/1` on close.
  """
  @spec pin_file(String.t(), String.t(), pid()) ::
          {:ok, %{file_id: binary(), claim_id: String.t(), file: FileMeta.t()}}
          | {:error, term()}
  def pin_file(volume_name, path, holder \\ self()) when is_pid(holder) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      normalized = normalize_path(path)

      with_namespace_claim(:path, :shared, volume.id, normalized, fn ->
        acquire_pin(volume.id, normalized, holder)
      end)
    end
  end

  @doc """
  Releases a pin taken by `pin_file/3`. Idempotent — releasing an
  unknown or already-released claim id is `:ok`.
  """
  @spec unpin_file(String.t()) :: :ok
  def unpin_file(claim_id) when is_binary(claim_id) do
    safe_release(claim_id)
    :ok
  end

  @doc """
  Deletes a file or directory from a volume by path.

  Acquires a `NeonFS.Core.NamespaceCoordinator` subtree claim on the
  target so concurrent `mkdir` / `delete_file` / `rename_file` on the
  same path (or any descendant — important for the rmdir
  empty-directory check, which would otherwise race against creates
  inside the target) serialise across interface nodes. See sub-issue


  A file held open anywhere in the cluster (a `:pinned` claim on its
  identity, see `pin_file/3`) is tombstoned rather than hard-deleted.
  When the pin state cannot be established — the coordinator is
  unreachable or its Ra query fails — the delete fails with a
  `class: :unavailable` error rather than assuming "no pins" and
  discarding a live handle's chunks.

  Honours `:uid` / `:gids` opts for `:write` authorisation (default
  uid 0 bypasses), so an NFS REMOVE/RMDIR is held to the volume ACL.
  """
  @spec delete_file(String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def delete_file(volume_name, path, opts \\ []) do
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, volume} <- resolve_volume(volume_name),
         :ok <-
           authorise_posix(
             uid,
             gids,
             :write,
             volume.id,
             {:create, volume.id, normalize_path(path)}
           ) do
      normalized = normalize_path(path)
      do_delete_dispatch(volume.id, normalized)
    end
  end

  # Volume-wide grants (POSIX-shaped VolumeACL) gate the volume; the
  # per-object/per-parent-dir POSIX `resource` governs the specific target.
  # uid 0 bypasses both inside `Authorise.check/4`.
  defp authorise_posix(uid, gids, action, volume_id, resource) do
    with :ok <- Authorise.check(uid, gids, action, {:volume, volume_id}) do
      Authorise.check(uid, gids, action, resource)
    end
  end

  # Files take a `:shared :path` claim, directories the historical
  # `:exclusive :subtree` one. Pins no longer live on the path key
  # (they are keyed by file identity), so the shared
  # scope is what keeps an unlink compatible with the other shared
  # path holders — WebDAV shared locks, FUSE `LOCK_SH` flocks —
  # exactly as POSIX expects. Concurrent renames / mkdir / rmdir
  # still serialise because they hold `:exclusive :*`.
  #
  # Missing paths short-circuit to `:not_found` without acquiring any
  # claim, so a repeat delete of a detached file is `:not_found`
  # rather than a claim round-trip.
  #
  # `peek_path_type/2` only picks which claim to take; the delete
  # itself re-resolves the path *inside* the claim and acts on that
  # single resolution. If a concurrent rename swapped a file for a
  # directory (or vice versa) while we waited for the claim, the
  # resolution disagrees with the claim we hold — drop it and
  # re-dispatch rather than deleting a directory down the file path.
  @delete_dispatch_attempts 2

  defp do_delete_dispatch(volume_id, path, attempts \\ @delete_dispatch_attempts)

  defp do_delete_dispatch(volume_id, path, attempts) when attempts > 0 do
    case dispatch_delete_once(volume_id, path) do
      :retry -> do_delete_dispatch(volume_id, path, attempts - 1)
      result -> result
    end
  end

  defp do_delete_dispatch(_volume_id, _path, _attempts) do
    {:error, Conflict.from_reason(:busy)}
  end

  defp dispatch_delete_once(volume_id, path) do
    case peek_path_type(volume_id, path) do
      :file ->
        with_namespace_claim(:path, :shared, volume_id, path, fn ->
          delete_file_under_claim(volume_id, path)
        end)

      :dir ->
        with_namespace_claim(:subtree, volume_id, path, fn ->
          delete_dir_under_claim(volume_id, path)
        end)

      :not_found ->
        {:error, FileNotFound.exception(file_path: path, volume_id: volume_id)}
    end
  end

  defp delete_file_under_claim(volume_id, path) do
    case lookup_file(volume_id, path) do
      {:ok, %FileMeta{} = file} -> delete_resolved_file(volume_id, file)
      {:error, _} = err -> err
    end
  end

  defp delete_resolved_file(volume_id, file) do
    if directory?(file) do
      :retry
    else
      do_delete_file(volume_id, file)
    end
  end

  defp delete_dir_under_claim(volume_id, path) do
    case peek_path_type(volume_id, path) do
      :dir -> do_delete(volume_id, path)
      :file -> :retry
      :not_found -> {:error, FileNotFound.exception(file_path: path, volume_id: volume_id)}
    end
  end

  # The pin query and the delete it decides must be atomic against a
  # concurrent `pin_file/3` — otherwise a pin taken between the two
  # is invisible to the tombstone snapshot and its file is hard-
  # deleted under the open handle. Both sides serialise on an
  # `:exclusive` claim over the identity's pin-lock key.
  defp do_delete_file(volume_id, %FileMeta{} = file) do
    with_pin_lock(volume_id, file.id, fn -> delete_by_pin_state(volume_id, file) end)
  end

  defp delete_by_pin_state(volume_id, %FileMeta{} = file) do
    with {:ok, pin_ids} <- pinned_claim_ids(volume_id, file.id),
         :ok <- apply_delete(file, pin_ids) do
      release_file_usage(file)
      :ok
    end
  end

  # Frees the unlinked file's logical bytes from the volume counter.
  # Unlink-while-open (mark_detached) frees the accounting too:
  # the reconcile excludes detached tombstones, so the incremental path
  # must match. Best-effort — the file is already gone, so a counter
  # glitch (including a VolumeRegistry call timeout, which exits) must
  # not fail the delete.
  defp release_file_usage(%FileMeta{volume_id: volume_id, size: size} = file) do
    _ =
      VolumeRegistry.adjust_stats(volume_id,
        logical_size: -size,
        chunk_count: -referenced_chunk_count(file),
        file_count: -1
      )

    :ok
  catch
    :exit, _ -> :ok
  end

  # Where a file's chunks are recorded depends on its durability. A
  # replicated file lists them in `chunks`; an erasure-coded one leaves
  # that empty and reaches them through `stripes` → `StripeIndex`, which is
  # the same walk `GarbageCollector` does to decide what a file still
  # references. Counting `chunks` alone charged an erasure write for its
  # data *and* parity and then freed none of it on delete.
  defp referenced_chunk_count(%FileMeta{chunks: chunks} = file) do
    length(chunks) + stripe_chunk_count(file)
  end

  defp stripe_chunk_count(%FileMeta{stripes: stripes, volume_id: volume_id})
       when is_list(stripes) do
    Enum.reduce(stripes, 0, fn %{stripe_id: stripe_id}, acc ->
      case StripeIndex.get(volume_id, stripe_id) do
        {:ok, stripe} -> acc + length(stripe.chunks)
        {:error, :not_found} -> acc
      end
    end)
  end

  defp stripe_chunk_count(_file), do: 0

  defp apply_delete(file, []) do
    FileIndex.delete(file.id)
  end

  defp apply_delete(file, [_ | _] = pin_ids) do
    case FileIndex.mark_detached(file.id, pin_ids) do
      {:ok, _detached} -> :ok
      {:error, _} = err -> err
    end
  end

  # Directories carry no FileMeta, so the by-id `FileIndex.delete/1`
  # path can't remove them — `rmdir/2` works by path instead.
  defp do_delete(volume_id, path) do
    FileIndex.rmdir(volume_id, path)
  end

  defp peek_path_type(volume_id, path) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, %FileMeta{} = file} ->
        if directory?(file), do: :dir, else: :file

      _ ->
        :not_found
    end
  end

  defp directory?(%FileMeta{mode: mode}), do: (mode &&& 0o040000) == 0o040000

  # An unreachable coordinator is *not* "no pins" — it is an unknown
  # pin state, and hard-deleting on an unknown pin state is how an
  # open handle loses its chunks. Surface it so the caller retries or
  # fails the unlink.
  defp pinned_claim_ids(volume_id, file_id) do
    case NamespaceCoordinator.consistent_claims_for_path(pin_key(volume_id, file_id)) do
      {:ok, claims} -> {:ok, Enum.map(claims, &elem(&1, 0))}
      {:error, reason} -> {:error, reason}
    end
  catch
    :exit, _ -> {:error, Unavailable.from_reason(:coordinator_unavailable)}
  end

  # Runs `fun` holding the identity's pin-lock claim, the mutual
  # exclusion between taking a pin and acting on the pin set. The
  # lock key is disjoint from both the path key and the pin key, so
  # holding it never conflicts with the pins it guards, with a
  # rename, or with an advisory lock on the same file.
  defp with_pin_lock(volume_id, file_id, fun) do
    case safe_claim(:path, :exclusive, pin_lock_key(volume_id, file_id)) do
      {:ok, claim_id} ->
        try do
          fun.()
        after
          safe_release(claim_id)
        end

      {:error, %Conflict{}} ->
        {:error, Conflict.from_reason(:busy)}

      {:error, _reason} = err ->
        err
    end
  end

  # Resolution and pin acquisition are separated by a coordinator
  # round-trip, so a delete can complete in between. The pin lock
  # keeps the delete's "query pins then act" indivisible, and the
  # post-claim re-read catches the case where the delete finished
  # before we took the lock: the pin is dropped and the open fails
  # ENOENT, which is what POSIX permits for an open racing an unlink.
  defp acquire_pin(volume_id, path, holder) do
    with {:ok, file} <- lookup_file(volume_id, path) do
      with_pin_lock(volume_id, file.id, fn -> claim_pin(volume_id, file, holder) end)
    end
  end

  defp claim_pin(volume_id, %FileMeta{} = file, holder) do
    with {:ok, claim_id} <- safe_claim_pinned(pin_key(volume_id, file.id), holder),
         :ok <- ensure_indexed(volume_id, file, claim_id) do
      {:ok, %{file_id: file.id, claim_id: claim_id, file: file}}
    end
  end

  defp ensure_indexed(volume_id, %FileMeta{id: file_id, path: path}, claim_id) do
    case FileIndex.get(volume_id, file_id) do
      {:ok, %FileMeta{}} ->
        :ok

      _ ->
        safe_release(claim_id)
        {:error, FileNotFound.exception(file_path: path, volume_id: volume_id)}
    end
  end

  @doc """
  Gets file metadata by volume name and path.

  ## Options

    * `:uid` - Caller UID for authorisation (default: 0, root, which
      bypasses all checks)
    * `:gids` - Caller group IDs for authorisation (default: `[]`)

  Runs `Authorise.check/4` for `:read` against the volume so callers
  presenting a non-root identity (NFS AUTH_SYS) are held to the volume
  ACL.
  """
  @spec get_file_meta(String.t(), String.t(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, FileNotFound.t() | term()}
  def get_file_meta(volume_name, path, opts \\ []) do
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, volume} <- resolve_volume(volume_name),
         :ok <-
           authorise_posix(uid, gids, :read, volume.id, {:file, volume.id, normalize_path(path)}) do
      lookup_file(volume.id, normalize_path(path))
    end
  end

  @doc """
  Updates file metadata fields by volume name and path.

  Accepts a keyword list of fields to update on the FileMeta struct.
  Automatically increments the version and updates timestamps.

  Honours `:uid` / `:gids` opts for `:write` authorisation (default
  uid 0 bypasses), so NFS SETATTR is held to the file's POSIX mode.
  """
  @spec update_file_meta(String.t(), String.t(), keyword(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, FileNotFound.t() | term()}
  def update_file_meta(volume_name, path, updates, opts \\ []) do
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, volume} <- resolve_volume(volume_name),
         :ok <-
           authorise_posix(uid, gids, :write, volume.id, {:file, volume.id, normalize_path(path)}),
         {:ok, file} <- lookup_file(volume.id, normalize_path(path)) do
      apply_meta_updates(volume.id, normalize_path(path), file, updates)
    end
  end

  # A directory is a `dir:` record, not a `FileMeta`, so `FileIndex.update/2`
  # cannot reach it — its `file_id` resolves through the by-id file cache a
  # `dir:` record was never in, and the update reports `:not_found`. `dir:`
  # records are path-keyed, and the path is in hand here, so this is the one
  # place the branch can live. Interfaces keep calling `update_file_meta/4`
  # and never need to know which record type they are touching.
  #
  # Note the by-id counterpart cannot do this: nothing maps a directory id
  # back to its path, so `update_file_meta_by_id/4` still refuses
  # directories.
  defp apply_meta_updates(volume_id, path, %FileMeta{} = file, updates) do
    if directory?(file) do
      FileIndex.set_dir_attrs(volume_id, path, updates)
    else
      FileIndex.update(file.id, updates)
    end
  end

  @doc """
  Truncates a file to `new_size` and optionally applies additional
  metadata updates in the same write. Trims chunks / stripes when
  shrinking; sparse-extends when growing (no zero-filled chunks
  allocated). See `NeonFS.Core.FileIndex.truncate/3`.

  Used by NFSv3 SETATTR when the `size` field is set —
  combining truncate with mode/uid/gid/atime/mtime updates lets the
  whole sattr3 mutation land in a single FileIndex write.

  Honours `:uid` / `:gids` opts for `:write` authorisation (default
  uid 0 bypasses), so an NFS SETATTR that sets `size` is held to the
  file's POSIX mode just like the no-size SETATTR path.
  """
  @spec truncate_file(String.t(), String.t(), non_neg_integer(), keyword(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, FileNotFound.t() | term()}
  def truncate_file(volume_name, path, new_size, additional_updates \\ [], opts \\ []) do
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, volume} <- resolve_volume(volume_name),
         :ok <-
           authorise_posix(uid, gids, :write, volume.id, {:file, volume.id, normalize_path(path)}),
         {:ok, file} <- lookup_file(volume.id, normalize_path(path)),
         {:ok, updated} <- FileIndex.truncate(file.id, new_size, additional_updates) do
      adjust_logical_usage(volume.id, new_size - file.size)
      {:ok, updated}
    end
  end

  @doc """
  `file_id`-keyed counterpart to `get_file_meta/3`.

  Serves `stat` on an open handle without re-resolving its path — the
  path may have been renamed or unlinked since the handle was opened,
  and a `:detached` file has no path at all.

  Returns `{:error, :wrong_volume}` when the id resolves into a
  different volume than `volume_name`.
  """
  @spec get_file_meta_by_id(String.t(), binary(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, FileNotFound.t() | term()}
  def get_file_meta_by_id(volume_name, file_id, opts \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, file} <- lookup_file_by_id(volume.id, file_id),
         :ok <- authorise_file(opts, :read, volume.id, file) do
      {:ok, file}
    end
  end

  @doc """
  `file_id`-keyed counterpart to `update_file_meta/4` — `fchmod`,
  `fchown` and `futimens` on an open handle.
  """
  @spec update_file_meta_by_id(String.t(), binary(), keyword(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, FileNotFound.t() | term()}
  def update_file_meta_by_id(volume_name, file_id, updates, opts \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, file} <- lookup_file_by_id(volume.id, file_id),
         :ok <- authorise_file(opts, :write, volume.id, file) do
      FileIndex.update(file.id, updates)
    end
  end

  @doc """
  `file_id`-keyed counterpart to `truncate_file/5` — `ftruncate` on an
  open handle.
  """
  @spec truncate_file_by_id(String.t(), binary(), non_neg_integer(), keyword(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, FileNotFound.t() | term()}
  def truncate_file_by_id(volume_name, file_id, new_size, additional_updates \\ [], opts \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, file} <- lookup_file_by_id(volume.id, file_id),
         :ok <- authorise_file(opts, :write, volume.id, file),
         {:ok, updated} <- FileIndex.truncate(file.id, new_size, additional_updates) do
      adjust_logical_usage(volume.id, new_size - file.size)
      {:ok, updated}
    end
  end

  # By-ID callers authorise against the resolved record's own path, so
  # the check lands on the same POSIX resource its path-based sibling
  # would use. Resolution therefore has to happen first — the reverse
  # of the path-based order.
  defp authorise_file(opts, action, volume_id, %FileMeta{path: path}) do
    authorise_posix(
      Keyword.get(opts, :uid, 0),
      Keyword.get(opts, :gids, []),
      action,
      volume_id,
      {:file, volume_id, path}
    )
  end

  defp lookup_file_by_id(volume_id, file_id) do
    case FileIndex.get_in_volume(volume_id, file_id) do
      {:ok, file} ->
        {:ok, file}

      {:error, :wrong_volume} ->
        {:error, :wrong_volume}

      {:error, :not_found} ->
        {:error, FileNotFound.exception(file_path: "<id:#{file_id}>", volume_id: volume_id)}
    end
  end

  # Accounts a truncation's logical-byte delta against the volume counter;
  # the delta is negative for a shrink, positive for a sparse
  # grow. Best-effort — the metadata change is already committed.
  defp adjust_logical_usage(_volume_id, 0), do: :ok

  defp adjust_logical_usage(volume_id, delta) do
    _ = VolumeRegistry.adjust_stats(volume_id, logical_size: delta)
    :ok
  catch
    :exit, _ -> :ok
  end

  @doc """
  Lists all descendant files under a directory prefix within a volume.

  Returns all `FileMeta` records whose paths start with `dir_path`,
  at any depth. Does not include directory entries. For direct children
  only (including synthesised directory entries), use `list_dir/2`.
  """
  @spec list_files_recursive(String.t(), String.t()) ::
          {:ok, [NeonFS.Core.FileMeta.t()]} | {:error, term()}
  def list_files_recursive(volume_name, dir_path) do
    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, files} <- FileIndex.list_volume_authoritative(volume.id) do
      normalized = normalize_path(dir_path)

      # Pure prefix match — used for S3 `ListObjects`, whose `prefix` is a
      # string prefix, not a directory. A prefix that *exactly* equals an
      # object key must return that object, so don't exclude `== normalized`:
      # the old exclusion silently dropped a file whose path equalled the
      # prefix (e.g. `ls s3://bucket/exact-file.txt` returned nothing). No
      # directory is ever in this file list, so the only thing the exclusion
      # ever dropped was that exact-key file.
      filtered = Enum.filter(files, &String.starts_with?(&1.path, normalized))

      {:ok, filtered}
    end
  end

  @doc """
  Lists the direct children of a directory within a volume.

  Returns `FileMeta` structs for each child entry. Directory children
  are synthesised as `FileMeta` structs with `mode` including the
  S_IFDIR bit (`0o040000`), making them distinguishable from files.
  """
  @spec list_dir(String.t(), String.t()) ::
          {:ok, [NeonFS.Core.FileMeta.t()]} | {:error, term()}
  def list_dir(volume_name, dir_path) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      case FileIndex.list_dir_full(volume.id, normalize_path(dir_path)) do
        {:ok, entries} ->
          {:ok, Enum.map(entries, fn {_name, _path, attrs} -> attrs end)}

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  @doc """
  Creates a directory within a volume.

  Acquires a `NeonFS.Core.NamespaceCoordinator` path claim on the new
  directory's path before inserting into `FileIndex`, so concurrent
  `mkdir` / `delete_file` / `rename_file` on the same name (from
  different interface nodes) serialise cleanly — one `mkdir` wins, the
  rest see `FileIndex` already holds the entry and surface `:eexist`,
  rather than racing through quorum-write resolution.

  Honours `:uid` / `:gids` opts for `:write` authorisation (default
  uid 0 bypasses), so an NFS MKDIR is held to the volume ACL.
  """
  @spec mkdir(String.t(), String.t(), keyword()) ::
          {:ok, NeonFS.Core.DirectoryEntry.t()} | {:error, term()}
  def mkdir(volume_name, path, opts \\ []) do
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, volume} <- resolve_volume(volume_name),
         :ok <-
           authorise_posix(
             uid,
             gids,
             :write,
             volume.id,
             {:create, volume.id, normalize_path(path)}
           ) do
      normalized = normalize_path(path)

      # The new directory is owned by the creating client (POSIX), so the
      # client can populate it — without this it defaulted to uid 0 and an
      # NFS client couldn't write in a directory it had just created.
      with_namespace_claim(:path, volume.id, normalized, fn ->
        FileIndex.mkdir(volume.id, normalized, dir_create_opts(uid, gids, opts))
      end)
    end
  end

  defp dir_create_opts(uid, gids, opts) do
    base = [uid: uid, gid: List.first(gids) || 0]

    case Keyword.fetch(opts, :mode) do
      {:ok, mode} -> [{:mode, mode} | base]
      :error -> base
    end
  end

  @doc """
  Renames or moves a file/directory within a volume.

  Handles same-directory renames, cross-directory moves, and combined
  move-and-rename operations.

  Honours `:uid` / `:gids` opts for `:write` authorisation (default
  uid 0 bypasses). A rename adds a name in the destination directory and
  removes one from the source, so it requires write on both parents'
  POSIX modes.
  """
  @spec rename_file(String.t(), String.t(), String.t(), keyword()) :: :ok | {:error, term()}
  def rename_file(volume_name, src_path, dest_path, opts \\ []) do
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])
    src = normalize_path(src_path)
    dst = normalize_path(dest_path)

    with {:ok, volume} <- resolve_volume(volume_name),
         :ok <- authorise_posix(uid, gids, :write, volume.id, {:create, volume.id, src}),
         :ok <- authorise_posix(uid, gids, :write, volume.id, {:create, volume.id, dst}) do
      with_rename_claim(volume.id, src, dst, fn -> do_rename(volume.id, src, dst) end)
    end
  end

  # --- Private helpers ---

  # Wraps a single-path namespace operation in a coordinator claim so
  # concurrent operations on the same path (across interface nodes)
  # serialise cleanly. `claim_kind` is `:path` for point operations
  # (`mkdir`) and `:subtree` for ones that must fence concurrent
  # creations under the target (`rmdir` / directory `delete_file`).
  # Releases on completion (success or failure). When the coordinator
  # is unreachable (no Ra cluster, network split) we fall back to the
  # historical single-core-node serialisation — same posture WebDAV
  # took, and rename.
  defp with_namespace_claim(claim_kind, volume_id, path, fun)
       when claim_kind in [:path, :subtree] do
    with_namespace_claim(claim_kind, :exclusive, volume_id, path, fun)
  end

  # Variant taking an explicit `scope`. The file delete path uses
  # `:shared :path` so it coexists with `:pinned` claims (open file
  # handles) — the unlink-while-open story treats a
  # delete on a pinned file as a tombstone-mark rather than a
  # blocking conflict. Concurrent renames / mkdir / rmdir keep
  # serialising because they hold `:exclusive :*`, which still
  # conflicts with `:shared :path` on the same path.
  defp with_namespace_claim(claim_kind, scope, volume_id, path, fun)
       when claim_kind in [:path, :subtree] and scope in [:exclusive, :shared] do
    key = volume_scoped_path(volume_id, path)

    case safe_claim(claim_kind, scope, key) do
      {:ok, claim_id} ->
        try do
          fun.()
        after
          safe_release(claim_id)
        end

      {:error, %Conflict{}} ->
        {:error, Conflict.from_reason(:busy)}

      {:error, _reason} ->
        fun.()
    end
  end

  # Wraps a rename's `FileIndex` work in a coordinator-issued
  # `claim_rename` pair so concurrent cross-directory renames (across
  # interface nodes — WebDAV, NFS, FUSE) serialise cleanly. Claim is
  # always released, whether the inner work succeeds or errors. See

  defp with_rename_claim(volume_id, src, dst, fun) do
    src_key = volume_scoped_path(volume_id, src)
    dst_key = volume_scoped_path(volume_id, dst)

    case safe_claim_rename(src_key, dst_key) do
      {:ok, claim} ->
        try do
          fun.()
        after
          safe_release_rename(claim)
        end

      {:error, %Invalid{}} = err ->
        err

      {:error, %Conflict{}} = err ->
        err

      {:error, _reason} ->
        # Coordinator unavailable (no Ra cluster, network split, etc.).
        # Fall back to the historical single-core-node serialisation
        # property. Cross-node correctness regresses to "best-effort"
        # while the coordinator is down — same posture WebDAV took in

        fun.()
    end
  end

  defp safe_claim(:path, scope, key) do
    NamespaceCoordinator.claim_path(key, scope)
  catch
    :exit, _ -> {:error, Unavailable.from_reason(:coordinator_unavailable)}
  end

  defp safe_claim(:subtree, scope, key) do
    NamespaceCoordinator.claim_subtree(key, scope)
  catch
    :exit, _ -> {:error, Unavailable.from_reason(:coordinator_unavailable)}
  end

  defp safe_claim_pinned(key, holder) do
    NamespaceCoordinator.claim_pinned_for(NamespaceCoordinator, key, holder)
  catch
    :exit, _ -> {:error, Unavailable.from_reason(:coordinator_unavailable)}
  end

  defp safe_claim_rename(src_key, dst_key) do
    NamespaceCoordinator.claim_rename(src_key, dst_key)
  catch
    :exit, _ -> {:error, Unavailable.from_reason(:coordinator_unavailable)}
  end

  defp safe_release(claim_id) do
    NamespaceCoordinator.release(claim_id)
  catch
    :exit, _ -> :ok
  end

  defp safe_release_rename(claim) do
    NamespaceCoordinator.release_rename(claim)
  catch
    :exit, _ -> :ok
  end

  defp volume_scoped_path(volume_id, path) when is_binary(volume_id) and is_binary(path) do
    "vol:" <> volume_id <> ":" <> path
  end

  # Pins and their lock live in key namespaces of their own so a
  # rename — which claims both path keys exclusively — never
  # conflicts with a pinned handle on the file it is renaming.
  defp pin_key(volume_id, file_id) when is_binary(volume_id) and is_binary(file_id) do
    "vol:" <> volume_id <> ":id:" <> file_id
  end

  defp pin_lock_key(volume_id, file_id) when is_binary(volume_id) and is_binary(file_id) do
    "vol:" <> volume_id <> ":pinlock:" <> file_id
  end

  defp do_rename(volume_id, src_path, dest_path) do
    src_dir = Path.dirname(src_path)
    src_name = Path.basename(src_path)
    dest_dir = Path.dirname(dest_path)
    dest_name = Path.basename(dest_path)

    cond do
      src_dir == dest_dir ->
        FileIndex.rename(volume_id, src_dir, src_name, dest_name)

      src_name == dest_name ->
        FileIndex.move(volume_id, src_dir, dest_dir, src_name)

      # Both the directory and the basename change. Published as one
      # transition: a `move` followed by a `rename` would leave the file in
      # the destination directory under its old basename between the two, and
      # a concurrent reader can observe that intermediate path.
      true ->
        FileIndex.move_rename(volume_id, src_dir, dest_dir, src_name, dest_name)
    end
  end

  defp resolve_volume(volume_name) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, VolumeNotFound.exception(volume_name: volume_name)}
    end
  end

  # Rejects new client writes while the cluster is `:frozen` so
  # in-flight writes can settle before a planned power-down. Gates the
  # external write RPCs only — internal operations (repair, rebalance,
  # DR restore) call the `WriteOperation` / `CommitChunks` modules
  # directly, not this facade, so they are unaffected.
  defp ensure_writable do
    if ClusterMode.frozen?(), do: {:error, :cluster_frozen}, else: :ok
  end

  defp lookup_file(volume_id, path) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, meta} ->
        {:ok, meta}

      {:error, :not_found} ->
        {:error, FileNotFound.exception(file_path: path, volume_id: volume_id)}
    end
  end

  defp normalize_path("/" <> _ = path), do: path
  defp normalize_path(path), do: "/" <> path
end
