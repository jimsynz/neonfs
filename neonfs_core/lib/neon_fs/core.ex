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
  Whether the cluster is currently `:frozen` (#1378) — a coordinated
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
  pays (#1046). Reads the authoritative `root_entry.drive_locations`
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
  FUSE) that hold the id and issue writes through id-keyed APIs (#1087).
  """
  @spec volume_root_nodes_by_id(String.t()) :: {:ok, [node()]} | {:error, term()}
  def volume_root_nodes_by_id(volume_id) when is_binary(volume_id) do
    with {:ok, entry} <- fetch_volume_root(volume_id) do
      {:ok, entry.drive_locations |> Enum.map(& &1.node) |> Enum.uniq()}
    end
  end

  # The replica nodes are the same across a volume's shards at provision
  # time; shard 0 always exists, so it answers "which nodes hold this
  # volume's metadata" (#1307).
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
  volumes until streaming erasure encoding lands (see #195).
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
  story (#638 / #644) keeps chunks reachable by `file_id` until the
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
  that may have been detached by another peer (#638). Does not
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
  Deletes a file or directory from a volume by path.

  Acquires a `NeonFS.Core.NamespaceCoordinator` subtree claim on the
  target so concurrent `mkdir` / `delete_file` / `rename_file` on the
  same path (or any descendant — important for the rmdir
  empty-directory check, which would otherwise race against creates
  inside the target) serialise across interface nodes. See sub-issue
  #305.

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
  # uid 0 bypasses both inside `Authorise.check/4` (#1339).
  defp authorise_posix(uid, gids, action, volume_id, resource) do
    with :ok <- Authorise.check(uid, gids, action, {:volume, volume_id}) do
      Authorise.check(uid, gids, action, resource)
    end
  end

  # Files take a `:shared :path` claim so an open handle's `:pinned`
  # claim doesn't block delete — the file unlinks but keeps its
  # chunks reachable by `file_id` until the last pin releases (POSIX
  # unlink-while-open, #643 of #638). Concurrent renames / mkdir /
  # rmdir on the same path still serialise because they hold
  # `:exclusive :*`. Directories keep the historical
  # `:exclusive :subtree` claim — no pin support for dirs.
  #
  # Missing paths short-circuit to `:not_found` without acquiring any
  # claim. That keeps the historical surface intact and, importantly,
  # doesn't block on a stranded `:pinned` claim left over from a
  # detached file at the same path (which would otherwise turn a
  # repeat delete into a `:busy`).
  defp do_delete_dispatch(volume_id, path) do
    case peek_path_type(volume_id, path) do
      :file ->
        with_namespace_claim(:path, :shared, volume_id, path, fn ->
          do_delete_file(volume_id, path)
        end)

      :dir ->
        with_namespace_claim(:subtree, volume_id, path, fn ->
          do_delete(volume_id, path)
        end)

      :not_found ->
        {:error, FileNotFound.exception(file_path: path, volume_id: volume_id)}
    end
  end

  defp do_delete_file(volume_id, path) do
    with {:ok, file} <- lookup_file(volume_id, path) do
      delete_file_by_pin_state(file, pinned_claim_ids(volume_id, path))
    end
  end

  defp delete_file_by_pin_state(file, []) do
    FileIndex.delete(file.id)
  end

  defp delete_file_by_pin_state(file, [_ | _] = pin_ids) do
    case FileIndex.mark_detached(file.id, pin_ids) do
      {:ok, _detached} -> :ok
      {:error, _} = err -> err
    end
  end

  defp do_delete(volume_id, path) do
    with {:ok, file} <- FileIndex.get_by_path(volume_id, path) do
      FileIndex.delete(file.id)
    end
  end

  defp peek_path_type(volume_id, path) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, %FileMeta{mode: mode}} ->
        if (mode &&& 0o040000) == 0o040000, do: :dir, else: :file

      _ ->
        :not_found
    end
  end

  defp pinned_claim_ids(volume_id, path) do
    key = volume_scoped_path(volume_id, path)

    case NamespaceCoordinator.claims_for_path(key) do
      {:ok, claims} -> Enum.map(claims, &elem(&1, 0))
      _ -> []
    end
  catch
    :exit, _ -> []
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
  uid 0 bypasses), so NFS SETATTR is held to the file's POSIX mode (#1339).
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
      FileIndex.update(file.id, updates)
    end
  end

  @doc """
  Truncates a file to `new_size` and optionally applies additional
  metadata updates in the same write. Trims chunks / stripes when
  shrinking; sparse-extends when growing (no zero-filled chunks
  allocated). See `NeonFS.Core.FileIndex.truncate/3`.

  Used by NFSv3 SETATTR (#621) when the `size` field is set —
  combining truncate with mode/uid/gid/atime/mtime updates lets the
  whole sattr3 mutation land in a single FileIndex write.

  Honours `:uid` / `:gids` opts for `:write` authorisation (default
  uid 0 bypasses), so an NFS SETATTR that sets `size` is held to the
  file's POSIX mode just like the no-size SETATTR path (#1339).
  """
  @spec truncate_file(String.t(), String.t(), non_neg_integer(), keyword(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, FileNotFound.t() | term()}
  def truncate_file(volume_name, path, new_size, additional_updates \\ [], opts \\ []) do
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, volume} <- resolve_volume(volume_name),
         :ok <-
           authorise_posix(uid, gids, :write, volume.id, {:file, volume.id, normalize_path(path)}),
         {:ok, file} <- lookup_file(volume.id, normalize_path(path)) do
      FileIndex.truncate(file.id, new_size, additional_updates)
    end
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
      # ever dropped was that exact-key file (#1034).
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
  rather than racing through quorum-write resolution. Sub-issue #305.

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
      # NFS client couldn't write in a directory it had just created (#1339).
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
  POSIX modes (#1339).
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
  # took in #301 and rename in #304.
  defp with_namespace_claim(claim_kind, volume_id, path, fun)
       when claim_kind in [:path, :subtree] do
    with_namespace_claim(claim_kind, :exclusive, volume_id, path, fun)
  end

  # Variant taking an explicit `scope`. The file delete path uses
  # `:shared :path` so it coexists with `:pinned` claims (open file
  # handles) — the unlink-while-open story (#643 of #638) treats a
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
  # sub-issue #304.
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
        # sub-issue #301.
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

      true ->
        with :ok <- FileIndex.move(volume_id, src_dir, dest_dir, src_name) do
          FileIndex.rename(volume_id, dest_dir, src_name, dest_name)
        end
    end
  end

  defp resolve_volume(volume_name) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, VolumeNotFound.exception(volume_name: volume_name)}
    end
  end

  # Rejects new client writes while the cluster is `:frozen` (#1378) so
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
