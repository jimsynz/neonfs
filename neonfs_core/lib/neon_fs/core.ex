defmodule NeonFS.Core do
  @moduledoc """
  Public RPC facade for NeonFS core operations.

  Non-core nodes (S3, WebDAV, FUSE, NFS) call functions in this module via
  `NeonFS.Client.Router`, which dispatches `:rpc.call/5` to a core node.
  Each function resolves volume names to IDs and delegates to the
  appropriate internal module.
  """

  alias NeonFS.Core.CommitChunks
  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.FileMeta
  alias NeonFS.Core.NamespaceCoordinator
  alias NeonFS.Core.ReadOperation
  alias NeonFS.Core.S3CredentialManager
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Core.WriteOperation

  import Bitwise, only: [&&&: 2]

  # --- Credential operations ---

  @doc """
  Looks up an S3 credential by access key ID.

  Called by the S3 backend during SigV4 authentication.
  """
  @spec lookup_s3_credential(String.t()) :: {:ok, map()} | {:error, :not_found}
  def lookup_s3_credential(access_key_id) do
    case S3CredentialManager.lookup(access_key_id) do
      {:ok, credential} ->
        {:ok, %{secret_access_key: credential.secret_access_key, identity: credential.identity}}

      {:error, :not_found} ->
        {:error, :not_found}
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
  @spec get_volume(String.t()) :: {:ok, NeonFS.Core.Volume.t()} | {:error, :not_found}
  def get_volume(name) do
    VolumeRegistry.get_by_name(name)
  end

  @doc """
  Gets a volume by its UUID id.

  Counterpart to `get_volume/1` for callers (e.g. interface nodes
  resolving a filehandle that embeds the volume's UUID rather than
  its name) that hold a stable id but not the current name.
  """
  @spec get_volume_by_id(String.t()) :: {:ok, NeonFS.Core.Volume.t()} | {:error, :not_found}
  def get_volume_by_id(id) do
    VolumeRegistry.get(id)
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
    with {:ok, volume} <- VolumeRegistry.get_by_name(name) do
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
    with {:ok, volume} <- resolve_volume(volume_name) do
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
    with {:ok, volume} <- resolve_volume(volume_name) do
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
    with {:ok, volume} <- resolve_volume(volume_name) do
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
    with {:ok, volume} <- resolve_volume(volume_name) do
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
  """
  @spec delete_file(String.t(), String.t()) :: :ok | {:error, term()}
  def delete_file(volume_name, path) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      normalized = normalize_path(path)
      do_delete_dispatch(volume.id, normalized)
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
        {:error, :not_found}
    end
  end

  defp do_delete_file(volume_id, path) do
    with {:ok, file} <- FileIndex.get_by_path(volume_id, path) do
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
  """
  @spec get_file_meta(String.t(), String.t()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, :not_found | term()}
  def get_file_meta(volume_name, path) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      FileIndex.get_by_path(volume.id, normalize_path(path))
    end
  end

  @doc """
  Updates file metadata fields by volume name and path.

  Accepts a keyword list of fields to update on the FileMeta struct.
  Automatically increments the version and updates timestamps.
  """
  @spec update_file_meta(String.t(), String.t(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, :not_found | term()}
  def update_file_meta(volume_name, path, updates) do
    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, file} <- FileIndex.get_by_path(volume.id, normalize_path(path)) do
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
  """
  @spec truncate_file(String.t(), String.t(), non_neg_integer(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, :not_found | term()}
  def truncate_file(volume_name, path, new_size, additional_updates \\ []) do
    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, file} <- FileIndex.get_by_path(volume.id, normalize_path(path)) do
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
    with {:ok, volume} <- resolve_volume(volume_name) do
      normalized = normalize_path(dir_path)
      files = FileIndex.list_volume(volume.id)

      filtered =
        Enum.filter(files, fn file ->
          file.path != normalized and String.starts_with?(file.path, normalized)
        end)

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
  """
  @spec mkdir(String.t(), String.t()) ::
          {:ok, NeonFS.Core.DirectoryEntry.t()} | {:error, term()}
  def mkdir(volume_name, path) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      normalized = normalize_path(path)

      with_namespace_claim(:path, volume.id, normalized, fn ->
        FileIndex.mkdir(volume.id, normalized)
      end)
    end
  end

  @doc """
  Renames or moves a file/directory within a volume.

  Handles same-directory renames, cross-directory moves, and combined
  move-and-rename operations.
  """
  @spec rename_file(String.t(), String.t(), String.t()) :: :ok | {:error, term()}
  def rename_file(volume_name, src_path, dest_path) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      src = normalize_path(src_path)
      dst = normalize_path(dest_path)

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

      {:error, :conflict, _conflict_id} ->
        {:error, :busy}

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

      {:error, :einval} ->
        {:error, :einval}

      {:error, :conflict, _conflict_id} ->
        {:error, :conflict}

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
    :exit, _ -> {:error, :coordinator_unavailable}
  end

  defp safe_claim(:subtree, scope, key) do
    NamespaceCoordinator.claim_subtree(key, scope)
  catch
    :exit, _ -> {:error, :coordinator_unavailable}
  end

  defp safe_claim_rename(src_key, dst_key) do
    NamespaceCoordinator.claim_rename(src_key, dst_key)
  catch
    :exit, _ -> {:error, :coordinator_unavailable}
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
    VolumeRegistry.get_by_name(volume_name)
  end

  defp normalize_path("/" <> _ = path), do: path
  defp normalize_path(path), do: "/" <> path
end
