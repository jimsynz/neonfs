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
  alias NeonFS.Core.ReadOperation
  alias NeonFS.Core.S3CredentialManager
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Core.WriteOperation

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
  Deletes a file from a volume by path.
  """
  @spec delete_file(String.t(), String.t()) :: :ok | {:error, term()}
  def delete_file(volume_name, path) do
    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, file} <- FileIndex.get_by_path(volume.id, normalize_path(path)) do
      FileIndex.delete(file.id)
    end
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
  """
  @spec mkdir(String.t(), String.t()) ::
          {:ok, NeonFS.Core.DirectoryEntry.t()} | {:error, term()}
  def mkdir(volume_name, path) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      FileIndex.mkdir(volume.id, normalize_path(path))
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
      do_rename(volume.id, normalize_path(src_path), normalize_path(dest_path))
    end
  end

  # --- Private helpers ---

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
