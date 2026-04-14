defmodule NeonFS.Core do
  @moduledoc """
  Public RPC facade for NeonFS core operations.

  Non-core nodes (S3, WebDAV, FUSE, NFS) call functions in this module via
  `NeonFS.Client.Router`, which dispatches `:rpc.call/5` to a core node.
  Each function resolves volume names to IDs and delegates to the
  appropriate internal module.
  """

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
  """
  @spec read_file(String.t(), String.t()) :: {:ok, binary()} | {:error, term()}
  def read_file(volume_name, path) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      ReadOperation.read_file(volume.id, normalize_path(path))
    end
  end

  @doc """
  Writes content to a file in a volume.
  """
  @spec write_file(String.t(), String.t(), binary()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, term()}
  def write_file(volume_name, path, content) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      WriteOperation.write_file(volume.id, normalize_path(path), content)
    end
  end

  @doc """
  Writes content to a file in a volume with options.

  Options may include `:content_type`, `:metadata`, etc.
  """
  @spec write_file(String.t(), String.t(), binary(), keyword()) ::
          {:ok, NeonFS.Core.FileMeta.t()} | {:error, term()}
  def write_file(volume_name, path, content, opts) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      WriteOperation.write_file(volume.id, normalize_path(path), content, opts)
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
  Lists direct children of a directory within a volume.

  Returns file metadata maps for each child entry. Directory entries
  are returned as maps with the same fields as `FileMeta` for uniform
  access by callers.
  """
  @spec list_files(String.t(), String.t()) :: {:ok, [NeonFS.Core.FileMeta.t()]} | {:error, term()}
  def list_files(volume_name, dir_path) do
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
