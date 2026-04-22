defmodule NeonFS.Core.SystemVolume do
  @moduledoc """
  Convenience API for accessing the `_system` volume.

  The system volume stores cluster-wide operational data (CA keys, audit logs,
  DR snapshots). It is accessed programmatically — never mounted via FUSE.
  All operations delegate to the existing `ReadOperation` and `WriteOperation`
  modules with the system volume's ID.
  """

  alias NeonFS.Core.{FileIndex, ReadOperation, VolumeRegistry, WriteOperation}

  @volume_name "_system"

  @doc """
  Reads a file from the system volume.

  The system volume stores small, bounded operational state (PKI material,
  cluster identity, retention policies). Callers of this function are the
  only production users of the whole-file read path — audit log files and
  anything that could grow unbounded must use streaming APIs instead.
  """
  @spec read(String.t()) :: {:ok, binary()} | {:error, term()}
  def read(path) do
    with {:ok, volume} <- get_system_volume() do
      # audit:bounded system-volume files are small operational state
      ReadOperation.read_file(volume.id, path)
    end
  end

  @doc """
  Writes a file to the system volume. Creates or overwrites.
  """
  @spec write(String.t(), binary()) :: :ok | {:error, term()}
  def write(path, content) do
    with {:ok, volume} <- get_system_volume(),
         {:ok, _file_meta} <-
           WriteOperation.write_file_streamed(volume.id, path, stream_for(content)) do
      :ok
    end
  end

  @doc """
  Appends content to a file on the system volume.

  Reads the existing file content (if any), concatenates the new content,
  and writes the result back. Creates the file if it does not exist.

  As with `read/1`, this is intended for bounded system files — not for
  anything that could accumulate without rotation.
  """
  @spec append(String.t(), binary()) :: :ok | {:error, term()}
  def append(path, content) do
    with {:ok, volume} <- get_system_volume() do
      # audit:bounded system-volume files are small operational state
      existing =
        case ReadOperation.read_file(volume.id, path) do
          {:ok, data} -> data
          {:error, %{class: :not_found}} -> <<>>
        end

      case WriteOperation.write_file_streamed(volume.id, path, stream_for(existing <> content)) do
        {:ok, _file_meta} -> :ok
        {:error, _reason} = error -> error
      end
    end
  end

  @doc """
  Lists files and directories under a path on the system volume.

  Returns a list of entry names (both files and directories).
  """
  @spec list(String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list(path) do
    with {:ok, volume} <- get_system_volume() do
      case FileIndex.list_dir(volume.id, path) do
        {:ok, children} ->
          names = children |> Map.keys() |> Enum.sort()
          {:ok, names}

        {:error, _reason} = error ->
          error
      end
    end
  end

  @doc """
  Deletes a file from the system volume.
  """
  @spec delete(String.t()) :: :ok | {:error, term()}
  def delete(path) do
    with {:ok, volume} <- get_system_volume(),
         {:ok, file_meta} <- get_file(volume.id, path) do
      FileIndex.delete(file_meta.id)
    end
  end

  @doc """
  Checks whether a file exists on the system volume.

  Returns `true` if the file exists, `false` if it does not.
  """
  @spec exists?(String.t()) :: boolean()
  def exists?(path) do
    case get_system_volume() do
      {:ok, volume} ->
        match?({:ok, _}, FileIndex.get_by_path(volume.id, path))

      {:error, _} ->
        false
    end
  end

  # Private

  # Wrap a single binary as an Enumerable for `write_file_streamed/4`.
  # Empty binaries collapse to `[]` so the streaming pipeline skips the
  # chunker entirely and produces an empty file.
  defp stream_for(<<>>), do: []
  defp stream_for(content) when is_binary(content), do: [content]

  defp get_system_volume do
    case VolumeRegistry.get_by_name(@volume_name) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :system_volume_not_found}
    end
  end

  defp get_file(volume_id, path) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, file_meta} -> {:ok, file_meta}
      {:error, :not_found} -> {:error, :not_found}
    end
  end
end
