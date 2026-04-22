defmodule NeonFS.TestHelpers do
  @moduledoc """
  Helper functions for integration testing.

  These functions are called via RPC from test controllers to interact with
  NeonFS volumes running on containerized cluster nodes.
  """

  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.ReadOperation
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Core.WriteOperation

  @doc """
  Write a binary to a volume under `path`, creating the file if necessary.

  The helper branches on the volume's durability type so integration
  tests can exercise both replicated and erasure-coded volumes:

    * replicated → `WriteOperation.write_file_streamed/4` with a
      single-chunk `[content]` stream (empty content collapses to `[]`).
    * erasure    → `WriteOperation.write_file_at/5` with `offset: 0`,
      which routes through the existing `erasure_write` whole-binary
      path (streaming erasure writes are out of scope for #195).

  Returns whatever the underlying `WriteOperation` returns.
  """
  @spec write_file_from_binary(String.t(), String.t(), binary(), keyword()) ::
          {:ok, term()} | {:error, term()}
  def write_file_from_binary(volume_name, path, content, opts \\ []) when is_binary(content) do
    require Logger

    result =
      case VolumeRegistry.get_by_name(volume_name) do
        {:ok, volume} ->
          do_write_binary(volume, path, content, opts)

        {:error, _} = error ->
          error
      end

    Logger.info("TestHelpers.write_file_from_binary returned",
      volume_name: volume_name,
      path: path,
      result: inspect(result)
    )

    result
  end

  defp do_write_binary(%{durability: %{type: :replicate}} = volume, path, content, opts) do
    stream = if content == <<>>, do: [], else: [content]
    WriteOperation.write_file_streamed(volume.id, path, stream, opts)
  end

  defp do_write_binary(%{durability: %{type: :erasure}} = volume, path, content, opts) do
    WriteOperation.write_file_at(volume.id, path, 0, content, opts)
  end

  @doc """
  Read a file's content from a volume.

  Returns `{:ok, content}` on success.
  """
  @spec read_file(String.t(), String.t(), keyword()) :: {:ok, binary()} | {:error, term()}
  def read_file(volume_name, path, opts \\ []) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} ->
        ReadOperation.read_file(volume.id, path, opts)

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Generate random file content of specified size.
  """
  @spec random_content(pos_integer()) :: binary()
  def random_content(size_bytes) when is_integer(size_bytes) and size_bytes > 0 do
    :crypto.strong_rand_bytes(size_bytes)
  end

  @doc """
  List files in a volume.
  """
  @spec list_files(String.t()) :: {:ok, [map()]} | {:error, term()}
  def list_files(volume_name) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} ->
        files = FileIndex.list_volume(volume.id)
        {:ok, files}

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Delete a file from a volume.
  """
  @spec delete_file(String.t(), String.t()) :: :ok | {:error, term()}
  def delete_file(volume_name, path) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} ->
        # Look up the file by path to get its ID
        case FileIndex.get_by_path(volume.id, path) do
          {:ok, file} ->
            FileIndex.delete(file.id)

          {:error, _} = error ->
            error
        end

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Read a partial file from a volume with offset and length.

  Returns `{:ok, content}` on success.
  """
  @spec read_file_partial(String.t(), String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read_file_partial(volume_name, path, offset, length) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} ->
        ReadOperation.read_file(volume.id, path, offset: offset, length: length)

      {:error, _} = error ->
        error
    end
  end

  @doc """
  List files in a directory within a volume.

  Returns `{:ok, children_map}` where children_map is
  `%{name => %{type: :file | :dir, id: binary()}}`.
  """
  @spec list_dir(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def list_dir(volume_name, dir_path) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} ->
        FileIndex.list_dir(volume.id, dir_path)

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Get file metadata by path.
  """
  @spec get_file(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def get_file(volume_name, path) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} ->
        FileIndex.get_by_path(volume.id, path)

      {:error, _} = error ->
        error
    end
  end
end
