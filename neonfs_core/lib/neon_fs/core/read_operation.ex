defmodule NeonFS.Core.ReadOperation do
  @moduledoc """
  Handles read operations for NeonFS.

  Implements the read path that takes a file path, looks up metadata, fetches chunks
  from the blob store, and assembles the data. Supports partial reads with offset and length.
  For Phase 1, this assumes all chunks are local (no remote fetching).
  """

  alias NeonFS.Core.{ChunkFetcher, ChunkIndex, FileIndex, VolumeRegistry}

  require Logger

  @type read_result :: {:ok, binary()} | {:error, term()}

  @doc """
  Reads a file from a volume with optional offset and length.

  ## Parameters

    * `volume_id` - Volume identifier
    * `path` - File path within the volume
    * `opts` - Optional keyword list:
      * `:offset` - Byte offset to start reading from (default: 0)
      * `:length` - Number of bytes to read (default: :all for entire file)

  ## Returns

    * `{:ok, data}` - File data (or partial data if offset/length specified)
    * `{:error, reason}` - Read failed

  ## Examples

      # Read entire file
      {:ok, data} = ReadOperation.read_file(volume_id, "/docs/readme.txt")

      # Read with offset and length
      {:ok, data} = ReadOperation.read_file(volume_id, "/docs/readme.txt", offset: 100, length: 50)

  """
  @spec read_file(binary(), String.t(), keyword()) :: read_result()
  def read_file(volume_id, path, opts \\ []) do
    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, :all)

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :read_operation, :start],
      %{offset: offset},
      %{volume_id: volume_id, path: path, length: length}
    )

    result =
      with {:ok, volume} <- get_volume(volume_id),
           {:ok, file_meta} <- get_file(volume_id, path),
           {:ok, needed_chunks} <- calculate_needed_chunks(file_meta, offset, length),
           {:ok, chunk_data} <- fetch_chunks(needed_chunks, volume) do
        assemble_data(chunk_data, file_meta, offset, length)
      end

    duration = System.monotonic_time() - start_time

    case result do
      {:ok, data} ->
        :telemetry.execute(
          [:neonfs, :read_operation, :stop],
          %{duration: duration, bytes: byte_size(data)},
          %{volume_id: volume_id, path: path}
        )

      {:error, reason} ->
        :telemetry.execute(
          [:neonfs, :read_operation, :exception],
          %{duration: duration},
          %{volume_id: volume_id, path: path, error: reason}
        )
    end

    result
  end

  # Private Functions

  defp get_volume(volume_id) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :volume_not_found}
    end
  end

  defp get_file(volume_id, path) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, file_meta} -> {:ok, file_meta}
      {:error, :not_found} -> {:error, :file_not_found}
    end
  end

  defp calculate_needed_chunks(file_meta, offset, length) do
    # Determine the byte range to read
    end_byte =
      case length do
        :all -> file_meta.size
        len when is_integer(len) -> min(offset + len, file_meta.size)
      end

    # If offset is beyond file size, return empty
    if offset >= file_meta.size do
      {:ok, []}
    else
      # Build chunk info with cumulative offsets
      chunk_infos = build_chunk_info_list(file_meta.chunks, 0, [])

      # Filter chunks that contain bytes in the requested range
      needed =
        chunk_infos
        |> Enum.filter(fn {_hash, chunk_start, chunk_end} ->
          # Chunk overlaps with requested range if:
          # chunk_start < end_byte AND chunk_end > offset
          chunk_start < end_byte and chunk_end > offset
        end)
        |> Enum.map(fn {hash, chunk_start, chunk_end} ->
          # Calculate what portion of this chunk we need
          read_start = max(0, offset - chunk_start)
          read_end = min(chunk_end - chunk_start, end_byte - chunk_start)

          %{
            hash: hash,
            chunk_start: chunk_start,
            chunk_end: chunk_end,
            read_start: read_start,
            read_end: read_end
          }
        end)

      {:ok, needed}
    end
  end

  defp build_chunk_info_list([], _offset, acc), do: Enum.reverse(acc)

  defp build_chunk_info_list([hash | rest], current_offset, acc) do
    # Look up chunk metadata to get size
    case ChunkIndex.get(hash) do
      {:ok, chunk_meta} ->
        chunk_end = current_offset + chunk_meta.original_size
        new_acc = [{hash, current_offset, chunk_end} | acc]
        build_chunk_info_list(rest, chunk_end, new_acc)

      {:error, :not_found} ->
        # Chunk metadata not found - this is a data loss scenario
        Logger.error("Chunk metadata not found for hash: #{Base.encode16(hash)}")
        build_chunk_info_list(rest, current_offset, acc)
    end
  end

  defp fetch_chunks(needed_chunks, volume) do
    # Determine if we should verify based on volume settings
    should_verify = should_verify_on_read?(volume.verification)

    # Fetch each chunk
    results =
      Enum.map(needed_chunks, fn chunk_info ->
        fetch_single_chunk(chunk_info, should_verify)
      end)

    # Check if all fetches succeeded
    if Enum.all?(results, &match?({:ok, _}, &1)) do
      chunks = Enum.map(results, fn {:ok, data} -> data end)
      {:ok, Enum.zip(needed_chunks, chunks)}
    else
      # Find first error
      error = Enum.find(results, &match?({:error, _}, &1))
      error
    end
  end

  defp should_verify_on_read?(verification_config) do
    case verification_config.on_read do
      :always ->
        true

      :never ->
        false

      :sampling ->
        # Implement sampling: random chance based on sampling_rate
        sampling_rate = verification_config.sampling_rate || 0.0
        :rand.uniform() < sampling_rate
    end
  end

  defp fetch_single_chunk(chunk_info, should_verify) do
    hash = chunk_info.hash

    # Get chunk metadata for tier and compression info
    case ChunkIndex.get(hash) do
      {:ok, chunk_meta} ->
        fetch_chunk_data(chunk_meta, should_verify)

      {:error, :not_found} ->
        {:error, :chunk_not_found}
    end
  end

  defp fetch_chunk_data(chunk_meta, should_verify) do
    # Determine tier and drive_id (use first location for Phase 1/2)
    {tier, drive_id} =
      case chunk_meta.locations do
        [location | _] ->
          {Atom.to_string(location.tier), Map.get(location, :drive_id, "default")}

        [] ->
          {"hot", "default"}
      end

    # Determine if we need to decompress
    # We need to decompress if:
    # 1. Chunk was explicitly compressed (compression != :none), OR
    # 2. stored_size != original_size (data was transformed, either compressed or framed)
    # Note: Due to a bug in Task 0018, compression field may be :none even when compressed
    needs_decompress =
      chunk_meta.compression != :none or chunk_meta.stored_size != chunk_meta.original_size

    fetch_opts = [
      tier: tier,
      drive_id: drive_id,
      verify: should_verify,
      decompress: needs_decompress
    ]

    # Use ChunkFetcher to get chunk from local or remote node
    case ChunkFetcher.fetch_chunk(chunk_meta.hash, fetch_opts) do
      {:ok, data, _source} -> {:ok, data}
      {:error, reason} -> {:error, reason}
    end
  end

  defp assemble_data(chunk_data_pairs, _file_meta, _offset, _length) do
    # Assemble the data from chunks, extracting only the requested byte range
    # The exact portions have already been calculated in calculate_needed_chunks
    assembled =
      chunk_data_pairs
      |> Enum.map(fn {chunk_info, chunk_data} ->
        # Extract the portion of this chunk we need
        read_start = chunk_info.read_start
        read_length = chunk_info.read_end - chunk_info.read_start

        binary_part(chunk_data, read_start, read_length)
      end)
      |> IO.iodata_to_binary()

    {:ok, assembled}
  end
end
