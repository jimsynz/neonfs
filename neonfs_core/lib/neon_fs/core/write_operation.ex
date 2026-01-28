defmodule NeonFS.Core.WriteOperation do
  @moduledoc """
  Handles write operations for NeonFS.

  Implements the write path that takes file data, chunks it, stores chunks in the blob store,
  and creates file metadata. For Phase 1, this is single-node only (no replication).
  """

  alias NeonFS.Core.{BlobStore, ChunkIndex, ChunkMeta, FileIndex, FileMeta, VolumeRegistry}

  require Logger

  @type write_id :: binary()
  @type chunk_info :: %{
          hash: binary(),
          offset: non_neg_integer(),
          size: non_neg_integer(),
          stored_size: non_neg_integer(),
          compression: :none | :zstd
        }

  @doc """
  Writes file data to a volume with chunking and deduplication.

  ## Options
    * `:chunk_strategy` - Override volume's default chunking strategy
    * `:compression` - Override volume's compression settings

  ## Returns
    * `{:ok, file_meta}` - File successfully written
    * `{:error, reason}` - Write failed
  """
  @spec write_file(binary(), String.t(), binary(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, term()}
  def write_file(volume_id, path, data, opts \\ []) do
    write_id = generate_write_id()
    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :write_operation, :start],
      %{bytes: byte_size(data)},
      %{volume_id: volume_id, path: path, write_id: write_id}
    )

    result =
      with {:ok, volume} <- get_volume(volume_id),
           {:ok, chunks} <- chunk_and_store(data, volume, write_id, opts),
           {:ok, file_meta} <- create_file_metadata(volume_id, path, chunks, data),
           :ok <- commit_chunks(write_id, chunks) do
        {:ok, file_meta}
      else
        {:error, _reason} = error ->
          abort_chunks(write_id)
          error
      end

    duration = System.monotonic_time() - start_time

    case result do
      {:ok, file_meta} ->
        :telemetry.execute(
          [:neonfs, :write_operation, :stop],
          %{duration: duration, bytes: byte_size(data), chunks: length(file_meta.chunks)},
          %{volume_id: volume_id, path: path, write_id: write_id}
        )

      {:error, reason} ->
        :telemetry.execute(
          [:neonfs, :write_operation, :exception],
          %{duration: duration},
          %{volume_id: volume_id, path: path, write_id: write_id, error: reason}
        )
    end

    result
  end

  @doc """
  Generates a unique write ID for tracking write operations.
  """
  @spec generate_write_id() :: write_id()
  def generate_write_id do
    UUIDv7.generate()
  end

  # Private Functions

  defp get_volume(volume_id) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :volume_not_found}
    end
  end

  defp chunk_and_store(data, volume, write_id, opts) do
    # Determine chunk strategy
    chunk_strategy_opt = Keyword.get(opts, :chunk_strategy, :auto)

    # Convert strategy to BlobStore format
    chunk_strategy =
      case chunk_strategy_opt do
        :auto -> :auto
        :single -> {:single}
        {:fixed, size} -> {:fixed, size}
        {:fastcdc, avg_size} -> {:fastcdc, avg_size}
        other -> other
      end

    # Chunk the data
    case BlobStore.chunk_data(data, chunk_strategy) do
      {:ok, chunk_results} ->
        # Process each chunk: check for existing, store if needed
        process_chunks(chunk_results, volume, write_id, opts)

      {:error, _reason} = error ->
        error
    end
  end

  defp process_chunks(chunk_results, volume, write_id, opts) do
    # Determine compression settings
    compression_config = Keyword.get(opts, :compression, volume.compression)

    chunk_results
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {{data, hash, offset, size}, index}, {:ok, acc} ->
      case process_chunk(data, hash, offset, size, index, compression_config, volume, write_id) do
        {:ok, chunk_info} -> {:cont, {:ok, [chunk_info | acc]}}
        {:error, reason} -> {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
      error -> error
    end
  end

  defp process_chunk(data, hash, offset, size, index, compression_config, volume, write_id) do
    # Check if chunk already exists
    case ChunkIndex.get(hash) do
      {:ok, existing_chunk} ->
        # Chunk exists - add write reference
        :ok = ChunkIndex.add_write_ref(hash, write_id)

        {:ok,
         %{
           hash: hash,
           offset: offset,
           size: size,
           stored_size: existing_chunk.stored_size,
           compression: existing_chunk.compression,
           index: index
         }}

      {:error, :not_found} ->
        # New chunk - store it
        store_new_chunk(data, hash, offset, size, index, compression_config, volume, write_id)
    end
  end

  defp store_new_chunk(data, hash, offset, size, index, compression_config, _volume, write_id) do
    # Determine if we should compress this chunk
    {_should_compress, compression} = should_compress_chunk?(size, compression_config)

    # Write to blob store
    tier = "hot"

    write_opts =
      case compression do
        :none -> []
        {:zstd, level} -> [compression: "zstd", compression_level: level]
      end

    case BlobStore.write_chunk(data, tier, write_opts) do
      {:ok, _returned_hash, chunk_info} ->
        # Parse compression string back to atom
        compression_type =
          case chunk_info.compression do
            "none" -> :none
            "zstd" -> :zstd
            _ -> :none
          end

        # Create chunk metadata (uncommitted)
        chunk_meta = %ChunkMeta{
          hash: hash,
          original_size: size,
          stored_size: chunk_info.stored_size,
          compression: compression_type,
          locations: [%{node: node(), drive_id: "default", tier: :hot}],
          target_replicas: 1,
          commit_state: :uncommitted,
          active_write_refs: MapSet.new([write_id]),
          stripe_id: nil,
          stripe_index: nil,
          created_at: DateTime.utc_now(),
          last_verified: nil
        }

        # Store in chunk index
        :ok = ChunkIndex.put(chunk_meta)

        {:ok,
         %{
           hash: hash,
           offset: offset,
           size: size,
           stored_size: chunk_info.stored_size,
           compression: compression_type,
           index: index
         }}

      {:error, _reason} = error ->
        error
    end
  end

  defp should_compress_chunk?(_size, %{algorithm: :none}), do: {false, :none}

  defp should_compress_chunk?(size, %{algorithm: :zstd, min_size: min_size, level: _level})
       when size < min_size do
    {false, :none}
  end

  defp should_compress_chunk?(_size, %{algorithm: :zstd, level: level}) do
    {true, {:zstd, level}}
  end

  defp should_compress_chunk?(_size, _config), do: {false, :none}

  defp create_file_metadata(volume_id, path, chunks, data) do
    # Sort chunks by offset to maintain file order
    sorted_chunks = Enum.sort_by(chunks, & &1.offset)

    # Extract just the hashes for FileMeta
    chunk_hashes = Enum.map(sorted_chunks, & &1.hash)

    # Check if file already exists at this path (overwrite scenario)
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, existing_file} ->
        # Delete old file to allow overwrite
        FileIndex.delete(existing_file.id)

      {:error, :not_found} ->
        :ok
    end

    # Create file metadata
    file_meta = FileMeta.new(volume_id, path, chunks: chunk_hashes, size: byte_size(data))

    # Store in file index
    case FileIndex.create(file_meta) do
      {:ok, stored_meta} -> {:ok, stored_meta}
      {:error, _reason} = error -> error
    end
  end

  defp commit_chunks(write_id, chunks) do
    # Remove write_ref from all chunks
    results =
      Enum.map(chunks, fn chunk ->
        ChunkIndex.remove_write_ref(chunk.hash, write_id)
      end)

    # Check if all succeeded
    if Enum.all?(results, &(&1 == :ok)) do
      commit_all_chunks(chunks)
    else
      {:error, :commit_failed}
    end
  end

  defp commit_all_chunks(chunks) do
    commit_results =
      Enum.map(chunks, fn chunk ->
        commit_chunk_if_ready(chunk.hash)
      end)

    if Enum.all?(commit_results, &(&1 in [:ok, {:error, :has_active_writes}])) do
      :ok
    else
      {:error, :commit_failed}
    end
  end

  defp commit_chunk_if_ready(hash) do
    case ChunkIndex.get(hash) do
      {:ok, meta} ->
        if MapSet.size(meta.active_write_refs) == 0 do
          ChunkIndex.commit(hash)
        else
          :ok
        end

      {:error, :not_found} ->
        :ok
    end
  end

  defp abort_chunks(write_id) do
    # Find all uncommitted chunks with this write_id
    uncommitted = ChunkIndex.list_uncommitted()

    uncommitted
    |> Enum.filter(fn meta -> MapSet.member?(meta.active_write_refs, write_id) end)
    |> Enum.each(&abort_single_chunk(&1, write_id))

    :ok
  end

  defp abort_single_chunk(meta, write_id) do
    # Remove the write reference
    ChunkIndex.remove_write_ref(meta.hash, write_id)

    # If no more write refs, delete the chunk
    case ChunkIndex.get(meta.hash) do
      {:ok, updated_meta} ->
        delete_chunk_if_no_refs(updated_meta)

      {:error, :not_found} ->
        :ok
    end
  end

  defp delete_chunk_if_no_refs(meta) do
    if MapSet.size(meta.active_write_refs) == 0 do
      # Delete from blob store
      delete_chunk_from_storage(meta)

      # Delete from index
      ChunkIndex.delete(meta.hash)
    end
  end

  defp delete_chunk_from_storage(meta) do
    case meta.locations do
      [location | _] ->
        tier_str = Atom.to_string(location.tier)
        BlobStore.delete_chunk(meta.hash, tier_str)

      [] ->
        :ok
    end
  end
end
