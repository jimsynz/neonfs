defmodule NeonFS.Core.ReadOperation do
  @moduledoc """
  Handles read operations for NeonFS.

  Implements the read path that takes a file path, looks up metadata, fetches chunks
  from the blob store, and assembles the data. Supports partial reads with offset and length.

  For replicated volumes, reads use the chunk-based path.
  For erasure-coded volumes, reads use the stripe-based path with healthy/degraded handling.
  """

  alias NeonFS.Core.{
    Blob.Native,
    ChunkFetcher,
    ChunkIndex,
    FileIndex,
    StripeIndex,
    VolumeRegistry
  }

  require Logger

  @type read_result :: {:ok, binary()} | {:error, term()}

  @doc """
  Reads a file from a volume with optional offset and length.

  Branches on `file_meta.stripes`:
  - `nil` — uses existing chunk-based path (replicated volumes)
  - non-nil — uses stripe-based path (erasure-coded volumes)

  ## Parameters

    * `volume_id` - Volume identifier
    * `path` - File path within the volume
    * `opts` - Optional keyword list:
      * `:offset` - Byte offset to start reading from (default: 0)
      * `:length` - Number of bytes to read (default: :all for entire file)

  ## Returns

    * `{:ok, data}` - File data (or partial data if offset/length specified)
    * `{:error, reason}` - Read failed
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
           {:ok, file_meta} <- get_file(volume_id, path) do
        do_read(file_meta, volume, offset, length)
      end

    duration = System.monotonic_time() - start_time
    emit_read_telemetry(result, duration, volume_id, path)
    result
  end

  # ─── Routing ──────────────────────────────────────────────────────────

  defp do_read(file_meta, volume, offset, length) when is_list(file_meta.stripes) do
    read_from_stripes(file_meta, volume, offset, length)
  end

  defp do_read(file_meta, volume, offset, length) do
    read_from_chunks(file_meta, volume, offset, length)
  end

  # ─── Chunk-based Read Path (replicated) ───────────────────────────────

  defp read_from_chunks(file_meta, volume, offset, length) do
    with {:ok, needed_chunks} <- calculate_needed_chunks(file_meta, offset, length),
         {:ok, chunk_data} <- fetch_chunks(needed_chunks, volume) do
      assemble_data(chunk_data)
    end
  end

  defp calculate_needed_chunks(file_meta, offset, length) do
    end_byte = calculate_end_byte(file_meta.size, offset, length)

    if offset >= file_meta.size do
      {:ok, []}
    else
      chunk_infos = build_chunk_info_list(file_meta.chunks, 0, [])

      needed =
        chunk_infos
        |> Enum.filter(fn {_hash, chunk_start, chunk_end} ->
          chunk_start < end_byte and chunk_end > offset
        end)
        |> Enum.map(fn {hash, chunk_start, chunk_end} ->
          read_start = max(0, offset - chunk_start)
          read_end = min(chunk_end - chunk_start, end_byte - chunk_start)
          %{hash: hash, read_start: read_start, read_end: read_end}
        end)

      {:ok, needed}
    end
  end

  defp build_chunk_info_list([], _offset, acc), do: Enum.reverse(acc)

  defp build_chunk_info_list([hash | rest], current_offset, acc) do
    case ChunkIndex.get(hash) do
      {:ok, chunk_meta} ->
        chunk_end = current_offset + chunk_meta.original_size
        build_chunk_info_list(rest, chunk_end, [{hash, current_offset, chunk_end} | acc])

      {:error, :not_found} ->
        Logger.error("Chunk metadata not found for hash: #{Base.encode16(hash)}")
        build_chunk_info_list(rest, current_offset, acc)
    end
  end

  defp fetch_chunks(needed_chunks, volume) do
    should_verify = should_verify_on_read?(volume.verification)

    results = Enum.map(needed_chunks, &fetch_single_chunk(&1.hash, should_verify))

    if Enum.all?(results, &match?({:ok, _}, &1)) do
      chunks = Enum.map(results, fn {:ok, data} -> data end)
      {:ok, Enum.zip(needed_chunks, chunks)}
    else
      Enum.find(results, &match?({:error, _}, &1))
    end
  end

  defp assemble_data(chunk_data_pairs) do
    assembled =
      chunk_data_pairs
      |> Enum.map(fn {chunk_info, chunk_data} ->
        read_length = chunk_info.read_end - chunk_info.read_start
        binary_part(chunk_data, chunk_info.read_start, read_length)
      end)
      |> IO.iodata_to_binary()

    {:ok, assembled}
  end

  # ─── Stripe-based Read Path (erasure-coded) ───────────────────────────

  defp read_from_stripes(file_meta, volume, offset, length) do
    end_byte = calculate_end_byte(file_meta.size, offset, length)

    if offset >= file_meta.size do
      {:ok, <<>>}
    else
      should_verify = should_verify_on_read?(volume.verification)

      relevant =
        file_meta.stripes
        |> Enum.filter(fn %{byte_range: {s, e}} -> s < end_byte and e > offset end)

      read_stripe_segments(relevant, offset, end_byte, should_verify)
    end
  end

  defp read_stripe_segments(stripes, offset, end_byte, should_verify) do
    stripes
    |> Enum.reduce_while({:ok, []}, fn %{stripe_id: sid, byte_range: {s, _e}}, {:ok, acc} ->
      stripe_offset = max(0, offset - s)
      stripe_length = min_stripe_read_length(s, offset, end_byte)

      case read_stripe(sid, stripe_offset, stripe_length, should_verify) do
        {:ok, data} -> {:cont, {:ok, [data | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, parts} -> {:ok, parts |> Enum.reverse() |> IO.iodata_to_binary()}
      error -> error
    end
  end

  defp min_stripe_read_length(stripe_start, offset, end_byte) do
    read_start = max(offset, stripe_start)
    end_byte - read_start
  end

  defp read_stripe(stripe_id, offset_in_stripe, length, should_verify) do
    case StripeIndex.get(stripe_id) do
      {:ok, stripe} ->
        max_readable = max(0, stripe.data_bytes - offset_in_stripe)
        actual_length = min(length, max_readable)

        if actual_length <= 0 do
          {:ok, <<>>}
        else
          do_read_stripe(stripe, offset_in_stripe, actual_length, should_verify)
        end

      {:error, :not_found} ->
        {:error, :stripe_not_found}
    end
  end

  defp do_read_stripe(stripe, offset, length, should_verify) do
    case calculate_stripe_state(stripe) do
      :healthy ->
        read_stripe_healthy(stripe, offset, length, should_verify)

      :degraded ->
        emit_stripe_read_telemetry(stripe, :degraded)
        read_stripe_degraded(stripe, offset, length, should_verify)

      :critical ->
        emit_stripe_read_telemetry(stripe, :critical)
        {:error, :insufficient_chunks}
    end
  end

  defp calculate_stripe_state(stripe) do
    k = stripe.config.data_chunks
    n = k + stripe.config.parity_chunks

    available_count =
      stripe.chunks
      |> Enum.count(&chunk_available?/1)

    cond do
      available_count == n -> :healthy
      available_count >= k -> :degraded
      true -> :critical
    end
  end

  defp chunk_available?(chunk_hash) do
    match?({:ok, _}, ChunkIndex.get(chunk_hash))
  end

  defp read_stripe_healthy(stripe, offset, length, should_verify) do
    chunk_size = stripe.config.chunk_size
    start_idx = div(offset, chunk_size)
    end_idx = div(offset + length - 1, chunk_size)

    chunks_data =
      start_idx..end_idx
      |> Enum.map(fn idx -> Enum.at(stripe.chunks, idx) end)
      |> Enum.map(&fetch_single_chunk(&1, should_verify))

    if Enum.all?(chunks_data, &match?({:ok, _}, &1)) do
      raw =
        chunks_data
        |> Enum.map(fn {:ok, d} -> d end)
        |> IO.iodata_to_binary()

      chunk_offset = rem(offset, chunk_size)
      {:ok, binary_part(raw, chunk_offset, length)}
    else
      Enum.find(chunks_data, &match?({:error, _}, &1))
    end
  end

  defp read_stripe_degraded(stripe, offset, length, should_verify) do
    k = stripe.config.data_chunks

    available_with_idx =
      stripe.chunks
      |> Enum.with_index()
      |> Enum.filter(fn {hash, _idx} -> chunk_available?(hash) end)

    if Kernel.length(available_with_idx) < k do
      {:error, :insufficient_chunks}
    else
      shards_to_fetch = Enum.take(available_with_idx, k)
      fetch_and_reconstruct(shards_to_fetch, stripe, offset, length, should_verify)
    end
  end

  defp fetch_and_reconstruct(shards_to_fetch, stripe, offset, length, should_verify) do
    k = stripe.config.data_chunks
    chunk_size = stripe.config.chunk_size

    shard_results =
      Enum.map(shards_to_fetch, fn {hash, idx} ->
        case fetch_single_chunk(hash, should_verify) do
          {:ok, data} -> {:ok, {idx, data}}
          error -> error
        end
      end)

    if Enum.all?(shard_results, &match?({:ok, _}, &1)) do
      shards_with_indices = Enum.map(shard_results, fn {:ok, pair} -> pair end)
      reconstruct_and_extract(shards_with_indices, k, stripe, chunk_size, offset, length)
    else
      Enum.find(shard_results, &match?({:error, _}, &1))
    end
  end

  defp reconstruct_and_extract(shards, k, stripe, chunk_size, offset, length) do
    parity_count = stripe.config.parity_chunks

    case Native.erasure_decode(shards, k, parity_count, chunk_size) do
      {:ok, data_shards} ->
        all_data = IO.iodata_to_binary(data_shards)
        {:ok, binary_part(all_data, offset, length)}

      {:error, reason} ->
        {:error, {:reconstruction_failed, reason}}
    end
  end

  # ─── Shared Helpers ───────────────────────────────────────────────────

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

  defp calculate_end_byte(file_size, _offset, :all), do: file_size
  defp calculate_end_byte(file_size, offset, len), do: min(offset + len, file_size)

  defp should_verify_on_read?(verification_config) do
    case verification_config.on_read do
      :always ->
        true

      :never ->
        false

      :sampling ->
        sampling_rate = verification_config.sampling_rate || 0.0
        :rand.uniform() < sampling_rate
    end
  end

  defp fetch_single_chunk(hash, should_verify) do
    case ChunkIndex.get(hash) do
      {:ok, chunk_meta} -> fetch_chunk_data(chunk_meta, should_verify)
      {:error, :not_found} -> {:error, :chunk_not_found}
    end
  end

  defp fetch_chunk_data(chunk_meta, should_verify) do
    {tier, drive_id} =
      case chunk_meta.locations do
        [location | _] ->
          {Atom.to_string(location.tier), Map.get(location, :drive_id, "default")}

        [] ->
          {"hot", "default"}
      end

    needs_decompress =
      chunk_meta.compression != :none or chunk_meta.stored_size != chunk_meta.original_size

    fetch_opts = [
      tier: tier,
      drive_id: drive_id,
      verify: should_verify,
      decompress: needs_decompress
    ]

    case ChunkFetcher.fetch_chunk(chunk_meta.hash, fetch_opts) do
      {:ok, data, _source} -> {:ok, data}
      {:error, reason} -> {:error, reason}
    end
  end

  defp emit_read_telemetry(result, duration, volume_id, path) do
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
  end

  defp emit_stripe_read_telemetry(stripe, state) do
    available = Enum.count(stripe.chunks, &chunk_available?/1)
    total = Kernel.length(stripe.chunks)

    :telemetry.execute(
      [:neonfs, :read_operation, :stripe_read],
      %{available_chunks: available, total_chunks: total},
      %{stripe_id: stripe.id, state: state, degraded_count: total - available}
    )
  end
end
