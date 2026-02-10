defmodule NeonFS.Core.WriteOperation do
  @moduledoc """
  Handles write operations for NeonFS.

  Implements the write path that takes file data, chunks it, stores chunks in the blob store,
  and creates file metadata. Supports both replicated and erasure-coded volumes.

  For replicated volumes, each chunk is stored and replicated to N nodes.
  For erasure-coded volumes, chunks are grouped into stripes, parity is computed,
  and all chunks (data + parity) are distributed across nodes.
  """

  alias NeonFS.Core.{
    Blob.Native,
    BlobStore,
    ChunkIndex,
    ChunkMeta,
    DriveRegistry,
    FileIndex,
    FileMeta,
    Replication,
    ResolvedLookupCache,
    Stripe,
    StripeIndex,
    StripePlacement,
    VolumeRegistry
  }

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

  Branches on `volume.durability.type`:
  - `:replicate` — existing replicated write path
  - `:erasure` — stripe-based erasure-coded write path

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
      with {:ok, volume} <- get_volume(volume_id) do
        do_write(volume, path, data, write_id, opts)
      end

    # Evict resolved lookup cache on successful write to prevent serving stale data
    case result do
      {:ok, file_meta} -> evict_resolved_cache(file_meta.id)
      _ -> :ok
    end

    duration = System.monotonic_time() - start_time
    emit_completion_telemetry(result, duration, volume_id, path, write_id, data)
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

  defp do_write(%{durability: %{type: :erasure}} = volume, path, data, write_id, opts) do
    case erasure_write(volume, path, data, write_id, opts) do
      {:ok, _file_meta} = ok ->
        ok

      {:error, _reason} = error ->
        abort_chunks(write_id)
        abort_stripes(write_id)
        error
    end
  end

  defp do_write(volume, path, data, write_id, opts) do
    with {:ok, chunks} <- chunk_and_store(data, volume, write_id, opts),
         {:ok, file_meta} <- create_file_metadata(volume.id, path, chunks, data, opts),
         :ok <- commit_chunks(write_id, chunks),
         :ok <- update_volume_stats(volume.id, data, chunks) do
      {:ok, file_meta}
    else
      {:error, _reason} = error ->
        abort_chunks(write_id)
        error
    end
  end

  defp get_volume(volume_id) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, :volume_not_found}
    end
  end

  # ─── Erasure-Coded Write Path ───────────────────────────────────────────

  defp erasure_write(volume, path, data, write_id, opts) do
    with {:ok, chunk_results} <- chunk_data(data, opts),
         {:ok, stripe_results} <-
           build_and_store_stripes(chunk_results, volume, write_id, opts),
         all_chunks = collect_all_stripe_chunks(stripe_results),
         {:ok, file_meta} <-
           create_erasure_file_metadata(volume.id, path, stripe_results, data, opts),
         :ok <- commit_chunks(write_id, all_chunks),
         :ok <- validate_erasure_commit(stripe_results, data),
         :ok <- update_volume_stats(volume.id, data, all_chunks) do
      {:ok, file_meta}
    end
  end

  defp chunk_data(data, opts) do
    chunk_strategy = resolve_chunk_strategy(opts)
    BlobStore.chunk_data(data, chunk_strategy)
  end

  defp build_and_store_stripes(chunk_results, volume, write_id, opts) do
    data_chunks_per_stripe = volume.durability.data_chunks
    parity_count = volume.durability.parity_chunks

    batches = Enum.chunk_every(chunk_results, data_chunks_per_stripe)
    total_batches = length(batches)

    batches
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, [], 0}, fn {batch, batch_idx}, {:ok, acc, byte_offset} ->
      is_last = batch_idx == total_batches - 1
      partial = is_last and length(batch) < data_chunks_per_stripe

      case process_stripe_batch(
             batch,
             parity_count,
             data_chunks_per_stripe,
             volume,
             write_id,
             byte_offset,
             partial,
             opts
           ) do
        {:ok, stripe_result} ->
          new_offset = byte_offset + stripe_result.data_bytes
          {:cont, {:ok, [stripe_result | acc], new_offset}}

        {:error, _reason} = error ->
          {:halt, error}
      end
    end)
    |> case do
      {:ok, results, _offset} -> {:ok, Enum.reverse(results)}
      error -> error
    end
  end

  defp process_stripe_batch(
         batch,
         parity_count,
         data_chunks_per_stripe,
         volume,
         write_id,
         byte_offset,
         partial,
         opts
       ) do
    stripe_id = UUIDv7.generate()
    compression_config = Keyword.get(opts, :compression, volume.compression)
    tier = volume.tiering.initial_tier
    tier_str = Atom.to_string(tier)

    # Extract raw data from batch and track sizes
    data_entries = Enum.map(batch, fn {data, hash, _offset, size} -> {data, hash, size} end)
    actual_data_sizes = Enum.map(data_entries, fn {_data, _hash, size} -> size end)
    data_bytes = Enum.sum(actual_data_sizes)

    # Pad data chunks for parity computation
    padded = pad_for_parity(data_entries, data_chunks_per_stripe)
    max_chunk_size = padded_chunk_size(padded)
    padded_bytes = max_chunk_size * length(padded) - data_bytes

    # Compute parity
    padded_data_list = Enum.map(padded, fn {data, _hash, _orig_size} -> data end)

    stripe_config = %{data_chunks: data_chunks_per_stripe, parity_chunks: parity_count}

    with {:ok, parity_shards} <- Native.erasure_encode(padded_data_list, parity_count),
         {:ok, targets} <- StripePlacement.select_targets(stripe_config, tier: tier),
         data_targets = Enum.take(targets, length(padded)),
         parity_targets = Enum.drop(targets, length(padded)),
         {:ok, stored_data_chunks} <-
           store_data_chunks(
             padded,
             stripe_id,
             compression_config,
             tier,
             tier_str,
             data_targets,
             write_id
           ),
         {:ok, stored_parity_chunks} <-
           store_parity_chunks(
             parity_shards,
             stripe_id,
             data_chunks_per_stripe,
             tier,
             tier_str,
             parity_targets,
             write_id
           ) do
      all_chunk_hashes =
        Enum.map(stored_data_chunks, & &1.hash) ++
          Enum.map(stored_parity_chunks, & &1.hash)

      stripe =
        build_stripe(
          stripe_id,
          volume,
          all_chunk_hashes,
          max_chunk_size,
          partial,
          data_bytes,
          padded_bytes
        )

      store_stripe_metadata(stripe, stripe_id, write_id)

      all_chunks = stored_data_chunks ++ stored_parity_chunks

      emit_stripe_telemetry(stripe_id, volume, partial, all_chunks)

      {:ok,
       %{
         stripe_id: stripe_id,
         stripe: stripe,
         data_bytes: data_bytes,
         padded_bytes: padded_bytes,
         byte_offset: byte_offset,
         chunks: all_chunks
       }}
    end
  end

  defp pad_for_parity(data_entries, data_chunks_per_stripe) do
    max_size = data_entries |> Enum.map(fn {data, _h, _s} -> byte_size(data) end) |> Enum.max()

    # Pad existing chunks to max_size
    padded =
      Enum.map(data_entries, fn {data, hash, orig_size} ->
        padding_needed = max_size - byte_size(data)

        padded_data =
          if padding_needed > 0,
            do: data <> :binary.copy(<<0>>, padding_needed),
            else: data

        {padded_data, hash, orig_size}
      end)

    # Add zero-filled chunks if partial stripe
    fill_count = data_chunks_per_stripe - length(data_entries)

    if fill_count > 0 do
      zero_chunk = :binary.copy(<<0>>, max_size)
      zero_entries = for _ <- 1..fill_count, do: {zero_chunk, nil, 0}
      padded ++ zero_entries
    else
      padded
    end
  end

  defp padded_chunk_size([{data, _, _} | _]), do: byte_size(data)
  defp padded_chunk_size([]), do: 0

  defp store_data_chunks(
         data_entries,
         stripe_id,
         compression_config,
         tier,
         tier_str,
         targets,
         write_id
       ) do
    ctx = %{
      stripe_id: stripe_id,
      compression_config: compression_config,
      tier: tier,
      tier_str: tier_str,
      write_id: write_id
    }

    data_entries
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {{data, _hash, size}, stripe_idx}, {:ok, acc} ->
      target = Enum.at(targets, stripe_idx, default_target())

      case store_stripe_data_chunk(data, size, stripe_idx, target, ctx) do
        {:ok, chunk_info} -> {:cont, {:ok, [chunk_info | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
      error -> error
    end
  end

  defp store_stripe_data_chunk(data, size, stripe_idx, target, ctx) do
    {_should, compression} = should_compress_chunk?(size, ctx.compression_config)
    write_opts = build_write_opts(compression)
    # Use padded data size as original_size so decompression detection works correctly.
    # The pre-padding `size` is only used for compression min_size threshold.
    padded_size = byte_size(data)

    with {:ok, hash, chunk_info} <-
           write_chunk_to_target(data, target, ctx.tier_str, write_opts) do
      meta_args = %{
        hash: hash,
        size: padded_size,
        chunk_info: chunk_info,
        target: target,
        tier: ctx.tier,
        stripe_id: ctx.stripe_id,
        stripe_idx: stripe_idx,
        write_id: ctx.write_id
      }

      chunk_meta = build_erasure_chunk_meta(meta_args)

      case ChunkIndex.put(chunk_meta) do
        :ok ->
          {:ok,
           %{
             hash: hash,
             size: size,
             stored_size: chunk_info.stored_size,
             new: true,
             compression: parse_compression(chunk_info.compression)
           }}

        {:error, reason} ->
          {:error, {:chunk_index_failed, reason}}
      end
    end
  end

  defp store_parity_chunks(
         parity_shards,
         stripe_id,
         data_chunk_count,
         tier,
         tier_str,
         targets,
         write_id
       ) do
    ctx = %{stripe_id: stripe_id, tier: tier, tier_str: tier_str, write_id: write_id}

    parity_shards
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {parity_data, parity_idx}, {:ok, acc} ->
      stripe_idx = data_chunk_count + parity_idx
      target = Enum.at(targets, parity_idx, default_target())

      case store_parity_chunk(parity_data, stripe_idx, target, ctx) do
        {:ok, chunk_info} -> {:cont, {:ok, [chunk_info | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
      error -> error
    end
  end

  defp store_parity_chunk(parity_data, stripe_idx, target, ctx) do
    size = byte_size(parity_data)

    with {:ok, hash, chunk_info} <-
           write_chunk_to_target(parity_data, target, ctx.tier_str, []) do
      meta_args = %{
        hash: hash,
        size: size,
        chunk_info: chunk_info,
        target: target,
        tier: ctx.tier,
        stripe_id: ctx.stripe_id,
        stripe_idx: stripe_idx,
        write_id: ctx.write_id
      }

      chunk_meta = build_erasure_chunk_meta(meta_args)

      case ChunkIndex.put(chunk_meta) do
        :ok ->
          {:ok,
           %{
             hash: hash,
             size: size,
             stored_size: chunk_info.stored_size,
             new: true,
             compression: :none
           }}

        {:error, reason} ->
          {:error, {:chunk_index_failed, reason}}
      end
    end
  end

  defp build_erasure_chunk_meta(args) do
    %ChunkMeta{
      hash: args.hash,
      original_size: args.size,
      stored_size: args.chunk_info.stored_size,
      compression: parse_compression(args.chunk_info.compression),
      locations: [%{node: args.target.node, drive_id: args.target.drive_id, tier: args.tier}],
      target_replicas: 1,
      commit_state: :uncommitted,
      active_write_refs: MapSet.new([args.write_id]),
      stripe_id: args.stripe_id,
      stripe_index: args.stripe_idx,
      created_at: DateTime.utc_now(),
      last_verified: nil
    }
  end

  defp write_chunk_to_target(data, target, tier_str, write_opts) do
    if target.node == node() do
      BlobStore.write_chunk(data, target.drive_id, tier_str, write_opts)
    else
      case :rpc.call(
             target.node,
             BlobStore,
             :write_chunk,
             [data, target.drive_id, tier_str, write_opts],
             10_000
           ) do
        {:ok, hash, chunk_info} -> {:ok, hash, chunk_info}
        {:error, reason} -> {:error, {:remote_write_failed, target.node, reason}}
        {:badrpc, reason} -> {:error, {:rpc_failed, target.node, reason}}
      end
    end
  end

  defp default_target do
    %{node: node(), drive_id: "default"}
  end

  defp build_stripe(
         stripe_id,
         volume,
         chunk_hashes,
         chunk_size,
         partial,
         data_bytes,
         padded_bytes
       ) do
    Stripe.new(%{
      id: stripe_id,
      volume_id: volume.id,
      config: %{
        data_chunks: volume.durability.data_chunks,
        parity_chunks: volume.durability.parity_chunks,
        chunk_size: chunk_size
      },
      chunks: chunk_hashes,
      partial: partial,
      data_bytes: data_bytes,
      padded_bytes: padded_bytes
    })
  end

  defp store_stripe_metadata(stripe, _stripe_id, _write_id) do
    case StripeIndex.put(stripe) do
      {:ok, _id} -> :ok
      {:error, reason} -> Logger.warning("Failed to store stripe metadata: #{inspect(reason)}")
    end
  end

  defp emit_stripe_telemetry(stripe_id, volume, partial, all_chunks) do
    :telemetry.execute(
      [:neonfs, :write_operation, :stripe_created],
      %{chunk_count: length(all_chunks)},
      %{
        stripe_id: stripe_id,
        volume_id: volume.id,
        data_chunks: volume.durability.data_chunks,
        parity_chunks: volume.durability.parity_chunks,
        partial: partial
      }
    )
  end

  defp collect_all_stripe_chunks(stripe_results) do
    Enum.flat_map(stripe_results, & &1.chunks)
  end

  defp create_erasure_file_metadata(volume_id, path, stripe_results, data, opts) do
    # Handle file overwrite
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, existing_file} ->
        evict_resolved_cache(existing_file.id)
        FileIndex.delete(existing_file.id)

      {:error, :not_found} ->
        :ok
    end

    # Build stripe references with byte ranges
    stripes =
      Enum.map(stripe_results, fn sr ->
        %{stripe_id: sr.stripe_id, byte_range: {sr.byte_offset, sr.byte_offset + sr.data_bytes}}
      end)

    file_opts = [chunks: [], size: byte_size(data)]

    file_opts =
      case Keyword.fetch(opts, :mode) do
        {:ok, mode} -> Keyword.put(file_opts, :mode, mode)
        :error -> file_opts
      end

    file_meta = FileMeta.new(volume_id, path, file_opts)
    file_meta = %{file_meta | stripes: stripes}

    case FileIndex.create(file_meta) do
      {:ok, stored_meta} -> {:ok, stored_meta}
      {:error, _reason} = error -> error
    end
  end

  defp validate_erasure_commit(stripe_results, data) do
    total_data_bytes = Enum.sum(Enum.map(stripe_results, & &1.data_bytes))
    file_size = byte_size(data)

    if total_data_bytes == file_size do
      :ok
    else
      {:error, {:data_bytes_mismatch, expected: file_size, got: total_data_bytes}}
    end
  end

  defp abort_stripes(_write_id) do
    # Stripe cleanup happens via the existing chunk abort mechanism.
    # StripeIndex entries for uncommitted stripes will be cleaned up
    # by the garbage collector (task 0063).
    :ok
  end

  # ─── Replicated Write Path (existing) ──────────────────────────────────

  defp chunk_and_store(data, volume, write_id, opts) do
    chunk_strategy = resolve_chunk_strategy(opts)

    case BlobStore.chunk_data(data, chunk_strategy) do
      {:ok, chunk_results} ->
        process_chunks(chunk_results, volume, write_id, opts)

      {:error, _reason} = error ->
        error
    end
  end

  defp resolve_chunk_strategy(opts) do
    case Keyword.get(opts, :chunk_strategy, :auto) do
      :auto -> :auto
      :single -> {:single}
      {:fixed, size} -> {:fixed, size}
      {:fastcdc, avg_size} -> {:fastcdc, avg_size}
      other -> other
    end
  end

  defp process_chunks(chunk_results, volume, write_id, opts) do
    compression_config = Keyword.get(opts, :compression, volume.compression)

    chunk_results
    |> Enum.with_index()
    |> Enum.reduce_while({:ok, []}, fn {{data, hash, offset, size}, index}, {:ok, acc} ->
      case process_chunk(data, hash, offset, size, index, compression_config, volume, write_id) do
        {:ok, chunk_info} ->
          {:cont, {:ok, [chunk_info | acc]}}

        {:error, reason} ->
          Logger.debug("process_chunks: chunk processing failed with #{inspect(reason)}")
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
      error -> error
    end
  end

  defp process_chunk(data, hash, offset, size, index, compression_config, volume, write_id) do
    case ChunkIndex.get(hash) do
      {:ok, existing_chunk} ->
        case ChunkIndex.add_write_ref(hash, write_id) do
          :ok ->
            {:ok,
             %{
               hash: hash,
               offset: offset,
               size: size,
               stored_size: existing_chunk.stored_size,
               compression: existing_chunk.compression,
               index: index,
               new: false
             }}

          {:error, reason} ->
            {:error, {:add_write_ref_failed, reason}}
        end

      {:error, :not_found} ->
        store_new_chunk(data, hash, offset, size, index, compression_config, volume, write_id)
    end
  end

  defp store_new_chunk(data, hash, offset, size, index, compression_config, volume, write_id) do
    {_should_compress, compression} = should_compress_chunk?(size, compression_config)
    write_opts = build_write_opts(compression)

    tier = volume.tiering.initial_tier
    tier_str = Atom.to_string(tier)

    with {:ok, drive} <- select_drive_for_tier(tier),
         {:ok, _returned_hash, chunk_info} <-
           BlobStore.write_chunk(data, drive.id, tier_str, write_opts),
         chunk_meta <- build_chunk_meta(hash, size, chunk_info, drive.id, volume, write_id),
         :ok <- ChunkIndex.put(chunk_meta) do
      maybe_replicate_chunk(hash, data, volume, tier)
      build_chunk_result(hash, offset, size, chunk_info, index)
    else
      {:error, reason} when is_tuple(reason) -> {:error, reason}
      {:error, reason} -> {:error, {:chunk_index_failed, reason}}
    end
  end

  # ─── Shared Helpers ─────────────────────────────────────────────────────

  defp select_drive_for_tier(tier) do
    case DriveRegistry.select_drive(tier) do
      {:ok, drive} -> {:ok, drive}
      {:error, :no_drives_in_tier} -> {:error, :no_drives_available}
    end
  end

  defp build_write_opts(:none), do: []
  defp build_write_opts({:zstd, level}), do: [compression: "zstd", compression_level: level]

  defp build_chunk_meta(hash, size, chunk_info, drive_id, volume, write_id) do
    %ChunkMeta{
      hash: hash,
      original_size: size,
      stored_size: chunk_info.stored_size,
      compression: parse_compression(chunk_info.compression),
      locations: [%{node: node(), drive_id: drive_id, tier: volume.tiering.initial_tier}],
      target_replicas: volume.durability.factor,
      commit_state: :uncommitted,
      active_write_refs: MapSet.new([write_id]),
      stripe_id: nil,
      stripe_index: nil,
      created_at: DateTime.utc_now(),
      last_verified: nil
    }
  end

  defp parse_compression("zstd"), do: :zstd
  defp parse_compression(_), do: :none

  defp maybe_replicate_chunk(hash, data, volume, tier) do
    if volume.durability.factor > 1 do
      case Replication.replicate_chunk(hash, data, volume, tier: tier) do
        {:ok, _locations} -> :ok
        {:error, reason} -> Logger.warning("Chunk replication failed: #{inspect(reason)}")
      end
    end
  end

  defp build_chunk_result(hash, offset, size, chunk_info, index) do
    {:ok,
     %{
       hash: hash,
       offset: offset,
       size: size,
       stored_size: chunk_info.stored_size,
       compression: parse_compression(chunk_info.compression),
       index: index,
       new: true
     }}
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

  defp create_file_metadata(volume_id, path, chunks, data, opts) do
    sorted_chunks = Enum.sort_by(chunks, & &1.offset)
    chunk_hashes = Enum.map(sorted_chunks, & &1.hash)

    case FileIndex.get_by_path(volume_id, path) do
      {:ok, existing_file} ->
        evict_resolved_cache(existing_file.id)
        FileIndex.delete(existing_file.id)

      {:error, :not_found} ->
        :ok
    end

    file_opts = [chunks: chunk_hashes, size: byte_size(data)]

    file_opts =
      case Keyword.fetch(opts, :mode) do
        {:ok, mode} -> Keyword.put(file_opts, :mode, mode)
        :error -> file_opts
      end

    file_meta = FileMeta.new(volume_id, path, file_opts)

    case FileIndex.create(file_meta) do
      {:ok, stored_meta} -> {:ok, stored_meta}
      {:error, _reason} = error -> error
    end
  end

  defp commit_chunks(write_id, chunks) do
    results =
      Enum.map(chunks, fn chunk ->
        ChunkIndex.remove_write_ref(chunk.hash, write_id)
      end)

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
    uncommitted = ChunkIndex.list_uncommitted()

    to_abort =
      Enum.filter(uncommitted, fn meta ->
        MapSet.member?(meta.active_write_refs, write_id)
      end)

    Enum.each(to_abort, &abort_single_chunk(&1, write_id))
    :ok
  end

  defp abort_single_chunk(meta, write_id) do
    ChunkIndex.remove_write_ref(meta.hash, write_id)

    case ChunkIndex.get(meta.hash) do
      {:ok, updated_meta} -> delete_chunk_if_no_refs(updated_meta)
      {:error, :not_found} -> :ok
    end
  end

  defp delete_chunk_if_no_refs(meta) do
    if MapSet.size(meta.active_write_refs) == 0 do
      delete_chunk_from_storage(meta)
      ChunkIndex.delete(meta.hash)
    end
  end

  defp delete_chunk_from_storage(meta) do
    case meta.locations do
      [location | _] ->
        drive_id = Map.get(location, :drive_id, "default")
        BlobStore.delete_chunk(meta.hash, drive_id)

      [] ->
        :ok
    end
  end

  defp update_volume_stats(volume_id, data, chunks) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} ->
        new_chunks = Enum.filter(chunks, & &1.new)
        new_logical_size = volume.logical_size + byte_size(data)

        new_physical_size =
          volume.physical_size + Enum.sum(Enum.map(new_chunks, & &1.stored_size))

        new_chunk_count = volume.chunk_count + length(new_chunks)

        case VolumeRegistry.update_stats(volume_id,
               logical_size: new_logical_size,
               physical_size: new_physical_size,
               chunk_count: new_chunk_count
             ) do
          {:ok, _updated} -> :ok
          {:error, reason} -> {:error, {:stats_update_failed, reason}}
        end

      {:error, reason} ->
        {:error, {:volume_lookup_failed, reason}}
    end
  end

  defp evict_resolved_cache(file_id) do
    ResolvedLookupCache.evict(file_id)
  rescue
    _ -> :ok
  end

  defp emit_completion_telemetry(result, duration, volume_id, path, write_id, data) do
    case result do
      {:ok, file_meta} ->
        chunk_count = length(file_meta.chunks || []) + length(file_meta.stripes || [])

        :telemetry.execute(
          [:neonfs, :write_operation, :stop],
          %{duration: duration, bytes: byte_size(data), chunks: chunk_count},
          %{volume_id: volume_id, path: path, write_id: write_id}
        )

      {:error, reason} ->
        :telemetry.execute(
          [:neonfs, :write_operation, :exception],
          %{duration: duration},
          %{volume_id: volume_id, path: path, write_id: write_id, error: reason}
        )
    end
  end
end
