defmodule NeonFS.Core.WriteOperation do
  @moduledoc """
  Handles write operations for NeonFS.

  Implements the write path that takes file data, chunks it, stores chunks in the blob store,
  and creates file metadata. Supports both replicated and erasure-coded volumes.

  For replicated volumes, each chunk is stored and replicated to N nodes.
  For erasure-coded volumes, chunks are grouped into stripes, parity is computed,
  and all chunks (data + parity) are distributed across nodes.

  For encrypted volumes (`encryption.mode == :server_side`), the current volume key
  is fetched once per write and a unique 96-bit nonce is generated per chunk. The key
  and nonce are passed to the BlobStore NIF which handles compress → encrypt → write
  in a single call (no separate encryption NIF boundary crossing).
  """

  alias NeonFS.Client.Router

  alias NeonFS.Core.{
    Authorise,
    Blob.Native,
    BlobStore,
    ChunkCrypto,
    ChunkFetcher,
    ChunkIndex,
    ChunkMeta,
    DriveRegistry,
    FileIndex,
    FileMeta,
    KeyManager,
    LockManager,
    Replication,
    ResolvedLookupCache,
    Stripe,
    StripeIndex,
    StripePlacement,
    Volume,
    VolumeRegistry
  }

  alias NeonFS.IO.{Operation, Scheduler}

  alias NeonFS.Error.{Unavailable, VolumeNotFound}

  require Logger

  @type write_id :: binary()
  @type chunk_info :: %{
          hash: binary(),
          offset: non_neg_integer(),
          size: non_neg_integer(),
          stored_size: non_neg_integer(),
          compression: :none | :zstd
        }

  # Encryption context resolved once per write_file call.
  # nil for unencrypted volumes, map with key material for encrypted ones.
  @type encryption_ctx ::
          %{key: binary(), key_version: pos_integer()} | nil

  @doc """
  Writes file data to a volume with chunking and deduplication.

  Branches on `volume.durability.type`:
  - `:replicate` — existing replicated write path
  - `:erasure` — stripe-based erasure-coded write path

  ## Options
    * `:chunk_strategy` - Override volume's default chunking strategy
    * `:compression` - Override volume's compression settings
    * `:block_on_lock` - When `true`, wait for conflicting locks to be
      released instead of returning `{:error, :lock_conflict}` or
      `{:error, :share_denied}` immediately (default: `false`)
    * `:block_on_lock_timeout` - How long to wait in milliseconds when
      `:block_on_lock` is `true` (default: 5_000)

  ## Returns
    * `{:ok, file_meta}` - File successfully written
    * `{:error, reason}` - Write failed
  """
  @spec write_file(binary(), String.t(), binary(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, term()}
  def write_file(volume_id, path, data, opts \\ []) do
    Logger.metadata(component: :write, volume_id: volume_id, file_path: path)

    write_id = generate_write_id()
    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :write_operation, :start],
      %{bytes: byte_size(data)},
      %{volume_id: volume_id, path: path, write_id: write_id}
    )

    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    client_ref = Keyword.get(opts, :client_ref)

    result =
      with {:ok, volume} <- get_volume(volume_id),
           :ok <- Authorise.check(uid, gids, :write, {:volume, volume_id}),
           :ok <- check_lock(volume_id, path, client_ref, {0, byte_size(data)}, opts) do
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
  Writes data at a specific byte offset within an existing file.

  Instead of replacing the entire file, only the chunks (or stripes) overlapping
  the write range are read, modified, and re-stored. Chunks outside the write
  range are carried forward by hash reference without any I/O.

  Falls back to `write_file/4` when the file doesn't exist yet or has no chunks.

  ## Parameters

    * `volume_id` - Volume identifier
    * `path` - File path within the volume
    * `offset` - Byte offset to write at
    * `data` - Binary data to write
    * `opts` - Options (same as `write_file/4`, including `:block_on_lock`)
  """
  @spec write_file_at(binary(), String.t(), non_neg_integer(), binary(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, term()}
  def write_file_at(volume_id, path, offset, data, opts \\ []) do
    Logger.metadata(component: :write, volume_id: volume_id, file_path: path)

    write_id = generate_write_id()
    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :write_operation, :start],
      %{bytes: byte_size(data)},
      %{volume_id: volume_id, path: path, write_id: write_id, offset: offset}
    )

    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])
    client_ref = Keyword.get(opts, :client_ref)

    result =
      with {:ok, volume} <- get_volume(volume_id),
           :ok <- Authorise.check(uid, gids, :write, {:volume, volume_id}),
           :ok <- check_lock(volume_id, path, client_ref, {offset, byte_size(data)}, opts) do
        case FileIndex.get_by_path(volume_id, path) do
          {:ok, file_meta} ->
            do_write_at(volume, file_meta, offset, data, write_id, opts)

          {:error, :not_found} ->
            write_new_file_at_offset(volume, path, offset, data, write_id, opts)
        end
      end

    case result do
      {:ok, file_meta} -> evict_resolved_cache(file_meta.id)
      _ -> :ok
    end

    duration = System.monotonic_time() - start_time
    emit_completion_telemetry(result, duration, volume_id, path, write_id, data)
    result
  end

  @doc """
  Streams file data to a volume, chunking and storing each chunk as it
  arrives instead of buffering the whole binary in memory.

  Accepts an `Enumerable.t()` of binary segments — empty stream creates
  an empty file. The peak working set is bounded by the strategy's
  maximum chunk size, so multi-gigabyte streams complete without
  OOMing the core node.

  Currently supports replicated volumes only; erasure-coded volumes
  return `{:error, :streaming_writes_not_supported_for_erasure}` until
  streaming erasure encoding lands.

  Options match `write_file/4`.
  """
  @spec write_file_streamed(binary(), String.t(), Enumerable.t(), keyword()) ::
          {:ok, FileMeta.t()} | {:error, term()}
  def write_file_streamed(volume_id, path, stream, opts \\ []) do
    Logger.metadata(component: :write, volume_id: volume_id, file_path: path)

    write_id = generate_write_id()
    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :write_operation, :start],
      %{bytes: 0},
      %{volume_id: volume_id, path: path, write_id: write_id, streamed: true}
    )

    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])
    client_ref = Keyword.get(opts, :client_ref)

    result =
      with {:ok, volume} <- get_volume(volume_id),
           :ok <- Authorise.check(uid, gids, :write, {:volume, volume_id}),
           # Streaming writes don't know the byte range up front; lock check
           # uses {0, 0} which still validates exclusive/share semantics.
           :ok <- check_lock(volume_id, path, client_ref, {0, 0}, opts) do
        do_write_streamed(volume, path, stream, write_id, opts)
      end

    case result do
      {:ok, file_meta} -> evict_resolved_cache(file_meta.id)
      _ -> :ok
    end

    duration = System.monotonic_time() - start_time
    emit_streamed_completion_telemetry(result, duration, volume_id, path, write_id)
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

  defp write_new_file_at_offset(volume, path, 0, data, write_id, opts) do
    do_write(volume, path, data, write_id, opts)
  end

  defp write_new_file_at_offset(volume, path, offset, data, write_id, opts) do
    padded = :binary.copy(<<0>>, offset) <> data
    do_write(volume, path, padded, write_id, opts)
  end

  defp do_write(%{durability: %{type: :erasure}} = volume, path, data, write_id, opts) do
    with {:ok, enc_ctx} <- resolve_encryption(volume) do
      write_ctx = %{write_id: write_id, enc_ctx: enc_ctx}

      case erasure_write(volume, path, data, write_ctx, opts) do
        {:ok, _file_meta} = ok ->
          ok

        {:error, _reason} = error ->
          abort_chunks(write_id)
          abort_stripes(write_id)
          error
      end
    end
  end

  defp do_write(volume, path, data, write_id, opts) do
    with {:ok, enc_ctx} <- resolve_encryption(volume) do
      write_ctx = %{write_id: write_id, enc_ctx: enc_ctx}

      with {:ok, chunks} <- chunk_and_store(data, volume, write_ctx, opts),
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
  end

  defp do_write_streamed(%{durability: %{type: :erasure}}, _path, _stream, _write_id, _opts) do
    {:error, :streaming_writes_not_supported_for_erasure}
  end

  defp do_write_streamed(volume, path, stream, write_id, opts) do
    with {:ok, enc_ctx} <- resolve_encryption(volume) do
      write_ctx = %{write_id: write_id, enc_ctx: enc_ctx}
      strategy = resolve_chunk_strategy(opts)
      compression_config = Keyword.get(opts, :compression, volume.compression)

      with {:ok, chunker} <- BlobStore.chunker_init(strategy),
           {:ok, chunks, total_bytes} <-
             stream_chunks(stream, chunker, compression_config, volume, write_ctx),
           {:ok, file_meta} <-
             create_file_metadata_with_size(volume.id, path, chunks, total_bytes, opts),
           :ok <- commit_chunks(write_id, chunks),
           :ok <- update_volume_stats_with_size(volume.id, total_bytes, chunks) do
        {:ok, file_meta}
      else
        {:error, _reason} = error ->
          abort_chunks(write_id)
          error
      end
    end
  end

  defp stream_chunks(stream, chunker, compression_config, volume, write_ctx) do
    init_acc = {:ok, [], 0}

    fed =
      Enum.reduce_while(stream, init_acc, fn segment, {:ok, acc, index} ->
        emitted = BlobStore.chunker_feed(chunker, segment)

        case process_streamed_chunks(emitted, index, compression_config, volume, write_ctx) do
          {:ok, processed} ->
            {:cont, {:ok, acc ++ processed, index + length(processed)}}

          {:error, _} = err ->
            {:halt, err}
        end
      end)

    with {:ok, acc, index} <- fed,
         tail = BlobStore.chunker_finish(chunker),
         {:ok, processed} <-
           process_streamed_chunks(tail, index, compression_config, volume, write_ctx) do
      all = acc ++ processed
      {:ok, all, total_bytes_for_chunks(all)}
    end
  end

  defp process_streamed_chunks([], _base_index, _compression, _volume, _write_ctx), do: {:ok, []}

  defp process_streamed_chunks(raw_chunks, base_index, compression_config, volume, write_ctx) do
    raw_chunks
    |> Enum.with_index(base_index)
    |> Enum.reduce_while({:ok, []}, fn {{data, hash, offset, size}, index}, {:ok, acc} ->
      case process_chunk(data, hash, offset, size, index, compression_config, volume, write_ctx) do
        {:ok, chunk_info} ->
          {:cont, {:ok, [chunk_info | acc]}}

        {:error, _reason} = err ->
          {:halt, err}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
      err -> err
    end
  end

  defp total_bytes_for_chunks([]), do: 0

  defp total_bytes_for_chunks(chunks) do
    last = List.last(chunks)
    last.offset + last.size
  end

  defp get_volume(volume_id) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, VolumeNotFound.exception(volume_id: volume_id)}
    end
  end

  defp check_lock(_volume_id, _path, nil, _range, _opts), do: :ok

  defp check_lock(volume_id, path, client_ref, range, opts) do
    lock_file_id = lock_file_id(volume_id, path)

    if Keyword.get(opts, :block_on_lock, false) do
      timeout = Keyword.get(opts, :block_on_lock_timeout, 5_000)
      LockManager.check_write_blocking(lock_file_id, client_ref, range, timeout: timeout)
    else
      LockManager.check_write(lock_file_id, client_ref, range)
    end
  end

  @doc """
  Returns the lock file ID for a given volume and path.

  Protocol adapters must use this same key when acquiring locks so that
  the write pipeline's lock check matches.
  """
  @spec lock_file_id(binary(), String.t()) :: binary()
  def lock_file_id(volume_id, path), do: "#{volume_id}:#{path}"

  # ─── Offset Write (replicated) ─────────────────────────────────────────

  defp do_write_at(
         %{durability: %{type: :erasure}} = volume,
         file_meta,
         offset,
         data,
         write_id,
         opts
       ) do
    with {:ok, enc_ctx} <- resolve_encryption(volume) do
      write_ctx = %{write_id: write_id, enc_ctx: enc_ctx}
      erasure_write_at(volume, file_meta, offset, data, write_ctx, opts)
    end
  end

  defp do_write_at(volume, file_meta, offset, data, write_id, opts) do
    write_end = offset + byte_size(data)
    chunk_positions = build_chunk_info_list(file_meta.chunks, 0, [])
    {prefix, affected, suffix} = partition_chunks(chunk_positions, offset, write_end)

    with {:ok, enc_ctx} <- resolve_encryption(volume),
         write_ctx = %{write_id: write_id, enc_ctx: enc_ctx},
         {:ok, modified_data} <-
           splice_affected_chunks(affected, offset, data, write_end, volume.id),
         new_data = maybe_pad_before(modified_data, offset, file_meta.size, chunk_positions),
         {:ok, new_chunks} <- chunk_and_store(new_data, volume, write_ctx, opts) do
      prefix_hashes = Enum.map(prefix, fn {hash, _start, _end} -> hash end)
      suffix_hashes = Enum.map(suffix, fn {hash, _start, _end} -> hash end)
      new_hashes = Enum.map(Enum.sort_by(new_chunks, & &1.offset), & &1.hash)

      all_hashes = prefix_hashes ++ new_hashes ++ suffix_hashes
      new_size = max(file_meta.size, write_end)

      update_file_and_commit(file_meta.id, write_id, new_chunks,
        chunks: all_hashes,
        size: new_size
      )
    else
      {:error, _reason} = error ->
        abort_chunks(write_id)
        error
    end
  end

  defp build_chunk_info_list([], _offset, acc), do: Enum.reverse(acc)

  defp build_chunk_info_list([hash | rest], current_offset, acc) do
    case ChunkIndex.get(hash) do
      {:ok, chunk_meta} ->
        chunk_end = current_offset + chunk_meta.original_size
        build_chunk_info_list(rest, chunk_end, [{hash, current_offset, chunk_end} | acc])

      {:error, :not_found} ->
        Logger.error("Chunk metadata not found during offset write",
          chunk_hash: Base.encode16(hash)
        )

        build_chunk_info_list(rest, current_offset, acc)
    end
  end

  defp partition_chunks(chunk_positions, write_start, write_end) do
    prefix = Enum.filter(chunk_positions, fn {_h, _s, e} -> e <= write_start end)
    suffix = Enum.filter(chunk_positions, fn {_h, s, _e} -> s >= write_end end)

    affected =
      Enum.filter(chunk_positions, fn {_h, s, e} ->
        s < write_end and e > write_start
      end)

    {prefix, affected, suffix}
  end

  defp splice_affected_chunks([], offset, data, _write_end, _volume_id) do
    if offset > 0 do
      {:ok, :binary.copy(<<0>>, offset) <> data}
    else
      {:ok, data}
    end
  end

  defp splice_affected_chunks(affected, offset, data, write_end, volume_id) do
    first_chunk_start = affected |> List.first() |> elem(1)

    chunk_data_results =
      Enum.map(affected, fn {hash, _start, _end} ->
        fetch_chunk_for_write(hash, volume_id)
      end)

    if Enum.all?(chunk_data_results, &match?({:ok, _}, &1)) do
      existing_data =
        chunk_data_results
        |> Enum.map(fn {:ok, d} -> d end)
        |> IO.iodata_to_binary()

      splice_start = offset - first_chunk_start
      splice_end = write_end - first_chunk_start

      before_splice =
        if splice_start > 0 do
          binary_part(existing_data, 0, splice_start)
        else
          <<>>
        end

      after_splice =
        if splice_end < byte_size(existing_data) do
          binary_part(existing_data, splice_end, byte_size(existing_data) - splice_end)
        else
          <<>>
        end

      result = before_splice <> data <> after_splice

      if first_chunk_start < offset do
        {:ok, result}
      else
        {:ok, result}
      end
    else
      Enum.find(chunk_data_results, &match?({:error, _}, &1)) ||
        {:error, :chunk_fetch_failed}
    end
  end

  defp maybe_pad_before(data, _offset, _file_size, _chunk_positions), do: data

  defp fetch_chunk_for_write(hash, volume_id) do
    case ChunkIndex.get(hash) do
      {:ok, chunk_meta} ->
        fetch_opts = build_chunk_fetch_opts(chunk_meta, volume_id)

        case ChunkFetcher.fetch_chunk(hash, fetch_opts) do
          {:ok, data, _source} -> {:ok, data}
          {:error, _} = error -> error
        end

      {:error, :not_found} ->
        {:error, :chunk_not_found}
    end
  end

  defp build_chunk_fetch_opts(chunk_meta, volume_id) do
    {tier, drive_id} = resolve_chunk_location(chunk_meta.locations)

    needs_decompress =
      chunk_meta.compression != :none or
        (chunk_meta.crypto == nil and chunk_meta.stored_size != chunk_meta.original_size)

    base_opts = [
      tier: tier,
      drive_id: drive_id,
      verify: false,
      decompress: needs_decompress,
      compression: chunk_meta.compression,
      volume_id: volume_id
    ]

    base_opts ++ resolve_decrypt_opts(chunk_meta.crypto, volume_id)
  end

  defp resolve_chunk_location([location | _]) do
    {Atom.to_string(location.tier), Map.get(location, :drive_id, "default")}
  end

  defp resolve_chunk_location([]), do: {"hot", "default"}

  defp resolve_decrypt_opts(nil, _volume_id), do: []

  defp resolve_decrypt_opts(crypto, volume_id) do
    case KeyManager.get_volume_key(volume_id, crypto.key_version) do
      {:ok, key} -> [key: key, nonce: crypto.nonce]
      {:error, _} -> []
    end
  end

  # ─── Offset Write (erasure-coded) ──────────────────────────────────────

  defp erasure_write_at(volume, file_meta, offset, data, write_ctx, opts) do
    write_end = offset + byte_size(data)

    stripe_refs = file_meta.stripes || []

    {prefix_stripes, affected_stripes, suffix_stripes} =
      partition_stripes(stripe_refs, offset, write_end)

    case rewrite_affected_stripes(
           affected_stripes,
           offset,
           data,
           write_end,
           volume,
           write_ctx,
           opts
         ) do
      {:ok, new_stripe_results} ->
        all_new_chunks = Enum.flat_map(new_stripe_results, fn sr -> sr.chunks end)

        new_stripe_refs =
          Enum.map(new_stripe_results, fn sr ->
            %{stripe_id: sr.stripe_id, byte_range: sr.byte_range}
          end)

        new_size = max(file_meta.size, write_end)
        all_stripes = prefix_stripes ++ new_stripe_refs ++ suffix_stripes

        update_file_and_commit(
          file_meta.id,
          write_ctx.write_id,
          all_new_chunks,
          stripes: all_stripes,
          size: new_size
        )

      {:error, _reason} = error ->
        abort_chunks(write_ctx.write_id)
        error
    end
  end

  defp partition_stripes(stripe_refs, write_start, write_end) do
    prefix =
      Enum.filter(stripe_refs, fn %{byte_range: {_s, e}} -> e <= write_start end)

    suffix =
      Enum.filter(stripe_refs, fn %{byte_range: {s, _e}} -> s >= write_end end)

    affected =
      Enum.filter(stripe_refs, fn %{byte_range: {s, e}} ->
        s < write_end and e > write_start
      end)

    {prefix, affected, suffix}
  end

  defp rewrite_affected_stripes(
         affected_stripes,
         offset,
         data,
         write_end,
         volume,
         write_ctx,
         opts
       ) do
    results =
      Enum.reduce_while(affected_stripes, {:ok, []}, fn stripe_ref, {:ok, acc} ->
        case rewrite_single_stripe(stripe_ref, offset, data, write_end, volume, write_ctx, opts) do
          {:ok, stripe_result} -> {:cont, {:ok, [stripe_result | acc]}}
          {:error, _} = error -> {:halt, error}
        end
      end)

    case results do
      {:ok, list} -> {:ok, Enum.reverse(list)}
      error -> error
    end
  end

  defp rewrite_single_stripe(stripe_ref, offset, data, write_end, volume, write_ctx, opts) do
    %{stripe_id: old_stripe_id, byte_range: {stripe_start, stripe_end}} = stripe_ref

    case StripeIndex.get(old_stripe_id) do
      {:ok, stripe} ->
        data_chunk_count = stripe.config.data_chunks
        data_hashes = Enum.take(stripe.chunks, data_chunk_count)

        with {:ok, stripe_data} <- read_stripe_data(data_hashes, volume.id, stripe.data_bytes),
             modified <-
               splice_stripe_data(stripe_data, data, offset, write_end, stripe_start, stripe_end),
             {:ok, chunk_results} <- chunk_data(modified, opts),
             {:ok, [stripe_result]} <-
               build_and_store_stripes(chunk_results, volume, write_ctx, opts) do
          new_byte_range = {stripe_start, stripe_start + stripe_result.data_bytes}
          {:ok, %{stripe_result | byte_range: new_byte_range}}
        end

      {:error, _} = error ->
        error
    end
  end

  defp splice_stripe_data(stripe_data, data, offset, write_end, stripe_start, stripe_end) do
    splice_start = max(0, offset - stripe_start)
    splice_end = min(stripe_end - stripe_start, write_end - stripe_start)
    data_offset_in_write = max(0, stripe_start - offset)
    data_len = splice_end - splice_start

    new_data_slice = binary_part(data, data_offset_in_write, data_len)

    before = if splice_start > 0, do: binary_part(stripe_data, 0, splice_start), else: <<>>

    after_data =
      if splice_end < byte_size(stripe_data) do
        binary_part(stripe_data, splice_end, byte_size(stripe_data) - splice_end)
      else
        <<>>
      end

    before <> new_data_slice <> after_data
  end

  defp read_stripe_data(data_hashes, volume_id, data_bytes) do
    results = Enum.map(data_hashes, fn hash -> fetch_chunk_for_write(hash, volume_id) end)

    if Enum.all?(results, &match?({:ok, _}, &1)) do
      full_data =
        results
        |> Enum.map(fn {:ok, d} -> d end)
        |> IO.iodata_to_binary()

      {:ok, binary_part(full_data, 0, min(data_bytes, byte_size(full_data)))}
    else
      Enum.find(results, &match?({:error, _}, &1)) ||
        {:error, :chunk_fetch_failed}
    end
  end

  # ─── Encryption ────────────────────────────────────────────────────────

  @spec resolve_encryption(Volume.t()) :: {:ok, encryption_ctx()} | {:error, term()}
  defp resolve_encryption(volume) do
    if Volume.encrypted?(volume) do
      case KeyManager.get_current_key(volume.id) do
        {:ok, {key, key_version}} ->
          {:ok, %{key: key, key_version: key_version}}

        {:error, _reason} = error ->
          error
      end
    else
      {:ok, nil}
    end
  end

  defp generate_chunk_nonce, do: :crypto.strong_rand_bytes(12)

  defp add_encryption_write_opts(write_opts, nil), do: write_opts

  defp add_encryption_write_opts(write_opts, %{key: key}) do
    nonce = generate_chunk_nonce()
    write_opts ++ [key: key, nonce: nonce]
  end

  defp build_chunk_crypto(nil, _write_opts), do: nil

  defp build_chunk_crypto(%{key_version: key_version}, write_opts) do
    nonce = Keyword.fetch!(write_opts, :nonce)
    ChunkCrypto.new(nonce: nonce, key_version: key_version)
  end

  defp emit_encrypt_telemetry(nil, _hash, _volume_id), do: :ok

  defp emit_encrypt_telemetry(%{key_version: key_version}, hash, volume_id) do
    :telemetry.execute(
      [:neonfs, :write, :encrypt],
      %{},
      %{chunk_hash: hash, volume_id: volume_id, key_version: key_version}
    )
  end

  # ─── Erasure-Coded Write Path ───────────────────────────────────────────

  defp erasure_write(volume, path, data, write_ctx, opts) do
    with {:ok, chunk_results} <- chunk_data(data, opts),
         {:ok, stripe_results} <-
           build_and_store_stripes(chunk_results, volume, write_ctx, opts),
         all_chunks = collect_all_stripe_chunks(stripe_results),
         {:ok, file_meta} <-
           create_erasure_file_metadata(volume.id, path, stripe_results, data, opts),
         :ok <- commit_chunks(write_ctx.write_id, all_chunks),
         :ok <- validate_erasure_commit(stripe_results, data),
         :ok <- update_volume_stats(volume.id, data, all_chunks) do
      {:ok, file_meta}
    end
  end

  defp chunk_data(data, opts) do
    chunk_strategy = resolve_chunk_strategy(opts)
    BlobStore.chunk_data(data, chunk_strategy)
  end

  defp build_and_store_stripes(chunk_results, volume, write_ctx, opts) do
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
             write_ctx,
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
         write_ctx,
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

    store_ctx = %{
      stripe_id: stripe_id,
      compression_config: compression_config,
      tier: tier,
      tier_str: tier_str,
      write_id: write_ctx.write_id,
      encryption_ctx: write_ctx.enc_ctx,
      volume_id: volume.id
    }

    with {:ok, parity_shards} <- Native.erasure_encode(padded_data_list, parity_count),
         {:ok, targets} <- StripePlacement.select_targets(stripe_config, tier: tier),
         data_targets = Enum.take(targets, length(padded)),
         parity_targets = Enum.drop(targets, length(padded)),
         {:ok, stored_data_chunks} <-
           store_data_chunks(padded, data_targets, store_ctx),
         {:ok, stored_parity_chunks} <-
           store_parity_chunks(
             parity_shards,
             data_chunks_per_stripe,
             parity_targets,
             store_ctx
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

      store_stripe_metadata(stripe, stripe_id, write_ctx.write_id)

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

  defp store_data_chunks(data_entries, targets, ctx) do
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
    write_opts = add_encryption_write_opts(write_opts, ctx.encryption_ctx)
    # Use padded data size as original_size so decompression detection works correctly.
    # The pre-padding `size` is only used for compression min_size threshold.
    padded_size = byte_size(data)

    with {:ok, hash, chunk_info} <-
           write_chunk_to_target(data, target, ctx.tier_str, write_opts, ctx.volume_id) do
      crypto = build_chunk_crypto(ctx.encryption_ctx, write_opts)
      emit_encrypt_telemetry(ctx.encryption_ctx, hash, ctx.volume_id)

      meta_args = %{
        hash: hash,
        size: padded_size,
        chunk_info: chunk_info,
        target: target,
        tier: ctx.tier,
        stripe_id: ctx.stripe_id,
        stripe_idx: stripe_idx,
        write_id: ctx.write_id,
        crypto: crypto
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

  defp store_parity_chunks(parity_shards, data_chunk_count, targets, ctx) do
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
    write_opts = add_encryption_write_opts([], ctx.encryption_ctx)

    with {:ok, hash, chunk_info} <-
           write_chunk_to_target(parity_data, target, ctx.tier_str, write_opts, ctx.volume_id) do
      crypto = build_chunk_crypto(ctx.encryption_ctx, write_opts)
      emit_encrypt_telemetry(ctx.encryption_ctx, hash, ctx.volume_id)

      meta_args = %{
        hash: hash,
        size: size,
        chunk_info: chunk_info,
        target: target,
        tier: ctx.tier,
        stripe_id: ctx.stripe_id,
        stripe_idx: stripe_idx,
        write_id: ctx.write_id,
        crypto: crypto
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
      crypto: args.crypto,
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

  defp write_chunk_to_target(data, target, tier_str, write_opts, volume_id) do
    if target.node == node() do
      schedule_local_write(data, target.drive_id, tier_str, write_opts, volume_id)
    else
      write_chunk_to_remote(data, target, tier_str, write_opts)
    end
  end

  defp write_chunk_to_remote(data, target, tier_str, write_opts) do
    hash = Native.compute_hash(data)

    case Router.data_call(target.node, :put_chunk,
           hash: hash,
           volume_id: target.drive_id,
           write_id: nil,
           tier: tier_str,
           data: data
         ) do
      result when result in [:ok, {:error, :already_exists}] ->
        chunk_info = %{
          original_size: byte_size(data),
          stored_size: byte_size(data),
          compression: "none"
        }

        {:ok, hash, chunk_info}

      {:error, :no_data_endpoint} ->
        Logger.info("No data endpoint, falling back to distribution RPC", node: target.node)

        write_chunk_to_remote_rpc(data, target, tier_str, write_opts)

      {:error, reason} ->
        {:error, {:remote_write_failed, target.node, reason}}
    end
  end

  defp write_chunk_to_remote_rpc(data, target, tier_str, write_opts) do
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

  defp schedule_local_write(data, drive_id, tier_str, write_opts, volume_id) do
    op =
      Operation.new(
        priority: :user_write,
        volume_id: volume_id,
        drive_id: drive_id,
        type: :write,
        callback: fn -> BlobStore.write_chunk(data, drive_id, tier_str, write_opts) end
      )

    Scheduler.submit_sync(op)
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
      {:error, reason} -> Logger.warning("Failed to store stripe metadata", reason: reason)
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

    file_opts = maybe_forward_opt(file_opts, opts, :content_type)
    file_opts = maybe_forward_opt(file_opts, opts, :metadata)
    file_opts = apply_uid_gid_opts(file_opts, opts)
    file_opts = maybe_inherit_default_acl(file_opts, volume_id, path)

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

  defp chunk_and_store(data, volume, write_ctx, opts) do
    chunk_strategy = resolve_chunk_strategy(opts)

    case BlobStore.chunk_data(data, chunk_strategy) do
      {:ok, chunk_results} ->
        process_chunks(chunk_results, volume, write_ctx, opts)

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

  @max_chunk_concurrency 8

  defp process_chunks(chunk_results, volume, write_ctx, opts) do
    compression_config = Keyword.get(opts, :compression, volume.compression)

    indexed_chunks = Enum.with_index(chunk_results)

    if length(indexed_chunks) <= 1 do
      process_chunks_sequential(indexed_chunks, compression_config, volume, write_ctx)
    else
      process_chunks_parallel(indexed_chunks, compression_config, volume, write_ctx)
    end
  end

  defp process_chunks_sequential(indexed_chunks, compression_config, volume, write_ctx) do
    indexed_chunks
    |> Enum.reduce_while({:ok, []}, fn {{data, hash, offset, size}, index}, {:ok, acc} ->
      case process_chunk(data, hash, offset, size, index, compression_config, volume, write_ctx) do
        {:ok, chunk_info} ->
          {:cont, {:ok, [chunk_info | acc]}}

        {:error, reason} ->
          Logger.debug("Chunk processing failed", reason: reason)
          {:halt, {:error, reason}}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
      error -> error
    end
  end

  defp process_chunks_parallel(indexed_chunks, compression_config, volume, write_ctx) do
    max_concurrency =
      Application.get_env(:neonfs_core, :chunk_write_concurrency, @max_chunk_concurrency)

    results =
      indexed_chunks
      |> Task.async_stream(
        fn {{data, hash, offset, size}, index} ->
          process_chunk(data, hash, offset, size, index, compression_config, volume, write_ctx)
        end,
        max_concurrency: max_concurrency,
        ordered: true,
        timeout: 30_000
      )
      |> Enum.reduce_while({:ok, []}, fn
        {:ok, {:ok, chunk_info}}, {:ok, acc} ->
          {:cont, {:ok, [chunk_info | acc]}}

        {:ok, {:error, reason}}, _acc ->
          Logger.debug("Chunk processing failed", reason: reason)
          {:halt, {:error, reason}}

        {:exit, reason}, _acc ->
          Logger.debug("Chunk processing task exited", reason: reason)
          {:halt, {:error, {:chunk_task_failed, reason}}}
      end)

    case results do
      {:ok, chunks} -> {:ok, Enum.reverse(chunks)}
      error -> error
    end
  end

  defp process_chunk(data, hash, offset, size, index, compression_config, volume, write_ctx) do
    case ChunkIndex.get(hash) do
      {:ok, existing_chunk} ->
        case ChunkIndex.add_write_ref(hash, write_ctx.write_id) do
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
        store_new_chunk(data, hash, offset, size, index, compression_config, volume, write_ctx)
    end
  end

  defp store_new_chunk(data, hash, offset, size, index, compression_config, volume, write_ctx) do
    {_should_compress, compression} = should_compress_chunk?(size, compression_config)
    write_opts = build_write_opts(compression)
    write_opts = add_encryption_write_opts(write_opts, write_ctx.enc_ctx)

    tier = volume.tiering.initial_tier
    tier_str = Atom.to_string(tier)

    with {:ok, drive} <- select_drive_for_tier(tier),
         {:ok, _returned_hash, chunk_info} <-
           schedule_local_write(data, drive.id, tier_str, write_opts, volume.id) do
      crypto = build_chunk_crypto(write_ctx.enc_ctx, write_opts)
      emit_encrypt_telemetry(write_ctx.enc_ctx, hash, volume.id)

      chunk_meta =
        build_chunk_meta(hash, size, chunk_info, drive.id, volume, write_ctx.write_id, crypto)

      case ChunkIndex.put(chunk_meta) do
        :ok ->
          maybe_replicate_chunk(hash, data, volume, tier)
          build_chunk_result(hash, offset, size, chunk_info, index)

        {:error, reason} ->
          {:error, {:chunk_index_failed, reason}}
      end
    else
      {:error, %{class: :unavailable}} = error -> error
      {:error, reason} -> {:error, {:chunk_write_failed, reason}}
    end
  end

  # ─── Shared Helpers ─────────────────────────────────────────────────────

  defp select_drive_for_tier(tier) do
    case DriveRegistry.select_drive(tier) do
      {:ok, drive} ->
        {:ok, drive}

      {:error, :no_drives_in_tier} ->
        {:error,
         Unavailable.exception(
           message: "No drives available in tier #{tier}",
           details: %{tier: tier}
         )}
    end
  end

  defp build_write_opts(:none), do: []
  defp build_write_opts({:zstd, level}), do: [compression: "zstd", compression_level: level]

  defp build_chunk_meta(hash, size, chunk_info, drive_id, volume, write_id, crypto) do
    %ChunkMeta{
      hash: hash,
      original_size: size,
      stored_size: chunk_info.stored_size,
      compression: parse_compression(chunk_info.compression),
      crypto: crypto,
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
  defp parse_compression("zstd:" <> _level), do: :zstd
  defp parse_compression(_), do: :none

  defp maybe_replicate_chunk(hash, data, volume, tier) do
    if volume.durability.factor > 1 do
      case Replication.replicate_chunk(hash, data, volume, tier: tier) do
        {:ok, _locations} -> :ok
        {:error, reason} -> Logger.warning("Chunk replication failed", reason: reason)
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
    create_file_metadata_with_size(volume_id, path, chunks, byte_size(data), opts)
  end

  defp create_file_metadata_with_size(volume_id, path, chunks, size, opts) do
    sorted_chunks = Enum.sort_by(chunks, & &1.offset)
    chunk_hashes = Enum.map(sorted_chunks, & &1.hash)

    case FileIndex.get_by_path(volume_id, path) do
      {:ok, existing_file} ->
        evict_resolved_cache(existing_file.id)
        FileIndex.delete(existing_file.id)

      {:error, :not_found} ->
        :ok
    end

    file_opts = [chunks: chunk_hashes, size: size]

    file_opts =
      case Keyword.fetch(opts, :mode) do
        {:ok, mode} -> Keyword.put(file_opts, :mode, mode)
        :error -> file_opts
      end

    file_opts = maybe_forward_opt(file_opts, opts, :content_type)
    file_opts = maybe_forward_opt(file_opts, opts, :metadata)
    file_opts = apply_uid_gid_opts(file_opts, opts)
    file_opts = maybe_inherit_default_acl(file_opts, volume_id, path)

    file_meta = FileMeta.new(volume_id, path, file_opts)

    case FileIndex.create(file_meta) do
      {:ok, stored_meta} -> {:ok, stored_meta}
      {:error, _reason} = error -> error
    end
  end

  defp update_file_and_commit(file_id, write_id, new_chunks, update_attrs) do
    now = DateTime.utc_now()
    attrs = update_attrs ++ [modified_at: now, changed_at: now]

    case FileIndex.update(file_id, attrs) do
      {:ok, updated_meta} ->
        commit_chunks(write_id, new_chunks)
        {:ok, updated_meta}

      {:error, _reason} = error ->
        abort_chunks(write_id)
        error
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
        BlobStore.delete_chunk(meta.hash, drive_id, BlobStore.codec_opts_for_chunk(meta))

      [] ->
        :ok
    end
  end

  defp update_volume_stats(volume_id, data, chunks) do
    update_volume_stats_with_size(volume_id, byte_size(data), chunks)
  end

  defp update_volume_stats_with_size(volume_id, size, chunks) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} ->
        new_chunks = Enum.filter(chunks, & &1.new)
        new_logical_size = volume.logical_size + size

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

  defp maybe_forward_opt(file_opts, opts, key) do
    case Keyword.fetch(opts, key) do
      {:ok, value} -> Keyword.put(file_opts, key, value)
      :error -> file_opts
    end
  end

  defp apply_uid_gid_opts(file_opts, opts) do
    file_opts =
      case Keyword.fetch(opts, :uid) do
        {:ok, uid} -> Keyword.put(file_opts, :uid, uid)
        :error -> file_opts
      end

    case Keyword.fetch(opts, :gid) do
      {:ok, gid} -> Keyword.put(file_opts, :gid, gid)
      :error -> file_opts
    end
  end

  defp maybe_inherit_default_acl(file_opts, volume_id, path) do
    parent_path = FileMeta.parent_path(path)

    if parent_path do
      case FileIndex.get_by_path(volume_id, parent_path) do
        {:ok, parent_meta}
        when is_list(parent_meta.default_acl) and parent_meta.default_acl != [] ->
          Keyword.put(file_opts, :acl_entries, parent_meta.default_acl)

        _ ->
          file_opts
      end
    else
      file_opts
    end
  rescue
    _ -> file_opts
  catch
    :exit, _ -> file_opts
  end

  defp emit_streamed_completion_telemetry(result, duration, volume_id, path, write_id) do
    case result do
      {:ok, file_meta} ->
        chunk_count = length(file_meta.chunks) + length(file_meta.stripes || [])

        :telemetry.execute(
          [:neonfs, :write_operation, :stop],
          %{duration: duration, bytes: file_meta.size, chunks: chunk_count},
          %{volume_id: volume_id, path: path, write_id: write_id, streamed: true}
        )

      {:error, reason} ->
        :telemetry.execute(
          [:neonfs, :write_operation, :exception],
          %{duration: duration},
          %{volume_id: volume_id, path: path, write_id: write_id, streamed: true, error: reason}
        )
    end
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
