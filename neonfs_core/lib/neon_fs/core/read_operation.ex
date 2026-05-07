defmodule NeonFS.Core.ReadOperation do
  @moduledoc """
  Handles read operations for NeonFS.

  Implements the read path that takes a file path, looks up metadata, fetches chunks
  from the blob store, and assembles the data. Supports partial reads with offset and length.

  For replicated volumes, reads use the chunk-based path.
  For erasure-coded volumes, reads use the stripe-based path with healthy/degraded handling.
  """

  alias NeonFS.Core.{
    Authorise,
    Blob.Native,
    BlobStore,
    ChunkFetcher,
    ChunkIndex,
    ChunkMeta,
    FileIndex,
    KeyManager,
    ResolvedLookupCache,
    Stripe,
    StripeIndex,
    VolumeRegistry
  }

  alias NeonFS.IO.{Operation, Scheduler}

  alias NeonFS.Error.{ChunkNotFound, Internal, NotFound, Unavailable, VolumeNotFound}
  alias NeonFS.Error.FileNotFound, as: FileNotFoundError

  require Logger

  @type read_result :: {:ok, binary()} | {:error, term()}
  @type stream_result ::
          {:ok, %{stream: Enumerable.t(), file_size: non_neg_integer()}} | {:error, term()}

  @type chunk_ref :: %{
          hash: binary(),
          original_size: non_neg_integer(),
          stored_size: non_neg_integer(),
          chunk_offset: non_neg_integer(),
          read_start: non_neg_integer(),
          read_length: non_neg_integer(),
          compression: :none | :zstd,
          encrypted: boolean(),
          locations: [ChunkMeta.location()]
        }

  @type refs_result ::
          {:ok, %{file_size: non_neg_integer(), chunks: [chunk_ref()]}}
          | {:error, term()}

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
    Logger.metadata(component: :read, volume_id: volume_id, file_path: path)

    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, :all)

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :read_operation, :start],
      %{offset: offset},
      %{volume_id: volume_id, path: path, length: length}
    )

    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    result =
      with {:ok, volume} <- get_volume(volume_id),
           :ok <- Authorise.check(uid, gids, :read, {:volume, volume_id}) do
        read_with_cache(volume, volume_id, path, offset, length)
      end

    duration = System.monotonic_time() - start_time
    emit_read_telemetry(result, duration, volume_id, path)
    result
  end

  @doc """
  Counterpart to `read_file/3` that resolves the file by `file_id`
  instead of `path`.

  The unlink-while-open story (#638) keeps a deleted file's chunks
  reachable by `file_id` for as long as some `:pinned` claim holds.
  Callers that opened the file before the unlink (FUSE / NFSv4 fd
  holders) cache the `file_id` and read through here so the
  detached-but-pinned state is observable end-to-end.

  Honours the same `:offset` / `:length` / `:uid` / `:gids` opts.
  Returns `{:error, :wrong_volume}` when the resolved FileMeta lives
  in a different volume than `volume_id` — defence-in-depth against a
  caller passing a stale `file_id` into the wrong volume.
  """
  @spec read_file_by_id(binary(), binary(), keyword()) :: read_result()
  def read_file_by_id(volume_id, file_id, opts \\ []) do
    Logger.metadata(component: :read, volume_id: volume_id, file_id: file_id)

    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, :all)
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    start_time = System.monotonic_time()

    :telemetry.execute(
      [:neonfs, :read_operation, :start],
      %{offset: offset},
      %{volume_id: volume_id, file_id: file_id, length: length, by_id: true}
    )

    result =
      with {:ok, volume} <- get_volume(volume_id),
           :ok <- Authorise.check(uid, gids, :read, {:volume, volume_id}),
           {:ok, file_meta} <- get_file_by_id(volume_id, file_id) do
        read_with_cache_lookup(file_meta, volume, offset, length)
      end

    duration = System.monotonic_time() - start_time
    emit_read_telemetry(result, duration, volume_id, "<file_id:#{file_id}>")
    result
  end

  @doc """
  Returns a lazy stream of chunk data for a file's byte range.

  Performs authorisation, volume lookup, and file metadata resolution eagerly
  before returning the stream. Chunk data is fetched lazily as the stream is
  consumed, so at most one chunk is held in memory at a time.

  The stream yields raw `binary()` slices. If a chunk fetch fails mid-stream
  the stream halts and the error is logged; consumers can detect truncation by
  comparing received bytes against `file_size` in the returned metadata.

  **Note:** Elixir streams contain anonymous functions and cannot be serialised
  across Erlang distribution. This API is for local consumption on core nodes.
  Remote callers should use `read_file/3` instead.

  ## Parameters

    * `volume_id` - Volume identifier
    * `path` - File path within the volume
    * `opts` - Optional keyword list:
      * `:offset` - Byte offset to start streaming from (default: 0)
      * `:length` - Number of bytes to stream (default: :all for entire file)
      * `:uid` - User ID for authorisation (default: 0)
      * `:gids` - Group IDs for authorisation (default: [])

  ## Returns

    * `{:ok, %{stream: stream, file_size: size}}` on success
    * `{:error, reason}` if authorisation fails or volume/file not found
  """
  @spec read_file_stream(binary(), String.t(), keyword()) :: stream_result()
  def read_file_stream(volume_id, path, opts \\ []) do
    Logger.metadata(component: :read_stream, volume_id: volume_id, file_path: path)

    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, :all)
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, volume} <- get_volume(volume_id),
         :ok <- Authorise.check(uid, gids, :read, {:volume, volume_id}),
         {:ok, file_meta} <- get_file(volume_id, path) do
      :telemetry.execute(
        [:neonfs, :read_stream, :start],
        %{offset: offset, file_size: file_meta.size},
        %{volume_id: volume_id, path: path, length: length}
      )

      populate_resolved_cache(file_meta)
      stream = build_stream(file_meta, volume, offset, length)

      {:ok, %{stream: stream, file_size: file_meta.size}}
    end
  end

  @doc """
  `file_id`-keyed counterpart to `read_file_stream/3`. Same semantics
  as `read_file_by_id/3` plus the streaming-pipeline note that applies
  to `read_file_stream/3`: streams cannot be serialised across Erlang
  distribution. See `read_file_by_id/3` for the unlink-while-open
  motivation.
  """
  @spec read_file_stream_by_id(binary(), binary(), keyword()) :: stream_result()
  def read_file_stream_by_id(volume_id, file_id, opts \\ []) do
    Logger.metadata(component: :read_stream, volume_id: volume_id, file_id: file_id)

    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, :all)
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, volume} <- get_volume(volume_id),
         :ok <- Authorise.check(uid, gids, :read, {:volume, volume_id}),
         {:ok, file_meta} <- get_file_by_id(volume_id, file_id) do
      :telemetry.execute(
        [:neonfs, :read_stream, :start],
        %{offset: offset, file_size: file_meta.size},
        %{volume_id: volume_id, file_id: file_id, length: length, by_id: true}
      )

      populate_resolved_cache(file_meta)
      stream = build_stream(file_meta, volume, offset, length)

      {:ok, %{stream: stream, file_size: file_meta.size}}
    end
  end

  @doc """
  Returns chunk references for a file's byte range without fetching chunk data.

  Used by interface nodes (FUSE, NFS, S3, WebDAV) that call into core to
  retrieve metadata, then fetch chunk bytes directly over the TLS data plane
  via `NeonFS.Client.Router.data_call/4`. Keeps bulk data off the Erlang
  distribution control plane.

  Each returned ref carries the chunk hash, the chunk's byte offset within
  the file, the slice to read from the chunk, and the list of storage
  locations (node/drive/tier triples). Callers select a location and issue a
  `:get_chunk` data-plane call to read the raw bytes.

  Replicated (chunk-based) volumes return refs for each relevant file chunk.

  Erasure-coded (stripe-based) volumes return refs for the data chunks of
  each stripe that overlaps the read range. This only applies when every
  data chunk in each relevant stripe is available — if any data chunk is
  missing (i.e. the stripe would require parity-based reconstruction),
  `{:error, :stripe_refs_unsupported}` is returned so the caller can fall
  back to `read_file/3`.

  ## Options

    * `:offset` - Byte offset to start from (default: 0)
    * `:length` - Number of bytes to include (default: :all)
    * `:uid` - User ID for authorisation (default: 0)
    * `:gids` - Group IDs for authorisation (default: [])

  ## Returns

    * `{:ok, %{file_size: size, chunks: [ref, ...]}}` on success
    * `{:error, :stripe_refs_unsupported}` when a relevant stripe requires
      reconstruction (one or more data chunks are missing)
    * `{:error, reason}` on auth / lookup failure
  """
  @spec read_file_refs(binary(), String.t(), keyword()) :: refs_result()
  def read_file_refs(volume_id, path, opts \\ []) do
    Logger.metadata(component: :read_refs, volume_id: volume_id, file_path: path)

    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, :all)
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, _volume} <- get_volume(volume_id),
         :ok <- Authorise.check(uid, gids, :read, {:volume, volume_id}),
         {:ok, file_meta} <- get_file(volume_id, path) do
      build_refs_result(file_meta, offset, length)
    end
  end

  @doc """
  `file_id`-keyed counterpart to `read_file_refs/3`. See
  `read_file_by_id/3` for the unlink-while-open motivation.
  """
  @spec read_file_refs_by_id(binary(), binary(), keyword()) :: refs_result()
  def read_file_refs_by_id(volume_id, file_id, opts \\ []) do
    Logger.metadata(component: :read_refs, volume_id: volume_id, file_id: file_id)

    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, :all)
    uid = Keyword.get(opts, :uid, 0)
    gids = Keyword.get(opts, :gids, [])

    with {:ok, _volume} <- get_volume(volume_id),
         :ok <- Authorise.check(uid, gids, :read, {:volume, volume_id}),
         {:ok, file_meta} <- get_file_by_id(volume_id, file_id) do
      build_refs_result(file_meta, offset, length)
    end
  end

  defp build_refs_result(%{stripes: stripes} = file_meta, offset, length)
       when is_list(stripes) do
    end_byte = calculate_end_byte(file_meta.size, offset, length)

    if offset >= file_meta.size or end_byte <= offset do
      {:ok, %{file_size: file_meta.size, chunks: []}}
    else
      stripes
      |> Enum.filter(fn stripe_ref ->
        {s, e} = normalise_byte_range(stripe_ref.byte_range)
        s < end_byte and e > offset
      end)
      |> build_stripe_refs_list(offset, end_byte)
      |> case do
        {:ok, refs} -> {:ok, %{file_size: file_meta.size, chunks: refs}}
        {:error, _} = err -> err
      end
    end
  end

  defp build_refs_result(file_meta, offset, length) do
    end_byte = calculate_end_byte(file_meta.size, offset, length)

    refs =
      if offset >= file_meta.size or end_byte <= offset do
        []
      else
        file_meta.chunks
        |> build_chunk_info_list(0, [])
        |> Enum.filter(fn {_hash, chunk_start, chunk_end} ->
          chunk_start < end_byte and chunk_end > offset
        end)
        |> Enum.map(&to_chunk_ref(&1, offset, end_byte))
        |> Enum.reject(&is_nil/1)
      end

    {:ok, %{file_size: file_meta.size, chunks: refs}}
  end

  defp to_chunk_ref({hash, chunk_start, chunk_end}, offset, end_byte) do
    case ChunkIndex.get(hash) do
      {:ok, chunk_meta} ->
        read_start = max(0, offset - chunk_start)
        read_end = min(chunk_end - chunk_start, end_byte - chunk_start)

        %{
          hash: hash,
          original_size: chunk_meta.original_size,
          stored_size: chunk_meta.stored_size,
          chunk_offset: chunk_start,
          read_start: read_start,
          read_length: read_end - read_start,
          compression: chunk_meta.compression,
          encrypted: not is_nil(chunk_meta.crypto),
          locations: chunk_meta.locations
        }

      {:error, :not_found} ->
        Logger.error("Chunk metadata missing while building refs",
          chunk_hash: Base.encode16(hash, case: :lower)
        )

        nil
    end
  end

  defp build_stripe_refs_list(stripe_refs, offset, end_byte) do
    stripe_refs
    |> Enum.reduce_while({:ok, []}, fn stripe_ref, {:ok, acc} ->
      case build_stripe_refs(stripe_ref, offset, end_byte) do
        {:ok, refs} -> {:cont, {:ok, [refs | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, refs_lists} -> {:ok, refs_lists |> Enum.reverse() |> List.flatten()}
      error -> error
    end
  end

  defp build_stripe_refs(%{stripe_id: stripe_id} = stripe_ref, offset, end_byte) do
    case StripeIndex.get(stripe_id) do
      {:ok, stripe} ->
        {stripe_start, _} = normalise_byte_range(stripe_ref.byte_range)
        data_hashes = Stripe.data_chunk_hashes(stripe)

        if Enum.all?(data_hashes, &chunk_available?/1) do
          build_stripe_data_chunk_refs(stripe, stripe_start, offset, end_byte)
        else
          {:error, :stripe_refs_unsupported}
        end

      {:error, :not_found} ->
        {:error, :stripe_refs_unsupported}
    end
  end

  defp build_stripe_data_chunk_refs(stripe, stripe_start, offset, end_byte) do
    chunk_size = stripe.config.chunk_size
    data_bytes = stripe.data_bytes

    refs =
      stripe
      |> Stripe.data_chunk_hashes()
      |> Enum.with_index()
      |> Enum.map(fn {hash, idx} ->
        build_single_stripe_ref(hash, idx, chunk_size, data_bytes, stripe_start, offset, end_byte)
      end)
      |> Enum.reject(&is_nil/1)

    case Enum.find(refs, &match?({:error, _}, &1)) do
      nil -> {:ok, refs}
      {:error, _} = err -> err
    end
  end

  defp build_single_stripe_ref(hash, idx, chunk_size, data_bytes, stripe_start, offset, end_byte) do
    chunk_start_in_stripe = idx * chunk_size
    real_end_in_stripe = min((idx + 1) * chunk_size, data_bytes)

    cond do
      real_end_in_stripe <= chunk_start_in_stripe ->
        nil

      stripe_start + real_end_in_stripe <= offset ->
        nil

      stripe_start + chunk_start_in_stripe >= end_byte ->
        nil

      true ->
        chunk_file_start = stripe_start + chunk_start_in_stripe
        real_chunk_length = real_end_in_stripe - chunk_start_in_stripe
        read_start = max(0, offset - chunk_file_start)
        read_end = min(real_chunk_length, end_byte - chunk_file_start)

        to_stripe_chunk_ref(hash, chunk_file_start, read_start, read_end - read_start)
    end
  end

  defp to_stripe_chunk_ref(hash, chunk_offset, read_start, read_length) do
    case ChunkIndex.get(hash) do
      {:ok, chunk_meta} ->
        %{
          hash: hash,
          original_size: chunk_meta.original_size,
          stored_size: chunk_meta.stored_size,
          chunk_offset: chunk_offset,
          read_start: read_start,
          read_length: read_length,
          compression: chunk_meta.compression,
          encrypted: not is_nil(chunk_meta.crypto),
          locations: chunk_meta.locations
        }

      {:error, :not_found} ->
        {:error, :stripe_refs_unsupported}
    end
  end

  defp normalise_byte_range({s, e}), do: {s, e}
  defp normalise_byte_range(s..e//_), do: {s, e}

  # ─── Cache-aware read ─────────────────────────────────────────────────

  defp read_with_cache(volume, volume_id, path, offset, length) do
    with {:ok, file_meta} <- get_file(volume_id, path) do
      read_with_cache_lookup(file_meta, volume, offset, length)
    end
  end

  defp read_with_cache_lookup(file_meta, volume, offset, length) do
    case try_cache_read(file_meta, volume, offset, length) do
      {:ok, _data} = result ->
        result

      :miss ->
        read_and_populate_cache(file_meta, volume, offset, length)
    end
  end

  defp read_and_populate_cache(file_meta, volume, offset, length) do
    case do_read(file_meta, volume, offset, length) do
      {:ok, _data} = result ->
        populate_resolved_cache(file_meta)
        result

      error ->
        error
    end
  end

  defp try_cache_read(file_meta, volume, offset, length) do
    case ResolvedLookupCache.get(file_meta.id) do
      {:ok, _cached_metadata} ->
        do_read(file_meta, volume, offset, length)

      :miss ->
        :miss
    end
  rescue
    _ -> :miss
  catch
    :exit, _ -> :miss
  end

  defp populate_resolved_cache(file_meta) do
    ResolvedLookupCache.put(file_meta.id, %{
      file_meta: file_meta,
      chunks: file_meta.chunks,
      stripes: file_meta.stripes
    })
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  # ─── Routing ──────────────────────────────────────────────────────────

  defp do_read(file_meta, volume, offset, length) when is_list(file_meta.stripes) do
    read_from_stripes(file_meta, volume, offset, length)
  end

  defp do_read(file_meta, volume, offset, length) do
    read_from_chunks(file_meta, volume, offset, length)
  end

  # ─── Stream Builders ──────────────────────────────────────────────────

  defp build_stream(file_meta, volume, offset, length) when is_list(file_meta.stripes) do
    build_stripe_stream(file_meta, volume, offset, length)
  end

  defp build_stream(file_meta, volume, offset, length) do
    build_chunk_stream(file_meta, volume, offset, length)
  end

  defp build_chunk_stream(file_meta, volume, offset, length) do
    end_byte = calculate_end_byte(file_meta.size, offset, length)

    if offset >= file_meta.size or end_byte <= offset do
      Stream.unfold(nil, fn _ -> nil end)
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

      should_verify = should_verify_on_read?(volume.verification)
      volume_id = volume.id

      Stream.unfold(needed, &stream_next_chunk(&1, should_verify, volume_id))
    end
  end

  defp stream_next_chunk([], _should_verify, _volume_id), do: nil

  defp stream_next_chunk([chunk_info | rest], should_verify, volume_id) do
    case fetch_single_chunk(chunk_info.hash, should_verify, volume_id) do
      {:ok, chunk_data} ->
        read_length = chunk_info.read_end - chunk_info.read_start
        sliced = binary_part(chunk_data, chunk_info.read_start, read_length)
        {sliced, rest}

      {:error, reason} ->
        Logger.error("Stream chunk fetch failed",
          chunk_hash: Base.encode16(chunk_info.hash, case: :lower),
          volume_id: volume_id,
          reason: inspect(reason)
        )

        nil
    end
  end

  defp build_stripe_stream(file_meta, volume, offset, length) do
    end_byte = calculate_end_byte(file_meta.size, offset, length)

    if offset >= file_meta.size or end_byte <= offset do
      Stream.unfold(nil, fn _ -> nil end)
    else
      should_verify = should_verify_on_read?(volume.verification)
      volume_id = volume.id

      relevant =
        file_meta.stripes
        |> Enum.filter(fn %{byte_range: {s, e}} -> s < end_byte and e > offset end)

      Stream.unfold({relevant, offset, end_byte, should_verify, volume_id}, &stream_next_stripe/1)
    end
  end

  defp stream_next_stripe({[], _offset, _end_byte, _should_verify, _volume_id}), do: nil

  defp stream_next_stripe(
         {[%{stripe_id: sid, byte_range: {s, _e}} | rest], offset, end_byte, should_verify,
          volume_id}
       ) do
    stripe_offset = max(0, offset - s)
    stripe_length = min_stripe_read_length(s, offset, end_byte)

    case read_stripe(sid, stripe_offset, stripe_length, should_verify, volume_id) do
      {:ok, data} ->
        {data, {rest, offset, end_byte, should_verify, volume_id}}

      {:error, reason} ->
        Logger.error("Stream stripe fetch failed",
          volume_id: volume_id,
          reason: inspect(reason)
        )

        nil
    end
  end

  # ─── Chunk-based Read Path (replicated) ───────────────────────────────

  defp read_from_chunks(file_meta, volume, offset, length) do
    with {:ok, needed_chunks} <- calculate_needed_chunks(file_meta, offset, length),
         {:ok, chunk_data} <- fetch_chunks(needed_chunks, volume, volume.id) do
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
        Logger.error("Chunk metadata not found", chunk_hash: Base.encode16(hash))
        build_chunk_info_list(rest, current_offset, acc)
    end
  end

  defp fetch_chunks(needed_chunks, volume, volume_id) do
    should_verify = should_verify_on_read?(volume.verification)

    results = Enum.map(needed_chunks, &fetch_single_chunk(&1.hash, should_verify, volume_id))

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

      read_stripe_segments(relevant, offset, end_byte, should_verify, volume.id)
    end
  end

  defp read_stripe_segments(stripes, offset, end_byte, should_verify, volume_id) do
    stripes
    |> Enum.reduce_while({:ok, []}, fn %{stripe_id: sid, byte_range: {s, _e}}, {:ok, acc} ->
      stripe_offset = max(0, offset - s)
      stripe_length = min_stripe_read_length(s, offset, end_byte)

      case read_stripe(sid, stripe_offset, stripe_length, should_verify, volume_id) do
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

  defp read_stripe(stripe_id, offset_in_stripe, length, should_verify, volume_id) do
    case StripeIndex.get(stripe_id) do
      {:ok, stripe} ->
        max_readable = max(0, stripe.data_bytes - offset_in_stripe)
        actual_length = min(length, max_readable)

        if actual_length <= 0 do
          {:ok, <<>>}
        else
          do_read_stripe(stripe, offset_in_stripe, actual_length, should_verify, volume_id)
        end

      {:error, :not_found} ->
        {:error,
         NotFound.exception(
           message: "Stripe not found: #{stripe_id}",
           details: %{stripe_id: stripe_id}
         )}
    end
  end

  defp do_read_stripe(stripe, offset, length, should_verify, volume_id) do
    case calculate_stripe_state(stripe) do
      :healthy ->
        read_stripe_healthy(stripe, offset, length, should_verify, volume_id)

      :degraded ->
        emit_stripe_read_telemetry(stripe, :degraded)
        read_stripe_degraded(stripe, offset, length, should_verify, volume_id)

      :critical ->
        emit_stripe_read_telemetry(stripe, :critical)
        {:error, Unavailable.exception(message: "Insufficient chunks for stripe reconstruction")}
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

  defp read_stripe_healthy(stripe, offset, length, should_verify, volume_id) do
    chunk_size = stripe.config.chunk_size
    start_idx = div(offset, chunk_size)
    end_idx = div(offset + length - 1, chunk_size)

    chunks_data =
      start_idx..end_idx
      |> Enum.map(fn idx -> Enum.at(stripe.chunks, idx) end)
      |> Enum.map(&fetch_single_chunk(&1, should_verify, volume_id))

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

  defp read_stripe_degraded(stripe, offset, length, should_verify, volume_id) do
    k = stripe.config.data_chunks

    available_with_idx =
      stripe.chunks
      |> Enum.with_index()
      |> Enum.filter(fn {hash, _idx} -> chunk_available?(hash) end)

    if Kernel.length(available_with_idx) < k do
      {:error, Unavailable.exception(message: "Insufficient chunks for stripe reconstruction")}
    else
      shards_to_fetch = Enum.take(available_with_idx, k)
      fetch_and_reconstruct(shards_to_fetch, stripe, offset, length, should_verify, volume_id)
    end
  end

  defp fetch_and_reconstruct(shards_to_fetch, stripe, offset, length, should_verify, volume_id) do
    k = stripe.config.data_chunks
    chunk_size = stripe.config.chunk_size

    shard_results =
      Enum.map(shards_to_fetch, fn {hash, idx} ->
        case fetch_single_chunk(hash, should_verify, volume_id) do
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

  defp handle_fetch_result({:ok, data, _source}, chunk_meta, _fetch_opts, _drive_id, volume_id) do
    emit_decrypt_telemetry(chunk_meta.crypto, chunk_meta.hash, volume_id)
    {:ok, data}
  end

  defp handle_fetch_result({:error, reason}, chunk_meta, fetch_opts, drive_id, volume_id)
       when is_binary(reason) do
    if verification_failure?(reason) do
      handle_verification_failure(chunk_meta, fetch_opts, drive_id, volume_id)
    else
      {:error, translate_decrypt_error(reason, chunk_meta.hash)}
    end
  end

  defp handle_fetch_result({:error, reason}, chunk_meta, _fetch_opts, _drive_id, _volume_id) do
    {:error, translate_decrypt_error(reason, chunk_meta.hash)}
  end

  # ─── Verification Failure Repair ─────────────────────────────────────

  defp verification_failure?(reason) when is_binary(reason) do
    String.starts_with?(reason, "corrupt chunk")
  end

  defp handle_verification_failure(chunk_meta, fetch_opts, drive_id, volume_id) do
    chunk_hash = chunk_meta.hash

    emit_verification_failed_telemetry(chunk_hash, volume_id, drive_id)

    Logger.error("Chunk verification failed, attempting remote recovery",
      chunk_hash: Base.encode16(chunk_hash, case: :lower),
      volume_id: volume_id,
      drive_id: drive_id,
      node: node()
    )

    remote_opts = Keyword.put(fetch_opts, :exclude_nodes, [node()])

    case ChunkFetcher.fetch_chunk(chunk_hash, remote_opts) do
      {:ok, data, _source} ->
        submit_local_repair(chunk_hash, data, drive_id, fetch_opts)
        {:ok, data}

      {:error, _reason} ->
        {:error,
         Internal.exception(
           message: "Chunk corrupted and no remote replica available",
           details: %{chunk_hash: Base.encode16(chunk_hash, case: :lower), volume_id: volume_id}
         )}
    end
  end

  defp emit_verification_failed_telemetry(chunk_hash, volume_id, drive_id) do
    :telemetry.execute(
      [:neonfs, :read_operation, :verification_failed],
      %{count: 1},
      %{
        chunk_hash: chunk_hash,
        volume_id: volume_id,
        drive_id: drive_id,
        node: node()
      }
    )
  end

  defp submit_local_repair(chunk_hash, verified_data, drive_id, fetch_opts) do
    tier = Keyword.get(fetch_opts, :tier, "hot")
    volume_id = Keyword.get(fetch_opts, :volume_id, "_repair")

    op =
      Operation.new(
        priority: :read_repair,
        volume_id: volume_id,
        drive_id: drive_id,
        type: :write,
        callback: fn ->
          BlobStore.write_chunk(verified_data, drive_id, tier)

          Logger.info("Repaired corrupt local chunk",
            chunk_hash: Base.encode16(chunk_hash, case: :lower),
            drive_id: drive_id,
            tier: tier
          )

          :ok
        end
      )

    Scheduler.submit_async(op)
  end

  # ─── Shared Helpers ───────────────────────────────────────────────────

  defp get_volume(volume_id) do
    case VolumeRegistry.get(volume_id) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, VolumeNotFound.exception(volume_id: volume_id)}
    end
  end

  defp get_file(volume_id, path) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, file_meta} ->
        {:ok, file_meta}

      {:error, :not_found} ->
        {:error, FileNotFoundError.exception(file_path: path, volume_id: volume_id)}
    end
  end

  # Counterpart to `get_file/2` keyed by `file_id`. Resolves through
  # `FileIndex.get/2` which works for both live and `:detached` files
  # — that's the whole point of the unlink-while-open story (#638).
  # Verifies the resolved FileMeta belongs to the requested volume so
  # a stale `file_id` from another volume can't slip through.
  defp get_file_by_id(volume_id, file_id) do
    case FileIndex.get(volume_id, file_id) do
      {:ok, %{volume_id: ^volume_id} = file_meta} ->
        {:ok, file_meta}

      {:ok, _other_volume_meta} ->
        {:error, :wrong_volume}

      {:error, :not_found} ->
        {:error,
         FileNotFoundError.exception(file_path: "<file_id:#{file_id}>", volume_id: volume_id)}
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

  defp fetch_single_chunk(hash, should_verify, volume_id) do
    case ChunkIndex.get(hash) do
      {:ok, chunk_meta} ->
        fetch_chunk_data(chunk_meta, should_verify, volume_id)

      {:error, :not_found} ->
        {:error,
         ChunkNotFound.exception(
           chunk_hash: Base.encode16(hash, case: :lower),
           volume_id: volume_id
         )}
    end
  end

  defp fetch_chunk_data(chunk_meta, should_verify, volume_id) do
    {tier, drive_id} =
      case chunk_meta.locations do
        [location | _] ->
          {Atom.to_string(location.tier), Map.get(location, :drive_id, "default")}

        [] ->
          {"hot", "default"}
      end

    needs_decompress =
      chunk_meta.compression != :none or
        (chunk_meta.crypto == nil and chunk_meta.stored_size != chunk_meta.original_size)

    with {:ok, decrypt_opts} <- resolve_decrypt_opts(chunk_meta.crypto, volume_id) do
      fetch_opts =
        [
          tier: tier,
          drive_id: drive_id,
          verify: should_verify,
          decompress: needs_decompress,
          compression: chunk_meta.compression,
          volume_id: volume_id
        ] ++ decrypt_opts

      chunk_meta.hash
      |> ChunkFetcher.fetch_chunk(fetch_opts)
      |> handle_fetch_result(chunk_meta, fetch_opts, drive_id, volume_id)
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

  # ─── Encryption Helpers ──────────────────────────────────────────────

  defp resolve_decrypt_opts(nil, _volume_id), do: {:ok, []}

  defp resolve_decrypt_opts(crypto, volume_id) do
    case KeyManager.get_volume_key(volume_id, crypto.key_version) do
      {:ok, key} ->
        {:ok, [key: key, nonce: crypto.nonce]}

      {:error, :unknown_key_version} = error ->
        error

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp emit_decrypt_telemetry(nil, _hash, _volume_id), do: :ok

  defp emit_decrypt_telemetry(crypto, hash, volume_id) do
    :telemetry.execute(
      [:neonfs, :read, :decrypt],
      %{},
      %{chunk_hash: hash, volume_id: volume_id, key_version: crypto.key_version}
    )
  end

  defp translate_decrypt_error(reason, chunk_hash) when is_binary(reason) do
    if String.contains?(reason, "encryption error") do
      Internal.exception(
        message: "Decryption failed for chunk #{Base.encode16(chunk_hash, case: :lower)}",
        details: %{chunk_hash: chunk_hash, reason: reason}
      )
    else
      Internal.exception(message: reason, details: %{chunk_hash: chunk_hash})
    end
  end

  defp translate_decrypt_error(reason, chunk_hash) do
    Internal.exception(message: inspect(reason), details: %{chunk_hash: chunk_hash})
  end
end
