defmodule NeonFS.Client.ChunkReader do
  @moduledoc """
  Assembles file contents by fetching chunk references from core and reading
  the chunk bytes directly over the TLS data plane.

  Interface nodes (FUSE, NFS, S3, WebDAV) use this helper in place of
  `NeonFS.Core.read_file/3` to keep bulk data off the Erlang distribution
  control plane. The flow:

    1. `NeonFS.Client.Router.call/4` fetches `read_file_refs` metadata from
       a core node (metadata only, small payload).
    2. For each chunk ref, a location is selected and the chunk bytes are
       fetched via `Router.data_call(:get_chunk, ...)` over TLS.
    3. The byte range is sliced and assembled.

  Both a buffered API (`read_file/3`) and a streaming API
  (`read_file_stream/3`) are provided. Streaming iterates chunk by chunk
  so at most one chunk's bytes are held in memory at a time, making it
  safe for interface nodes to serve arbitrarily large files without
  co-locating a core node.

  Each has a `_by_id` sibling (`read_file_by_id/3`,
  `read_file_stream_by_id/3`) for callers holding an immutable file id
  rather than a path — an open FUSE handle or SMB fd whose name may have
  been renamed away or unlinked. Both forms share one pipeline:
  only the three calls back to core (refs, whole-file read, metadata)
  differ, so every fallback below applies identically to either.

  Chunks that require server-side processing (decompression or decryption)
  cannot be read through the raw-bytes data plane — the bytes would arrive
  opaque. For those chunks this helper falls back to a bounded
  `NeonFS.Core.read_file/3` call that fetches just that chunk's processed
  bytes (range-limited to the chunk). The data plane optimisation applies
  to uncompressed, unencrypted volumes.

  Erasure-coded (stripe-based) files return data-chunk refs for each
  overlapping stripe when every data chunk is available. When any data
  chunk is missing and parity-based reconstruction is required, core
  returns `{:error, :stripe_refs_unsupported}`; this helper then falls
  back to reading the file **one stripe at a time** via
  `NeonFS.Core.read_file/3`, using stripe ranges from `get_file_meta/2`.
  The server does reconstruction per stripe, so the peak working set is
  bounded by the stripe size rather than the file size.

  If every location for a chunk returns `:no_data_endpoint` (no TLS pool
  configured to that peer), the chunk is fetched via the per-chunk core
  RPC fallback so that callers on nodes without a data-plane pool still
  get correct results. All other data-plane errors propagate.

  ## Telemetry

  Each chunk fetched for a range read emits
  `[:neonfs, :client, :chunk_reader, :chunk_fetched]` with measurements
  `%{read_length, chunk_size, duration}` and metadata
  `%{volume, node, hash, tier, source}`. `read_length` is the bytes the
  caller asked for from this chunk and `chunk_size` is the whole-chunk
  bytes the fetch had to move to serve them, so `chunk_size /
  read_length` is the read-amplification factor and the event count is
  the fetch count — the signal behind the small-window
  large-image-pull pathology. `duration` is in `:native` time units.

  `source` names which fetch served the chunk:

    * `:data_plane` — a whole chunk pulled over TLS. `chunk_size` is the
      measured byte size of what arrived, and `node`/`tier` name the
      replica it came from.
    * `:core_rpc` — a range-limited `NeonFS.Core.read_file/3` call, used
      for a chunk needing server-side processing or one with no data
      endpoint. Only `read_length` bytes cross the wire, but core reads
      and processes the whole chunk to produce them, so `chunk_size` is
      the chunk's `original_size` — the chunk-layer bytes moved, the
      same quantity the data-plane branch reports. `node` and `tier` are
      `nil`: `Router` picks the core node internally and does not report
      which.

  Two paths emit nothing, because neither makes a per-chunk fetch whose
  bytes or duration could be attributed honestly: a cache hit (which
  costs no fetch at all, and correctly contributes zero to the ratio),
  and the whole-range fallbacks — the buffered `read_file/3` fallback
  and the degraded-erasure stripe walk — where one core call covers many
  chunks.

  Callers can attribute these events to their own domain object by
  passing `:telemetry_metadata`; the map is merged into the event's
  metadata. `neonfs_block` uses it to tag chunk fetches with the NBD
  export they served, so read amplification is separable per device.
  """

  require Logger

  alias NeonFS.Client.ChunkCache
  alias NeonFS.Client.ChunkReader.StreamError
  alias NeonFS.Client.Router

  @default_chunk_timeout 30_000

  # Sparse regions are synthesised in bounded blocks, matching core's read path.
  @zero_fill_block_bytes 64 * 1024

  @type read_opts :: [
          offset: non_neg_integer(),
          length: non_neg_integer() | :all,
          timeout: timeout(),
          exclude_nodes: [node()],
          telemetry_metadata: map()
        ]

  @type stream_result ::
          {:ok, %{stream: Enumerable.t(), file_size: non_neg_integer()}}
          | {:error, term()}

  @doc """
  Reads a byte range from a file, fetching chunks over the data plane where
  possible and falling back to `read_file/3` when chunks require server-side
  processing or when erasure-coded.

  Options:

    * `:offset` - byte offset to start reading (default 0)
    * `:length` - number of bytes to read (default `:all`)
    * `:timeout` - per-chunk data-plane timeout in ms (default 30_000)
    * `:exclude_nodes` - nodes to skip when selecting a chunk location
      (useful for avoiding known-bad replicas)
    * `:telemetry_metadata` - a map merged into the metadata of every
      `chunk_fetched` event this read emits
  """
  @spec read_file(String.t(), String.t(), read_opts()) ::
          {:ok, binary()} | {:error, term()}
  def read_file(volume_name, path, opts \\ []) do
    do_read(volume_name, {:path, path}, opts)
  end

  @doc """
  `file_id`-keyed counterpart to `read_file/3`.

  For callers holding an immutable handle — a FUSE `fh`, an SMB open —
  whose path may have been renamed or unlinked since it was opened. A
  detached file has no path to resolve, so the path-based read would
  404 while the handle's chunks are still perfectly readable.

  Every fallback the path-based version has is preserved: per-chunk
  core RPC for chunks needing server-side processing, buffered
  fallback, and per-stripe fallback for degraded erasure reads.
  """
  @spec read_file_by_id(String.t(), binary(), read_opts()) ::
          {:ok, binary()} | {:error, term()}
  def read_file_by_id(volume_name, file_id, opts \\ []) do
    do_read(volume_name, {:id, file_id}, opts)
  end

  defp do_read(volume_name, target, opts) do
    refs_opts = Keyword.take(opts, [:offset, :length, :uid, :gids])

    case refs_call(volume_name, target, refs_opts) do
      {:ok, %{chunks: chunks} = result} ->
        dispatch_read(chunks, result.hole_bytes, volume_name, target, opts)

      {:error, :stripe_refs_unsupported} ->
        fallback_read(volume_name, target, opts)

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Returns a lazy stream of chunk data for a file's byte range.

  Performs the `read_file_refs` lookup eagerly (small payload) and returns
  a `Stream` that fetches each chunk as it is consumed. At most one
  chunk's bytes are held in memory at a time, so callers can serve files
  much larger than available RAM without buffering.

  The returned stream yields raw `binary()` slices corresponding to the
  caller's requested byte range. If a chunk fetch fails mid-stream the
  stream raises `NeonFS.Client.ChunkReader.StreamError` rather than
  halting silently — a silent halt would be indistinguishable from a
  clean end-of-file and hand the consumer a truncated read.

  Unlike `NeonFS.Core.read_file_stream/3`, this stream is built entirely
  on the caller's node — it is safe to use from non-co-located interface
  nodes (S3, WebDAV, NFS, FUSE). Each chunk is fetched either via the TLS
  data plane (`Router.data_call/4`) for uncompressed/unencrypted chunks
  or via a range-limited `NeonFS.Core.read_file/3` RPC when server-side
  processing is required; in either case the peak working set is bounded
  by the chunk size.

  Options match `read_file/3`.
  """
  @spec read_file_stream(String.t(), String.t(), read_opts()) :: stream_result()
  def read_file_stream(volume_name, path, opts \\ []) do
    do_read_stream(volume_name, {:path, path}, opts)
  end

  @doc """
  `file_id`-keyed counterpart to `read_file_stream/3`.

  Same laziness guarantee: chunks are pulled as the stream is consumed,
  so the working set stays at chunk granularity no matter how large the
  file is. Same node-locality constraint too — a `Stream.t()` does not
  survive Erlang distribution, so build and consume it on one node.
  """
  @spec read_file_stream_by_id(String.t(), binary(), read_opts()) :: stream_result()
  def read_file_stream_by_id(volume_name, file_id, opts \\ []) do
    do_read_stream(volume_name, {:id, file_id}, opts)
  end

  defp do_read_stream(volume_name, target, opts) do
    refs_opts = Keyword.take(opts, [:offset, :length, :uid, :gids])

    case refs_call(volume_name, target, refs_opts) do
      {:ok, %{chunks: chunks, file_size: file_size, hole_bytes: hole_bytes}} ->
        stream = build_chunk_stream(chunks, hole_bytes, volume_name, target, opts)
        {:ok, %{stream: stream, file_size: file_size}}

      {:error, :stripe_refs_unsupported} ->
        fallback_stream(volume_name, target, opts)

      {:error, _} = error ->
        error
    end
  end

  # How a file is addressed when a fetch has to go back to core: by
  # path, or by the immutable id an open handle holds. Everything
  # between the two entry points and these three calls is shared —
  # the chunk pipeline neither knows nor cares which it is.
  defp refs_call(volume_name, {:path, path}, opts),
    do: Router.call(NeonFS.Core, :read_file_refs, [volume_name, path, opts])

  defp refs_call(volume_name, {:id, file_id}, opts),
    do: Router.call(NeonFS.Core, :read_file_refs_by_id, [volume_name, file_id, opts])

  defp core_read(volume_name, {:path, path}, opts),
    do: Router.call(NeonFS.Core, :read_file, [volume_name, path, opts])

  defp core_read(volume_name, {:id, file_id}, opts),
    do: Router.call(NeonFS.Core, :read_file_by_id, [volume_name, file_id, opts])

  defp meta_call(volume_name, {:path, path}, opts),
    do: Router.call(NeonFS.Core, :get_file_meta, [volume_name, path, opts])

  defp meta_call(volume_name, {:id, file_id}, opts),
    do: Router.call(NeonFS.Core, :get_file_meta_by_id, [volume_name, file_id, opts])

  defp dispatch_read(chunks, hole_bytes, volume_name, target, opts) do
    if Enum.any?(chunks, &needs_server_processing?/1) do
      fallback_read(volume_name, target, opts)
    else
      case assemble(chunks, hole_bytes, fetch_ctx(volume_name, opts)) do
        {:error, :no_data_endpoint} -> fallback_read(volume_name, target, opts)
        other -> other
      end
    end
  end

  # Everything a chunk fetch needs that is not the ref itself, so the
  # pipeline threads one value rather than four.
  defp fetch_ctx(volume_name, opts) do
    %{
      volume: volume_name,
      exclude: Keyword.get(opts, :exclude_nodes, []),
      timeout: Keyword.get(opts, :timeout, @default_chunk_timeout),
      telemetry_metadata: Keyword.get(opts, :telemetry_metadata, %{})
    }
  end

  # `hole_bytes` is the sparse tail core reported for this range — a file grown
  # by `truncate` has bytes inside its own size that no chunk backs, and POSIX
  # requires them to read as zeros. Core decides how many, because it is the
  # only side that can tell an unwritten region from a chunk whose metadata is
  # missing.
  defp assemble(chunks, hole_bytes, ctx) do
    chunks
    |> Enum.reduce_while({:ok, []}, fn ref, {:ok, acc} ->
      case fetch_chunk_bytes(ctx, ref) do
        {:ok, bytes} ->
          sliced = binary_part(bytes, ref.read_start, ref.read_length)
          {:cont, {:ok, [sliced | acc]}}

        {:error, _} = err ->
          {:halt, err}
      end
    end)
    |> case do
      {:ok, parts} ->
        {:ok, [Enum.reverse(parts), :binary.copy(<<0>>, hole_bytes)] |> IO.iodata_to_binary()}

      error ->
        error
    end
  end

  defp fetch_chunk_bytes(ctx, ref) do
    case ChunkCache.get({ctx.volume, ref.hash}) do
      {:ok, bytes} ->
        {:ok, bytes}

      :miss ->
        ordered =
          ref.locations
          |> Enum.reject(&(&1.node in ctx.exclude))
          |> prefer_local()

        case ordered do
          [] ->
            {:error, :no_available_locations}

          locations ->
            try_locations(ctx, locations, ref, :no_locations_tried)
        end
    end
  end

  defp try_locations(_ctx, [], _ref, last_error), do: {:error, last_error}

  defp try_locations(ctx, [loc | rest], ref, _last_error) do
    tier = tier_to_string(Map.get(loc, :tier, :hot))
    drive_id = Map.get(loc, :drive_id, "default")

    args = [hash: ref.hash, volume_id: drive_id, tier: tier]

    start = System.monotonic_time()

    case Router.data_call(loc.node, :get_chunk, args, timeout: ctx.timeout) do
      {:ok, bytes} ->
        emit_chunk_fetched(
          ctx,
          ref,
          byte_size(bytes),
          %{source: :data_plane, node: loc.node, tier: tier},
          start
        )

        verify_and_accept(ctx, bytes, loc, ref, rest)

      {:error, reason} ->
        Logger.debug("Data-plane chunk fetch failed, trying next location",
          node: loc.node,
          reason: inspect(reason)
        )

        try_locations(ctx, rest, ref, reason)
    end
  end

  # The content-address invariant: a chunk's bytes must hash to its id.
  # The data plane fetches whole chunks by hash, so verify SHA-256 here —
  # one place, end-to-end (disk rot, transit, handler bugs), inherited by
  # every interface. A mismatch means this replica is corrupt:
  # emit telemetry (feeds repair/alerting) and fail over to the next
  # location, which may hold a good copy. Compressed/encrypted chunks are
  # fetched range-decoded via the core RPC path, not whole, so they can't
  # be verified against the whole-chunk hash here — that's core/scrub's
  # responsibility.
  defp verify_and_accept(ctx, bytes, loc, ref, rest) do
    if :crypto.hash(:sha256, bytes) == ref.hash do
      ChunkCache.put({ctx.volume, ref.hash}, bytes)
      {:ok, bytes}
    else
      :telemetry.execute(
        [:neonfs, :client, :chunk_reader, :verify_failed],
        %{size: byte_size(bytes)},
        %{node: loc.node, hash: ref.hash}
      )

      Logger.warning("Chunk failed content-hash verification, trying next location",
        node: loc.node,
        chunk_hash: Base.encode16(ref.hash, case: :lower)
      )

      try_locations(ctx, rest, ref, {:chunk_verify_failed, ref.hash})
    end
  end

  # A whole chunk is moved to hand back only `read_length` bytes:
  # `chunk_size / read_length` is the amplification, the event count is
  # the fetch count. `source` says which fetch moved them — see the
  # module doc for what `chunk_size` measures on each.
  defp emit_chunk_fetched(ctx, ref, chunk_size, source_metadata, start) do
    metadata =
      %{volume: ctx.volume, hash: ref.hash, node: nil, tier: nil}
      |> Map.merge(source_metadata)
      |> Map.merge(ctx.telemetry_metadata)

    :telemetry.execute(
      [:neonfs, :client, :chunk_reader, :chunk_fetched],
      %{
        read_length: ref.read_length,
        chunk_size: chunk_size,
        duration: System.monotonic_time() - start
      },
      metadata
    )
  end

  defp prefer_local(locations) do
    local = Node.self()
    {local_locs, remote_locs} = Enum.split_with(locations, &(&1.node == local))
    local_locs ++ Enum.shuffle(remote_locs)
  end

  defp tier_to_string(:hot), do: "hot"
  defp tier_to_string(:warm), do: "warm"
  defp tier_to_string(:cold), do: "cold"
  defp tier_to_string(tier) when is_binary(tier), do: tier

  defp needs_server_processing?(%{compression: compression, encrypted: encrypted}) do
    compression != :none or encrypted
  end

  defp fallback_read(volume_name, target, opts) do
    forward_opts = Keyword.take(opts, [:offset, :length, :uid, :gids])
    core_read(volume_name, target, forward_opts)
  end

  defp build_chunk_stream(chunks, hole_bytes, volume_name, target, opts) do
    ctx = fetch_ctx(volume_name, opts)

    chunk_stream =
      Stream.unfold(chunks, fn
        [] ->
          nil

        [ref | rest] ->
          case stream_fetch_chunk(ref, ctx, target) do
            {:ok, bytes} ->
              {bytes, rest}

            {:error, reason} ->
              raise StreamError, reason: reason
          end
      end)

    Stream.concat(chunk_stream, zero_fill_stream(hole_bytes))
  end

  # A block at a time. A freshly sized sparse file is entirely hole, so
  # materialising it as one binary would trade a short read for an OOM.
  defp zero_fill_stream(0), do: []

  defp zero_fill_stream(total) do
    Stream.unfold(total, fn
      0 ->
        nil

      remaining ->
        emit = min(remaining, @zero_fill_block_bytes)
        {:binary.copy(<<0>>, emit), remaining - emit}
    end)
  end

  defp stream_fetch_chunk(ref, ctx, target) do
    if needs_server_processing?(ref) do
      stream_fetch_via_core(ref, ctx, target)
    else
      case fetch_chunk_bytes(ctx, ref) do
        {:ok, bytes} ->
          {:ok, binary_part(bytes, ref.read_start, ref.read_length)}

        {:error, :no_data_endpoint} ->
          stream_fetch_via_core(ref, ctx, target)

        {:error, _} = err ->
          err
      end
    end
  end

  # Core hands back only the requested slice, but it had to read and
  # process the whole chunk to produce it — so the chunk-layer bytes this
  # fetch moved are the chunk's `original_size`, not the slice's length.
  defp stream_fetch_via_core(ref, ctx, target) do
    offset = ref.chunk_offset + ref.read_start
    length = ref.read_length

    start = System.monotonic_time()

    with {:ok, _bytes} = ok <- core_read(ctx.volume, target, offset: offset, length: length) do
      emit_chunk_fetched(ctx, ref, ref.original_size, %{source: :core_rpc}, start)
      ok
    end
  end

  defp fallback_stream(volume_name, target, opts) do
    meta_opts = Keyword.take(opts, [:uid, :gids])

    case meta_call(volume_name, target, meta_opts) do
      {:ok, %{stripes: stripes} = meta} when is_list(stripes) ->
        {:ok,
         %{stream: stripe_fallback_stream(meta, volume_name, target, opts), file_size: meta.size}}

      {:ok, meta} ->
        buffered_fallback_stream(meta, volume_name, target, opts)

      {:error, _} = err ->
        err
    end
  end

  defp buffered_fallback_stream(meta, volume_name, target, opts) do
    with {:ok, bytes} <- fallback_read(volume_name, target, opts) do
      stream =
        Stream.unfold(bytes, fn
          <<>> -> nil
          data -> {data, <<>>}
        end)

      {:ok, %{stream: stream, file_size: meta.size}}
    end
  end

  defp stripe_fallback_stream(meta, volume_name, target, opts) do
    offset = Keyword.get(opts, :offset, 0)
    length = Keyword.get(opts, :length, :all)
    end_byte = compute_end_byte(meta.size, offset, length)

    if offset >= meta.size or end_byte <= offset do
      Stream.unfold(nil, fn _ -> nil end)
    else
      segments =
        meta.stripes
        |> Enum.map(&stripe_segment(&1, offset, end_byte))
        |> Enum.reject(&is_nil/1)

      Stream.unfold(segments, &pull_stripe_segment(&1, volume_name, target))
    end
  end

  defp stripe_segment(%{byte_range: byte_range}, offset, end_byte) do
    {s, e} = normalise_byte_range(byte_range)
    read_start = max(s, offset)
    read_end = min(e, end_byte)

    if read_start < read_end do
      %{offset: read_start, length: read_end - read_start}
    end
  end

  defp pull_stripe_segment([], _volume_name, _target), do: nil

  defp pull_stripe_segment([%{offset: offset, length: length} | rest], volume_name, target) do
    case core_read(volume_name, target, offset: offset, length: length) do
      {:ok, <<>>} ->
        pull_stripe_segment(rest, volume_name, target)

      {:ok, bytes} ->
        {bytes, rest}

      {:error, reason} ->
        raise StreamError, reason: reason
    end
  end

  defp compute_end_byte(file_size, _offset, :all), do: file_size
  defp compute_end_byte(file_size, offset, length), do: min(file_size, offset + length)

  defp normalise_byte_range({s, e}), do: {s, e}
  defp normalise_byte_range(s..e//_), do: {s, e}
end
