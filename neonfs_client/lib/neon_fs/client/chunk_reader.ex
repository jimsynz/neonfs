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
  co-locating a core node (issue #207).

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
  """

  require Logger

  alias NeonFS.Client.Router

  @default_chunk_timeout 30_000

  @type read_opts :: [
          offset: non_neg_integer(),
          length: non_neg_integer() | :all,
          timeout: timeout(),
          exclude_nodes: [node()]
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
  """
  @spec read_file(String.t(), String.t(), read_opts()) ::
          {:ok, binary()} | {:error, term()}
  def read_file(volume_name, path, opts \\ []) do
    refs_opts = Keyword.take(opts, [:offset, :length])

    case Router.call(NeonFS.Core, :read_file_refs, [volume_name, path, refs_opts]) do
      {:ok, %{chunks: chunks} = result} ->
        dispatch_read(chunks, result.file_size, volume_name, path, opts)

      {:error, :stripe_refs_unsupported} ->
        fallback_read(volume_name, path, opts)

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
  stream logs the reason and halts; consumers can detect truncation by
  comparing total bytes received against the returned `file_size`.

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
    refs_opts = Keyword.take(opts, [:offset, :length])

    case Router.call(NeonFS.Core, :read_file_refs, [volume_name, path, refs_opts]) do
      {:ok, %{chunks: chunks, file_size: file_size}} ->
        stream = build_chunk_stream(chunks, volume_name, path, opts)
        {:ok, %{stream: stream, file_size: file_size}}

      {:error, :stripe_refs_unsupported} ->
        fallback_stream(volume_name, path, opts)

      {:error, _} = error ->
        error
    end
  end

  defp dispatch_read(chunks, file_size, volume_name, path, opts) do
    if Enum.any?(chunks, &needs_server_processing?/1) do
      fallback_read(volume_name, path, opts)
    else
      case assemble(chunks, file_size, opts) do
        {:error, :no_data_endpoint} -> fallback_read(volume_name, path, opts)
        other -> other
      end
    end
  end

  defp assemble(chunks, _file_size, opts) do
    timeout = Keyword.get(opts, :timeout, @default_chunk_timeout)
    exclude = Keyword.get(opts, :exclude_nodes, [])

    chunks
    |> Enum.reduce_while({:ok, []}, fn ref, {:ok, acc} ->
      case fetch_chunk_bytes(ref, exclude, timeout) do
        {:ok, bytes} ->
          sliced = binary_part(bytes, ref.read_start, ref.read_length)
          {:cont, {:ok, [sliced | acc]}}

        {:error, _} = err ->
          {:halt, err}
      end
    end)
    |> case do
      {:ok, parts} -> {:ok, parts |> Enum.reverse() |> IO.iodata_to_binary()}
      error -> error
    end
  end

  defp fetch_chunk_bytes(ref, exclude, timeout) do
    ordered =
      ref.locations
      |> Enum.reject(&(&1.node in exclude))
      |> prefer_local()

    case ordered do
      [] ->
        {:error, :no_available_locations}

      locations ->
        try_locations(locations, ref, timeout, :no_locations_tried)
    end
  end

  defp try_locations([], _ref, _timeout, last_error), do: {:error, last_error}

  defp try_locations([loc | rest], ref, timeout, _last_error) do
    tier = tier_to_string(Map.get(loc, :tier, :hot))
    drive_id = Map.get(loc, :drive_id, "default")

    args = [hash: ref.hash, volume_id: drive_id, tier: tier]

    case Router.data_call(loc.node, :get_chunk, args, timeout: timeout) do
      {:ok, bytes} ->
        {:ok, bytes}

      {:error, reason} ->
        Logger.debug("Data-plane chunk fetch failed, trying next location",
          node: loc.node,
          reason: inspect(reason)
        )

        try_locations(rest, ref, timeout, reason)
    end
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

  defp fallback_read(volume_name, path, opts) do
    forward_opts = Keyword.take(opts, [:offset, :length])
    Router.call(NeonFS.Core, :read_file, [volume_name, path, forward_opts])
  end

  defp build_chunk_stream(chunks, volume_name, path, opts) do
    timeout = Keyword.get(opts, :timeout, @default_chunk_timeout)
    exclude = Keyword.get(opts, :exclude_nodes, [])

    Stream.unfold(chunks, fn
      [] ->
        nil

      [ref | rest] ->
        case stream_fetch_chunk(ref, volume_name, path, exclude, timeout) do
          {:ok, bytes} ->
            {bytes, rest}

          {:error, reason} ->
            Logger.error("Streaming chunk fetch failed, halting stream",
              chunk_hash: Base.encode16(ref.hash, case: :lower),
              reason: inspect(reason)
            )

            nil
        end
    end)
  end

  defp stream_fetch_chunk(ref, volume_name, path, exclude, timeout) do
    if needs_server_processing?(ref) do
      stream_fetch_via_core(ref, volume_name, path)
    else
      case fetch_chunk_bytes(ref, exclude, timeout) do
        {:ok, bytes} ->
          {:ok, binary_part(bytes, ref.read_start, ref.read_length)}

        {:error, :no_data_endpoint} ->
          stream_fetch_via_core(ref, volume_name, path)

        {:error, _} = err ->
          err
      end
    end
  end

  defp stream_fetch_via_core(ref, volume_name, path) do
    offset = ref.chunk_offset + ref.read_start
    length = ref.read_length

    Router.call(NeonFS.Core, :read_file, [volume_name, path, [offset: offset, length: length]])
  end

  defp fallback_stream(volume_name, path, opts) do
    case Router.call(NeonFS.Core, :get_file_meta, [volume_name, path]) do
      {:ok, %{stripes: stripes} = meta} when is_list(stripes) ->
        {:ok,
         %{stream: stripe_fallback_stream(meta, volume_name, path, opts), file_size: meta.size}}

      {:ok, meta} ->
        buffered_fallback_stream(meta, volume_name, path, opts)

      {:error, _} = err ->
        err
    end
  end

  defp buffered_fallback_stream(meta, volume_name, path, opts) do
    with {:ok, bytes} <- fallback_read(volume_name, path, opts) do
      stream =
        Stream.unfold(bytes, fn
          <<>> -> nil
          data -> {data, <<>>}
        end)

      {:ok, %{stream: stream, file_size: meta.size}}
    end
  end

  defp stripe_fallback_stream(meta, volume_name, path, opts) do
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

      Stream.unfold(segments, &pull_stripe_segment(&1, volume_name, path))
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

  defp pull_stripe_segment([], _volume_name, _path), do: nil

  defp pull_stripe_segment([%{offset: offset, length: length} | rest], volume_name, path) do
    case Router.call(NeonFS.Core, :read_file, [
           volume_name,
           path,
           [offset: offset, length: length]
         ]) do
      {:ok, <<>>} ->
        pull_stripe_segment(rest, volume_name, path)

      {:ok, bytes} ->
        {bytes, rest}

      {:error, reason} ->
        Logger.error("Stripe fallback segment read failed, halting stream",
          reason: inspect(reason)
        )

        nil
    end
  end

  defp compute_end_byte(file_size, _offset, :all), do: file_size
  defp compute_end_byte(file_size, offset, length), do: min(file_size, offset + length)

  defp normalise_byte_range({s, e}), do: {s, e}
  defp normalise_byte_range(s..e//_), do: {s, e}
end
