defmodule NeonFS.Block.Device do
  @moduledoc """
  Maps NBD transmission commands onto a NeonFS block device.

  One module between the wire and the cluster, so the connection handler
  deals in protocol terms and this deals in storage terms.

  ## No device byte crosses Erlang distribution

  This node resolves the device's extents through core and moves their
  bytes itself, over the TLS data plane: `NeonFS.Client.ChunkReader` to
  pull an extent's chunk, `NeonFS.Client.ChunkWriter` to place a rewritten
  one. What crosses distribution is the map — `read_refs` on the way in and
  a batched commit on the way out — which is bounded by how many extents a
  request touches rather than by its bytes.

  Two things stay on core because they cannot be done here. A **compressed
  or encrypted** chunk's stored bytes do not hash to its id and decoding
  them needs the volume's key, so those extents are read through a core
  call; `ChunkReader.chunk_readable?/1` is the test. And the **commit**
  verifies this node's claim about where it put the chunks before it
  publishes anything, because a writer's report is the very thing in doubt
  when a chunk turns out to be missing.

  ## Export names

  An export names a device as `<volume>:<path>` — `blockvol:/dev.img`. A
  volume name cannot contain a colon, so the split is unambiguous, and the
  path keeps its leading slash exactly as the device records it.

  A bare `<volume>` names that volume's own device, at the cluster-wide
  constant `NeonFS.Core.BlockAttachment.default_device_path/0` that core
  provisions against. That is the form `nbd-client -N blockvol` and CSI want,
  and one device per volume makes it unambiguous.

  ## Reads stream

  A read never materialises its range: it is answered one extent at a time
  and each piece is written to the socket as it arrives, so a client asking
  for the largest request the export advertises still costs one extent of
  memory. The reply header carries no length — the client already knows it
  from its own request — so the payload can follow lazily.

  An extent nothing has written is a hole, and its zeroes are synthesised
  here rather than fetched: there is nothing to fetch. An extent the write
  window is still holding is answered from the window, because a read-back
  of a write that has not landed still has to see it.

  ## Writes read what they modify

  A write narrower than an extent is a read-modify-write of that whole
  extent, done on this node — pull the extent, splice the guest's bytes in,
  place the result as a new chunk. An extent the write covers end to end is
  not read at all.

  The commit names what those reads saw, so two writes into one extent
  cannot silently discard each other. A write that loses that race redoes
  itself; one that keeps losing answers `:stale_chunks`, which this module
  retries before the frontend ever sees it.

  ## Writes carry the attachment's epoch

  `open/1` reads the device's fencing epoch and every commit is stamped
  with it. A later attacher preempting this one bumps the epoch, so these
  writes start failing with `{:fenced, current}` — the signal to tear the
  device down rather than retry.

  ## Flush is a promise

  `flush/1` returns only once `BlockBacking.flush/2` has driven replication to
  the volume's `min_copies`. Acknowledging a flush before that would tell a
  guest filesystem its journal is durable when it is not, which is the one
  thing a block device must never do.

  ## Telemetry

    * `[:neonfs, :block, :command]` — Measurements: `bytes`, `duration`,
      `chunk_bytes` on a write or a zero-fill, and `chunks_replaced` on a
      zero-fill. Metadata: `export`, `command`, `status`.

  `bytes` is what the guest asked for; `chunk_bytes` is what the chunk
  layer moved to serve it, so their ratio is the request's amplification.
  Both directions are measurable on this node now that both move their own
  bytes: a write knows the extents it rewrote, and a read's fetches arrive
  as `NeonFS.Client.ChunkReader`'s `chunk_fetched` events, tagged with this
  export through `:telemetry_metadata`. `NeonFS.Block.Telemetry` exports
  both.

  A zero-fill answers with both numbers because neither describes it
  alone: it rewrites only the extents it clips, and drops the ones it
  covers for the price of a metadata entry each. Reported as
  `chunk_bytes` alone a full-device TRIM looks free; `chunks_replaced` is
  what it actually cost.
  """

  @behaviour NeonFS.Block.Frontend

  alias NeonFS.Block.WriteWindow
  alias NeonFS.Client
  alias NeonFS.Client.{ChunkReader, ChunkWriter}
  alias NeonFS.Core.BlockAttachment

  @backing NeonFS.Core.BlockBacking

  # `NeonFS.Core.BlockIndex.target/0` lives in neonfs_core, which this
  # package does not depend on — the same dependency inversion
  # `NeonFS.Client.ChunkWriter` documents for `ChunkMeta.compression/0`.
  # Mirror the literal here; core is what interprets it.
  @type extent_target :: :hole | {:chunk, binary()} | {:stripe, binary(), non_neg_integer()}

  @type t :: %{
          export: String.t(),
          volume: String.t(),
          path: String.t(),
          id: binary(),
          size: non_neg_integer(),
          chunk_bytes: pos_integer(),
          epoch: non_neg_integer(),
          window: pid() | nil,
          logical_block_size: pos_integer(),
          physical_block_size: pos_integer(),
          read_only: boolean()
        }

  @doc """
  Resolves an export name into a device handle.

  Fails rather than inventing a device: the device has to exist, which is
  `create_device/4`'s job and not something an attach should do implicitly.
  """
  @spec open(String.t()) :: {:ok, t()} | {:error, term()}
  def open(export) when is_binary(export) do
    with {:ok, volume, path} <- split_export(export),
         {:ok, info} <- core_call(:open_device, [volume, path]) do
      {:ok,
       %{
         export: export,
         volume: volume,
         path: path,
         id: info.id,
         size: info.size,
         chunk_bytes: info.chunk_bytes,
         epoch: info.epoch,
         window: nil,
         logical_block_size: info.logical_block_bytes,
         physical_block_size: info.physical_block_bytes,
         read_only: false
       }}
    end
  end

  @doc """
  The export description the handshake advertises.
  """
  @spec export_info(t()) :: NeonFS.Block.Protocol.export()
  def export_info(device) do
    %{
      size: device.size,
      read_only: device.read_only,
      logical_block_size: device.logical_block_size,
      physical_block_size: device.physical_block_size
    }
  end

  @doc """
  Streams `length` bytes at `offset`, one element per extent.

  Returns the stream rather than the bytes so the caller can write each piece
  to the socket as it arrives.
  """
  @spec read_stream(t(), non_neg_integer(), pos_integer()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def read_stream(device, offset, length) do
    case Application.get_env(:neonfs_block, :read_stream_fn) do
      nil -> extent_stream(device, offset, length)
      fun when is_function(fun, 2) -> stream_from(fun.(device, read_opts(device, offset, length)))
    end
  end

  @doc """
  Writes `data` at `offset`.
  """
  @spec write(t(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def write(device, offset, data) do
    retrying_stale(:write, fn ->
      measure(device, :write, byte_size(data), fn ->
        WriteWindow.write(device.window, offset, data)
      end)
    end)
  end

  @doc """
  Durability barrier — returns only once the write is replicated.
  """
  @spec flush(t()) :: :ok | {:error, term()}
  def flush(device) do
    measure(device, :flush, 0, fn ->
      with :ok <- WriteWindow.flush(device.window) do
        core_call(:flush, [device.volume, device.path])
      end
    end)
  end

  @doc """
  Zero-fills `length` bytes at `offset`, serving both TRIM and WRITE ZEROES.
  """
  @spec write_zeroes(t(), non_neg_integer(), pos_integer()) :: :ok | {:error, term()}
  def write_zeroes(device, offset, length) do
    retrying_stale(:write_zeroes, fn ->
      measure(device, :write_zeroes, length, fn -> do_write_zeroes(device, offset, length) end)
    end)
  end

  # The window drains first, or a punch issued after a write would land
  # before it and the write would come back from the dead.
  defp do_write_zeroes(device, offset, length) do
    with :ok <- WriteWindow.drain(device.window) do
      publish(device, offset, length, &zeroed_extent(device, &1))
    end
  end

  @doc """
  Emits the telemetry for a read, whose bytes are counted by the caller as it
  drains the stream rather than here.
  """
  @spec measure_read(t(), non_neg_integer(), integer(), :ok | :error) :: :ok
  def measure_read(device, bytes, start_time, status) do
    emit(device, :read, bytes, %{}, start_time, status)
  end

  # ─── Reads ─────────────────────────────────────────────────────────────

  # The first piece is fetched eagerly so the failures a caller can be told
  # about — a range past the end of the device, an unreachable core, a
  # device that has gone — still come back as an error status. Past that
  # point the reply header has gone out and NBD has no status left to send,
  # so a failure can only end the connection.
  defp extent_stream(device, offset, length) do
    with {:ok, %{extents: [first | rest]}} <- read_refs(device, offset, length),
         {:ok, head} <- ref_bytes(device, first) do
      {:ok, Stream.concat([head], Stream.map(rest, &ref_bytes!(device, &1)))}
    end
  end

  defp read_refs(device, offset, length) do
    core_call(:read_refs, [device.volume, device.path, offset, length])
  end

  # The window is consulted first, because a read-back of a write it is still
  # holding has to see that write. A cache that does not answer reads is a
  # correctness bug rather than a slower cache.
  defp ref_bytes(device, ref) do
    case WriteWindow.buffered(device.window, ref.index) do
      {:ok, buffered} -> {:ok, binary_part(buffered, ref.read_start, ref.read_length)}
      :miss -> committed_bytes(device, ref)
    end
  end

  defp committed_bytes(_device, %{target: :hole} = ref), do: {:ok, zeroes(ref.read_length)}

  defp committed_bytes(device, ref) do
    with {:ok, bytes} <- extent_bytes(device, ref) do
      {:ok, binary_part(bytes, ref.read_start, ref.read_length)}
    end
  end

  defp ref_bytes!(device, ref) do
    case ref_bytes(device, ref) do
      {:ok, bytes} -> bytes
      {:error, reason} -> raise "block device read failed: #{inspect(reason)}"
    end
  end

  # The whole extent, which is what a read-modify-write needs and what a
  # partial read slices out of. A chunk the data plane cannot serve — the
  # stored bytes do not hash to its id, and only core holds the key — is
  # asked of core by byte range instead.
  defp extent_bytes(_device, %{target: :hole} = ref), do: {:ok, zeroes(ref.width)}

  defp extent_bytes(device, ref) do
    if ChunkReader.chunk_readable?(ref) do
      device.volume
      |> fetch_chunk(ref, telemetry_metadata: %{export: device.export})
      |> fit(ref)
    else
      core_call(:read, [device.volume, device.path, extent_offset(device, ref), ref.width])
    end
  end

  # The last extent of a device whose size is not a whole multiple of the
  # extent width is short, and a splice that trusted the chunk's own length
  # over the extent's would grow the device by the difference.
  defp fit({:ok, bytes}, ref) when byte_size(bytes) >= ref.width,
    do: {:ok, binary_part(bytes, 0, ref.width)}

  defp fit({:ok, short}, ref),
    do: {:error, {:short_extent, ref.index, byte_size(short), ref.width}}

  defp fit({:error, _reason} = error, _ref), do: error

  defp extent_offset(device, ref), do: ref.index * device.chunk_bytes

  @doc """
  One extent's whole bytes and the target they came from.

  What a read-modify-write needs to start from, and what its commit has to
  name so a snapshot that moved is refused. The window takes this once per
  extent and then splices in memory, which is where the coalescing comes
  from.
  """
  @spec extent_snapshot(t(), non_neg_integer()) ::
          {:ok, binary(), extent_target()} | {:error, term()}
  def extent_snapshot(device, index) do
    offset = index * device.chunk_bytes
    width = min(device.chunk_bytes, device.size - offset)

    with {:ok, %{extents: [ref]}} <- read_refs(device, offset, width),
         {:ok, bytes} <- extent_bytes(device, ref) do
      {:ok, bytes, ref.target}
    end
  end

  @doc """
  Places `extents` over the data plane and publishes them in one commit.

  `extents` is `[{extent_index, bytes | :hole}]` and `expect` is
  `[{extent_index, target}]` for the ones whose contents were read — which
  for the window is all of them, since it read each once before buffering.

  One call for the whole batch is the point: a window that drained per
  extent would have saved the reads and none of the commits.
  """
  @spec publish_extents(
          t(),
          [{non_neg_integer(), binary() | :hole}],
          [{non_neg_integer(), extent_target()}]
        ) :: :ok | {:error, term()}
  def publish_extents(device, extents, expect) do
    planned = Enum.map(extents, fn {index, target} -> {%{index: index}, target} end)

    with {:ok, written} <- place(device, planned),
         {:ok, _cost} <- commit_planned(device, planned, written, expect) do
      :ok
    end
  end

  # ─── Writes ────────────────────────────────────────────────────────────

  # Both mutating paths are the same transaction, and it happens here rather
  # than on core: resolve the extents the request touches, build each one's
  # new contents (reading only the ones the request does not cover end to
  # end), place them over the data plane, and publish the map in one call
  # that names what those reads saw.
  defp publish(device, offset, length, build) do
    with {:ok, %{extents: refs}} <- read_refs(device, offset, length),
         {:ok, planned} <- plan(refs, build),
         {:ok, written} <- place(device, planned) do
      commit(device, planned, written)
    end
  end

  defp plan(refs, build) do
    refs
    |> Enum.reduce_while({:ok, []}, fn ref, {:ok, acc} ->
      case build.(ref) do
        {:ok, target} -> {:cont, {:ok, [{ref, target} | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, planned} -> {:ok, Enum.reverse(planned)}
      {:error, _reason} = error -> error
    end
  end

  defp zeroed_extent(device, ref) do
    if ref.read_length == ref.width do
      {:ok, :hole}
    else
      with {:ok, bytes} <- splice(device, ref, zeroes(ref.read_length)) do
        {:ok, punch_if_empty(bytes)}
      end
    end
  end

  defp splice(device, ref, slice) do
    with {:ok, existing} <- extent_bytes(device, ref) do
      tail_start = ref.read_start + ref.read_length

      {:ok,
       <<binary_part(existing, 0, ref.read_start)::binary, slice::binary,
         binary_part(existing, tail_start, byte_size(existing) - tail_start)::binary>>}
    end
  end

  defp punch_if_empty(bytes) do
    if bytes == zeroes(byte_size(bytes)), do: :hole, else: bytes
  end

  # One data-plane call for the whole request, so a write spanning several
  # extents selects a target once. A punch places nothing.
  defp place(device, planned) do
    case for {_ref, bytes} <- planned, is_binary(bytes), do: bytes do
      [] -> {:ok, []}
      bytes -> write_chunks(device.volume, bytes)
    end
  end

  defp commit(device, planned, written) do
    commit_planned(device, planned, written, expectations(planned))
  end

  defp commit_planned(device, planned, written, expect) do
    %{locations: locations, chunk_codecs: codecs} =
      ChunkWriter.chunk_refs_to_commit_opts(written)

    hashes = Enum.map(written, & &1.hash)

    core_call(:commit_written, [
      device.volume,
      device.path,
      extent_targets(planned, hashes),
      [locations: locations, chunk_codecs: codecs, epoch: device.epoch, expect: expect]
    ])
    |> case do
      {:ok, _published} -> {:ok, cost(planned, written)}
      {:error, _reason} = error -> error
    end
  end

  # The writer answers in the order it was given, so zipping the hashes back
  # onto the extents that produced them is positional — the punches keep
  # their place and take no hash.
  defp extent_targets(planned, hashes) do
    {targets, []} =
      Enum.map_reduce(planned, hashes, fn
        {ref, :hole}, remaining -> {{ref.index, :hole}, remaining}
        {ref, _bytes}, [hash | rest] -> {{ref.index, hash}, rest}
      end)

    targets
  end

  # Only the extents this write actually read are compared. One it overwrote
  # end to end owes nothing to what was there before, so naming it would
  # make disjoint writers collide for no reason.
  defp expectations(planned) do
    for {ref, _target} <- planned, ref.read_length != ref.width, do: {ref.index, ref.target}
  end

  defp cost(planned, written) do
    %{
      chunk_bytes: Enum.reduce(written, 0, &(&1.size + &2)),
      chunks_rewritten: length(written),
      chunks_replaced: Enum.count(planned, &match?({_ref, :hole}, &1))
    }
  end

  # ─── Retry, telemetry and plumbing ─────────────────────────────────────

  # A write that lost a race has lost nothing, so every frontend wants it
  # retried rather than failed — which is why the retry lives here and not
  # in one of them. NBD has no "retry this" status to hand back, and
  # `ublk`'s `-EAGAIN` means something else again; the *reply* is the
  # frontend's problem, the retrying is not.
  #
  # Past the budget the error is returned unchanged, for the frontend to
  # render however its protocol can.
  defp retrying_stale(command, fun, attempt \\ 0) do
    case fun.() do
      {:error, reason} = error ->
        if attempt < stale_retries() and contended?(reason) do
          :telemetry.execute(
            [:neonfs, :block, :stale_write_retry],
            %{attempt: attempt + 1},
            %{command: command}
          )

          Process.sleep(stale_backoff_ms() * 2 ** attempt)
          retrying_stale(command, fun, attempt + 1)
        else
          error
        end

      result ->
        result
    end
  end

  # Two ways a write loses a race it should simply run again. `:stale_chunks`
  # is an extent that moved under this write's read. A compare-and-swap that
  # ran out of attempts is the metadata layer giving up on a burst — a device
  # write publishes a chunk record and an extent map against one volume, so a
  # queue of them collides with itself. Neither is a reason to hand a guest an
  # IO error.
  defp contended?(:stale_chunks), do: true
  defp contended?({:cas_retries_exhausted, _}), do: true
  defp contended?({_stage, {:cas_retries_exhausted, _}}), do: true
  defp contended?(_reason), do: false

  defp stale_retries, do: Application.get_env(:neonfs_block, :stale_write_retries, 3)

  defp stale_backoff_ms, do: Application.get_env(:neonfs_block, :stale_write_backoff_ms, 10)

  defp read_opts(device, offset, length) do
    [offset: offset, length: length, telemetry_metadata: %{export: device.export}]
  end

  defp stream_from({:ok, %{stream: stream}}), do: {:ok, stream}
  defp stream_from({:error, _reason} = error), do: error

  defp measure(device, command, bytes, fun) do
    start_time = System.monotonic_time()
    result = fun.()
    status = if match?({:error, _}, result), do: :error, else: :ok
    emit(device, command, bytes, chunk_measurements(result), start_time, status)

    case result do
      {:ok, _} -> :ok
      other -> other
    end
  end

  # Only a write and a zero-fill come back carrying their chunk-layer cost;
  # the others have no such quantity, and inventing a zero for them would
  # read as "this write moved no chunks" on the same series.
  defp chunk_measurements({:ok, %{chunk_bytes: bytes, chunks_replaced: replaced}}),
    do: %{chunk_bytes: bytes, chunks_replaced: replaced}

  defp chunk_measurements({:ok, %{chunk_bytes: chunk_bytes}}), do: %{chunk_bytes: chunk_bytes}
  defp chunk_measurements(_result), do: %{}

  defp emit(device, command, bytes, extra_measurements, start_time, status) do
    measurements =
      Map.merge(
        %{bytes: bytes, duration: System.monotonic_time() - start_time},
        extra_measurements
      )

    :telemetry.execute(
      [:neonfs, :block, :command],
      measurements,
      %{export: device.export, command: command, status: status}
    )
  end

  defp zeroes(size), do: :binary.copy(<<0>>, size)

  defp split_export(export) do
    case String.split(export, ":", parts: 2) do
      [volume, path] when volume != "" and path != "" ->
        {:ok, volume, path}

      [volume] when volume != "" ->
        {:ok, volume, BlockAttachment.default_device_path()}

      _otherwise ->
        {:error, {:malformed_export_name, export}}
    end
  end

  # The three seams below are overridable so a test can drive the server
  # without a cluster behind it — the same `:core_call_fn` shape
  # `neonfs_webdav` uses, extended to the two data-plane calls now that this
  # node moves the bytes itself. Ordering guarantees (a flush that must not
  # ack early) are only assertable against a callee the test can hold open.
  defp write_chunks(volume, chunks) do
    case Application.get_env(:neonfs_block, :write_chunks_fn) do
      nil -> ChunkWriter.write_chunks(volume, chunks)
      fun when is_function(fun, 2) -> fun.(volume, chunks)
    end
  end

  defp fetch_chunk(volume, ref, opts) do
    case Application.get_env(:neonfs_block, :fetch_chunk_fn) do
      nil -> ChunkReader.fetch_chunk(volume, ref, opts)
      fun when is_function(fun, 3) -> fun.(volume, ref, opts)
    end
  end

  defp core_call(function, args) do
    case Application.get_env(:neonfs_block, :core_call_fn) do
      nil -> Client.core_call(@backing, function, args)
      fun when is_function(fun, 3) -> fun.(@backing, function, args)
    end
  end
end
