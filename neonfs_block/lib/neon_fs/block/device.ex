defmodule NeonFS.Block.Device do
  @moduledoc """
  Maps NBD transmission commands onto `NeonFS.Core.BlockBacking`.

  One module between the wire and the cluster, so the connection handler
  deals in protocol terms and this deals in storage terms.

  ## Export names

  An export names a device as `<volume>:<path>` — `blockvol:/dev.img`. A
  volume name cannot contain a colon, so the split is unambiguous, and the
  path keeps its leading slash exactly as the device records it.

  A bare `<volume>` names that volume's own device, at the cluster-wide
  constant `NeonFS.Core.BlockAttachment.default_device_path/0` that core
  provisions against. That is the form `nbd-client -N blockvol` and CSI want,
  and one device per volume makes it unambiguous.

  ## Reads stream

  A read never materialises its range: it is cut into extent-sized pieces
  here and each is fetched with its own call, so a client asking for the
  largest request the export advertises still costs one extent of memory.
  The reply header carries no length — the client already knows it from its
  own request — so the payload can follow lazily.

  Those pieces cross Erlang distribution rather than the data plane, which
  they did not while a device was a file: an extent is addressed by the map
  on core, and resolving it to chunk references this node could fetch
  itself is the next slice of the block work.

  ## Writes carry the attachment's epoch

  `open/1` reads the device's fencing epoch and every write is stamped with
  it. A later attacher preempting this one bumps the epoch, so these writes
  start failing with `{:fenced, current}` — the signal to tear the device
  down rather than retry.

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
  A write gets its `chunk_bytes` from `BlockBacking.write/5`'s reply,
  because the arithmetic depends on the extent geometry, which lives on
  core. A read has no counterpart: the extent map is resolved on core, so
  this node asks for a byte range and is handed bytes, with nothing of
  the chunk layer visible to count.

  A zero-fill answers with both numbers because neither describes it
  alone: it rewrites only the extents it clips, and drops the ones it
  covers for the price of a metadata entry each. Reported as
  `chunk_bytes` alone a full-device TRIM looks free; `chunks_replaced` is
  what it actually cost.
  """

  @behaviour NeonFS.Block.Frontend

  alias NeonFS.Client
  alias NeonFS.Core.BlockAttachment

  @backing NeonFS.Core.BlockBacking

  @type t :: %{
          export: String.t(),
          volume: String.t(),
          path: String.t(),
          id: binary(),
          size: non_neg_integer(),
          chunk_bytes: pos_integer(),
          epoch: non_neg_integer(),
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
  to the socket as it arrives. The pieces are cut to the device's extents so
  that each call answers from one extent, which is what bounds the memory a
  whole-device read costs.
  """
  @spec read_stream(t(), non_neg_integer(), pos_integer()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def read_stream(device, offset, length) do
    case Application.get_env(:neonfs_block, :read_stream_fn) do
      nil -> extent_stream(device, offset, length)
      fun when is_function(fun, 2) -> stream_from(fun.(device, read_opts(device, offset, length)))
    end
  end

  # The first piece is fetched eagerly so the failures a caller can be told
  # about — a range past the end of the device, an unreachable core, a
  # device that has gone — still come back as an error status. Past that
  # point the reply header has gone out and NBD has no status left to send,
  # so a failure can only end the connection.
  defp extent_stream(device, offset, length) do
    [first | rest] = extent_spans(device, offset, length)

    with {:ok, head} <- read_span(device, first) do
      {:ok, Stream.concat([head], Stream.map(rest, &read_span!(device, &1)))}
    end
  end

  defp read_opts(device, offset, length) do
    [offset: offset, length: length, telemetry_metadata: %{export: device.export}]
  end

  defp stream_from({:ok, %{stream: stream}}), do: {:ok, stream}
  defp stream_from({:error, _reason} = error), do: error

  defp read_span(device, {offset, length}) do
    core_call(:read, [device.volume, device.path, offset, length])
  end

  defp read_span!(device, span) do
    case read_span(device, span) do
      {:ok, data} -> data
      {:error, reason} -> raise "block device read failed: #{inspect(reason)}"
    end
  end

  defp extent_spans(%{chunk_bytes: chunk_bytes}, offset, length) do
    first = div(offset, chunk_bytes)
    last = div(offset + length - 1, chunk_bytes)

    Enum.map(first..last, fn index ->
      extent_start = index * chunk_bytes
      span_start = max(offset, extent_start)
      span_end = min(offset + length, extent_start + chunk_bytes)
      {span_start, span_end - span_start}
    end)
  end

  @doc """
  Writes `data` at `offset`.
  """
  @spec write(t(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def write(device, offset, data) do
    retrying_stale(:write, fn ->
      measure(device, :write, byte_size(data), fn ->
        core_call(:write, [device.volume, device.path, offset, data, epoch_opts(device)])
      end)
    end)
  end

  @doc """
  Durability barrier — returns only once the write is replicated.
  """
  @spec flush(t()) :: :ok | {:error, term()}
  def flush(device) do
    measure(device, :flush, 0, fn ->
      core_call(:flush, [device.volume, device.path])
    end)
  end

  @doc """
  Zero-fills `length` bytes at `offset`, serving both TRIM and WRITE ZEROES.
  """
  @spec write_zeroes(t(), non_neg_integer(), pos_integer()) :: :ok | {:error, term()}
  def write_zeroes(device, offset, length) do
    retrying_stale(:write_zeroes, fn ->
      measure(device, :write_zeroes, length, fn ->
        core_call(:write_zeroes, [
          device.volume,
          device.path,
          offset,
          length,
          epoch_opts(device)
        ])
      end)
    end)
  end

  # A write that exhausted core's retry budget against a contended span has
  # lost nothing, so every frontend wants it retried rather than failed —
  # which is why the retry lives here and not in one of them. NBD has no
  # "retry this" status to hand back, and `ublk`'s `-EAGAIN` means something
  # else again; the *reply* is the frontend's problem, the retrying is not.
  #
  # Past the budget the error is returned unchanged, for the frontend to
  # render however its protocol can.
  defp retrying_stale(command, fun, attempt \\ 0) do
    case fun.() do
      {:error, :stale_chunks} = error ->
        if attempt < stale_retries() do
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

  defp epoch_opts(device), do: [epoch: device.epoch]

  defp stale_retries, do: Application.get_env(:neonfs_block, :stale_write_retries, 3)

  defp stale_backoff_ms, do: Application.get_env(:neonfs_block, :stale_write_backoff_ms, 10)

  @doc """
  Emits the telemetry for a read, whose bytes are counted by the caller as it
  drains the stream rather than here.
  """
  @spec measure_read(t(), non_neg_integer(), integer(), :ok | :error) :: :ok
  def measure_read(device, bytes, start_time, status) do
    emit(device, :read, bytes, %{}, start_time, status)
  end

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

  # Overridable so a test can drive the server without a cluster behind it —
  # the same `:core_call_fn` seam `neonfs_webdav` uses. Ordering guarantees
  # (a flush that must not ack early) are only assertable against a callee the
  # test can hold open.
  defp core_call(function, args) do
    case Application.get_env(:neonfs_block, :core_call_fn) do
      nil -> Client.core_call(@backing, function, args)
      fun when is_function(fun, 3) -> fun.(@backing, function, args)
    end
  end
end
