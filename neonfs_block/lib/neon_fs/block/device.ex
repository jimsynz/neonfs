defmodule NeonFS.Block.Device do
  @moduledoc """
  Maps NBD transmission commands onto `NeonFS.Core.BlockBacking`.

  One module between the wire and the cluster, so the connection handler
  deals in protocol terms and this deals in storage terms.

  ## Export names

  An export names a backing file as `<volume>:<path>` — `blockvol:/dev.img`.
  A volume name cannot contain a colon, so the split is unambiguous, and the
  path keeps its leading slash exactly as it appears in the volume.

  ## Reads stream

  A read never materialises its range. `NeonFS.Client.ChunkReader` yields one
  chunk at a time and each is written to the socket as it arrives, so a client
  asking for the largest request the export advertises still costs one chunk
  of memory here. The reply header carries no length — the client already knows
  it from its own request — so the payload can follow lazily.

  ## Flush is a promise

  `flush/1` returns only once `BlockBacking.flush/2` has driven replication to
  the volume's `min_copies`. Acknowledging a flush before that would tell a
  guest filesystem its journal is durable when it is not, which is the one
  thing a block device must never do.

  ## Telemetry

    * `[:neonfs, :block, :command]` — Measurements: `bytes`, `duration`,
      and on a write `chunk_bytes`. Metadata: `export`, `command`,
      `status`.

  `bytes` is what the guest asked for; `chunk_bytes` is what the chunk
  layer moved to serve it, so their ratio is the request's amplification.
  A write gets its `chunk_bytes` from `BlockBacking.write/5`'s reply,
  because the arithmetic depends on the chunk geometry, which lives on
  core. A read's equivalent cannot come from here at all — only
  `NeonFS.Client.ChunkReader` knows which chunks a range read fetched —
  so it arrives as that module's `chunk_fetched` events, tagged with this
  export through `:telemetry_metadata`. `NeonFS.Block.Telemetry` exports
  both.
  """

  alias NeonFS.Client
  alias NeonFS.Client.ChunkReader

  @backing NeonFS.Core.BlockBacking

  @type t :: %{
          export: String.t(),
          volume: String.t(),
          path: String.t(),
          file_id: binary(),
          size: non_neg_integer(),
          logical_block_size: pos_integer(),
          physical_block_size: pos_integer(),
          read_only: boolean()
        }

  @doc """
  Resolves an export name into a device handle.

  Fails rather than inventing a device: the backing file has to exist, which
  is `create_device/4`'s job and not something an attach should do implicitly.
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
         file_id: info.file_id,
         size: info.size,
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
  Streams `length` bytes at `offset`, one element per chunk.

  Returns the stream rather than the bytes so the caller can write each chunk
  to the socket as it arrives.
  """
  @spec read_stream(t(), non_neg_integer(), pos_integer()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def read_stream(device, offset, length) do
    case read_call(device, offset, length) do
      {:ok, %{stream: stream}} -> {:ok, stream}
      {:error, _reason} = error -> error
    end
  end

  defp read_call(device, offset, length) do
    opts = [
      offset: offset,
      length: length,
      telemetry_metadata: %{export: device.export}
    ]

    case Application.get_env(:neonfs_block, :read_stream_fn) do
      nil -> ChunkReader.read_file_stream_by_id(device.volume, device.file_id, opts)
      fun when is_function(fun, 2) -> fun.(device, opts)
    end
  end

  @doc """
  Writes `data` at `offset`.
  """
  @spec write(t(), non_neg_integer(), binary()) :: :ok | {:error, term()}
  def write(device, offset, data) do
    measure(device, :write, byte_size(data), fn ->
      core_call(:write, [device.volume, device.file_id, offset, data])
    end)
  end

  @doc """
  Durability barrier — returns only once the write is replicated.
  """
  @spec flush(t()) :: :ok | {:error, term()}
  def flush(device) do
    measure(device, :flush, 0, fn ->
      core_call(:flush, [device.volume, device.file_id])
    end)
  end

  @doc """
  Zero-fills `length` bytes at `offset`, serving both TRIM and WRITE ZEROES.
  """
  @spec write_zeroes(t(), non_neg_integer(), pos_integer()) :: :ok | {:error, term()}
  def write_zeroes(device, offset, length) do
    measure(device, :write_zeroes, length, fn ->
      core_call(:write_zeroes, [device.volume, device.file_id, offset, length])
    end)
  end

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

  # Only a write comes back carrying its chunk-layer cost; the others have
  # no such quantity, and inventing a zero for them would read as "this
  # write moved no chunks" on the same series.
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
      [volume, path] when volume != "" and path != "" -> {:ok, volume, path}
      _otherwise -> {:error, {:malformed_export_name, export}}
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
