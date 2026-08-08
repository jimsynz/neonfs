defmodule NeonFS.Core.BlockBacking do
  @moduledoc """
  File-backed block device store — the spike backing for block volumes.

  A device is one sized file in a NeonFS volume, written only through a
  forced fixed-size chunk strategy so every guest write lands on a
  predictable chunk boundary. Content-defined chunking is refused
  outright: FastCDC boundaries move when the bytes under them change, so
  one random overwrite re-chunks everything after it — the opposite of
  what a block device needs.

  ## Geometry

  Devices advertise 4Kn — 4 KiB logical and physical blocks. Sub-block
  guest writes are absorbed by the guest's page cache, so writes reaching
  this module are block-aligned; each is read-modify-written at the chunk
  layer, making a single 4 KiB write cost a #{div(131_072, 1024)} KiB
  chunk rewrite. `write/4` reports that amplification as telemetry rather
  than leaving it to be inferred.

  ## Zeroes, discard and holes

  A `NeonFS.Core.FileMeta` holds an ordered list of chunk hashes and no
  per-chunk offset — a chunk's position *is* the sum of its predecessors'
  sizes. There is nowhere to record "these bytes are absent", and dropping
  a chunk from the middle shifts every later chunk down, corrupting the
  rest of the device. Discard and WRITE ZEROES therefore zero-fill instead
  of punching a hole. Every zero-filled chunk hashes to the same value and
  dedups to one stored blob, so a fully zeroed device costs a single chunk
  of storage plus its metadata entries. Real holes need per-extent
  offsets, which is what the extent-map backend that replaces this path
  introduces.

  ## Durability

  `write/4` returns when the volume's write acknowledgement policy is
  satisfied, which on a `write_ack: :local` volume is before the extra
  replicas exist. A guest flush or FUA must therefore call `flush/2`,
  which drives replication to `min_copies` before returning — the same
  barrier `fsync` uses.

  Replicated volumes only: streamed writes are unsupported on
  erasure-coded volumes, so `create_device/4` refuses one. Encrypted and
  compressed volumes work, but lose the direct data-plane read path.

  ## Telemetry

    * `[:neonfs, :block, :device_created]` — Measurements: `size`,
      `chunk_count`, `duration`. Metadata: `volume`, `path`, `file_id`.
    * `[:neonfs, :block, :write]` — Measurements: `guest_bytes`,
      `chunk_bytes`, `chunks_rewritten`, `duration`. Metadata: `volume`,
      `file_id`, `offset`. `chunk_bytes / guest_bytes` is the write
      amplification of that request.
    * `[:neonfs, :block, :read]` — Measurements: `guest_bytes`,
      `duration`. Metadata: `volume`, `file_id`, `offset`.
    * `[:neonfs, :block, :flush]` — Measurements: `duration`. Metadata:
      `volume`, `file_id`, `status`.
  """

  alias NeonFS.Core

  @chunk_bytes 131_072
  @chunk_strategy {:fixed, @chunk_bytes}

  @logical_block_bytes 4096
  @physical_block_bytes 4096

  # A single request is materialised in memory, so it is bounded here
  # rather than trusting the frontend. NBD's own maximum block size for a
  # read or write is 32 MiB; anything larger is a caller bug, not a large
  # request.
  @max_request_bytes 32 * 1024 * 1024

  # Zeroing a range writes it in batches so neither the working set nor a
  # single metadata commit scales with the length being zeroed.
  @zero_batch_bytes 8 * @chunk_bytes

  @type device :: %{
          volume: String.t(),
          file_id: binary(),
          path: String.t(),
          size: non_neg_integer(),
          chunk_bytes: pos_integer(),
          logical_block_bytes: pos_integer(),
          physical_block_bytes: pos_integer()
        }

  @doc """
  The fixed chunk size every block backing file is written with.
  """
  @spec chunk_bytes() :: pos_integer()
  def chunk_bytes, do: @chunk_bytes

  @doc """
  Creates a backing file of exactly `size_bytes` at `path` in `volume`.

  The file is written as zeroes through the forced fixed chunk strategy,
  so creation costs one stored chunk (every zero chunk dedups to it) plus
  one metadata entry per chunk. `size_bytes` must be a positive multiple
  of the 4 KiB logical block size and must fit the volume's `max_size`.

  Fails with a `NeonFS.Error.AlreadyExists` if `path` is already taken, so
  two concurrent creations cannot both believe they own the device.
  """
  @spec create_device(String.t(), String.t(), pos_integer(), keyword()) ::
          {:ok, device()} | {:error, term()}
  def create_device(volume, path, size_bytes, opts \\ []) do
    start_time = System.monotonic_time()

    with :ok <- validate_device_size(size_bytes),
         {:ok, write_opts} <- force_fixed_chunking(opts),
         {:ok, volume_record} <- Core.get_volume(volume),
         :ok <- validate_durability(volume_record),
         :ok <- validate_capacity(volume_record, size_bytes),
         {:ok, meta} <-
           Core.write_file_streamed(
             volume,
             path,
             zero_stream(size_bytes),
             Keyword.put(write_opts, :create_only, true)
           ) do
      :telemetry.execute(
        [:neonfs, :block, :device_created],
        %{
          size: meta.size,
          chunk_count: length(meta.chunks),
          duration: System.monotonic_time() - start_time
        },
        %{volume: volume, path: path, file_id: meta.id}
      )

      {:ok, device_from_meta(volume, meta)}
    end
  end

  @doc """
  Resolves an existing backing file at `path` into a device handle.

  The handle carries the `file_id`, so every later operation addresses the
  device by identity — a rename of the backing file does not disturb an
  attached device.
  """
  @spec open_device(String.t(), String.t()) :: {:ok, device()} | {:error, term()}
  def open_device(volume, path) do
    with {:ok, meta} <- Core.get_file_meta(volume, path) do
      {:ok, device_from_meta(volume, meta)}
    end
  end

  @doc """
  Current geometry and size of the device identified by `file_id`.
  """
  @spec device_info(String.t(), binary()) :: {:ok, device()} | {:error, term()}
  def device_info(volume, file_id) do
    with {:ok, meta} <- Core.get_file_meta_by_id(volume, file_id) do
      {:ok, device_from_meta(volume, meta)}
    end
  end

  @doc """
  Reads `length` bytes at `offset` from the device.

  Both must be 4 KiB-aligned and the range must fall inside the device.
  Regions never written read as zeroes.
  """
  @spec read(String.t(), binary(), non_neg_integer(), pos_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read(volume, file_id, offset, length) do
    start_time = System.monotonic_time()

    with :ok <- validate_request(offset, length),
         {:ok, device} <- device_info(volume, file_id),
         :ok <- validate_range(device, offset, length),
         {:ok, data} <- Core.read_file_by_id(volume, file_id, offset: offset, length: length) do
      :telemetry.execute(
        [:neonfs, :block, :read],
        %{guest_bytes: byte_size(data), duration: System.monotonic_time() - start_time},
        %{volume: volume, file_id: file_id, offset: offset}
      )

      {:ok, data}
    end
  end

  @doc """
  Lazy-stream counterpart to `read/4` for a range too large to hold at
  once — one element per chunk of the range.
  """
  @spec read_stream(String.t(), binary(), non_neg_integer(), pos_integer()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def read_stream(volume, file_id, offset, length) do
    with :ok <- validate_alignment(offset, length),
         {:ok, device} <- device_info(volume, file_id),
         :ok <- validate_range(device, offset, length),
         {:ok, %{stream: stream}} <-
           Core.read_file_stream_by_id(volume, file_id, offset: offset, length: length) do
      {:ok, stream}
    end
  end

  @doc """
  Writes `data` at `offset`, rewriting only the chunks it overlaps.

  The device cannot grow, so a write past the end is refused rather than
  extending the backing file. `opts` may not select a chunk strategy other
  than the forced fixed one.
  """
  @spec write(String.t(), binary(), non_neg_integer(), binary(), keyword()) ::
          :ok | {:error, term()}
  def write(volume, file_id, offset, data, opts \\ []) do
    start_time = System.monotonic_time()
    guest_bytes = byte_size(data)

    with :ok <- validate_request(offset, guest_bytes),
         {:ok, write_opts} <- force_fixed_chunking(opts),
         {:ok, device} <- device_info(volume, file_id),
         :ok <- validate_range(device, offset, guest_bytes),
         {:ok, _meta} <- Core.write_file_at_by_id(volume, file_id, offset, data, write_opts) do
      {chunks, chunk_bytes} = rewritten_chunks(device, offset, guest_bytes)

      :telemetry.execute(
        [:neonfs, :block, :write],
        %{
          guest_bytes: guest_bytes,
          chunk_bytes: chunk_bytes,
          chunks_rewritten: chunks,
          duration: System.monotonic_time() - start_time
        },
        %{volume: volume, file_id: file_id, offset: offset}
      )

      :ok
    end
  end

  @doc """
  Zero-fills `length` bytes at `offset` — the device's WRITE ZEROES.

  Written in bounded batches, so neither memory nor a single metadata
  commit scales with `length`. Whole zeroed chunks dedup to one stored
  blob; see the module doc for why this cannot drop the extent instead.
  """
  @spec write_zeroes(String.t(), binary(), non_neg_integer(), pos_integer()) ::
          :ok | {:error, term()}
  def write_zeroes(volume, file_id, offset, length) do
    with :ok <- validate_alignment(offset, length),
         {:ok, device} <- device_info(volume, file_id),
         :ok <- validate_range(device, offset, length) do
      write_zero_batches(volume, file_id, zero_batches(offset, length))
    end
  end

  @doc """
  Discards `length` bytes at `offset`.

  Identical to `write_zeroes/4`: a chunk list cannot express an absent
  extent, so even a chunk-aligned discard zero-fills. The zeroes dedup, so
  the storage cost collapses even though the metadata entries remain.
  """
  @spec discard(String.t(), binary(), non_neg_integer(), pos_integer()) :: :ok | {:error, term()}
  def discard(volume, file_id, offset, length) do
    write_zeroes(volume, file_id, offset, length)
  end

  @doc """
  Durability barrier for the device — the guest's flush and FUA.

  Returns once every chunk of the backing file has the volume's
  `min_copies` durable replicas.
  """
  @spec flush(String.t(), binary()) :: :ok | {:error, term()}
  def flush(volume, file_id) do
    start_time = System.monotonic_time()
    result = Core.sync_file_by_id(volume, file_id)

    :telemetry.execute(
      [:neonfs, :block, :flush],
      %{duration: System.monotonic_time() - start_time},
      %{volume: volume, file_id: file_id, status: if(result == :ok, do: :ok, else: :error)}
    )

    result
  end

  defp write_zero_batches(volume, file_id, batches) do
    Enum.reduce_while(batches, :ok, fn {offset, length}, :ok ->
      case write(volume, file_id, offset, zeroes(length)) do
        :ok -> {:cont, :ok}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
  end

  defp device_from_meta(volume, meta) do
    %{
      volume: volume,
      file_id: meta.id,
      path: meta.path,
      size: meta.size,
      chunk_bytes: @chunk_bytes,
      logical_block_bytes: @logical_block_bytes,
      physical_block_bytes: @physical_block_bytes
    }
  end

  defp force_fixed_chunking(opts) do
    case Keyword.fetch(opts, :chunk_strategy) do
      :error -> {:ok, Keyword.put(opts, :chunk_strategy, @chunk_strategy)}
      {:ok, @chunk_strategy} -> {:ok, opts}
      {:ok, other} -> {:error, {:unsupported_chunk_strategy, other}}
    end
  end

  defp validate_device_size(size)
       when is_integer(size) and size > 0 and rem(size, @logical_block_bytes) == 0,
       do: :ok

  defp validate_device_size(size), do: {:error, {:invalid_device_size, size}}

  defp validate_durability(%{durability: %{type: :erasure}}),
    do: {:error, :erasure_volumes_unsupported}

  defp validate_durability(_volume), do: :ok

  defp validate_capacity(%{max_size: nil}, _size), do: :ok

  defp validate_capacity(%{max_size: max_size}, size) when size <= max_size, do: :ok

  defp validate_capacity(%{max_size: max_size}, size),
    do: {:error, {:device_exceeds_volume_max_size, size, max_size}}

  defp validate_request(offset, length) do
    with :ok <- validate_alignment(offset, length) do
      validate_request_size(length)
    end
  end

  defp validate_alignment(offset, length)
       when is_integer(offset) and offset >= 0 and is_integer(length) and length > 0 and
              rem(offset, @logical_block_bytes) == 0 and rem(length, @logical_block_bytes) == 0,
       do: :ok

  defp validate_alignment(offset, length), do: {:error, {:unaligned_request, offset, length}}

  defp validate_request_size(length) when length <= @max_request_bytes, do: :ok

  defp validate_request_size(length),
    do: {:error, {:request_too_large, length, @max_request_bytes}}

  defp validate_range(%{size: size}, offset, length) when offset + length <= size, do: :ok

  defp validate_range(%{size: size}, offset, length),
    do: {:error, {:out_of_range, offset, length, size}}

  # Chunk boundaries are fixed, so which chunks a write touches — and what
  # they cost to rewrite — is arithmetic rather than a metadata read. The
  # final chunk is short when the device size is not a whole multiple of
  # the chunk size.
  defp rewritten_chunks(%{size: size}, offset, length) do
    first = div(offset, @chunk_bytes)
    last = div(offset + length - 1, @chunk_bytes)

    chunk_bytes =
      Enum.reduce(first..last, 0, fn index, acc ->
        chunk_start = index * @chunk_bytes
        acc + min(chunk_start + @chunk_bytes, size) - chunk_start
      end)

    {last - first + 1, chunk_bytes}
  end

  defp zero_batches(offset, length) do
    Stream.unfold({offset, length}, fn
      {_offset, 0} ->
        nil

      {batch_offset, remaining} ->
        batch = min(remaining, @zero_batch_bytes)
        {{batch_offset, batch}, {batch_offset + batch, remaining - batch}}
    end)
  end

  defp zero_stream(size) do
    Stream.unfold(size, fn
      0 ->
        nil

      remaining ->
        segment = min(remaining, @chunk_bytes)
        {zeroes(segment), remaining - segment}
    end)
  end

  defp zeroes(size), do: :binary.copy(<<0>>, size)
end
