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
  chunk rewrite. `write/5` reports that amplification as telemetry rather
  than leaving it to be inferred, and returns it as well so a caller on
  another node — which never sees this node's telemetry — can attribute
  it to whatever it calls the device.

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

  `write_zeroes/4` reports that as two costs rather than one, because they
  are not the same quantity: the chunks it clips at either end are
  read-modify-written and cost their bytes, while the chunks it covers end
  to end cost a metadata entry each and no bytes beyond the single zero
  chunk they all share. Charging a 64 GiB TRIM for 64 GiB of rewrites is
  not a rounding error, and charging it for nothing hides the half-million
  metadata entries it does cost.

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
    * `[:neonfs, :block, :write_zeroes]` — Measurements: `guest_bytes`,
      `chunk_bytes`, `chunks_rewritten`, `chunks_replaced`, `duration`.
      Metadata: `volume`, `file_id`, `offset`. Serves discard too.
    * `[:neonfs, :block, :read]` — Measurements: `guest_bytes`,
      `duration`. Metadata: `volume`, `file_id`, `offset`.
    * `[:neonfs, :block, :flush]` — Measurements: `duration`. Metadata:
      `volume`, `file_id`, `status`.
  """

  alias NeonFS.Core

  @chunk_bytes 131_072
  @chunk_strategy {:fixed, @chunk_bytes}

  # One device per volume means the backing file's name is the same
  # everywhere, so it is defined once here rather than spelled out by core,
  # the CLI, CSI and the acceptance rig. It is deliberately not a field on
  # the volume record, which could only ever hold this one value.
  @device_path "/dev.img"

  @logical_block_bytes 4096
  @physical_block_bytes 4096

  # A single request is materialised in memory, so it is bounded here
  # rather than trusting the frontend. NBD's own maximum block size for a
  # read or write is 32 MiB; anything larger is a caller bug, not a large
  # request.
  @max_request_bytes 32 * 1024 * 1024

  @type device :: %{
          volume: String.t(),
          file_id: binary(),
          path: String.t(),
          size: non_neg_integer(),
          chunk_bytes: pos_integer(),
          logical_block_bytes: pos_integer(),
          physical_block_bytes: pos_integer()
        }

  @type zero_fill_cost :: %{
          chunk_bytes: non_neg_integer(),
          chunks_rewritten: non_neg_integer(),
          chunks_replaced: non_neg_integer()
        }

  @doc """
  The fixed chunk size every block backing file is written with.
  """
  @spec chunk_bytes() :: pos_integer()
  def chunk_bytes, do: @chunk_bytes

  @doc """
  The path of the single backing file a block volume holds.
  """
  @spec device_path() :: String.t()
  def device_path, do: @device_path

  @doc """
  Provisions the device a freshly-created block volume owns.

  A block volume is its device: `max_size` is both the volume's quota and
  the device's size, so creating one provisions the backing file rather
  than leaving the volume half-made until a second command runs. Volumes
  of any other type are left alone.

  A device that cannot be written takes its volume with it — the volume is
  deleted and the device's error returned, so a block volume without its
  device is never observable.
  """
  @spec provision_volume_device(NeonFS.Core.Volume.t()) :: :ok | {:error, term()}
  def provision_volume_device(%{type: :block, name: name, max_size: max_size}) do
    case create_device(name, @device_path, max_size) do
      {:ok, _device} -> :ok
      {:error, _reason} = error -> rollback(error, name)
    end
  end

  def provision_volume_device(_volume), do: :ok

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

  Returns the chunk-layer cost of the write alongside the acknowledgement:
  `chunk_bytes` is what the chunk layer rewrote to store `byte_size(data)`
  guest bytes, so their ratio is the write amplification of this request.
  """
  @spec write(String.t(), binary(), non_neg_integer(), binary(), keyword()) ::
          {:ok, %{chunk_bytes: non_neg_integer(), chunks_rewritten: pos_integer()}}
          | {:error, term()}
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

      {:ok, %{chunk_bytes: chunk_bytes, chunks_rewritten: chunks}}
    end
  end

  @doc """
  Zero-fills `length` bytes at `offset` — the device's WRITE ZEROES.

  One metadata commit for the whole range, however long: whole covered
  chunks are replaced by the canonical zero chunk rather than rewritten,
  and only the partial chunks at either end cost a read-modify-write. A
  full-device TRIM is therefore one commit, not one per megabyte. Memory
  stays bounded at a single chunk. See the module doc for why this cannot
  drop the extent instead.

  Returns that cost as its two halves: `chunk_bytes` is what the chunk
  layer wrote — the clipped chunks it read-modify-wrote plus one stored
  zero chunk per distinct covered size — and `chunks_replaced` counts the
  chunks that cost a metadata entry and no bytes at all.
  """
  @spec write_zeroes(String.t(), binary(), non_neg_integer(), pos_integer()) ::
          {:ok, zero_fill_cost()} | {:error, term()}
  def write_zeroes(volume, file_id, offset, length) do
    start_time = System.monotonic_time()

    with :ok <- validate_alignment(offset, length),
         {:ok, device} <- device_info(volume, file_id),
         :ok <- validate_range(device, offset, length),
         {:ok, _meta} <- Core.write_zeroes_by_id(volume, file_id, offset, length) do
      cost = zero_fill_cost(device, offset, length)

      :telemetry.execute(
        [:neonfs, :block, :write_zeroes],
        Map.merge(cost, %{
          guest_bytes: length,
          duration: System.monotonic_time() - start_time
        }),
        %{volume: volume, file_id: file_id, offset: offset}
      )

      {:ok, cost}
    end
  end

  @doc """
  Discards `length` bytes at `offset`.

  Identical to `write_zeroes/4`: a chunk list cannot express an absent
  extent, so even a chunk-aligned discard zero-fills. The zeroes dedup, so
  the storage cost collapses even though the metadata entries remain.
  """
  @spec discard(String.t(), binary(), non_neg_integer(), pos_integer()) ::
          {:ok, zero_fill_cost()} | {:error, term()}
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

  defp rollback(error, volume) do
    _ = Core.delete_volume(volume)
    error
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
      Enum.reduce(first..last, 0, fn index, acc -> acc + span_size(chunk_span(size, index)) end)

    {last - first + 1, chunk_bytes}
  end

  # Which chunks a zero-fill covers end to end and which it merely clips is
  # the same arithmetic, but the two cannot be added into one number. A
  # clipped chunk is read back and rewritten, costing its whole size; a
  # covered one is replaced by the hash of a zero chunk, so the entire
  # covered run costs one stored chunk per distinct size it contains. Every
  # chunk strictly between the first and the last is covered and full-sized
  # — the file's short tail chunk can only ever be the last — so the two
  # edges are the only ones that need examining.
  defp zero_fill_cost(%{size: size}, offset, length) do
    write_end = offset + length
    first = div(offset, @chunk_bytes)
    last = div(write_end - 1, @chunk_bytes)
    edges = Enum.uniq([first, last])

    {clipped, covered_edges} =
      edges
      |> Enum.map(&chunk_span(size, &1))
      |> Enum.split_with(fn {start, stop} -> start < offset or stop > write_end end)

    covered_middle = last - first + 1 - Enum.count(edges)

    stored_zeroes =
      Enum.uniq(middle_chunk_sizes(covered_middle) ++ Enum.map(covered_edges, &span_size/1))

    %{
      chunk_bytes: Enum.sum(Enum.map(clipped, &span_size/1)) + Enum.sum(stored_zeroes),
      chunks_rewritten: Enum.count(clipped),
      chunks_replaced: covered_middle + Enum.count(covered_edges)
    }
  end

  defp middle_chunk_sizes(0), do: []
  defp middle_chunk_sizes(_covered_middle), do: [@chunk_bytes]

  defp chunk_span(size, index) do
    start = index * @chunk_bytes
    {start, min(start + @chunk_bytes, size)}
  end

  defp span_size({start, stop}), do: stop - start

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
