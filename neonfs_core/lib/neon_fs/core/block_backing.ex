defmodule NeonFS.Core.BlockBacking do
  @moduledoc """
  Extent-map block device store — identity, reads, writes, zeroes and
  discard for a block volume's device.

  A device is a `NeonFS.Core.Volume.BlockDevice` header plus one
  `NeonFS.Core.BlockIndex` extent per `block_chunk_bytes` of its address
  space. It is not a file: there is no `NeonFS.Core.FileMeta`, no chunk
  list, and nothing to rewrite when one extent changes.

  ## Geometry

  Devices advertise 4Kn — 4 KiB logical and physical blocks. Sub-block
  guest writes are absorbed by the guest's page cache, so writes reaching
  this module are block-aligned; each is read-modify-written at the extent
  layer, making a single 4 KiB write cost a #{div(131_072, 1024)} KiB
  extent rewrite. `write/5` reports that amplification as telemetry rather
  than leaving it to be inferred, and returns it as well so a caller on
  another node — which never sees this node's telemetry — can attribute
  it to whatever it calls the device.

  ## Holes

  An extent with no entry reads as zeroes, so creation writes no data at
  all: a device is provisioned by publishing its header. Discard and
  WRITE ZEROES **punch** — they drop the extents they cover rather than
  storing zeroes for them — so a full-device TRIM costs one commit and no
  bytes, and the chunks it dropped become GC's problem.

  Only the extents a zero-fill *clips* are read-modify-written, and an
  extent that is entirely zeroes once its clipped part is zeroed is
  punched as well. `write_zeroes/5` reports those as two quantities
  because they are not the same cost: the clipped extents cost their bytes,
  the punched ones cost a metadata entry each and nothing else.

  ## Where placement happens

  Chunks are placed **on core**, through
  `NeonFS.Core.WriteOperation.place_chunk/4`, and the extent map commits
  second — the ordering `NeonFS.Core.BlockIndex` documents as its crash
  contract. That means a guest write's bytes cross Erlang distribution to
  get here, and a read's bytes cross it going back. Moving both onto the
  TLS data plane, with the interface node placing its own chunks, is the
  next slice of this work.

  ## What the volume is charged

  Creating a device charges the volume's logical size once: the device *is*
  the volume, and its address space is reserved whether or not the guest has
  written to it. Physical usage is charged per placed chunk and discharged by
  the sweep that reclaims a replaced one, so a thinly-written device reports
  what it actually occupies.

  The charge is per *placement*, not per distinct chunk, so writing bytes
  that already hash to a stored chunk charges twice for one blob while the
  sweep discharges it once. That is the same dedup-aware drift the scrub's
  reconcile already declines to rebuild for files, and the counters clamp at
  zero rather than going negative. Making it exact needs a cluster-truth
  index read per extent written, which is a cost on every guest write to fix
  a gauge.

  ## Durability

  `write/5` returns when the volume's write acknowledgement policy is
  satisfied, which on a `write_ack: :local` volume is before the extra
  replicas exist. A guest flush or FUA must therefore call `flush/2`,
  which drives replication to `min_copies` before returning — the same
  barrier `fsync` uses.

  Replicated volumes only: an erasure-coded volume is refused at
  `create_device/4`, so no extent here ever names a stripe member.

  ## Telemetry

    * `[:neonfs, :block, :device_created]` — Measurements: `size`,
      `extent_count`, `duration`. Metadata: `volume`, `path`, `device_id`.
    * `[:neonfs, :block, :write]` — Measurements: `guest_bytes`,
      `chunk_bytes`, `chunks_rewritten`, `duration`. Metadata: `volume`,
      `path`, `offset`. `chunk_bytes / guest_bytes` is the write
      amplification of that request.
    * `[:neonfs, :block, :write_zeroes]` — Measurements: `guest_bytes`,
      `chunk_bytes`, `chunks_rewritten`, `chunks_replaced`, `duration`.
      Metadata: `volume`, `path`, `offset`. Serves discard too.
    * `[:neonfs, :block, :read]` — Measurements: `guest_bytes`,
      `duration`. Metadata: `volume`, `path`, `offset`.
    * `[:neonfs, :block, :flush]` — Measurements: `duration`. Metadata:
      `volume`, `path`, `status`.
  """

  alias NeonFS.Core

  alias NeonFS.Core.{
    BlockAttachment,
    BlockEpoch,
    BlockIndex,
    ChunkIndex,
    ChunkReconciler,
    Replication,
    VolumeRegistry,
    WriteOperation
  }

  alias NeonFS.Core.Volume.BlockDevice
  alias NeonFS.Error.AlreadyExists

  @chunk_bytes 131_072

  # One device per volume means the device's name is the same everywhere, so
  # it is defined once rather than spelled out by core, the CLI, CSI and the
  # acceptance rig. It lives in `neonfs_client` because CSI names a volume's
  # device to build its attachment claim path and cannot depend on core to
  # ask. It is deliberately not a field on the volume record, which could
  # only ever hold this one value.
  @device_path BlockAttachment.default_device_path()

  @logical_block_bytes 4096
  @physical_block_bytes 4096

  # A single request is materialised in memory, so it is bounded here
  # rather than trusting the frontend. NBD's own maximum block size for a
  # read or write is 32 MiB; anything larger is a caller bug, not a large
  # request.
  @max_request_bytes 32 * 1024 * 1024

  @type device :: %{
          volume: String.t(),
          id: binary(),
          path: String.t(),
          size: non_neg_integer(),
          chunk_bytes: pos_integer(),
          logical_block_bytes: pos_integer(),
          physical_block_bytes: pos_integer()
        }

  @type attached_device :: %{
          volume: String.t(),
          id: binary(),
          path: String.t(),
          size: non_neg_integer(),
          chunk_bytes: pos_integer(),
          logical_block_bytes: pos_integer(),
          physical_block_bytes: pos_integer(),
          epoch: non_neg_integer()
        }

  @type extent_ref :: %{
          index: non_neg_integer(),
          width: pos_integer(),
          read_start: non_neg_integer(),
          read_length: pos_integer(),
          target: BlockIndex.target(),
          hash: binary() | nil,
          locations: [NeonFS.Core.ChunkMeta.location()],
          compression: atom(),
          encrypted: boolean()
        }

  @type write_cost :: %{chunk_bytes: non_neg_integer(), chunks_rewritten: non_neg_integer()}

  @type zero_fill_cost :: %{
          chunk_bytes: non_neg_integer(),
          chunks_rewritten: non_neg_integer(),
          chunks_replaced: non_neg_integer()
        }

  @doc """
  The extent width a block volume is written with when it names none.

  The size is per-volume (`NeonFS.Core.Volume.block_chunk_bytes`) and fixed for
  the volume's life. This is the value a volume created before that field
  existed reads as, and the default a new one gets — so a device's figures
  stay comparable with the ones measured before the extent map.
  """
  @spec chunk_bytes() :: pos_integer()
  def chunk_bytes, do: @chunk_bytes

  @doc """
  The extent width `volume_record`'s device is stored at.
  """
  @spec chunk_bytes_for(NeonFS.Core.Volume.t() | map()) :: pos_integer()
  def chunk_bytes_for(%{block_chunk_bytes: size}) when is_integer(size) and size > 0, do: size
  def chunk_bytes_for(_volume), do: @chunk_bytes

  @doc """
  The path of the single device a block volume holds.
  """
  @spec device_path() :: String.t()
  def device_path, do: @device_path

  @doc """
  Provisions the device a freshly-created block volume owns.

  A block volume is its device: `max_size` is both the volume's quota and
  the device's size, so creating one provisions the device rather than
  leaving the volume half-made until a second command runs. Volumes of any
  other type are left alone.

  A device that cannot be published takes its volume with it — the volume is
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
  Publishes a device of exactly `size_bytes` at `path` in `volume`.

  Creation writes no device data: every extent starts as a hole and a hole
  reads as zeroes, so provisioning is one metadata commit however large the
  device. `size_bytes` must be a positive multiple of the 4 KiB logical
  block size and must fit the volume's `max_size`.

  Fails with a `NeonFS.Error.AlreadyExists` naming the device the volume
  already holds, so two concurrent creations cannot both believe they own it.
  """
  @spec create_device(String.t(), String.t(), pos_integer(), keyword()) ::
          {:ok, device()} | {:error, term()}
  def create_device(volume, path, size_bytes, opts \\ []) do
    start_time = System.monotonic_time()

    with :ok <- validate_device_size(size_bytes),
         {:ok, volume_record} <- Core.get_volume(volume),
         :ok <- validate_durability(volume_record),
         :ok <- validate_capacity(volume_record, size_bytes),
         :ok <- refuse_existing_device(volume),
         header <- new_header(path, size_bytes, chunk_bytes_for(volume_record)),
         {:ok, _roots} <- BlockIndex.put_device(volume, header, opts) do
      charge_usage(volume_record.id, logical_size: size_bytes)

      :telemetry.execute(
        [:neonfs, :block, :device_created],
        %{
          size: header.size_bytes,
          extent_count: extent_count(header),
          duration: System.monotonic_time() - start_time
        },
        %{volume: volume, path: path, device_id: header.id}
      )

      {:ok, device_from_header(volume, header)}
    end
  end

  @doc """
  Resolves an existing device at `path` into an attached handle.

  The handle carries the device's current fencing epoch, read consistently:
  every write the holder makes is stamped with it, and a later attacher
  preempting this one bumps it so those writes start being refused.

  Fails rather than inventing a device.
  """
  @spec open_device(String.t(), String.t()) :: {:ok, attached_device()} | {:error, term()}
  def open_device(volume, path) do
    with {:ok, volume_record} <- Core.get_volume(volume),
         {:ok, header} <- device_header(volume, path),
         {:ok, epoch} <- BlockEpoch.current({volume_record.id, path}) do
      {:ok, Map.put(device_from_header(volume, header), :epoch, epoch)}
    end
  end

  @doc """
  Current geometry and size of the device at `path`.

  The geometry alone, without the consensus read `open_device/2` pays for
  the epoch: this is what every IO resolves itself against, so it must not
  cost a Ra query per request.
  """
  @spec device_info(String.t(), String.t()) :: {:ok, device()} | {:error, term()}
  def device_info(volume, path) do
    with {:ok, header} <- device_header(volume, path) do
      {:ok, device_from_header(volume, header)}
    end
  end

  @doc """
  Reads `length` bytes at `offset` from the device.

  Both must be 4 KiB-aligned and the range must fall inside the device.
  Regions never written read as zeroes.
  """
  @spec read(String.t(), String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, binary()} | {:error, term()}
  def read(volume, path, offset, length) do
    start_time = System.monotonic_time()

    with :ok <- validate_request(offset, length),
         {:ok, device} <- device_info(volume, path),
         :ok <- validate_range(device, offset, length),
         {:ok, data} <- read_range(device, offset, length) do
      :telemetry.execute(
        [:neonfs, :block, :read],
        %{guest_bytes: byte_size(data), duration: System.monotonic_time() - start_time},
        %{volume: volume, path: path, offset: offset}
      )

      {:ok, data}
    end
  end

  @doc """
  Lazy-stream counterpart to `read/4` for a range too large to hold at
  once — one element per extent of the range.

  The stream resolves one extent at a time, so a range covering the whole
  device costs one extent of memory. A failure part-way through raises,
  because a lazily-consumed range has no reply left to fail.
  """
  @spec read_stream(String.t(), String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, Enumerable.t()} | {:error, term()}
  def read_stream(volume, path, offset, length) do
    with :ok <- validate_alignment(offset, length),
         {:ok, device} <- device_info(volume, path),
         :ok <- validate_range(device, offset, length) do
      {:ok, Stream.map(extent_spans(device, offset, length), &read_span!(device, &1))}
    end
  end

  @doc """
  What an interface node needs to read `length` bytes at `offset` itself.

  One entry per extent the range touches, in order, carrying the chunk the
  extent resolves to and where that chunk can be fetched from — so the
  caller pulls the bytes over the data plane rather than having core read
  them and ship them back over distribution.

  An extent with no entry is reported as a hole rather than omitted: the
  caller has to emit its zeroes, and it cannot tell an unwritten extent
  from one this call forgot. A hole's `hash` and `locations` are empty for
  the same reason — there is nothing to fetch, and the caller has to be able
  to see that without interpreting the target.

  Each entry also carries the `target` it resolved, which is what a
  read-modify-write passes back as `:expect` so its commit can refuse a
  snapshot that has since moved.
  """
  @spec read_refs(String.t(), String.t(), non_neg_integer(), pos_integer()) ::
          {:ok, %{chunk_bytes: pos_integer(), size: non_neg_integer(), extents: [extent_ref()]}}
          | {:error, term()}
  def read_refs(volume, path, offset, length) do
    with :ok <- validate_request(offset, length),
         {:ok, device} <- device_info(volume, path),
         :ok <- validate_range(device, offset, length),
         {:ok, volume_record} <- Core.get_volume(volume),
         {:ok, extents} <- resolve_refs(volume_record, device, offset, length) do
      {:ok, %{chunk_bytes: device.chunk_bytes, size: device.size, extents: extents}}
    end
  end

  @doc """
  Publishes extents whose chunks an interface node has already written.

  The inverse half of `write/5`: the caller placed the bytes itself through
  `NeonFS.Client.ChunkWriter` and reports where it put them, so this call
  only has to verify the claim and publish the map.

  Verification is `NeonFS.Core.ChunkReconciler.reconcile/5`, shared with the
  file commit — it asks each reported location whether it really holds the
  chunk, because the writer's report is the very thing in doubt when a chunk
  is missing. A map published over a chunk that is not there has no correct
  answer to give a read.

  `extents` is `[{extent_index, :hole | chunk_hash}]`. `opts` takes
  `:locations` and `:chunk_codecs` keyed by hash (both from
  `ChunkWriter.chunk_refs_to_commit_opts/1`), plus the `:epoch` and
  `:expect` that `write/5` takes.
  """
  @spec commit_written(String.t(), String.t(), [{non_neg_integer(), :hole | binary()}], keyword()) ::
          {:ok, %{chunks_published: non_neg_integer()}} | {:error, term()}
  def commit_written(volume, path, extents, opts \\ []) do
    start_time = System.monotonic_time()
    hashes = for {_index, target} <- extents, is_binary(target), do: target
    write_id = WriteOperation.generate_write_id()

    with {:ok, volume_record} <- Core.get_volume(volume),
         {:ok, device} <- device_info(volume, path),
         :ok <- validate_extent_indices(device, extents),
         {:ok, metas} <- reconcile(volume_record, hashes, opts, write_id),
         {:ok, _roots} <- publish_written(device, extents, write_id, hashes, opts) do
      charge_usage(volume_record.id,
        physical_size: stored_bytes(metas),
        chunk_count: length(hashes)
      )

      :telemetry.execute(
        [:neonfs, :block, :commit_written],
        %{
          extents: length(extents),
          chunks: length(hashes),
          duration: System.monotonic_time() - start_time
        },
        %{volume: volume, path: path}
      )

      {:ok, %{chunks_published: length(hashes)}}
    else
      {:error, _reason} = error -> abort(error, write_id)
    end
  end

  @doc """
  Durability barrier for the device — the guest's flush and FUA.

  Returns once every chunk the extent map names has the volume's
  `min_copies` durable replicas.
  """
  @spec flush(String.t(), String.t()) :: :ok | {:error, term()}
  def flush(volume, path) do
    start_time = System.monotonic_time()
    result = ensure_device_durable(volume)

    :telemetry.execute(
      [:neonfs, :block, :flush],
      %{duration: System.monotonic_time() - start_time},
      %{volume: volume, path: path, status: if(result == :ok, do: :ok, else: :error)}
    )

    result
  end

  defp ensure_device_durable(volume) do
    with {:ok, volume_record} <- Core.get_volume(volume),
         {:ok, %{chunks: chunks}} <- BlockIndex.referenced_targets(volume) do
      Enum.reduce_while(chunks, :ok, &durable_or_halt(&1, volume_record, &2))
    end
  end

  defp durable_or_halt(hash, volume_record, :ok) do
    case Replication.ensure_min_copies(hash, volume_record) do
      :ok -> {:cont, :ok}
      {:error, _reason} = error -> {:halt, error}
    end
  end

  defp rollback(error, volume) do
    _ = Core.delete_volume(volume)
    error
  end

  defp new_header(path, size_bytes, chunk_bytes) do
    BlockDevice.new(
      id: UUIDv7.generate(),
      path: path,
      size_bytes: size_bytes,
      chunk_bytes: chunk_bytes
    )
  end

  defp refuse_existing_device(volume) do
    case BlockIndex.get_device(volume) do
      {:error, :not_found} -> :ok
      {:ok, %BlockDevice{path: path}} -> {:error, AlreadyExists.exception(resource: path)}
      {:error, _reason} = error -> error
    end
  end

  # A volume holds one device, so a header under a different name is not a
  # second device to fall through to — it is the caller naming the wrong
  # one, and answering with the volume's only device would silently alias
  # the two.
  defp device_header(volume, path) do
    case BlockIndex.get_device(volume) do
      {:ok, %BlockDevice{path: ^path} = header} -> {:ok, header}
      {:ok, %BlockDevice{path: other}} -> {:error, {:device_path_mismatch, path, other}}
      {:error, :not_found} -> {:error, {:device_not_found, volume, path}}
      {:error, _reason} = error -> error
    end
  end

  defp device_from_header(volume, %BlockDevice{} = header) do
    %{
      volume: volume,
      id: header.id,
      path: header.path,
      size: header.size_bytes,
      chunk_bytes: header.chunk_bytes,
      logical_block_bytes: @logical_block_bytes,
      physical_block_bytes: @physical_block_bytes
    }
  end

  defp extent_count(%BlockDevice{size_bytes: size, chunk_bytes: chunk_bytes}),
    do: ceil_div(size, chunk_bytes)

  # ─── The data-plane boundary ───────────────────────────────────────────

  defp resolve_refs(volume_record, device, offset, length) do
    device
    |> extent_spans(offset, length)
    |> Enum.reduce_while({:ok, []}, fn span, {:ok, acc} ->
      case extent_ref(volume_record, device, span) do
        {:ok, ref} -> {:cont, {:ok, [ref | acc]}}
        {:error, _reason} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, refs} -> {:ok, Enum.reverse(refs)}
      {:error, _reason} = error -> error
    end
  end

  defp extent_ref(volume_record, device, {index, within, count}) do
    with {:ok, target} <- BlockIndex.get(device.volume, index) do
      {:ok,
       target
       |> chunk_facts(volume_record)
       |> Map.merge(%{
         index: index,
         width: extent_width(device, index),
         read_start: within,
         read_length: count,
         target: target
       })}
    end
  end

  # A hole has no chunk to describe, and the caller emits its zeroes rather
  # than fetching anything. A missing chunk record is not a hole — it is a
  # map naming data the index cannot place, and inventing zeroes for it
  # would hand the guest a silently wrong read.
  defp chunk_facts(:hole, _volume_record), do: no_chunk()

  defp chunk_facts({:chunk, hash}, volume_record) do
    case ChunkIndex.get(volume_record.id, hash) do
      {:ok, meta} ->
        %{
          hash: hash,
          locations: meta.locations,
          compression: meta.compression,
          encrypted: not is_nil(meta.crypto)
        }

      {:error, :not_found} ->
        %{no_chunk() | hash: hash}
    end
  end

  defp chunk_facts({:stripe, _id, _member}, _volume_record), do: no_chunk()

  # The `hash` is what the caller dials the data plane with, so it is a field
  # of the ref rather than something to be dug out of the target — a hole has
  # none, and neither does a stripe member until erasure reaches this path.
  defp no_chunk, do: %{hash: nil, locations: [], compression: :none, encrypted: false}

  defp reconcile(_volume_record, [], _opts, _write_id), do: {:ok, []}

  defp reconcile(volume_record, hashes, opts, write_id) do
    ChunkReconciler.reconcile(
      volume_record.id,
      hashes,
      Keyword.get(opts, :locations, %{}),
      Keyword.get(opts, :chunk_codecs, %{}),
      write_id
    )
  end

  defp publish_written(device, extents, write_id, hashes, opts) do
    BlockIndex.commit(
      device.volume,
      Enum.map(extents, fn
        {index, :hole} -> {index, :hole}
        {index, hash} -> {index, {:chunk, hash}}
      end),
      opts
      |> Keyword.take([:epoch, :expect])
      |> Keyword.put(:device_path, device.path)
      |> Keyword.put(:chunk_commit, {write_id, hashes})
    )
  end

  defp abort(error, write_id) do
    WriteOperation.abort_chunks(write_id)
    error
  end

  # Placement charges the volume; the sweep that reclaims an extent's
  # replaced chunk is what discharges it, exactly as for a file's chunks.
  # Without the charge the sweep's decrement is the only movement there is,
  # and the counter walks down to its zero clamp.
  #
  # A charge that does not land is dropped rather than failed: the write it
  # describes is already durable, and refusing it after the fact to keep a
  # gauge honest trades data for reporting.
  defp charge_usage(_volume_id, physical_size: 0, chunk_count: 0), do: :ok

  defp charge_usage(volume_id, deltas) do
    _ = VolumeRegistry.adjust_stats(volume_id, deltas)
    :ok
  end

  # An index outside the device is a caller bug, and publishing it would put
  # an extent in the map that no read can ever reach — the device's size is
  # what bounds every read.
  defp validate_extent_indices(device, extents) do
    last = ceil_div(device.size, device.chunk_bytes) - 1

    case Enum.find(extents, fn {index, _target} -> index < 0 or index > last end) do
      nil -> :ok
      {index, _target} -> {:error, {:extent_out_of_range, index, last}}
    end
  end

  # The reconciler probed each location for the chunk's real on-disk size, so
  # what it hands back is what the volume is charged — the writer's report of
  # its own codec is not evidence of how many bytes landed.
  defp stored_bytes(metas), do: Enum.reduce(metas, 0, &(&1.stored_size + &2))

  # ─── Reads ─────────────────────────────────────────────────────────────

  defp read_range(device, offset, length) do
    device
    |> extent_spans(offset, length)
    |> Enum.reduce_while({:ok, []}, fn span, {:ok, acc} ->
      case read_span(device, span) do
        {:ok, bytes} -> {:cont, {:ok, [bytes | acc]}}
        {:error, _} = error -> {:halt, error}
      end
    end)
    |> case do
      {:ok, chunks} -> {:ok, chunks |> Enum.reverse() |> IO.iodata_to_binary()}
      {:error, _} = error -> error
    end
  end

  # The parts of `offset..offset + length` that fall in one extent each, as
  # `{extent_index, start_within_extent, byte_count}`. Every caller here
  # walks the same decomposition, and doing it once is what keeps the
  # working set at one extent.
  defp extent_spans(%{chunk_bytes: chunk_bytes}, offset, length) do
    first = div(offset, chunk_bytes)
    last = div(offset + length - 1, chunk_bytes)

    Enum.map(first..last, fn index ->
      extent_start = index * chunk_bytes
      span_start = max(offset, extent_start)
      span_end = min(offset + length, extent_start + chunk_bytes)
      {index, span_start - extent_start, span_end - span_start}
    end)
  end

  defp read_span(device, {index, within, count}) do
    with {:ok, bytes} <- extent_bytes(device, index) do
      {:ok, binary_part(bytes, within, count)}
    end
  end

  defp read_span!(device, span) do
    case read_span(device, span) do
      {:ok, bytes} -> bytes
      {:error, reason} -> raise "block device read failed: #{inspect(reason)}"
    end
  end

  # The width of the last extent is the device's tail, not the volume's
  # extent size, so a hole there must not read back long — and a stored
  # chunk that is short is corruption rather than a tail to pad out.
  defp extent_bytes(device, index) do
    with {:ok, target} <- BlockIndex.get(device.volume, index) do
      target_bytes(device, index, target)
    end
  end

  defp target_bytes(device, index, :hole), do: {:ok, zeroes(extent_width(device, index))}

  defp target_bytes(device, index, target) do
    width = extent_width(device, index)

    case BlockIndex.read_target(device.volume, target) do
      {:ok, <<bytes::binary-size(^width), _beyond::binary>>} -> {:ok, bytes}
      {:ok, short} -> {:error, {:short_extent, index, byte_size(short), width}}
      {:error, _reason} = error -> error
    end
  end

  defp extent_width(%{size: size, chunk_bytes: chunk_bytes}, index),
    do: min(chunk_bytes, size - index * chunk_bytes)

  # ─── Validation ────────────────────────────────────────────────────────

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

  defp ceil_div(numerator, denominator), do: div(numerator + denominator - 1, denominator)

  defp zeroes(size), do: :binary.copy(<<0>>, size)
end
