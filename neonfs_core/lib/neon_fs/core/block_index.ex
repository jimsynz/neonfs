defmodule NeonFS.Core.BlockIndex do
  @moduledoc """
  The extent map for a block volume: reads and commits of `block_index`.

  A block volume's contents are one `NeonFS.Core.Volume.BlockExtent` entry
  per extent of the device, keyed by extent index. This module is the
  storage half of that — the map, not the bytes. Chunk data is written
  first, over the data plane, and this call publishes the map that points
  at it.

  ## The ordering is the crash contract

  **Chunks land first, the map commits second.** A crash between the two
  leaks chunks nothing points at, which GC reclaims; the reverse ordering
  would leave a map entry pointing at data that was never written, which
  is never acceptable — a read of it returns whatever the hash resolves
  to, or nothing.

  So `commit/3` takes hashes of chunks that are **already durable**. It
  does not write chunk data, and it cannot check that the caller kept its
  half of the bargain: passing a hash for a chunk still in flight
  publishes exactly the state this ordering exists to prevent.

  An intent log replayed at attach was considered and rejected: a second
  durable structure on the write path, with its own ordering and failure
  modes, to save writes NBD never told the guest were durable.

  The consequence to watch is that a crashy device generates GC debt
  proportional to its in-flight window. `[:neonfs, :garbage_collector,
  :volume_reclaim]` is where it surfaces: reclaim per volume, tagged with
  the volume's type. Nothing distinguishes a crash-leaked chunk from one an
  overwrite or a discard orphaned — both are simply unreferenced by the time
  GC sees them — so what a crash loop looks like is a reclaim rate that does
  not track the device's write rate.

  ## One consensus round per commit, however many extents

  `commit/3` routes through `NeonFS.Core.VolumeCommitter`, which publishes
  every shard a batch touched in a single round. That is what makes a
  coalescing window worth having: a window of sequential writes shares an
  extent group, so a batch spanning it publishes one or two roots rather
  than one per extent.

  ## The boundary takes a volume name

  Every function here names its volume the way the rest of the block
  boundary does — by name, resolved to an id once per call. An interface
  node reaching this module over `NeonFS.Client.Router` holds the name it
  attached with, not the volume's id.

  ## Holes

  An extent with no entry is a hole, and a hole reads as zeroes. Discard
  drops entries rather than zero-filling: unlike a `NeonFS.Core.FileMeta`,
  whose ordered chunk list gives an extent no offset of its own, dropping
  an entry here shifts nothing.

  Committing `:hole` for an extent is therefore a delete, not a write of
  `BlockExtent.encode(:hole)`. The encoded hole is the absent-slot value
  of the fixed-width entry itself — what a packed group of extents stores
  for a slot nothing occupies — and it never reaches the tree while
  entries are stored one per key.
  """

  alias NeonFS.Core.{BlockBacking, ChunkIndex, ReadOperation, VolumeCommitter, VolumeRegistry}
  alias NeonFS.Core.Volume.{BlockExtent, MetadataReader}
  alias NeonFS.Error.VolumeNotFound

  @type extent_index :: BlockExtent.extent_index()
  @type target :: BlockExtent.target()
  @type extent :: {extent_index(), target()}

  @doc """
  The target `extent_index` maps to, or `:hole` where it has none.
  """
  @spec get(String.t(), extent_index(), keyword()) ::
          {:ok, target()} | {:error, term()}
  def get(volume_name, extent_index, opts \\ [])
      when is_binary(volume_name) and is_integer(extent_index) do
    with {:ok, volume} <- resolve_volume(volume_name) do
      do_get(volume.id, extent_index, opts)
    end
  end

  @doc """
  Every extent in `first..last`, ascending, as `{extent_index, target}`.

  Holes are omitted rather than reported: the range is a scan of what the
  map holds, and a device's unwritten regions are unbounded in a way its
  written ones are not.
  """
  @spec range(String.t(), extent_index(), extent_index(), keyword()) ::
          {:ok, [extent()]} | {:error, term()}
  def range(volume_name, first, last, opts \\ [])
      when is_binary(volume_name) and is_integer(first) and is_integer(last) and first <= last do
    reader = Keyword.get(opts, :metadata_reader, MetadataReader)

    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, entries} <-
           reader.range(
             volume.id,
             :block_index,
             BlockExtent.key(first),
             BlockExtent.key(last + 1),
             reader_opts(opts)
           ) do
      decode_all(entries)
    end
  end

  @doc """
  Every chunk hash and stripe id the volume's extent map names.

  The extent map is the *only* thing referring to a block volume's chunks —
  there is no `FileMeta` naming them — so anything deciding whether a chunk
  is live has to ask here. A mark phase that walks files alone concludes
  that every extent's chunk is garbage.

  Returns `%{chunks: MapSet.t(), stripes: MapSet.t()}`. A stripe target
  contributes its stripe id; resolving that to the stripe's own chunks is
  the caller's job, since it already holds the stripe index.
  """
  @spec referenced_targets(String.t(), keyword()) ::
          {:ok, %{chunks: MapSet.t(), stripes: MapSet.t()}} | {:error, term()}
  def referenced_targets(volume_name, opts \\ []) when is_binary(volume_name) do
    reader = Keyword.get(opts, :metadata_reader, MetadataReader)

    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, entries} <-
           reader.range(volume.id, :block_index, <<>>, <<>>, reader_opts(opts)) do
      empty = %{chunks: MapSet.new(), stripes: MapSet.new()}
      Enum.reduce_while(entries, {:ok, empty}, &fold_target/2)
    end
  end

  defp fold_target({_key, entry}, {:ok, acc}) do
    case BlockExtent.decode(entry) do
      {:ok, {:chunk, hash}} ->
        {:cont, {:ok, %{acc | chunks: MapSet.put(acc.chunks, hash)}}}

      {:ok, {:stripe, id, _index}} ->
        {:cont, {:ok, %{acc | stripes: MapSet.put(acc.stripes, id)}}}

      {:ok, :hole} ->
        {:cont, {:ok, acc}}

      {:error, _} = err ->
        {:halt, err}
    end
  end

  @doc """
  Publishes `extents` as one commit.

  Each element is `{extent_index, target}`. A `:hole` target drops the
  extent, so a batch may mix writes and punches. Every chunk hash named
  must already be durable — see the module doc.

  Returns `%{shard => root_chunk_hash}` for the shards the commit touched.
  """
  @spec commit(String.t(), [extent()], keyword()) ::
          {:ok, %{optional(non_neg_integer()) => binary()}} | {:error, term()}
  def commit(volume_name, extents, opts \\ [])
      when is_binary(volume_name) and is_list(extents) do
    committer = Keyword.get(opts, :volume_committer, VolumeCommitter)

    with {:ok, volume} <- resolve_volume(volume_name) do
      committer.commit(volume.id, mutations(extents), writer_opts(opts))
    end
  end

  @doc """
  Drops every extent in `first..last` — the device's discard.

  Dropping the entries is the whole operation: the extents read back as
  zeroes afterwards, and the chunks they referenced become GC's problem
  rather than being rewritten.
  """
  @spec discard(String.t(), extent_index(), extent_index(), keyword()) ::
          {:ok, %{optional(non_neg_integer()) => binary()}} | {:error, term()}
  def discard(volume_name, first, last, opts \\ [])
      when is_binary(volume_name) and is_integer(first) and is_integer(last) and first <= last do
    commit(volume_name, Enum.map(first..last, &{&1, :hole}), opts)
  end

  @doc """
  The bytes of one extent, `chunk_bytes` wide.

  A hole reads as zeroes the width of the volume's extents — an absent
  extent has no stored size of its own to recover that from.
  """
  @spec read_extent(String.t(), extent_index(), keyword()) ::
          {:ok, binary()} | {:error, term()}
  def read_extent(volume_name, extent_index, opts \\ [])
      when is_binary(volume_name) and is_integer(extent_index) do
    with {:ok, volume} <- resolve_volume(volume_name),
         {:ok, target} <- do_get(volume.id, extent_index, opts) do
      case target do
        :hole -> {:ok, <<0::size(BlockBacking.chunk_bytes_for(volume))-unit(8)>>}
        {:chunk, hash} -> fetch_chunk(volume.id, hash, opts)
        {:stripe, _stripe_id, _member} -> {:error, :erasure_extent_unsupported}
      end
    end
  end

  defp do_get(volume_id, extent_index, opts) do
    reader = Keyword.get(opts, :metadata_reader, MetadataReader)

    case reader.get(volume_id, :block_index, BlockExtent.key(extent_index), reader_opts(opts)) do
      {:ok, entry} -> BlockExtent.decode(entry)
      {:error, :not_found} -> {:ok, :hole}
      {:error, _} = err -> err
    end
  end

  defp resolve_volume(volume_name) do
    case VolumeRegistry.get_by_name(volume_name) do
      {:ok, volume} -> {:ok, volume}
      {:error, :not_found} -> {:error, VolumeNotFound.exception(volume_name: volume_name)}
    end
  end

  defp fetch_chunk(volume_id, hash, opts) do
    fetcher = Keyword.get(opts, :chunk_reader, &default_fetch_chunk/3)
    fetcher.(volume_id, hash, reader_opts(opts))
  end

  # An extent's chunk is an ordinary volume chunk, so its compression and
  # crypto are recorded where every other chunk's are — reuse the read
  # path's resolution rather than a second, subtly different one.
  defp default_fetch_chunk(volume_id, hash, _opts) do
    with {:ok, chunk_meta} <- ChunkIndex.get(volume_id, hash) do
      ReadOperation.fetch_chunk_data(chunk_meta, false, volume_id)
    end
  end

  defp decode_all(entries) do
    Enum.reduce_while(entries, {:ok, []}, fn {key, entry}, {:ok, acc} ->
      case BlockExtent.decode(entry) do
        {:ok, target} -> {:cont, {:ok, [{BlockExtent.extent_index(key), target} | acc]}}
        {:error, _} = err -> {:halt, err}
      end
    end)
    |> case do
      {:ok, decoded} -> {:ok, Enum.reverse(decoded)}
      {:error, _} = err -> err
    end
  end

  defp mutations(extents) do
    Enum.map(extents, fn
      {extent_index, :hole} ->
        {:delete, :block_index, BlockExtent.key(extent_index)}

      {extent_index, target} ->
        {:put, :block_index, BlockExtent.key(extent_index), BlockExtent.encode(target)}
    end)
  end

  @injection_opts [:volume_committer, :metadata_reader, :chunk_reader]

  defp writer_opts(opts), do: Keyword.drop(opts, @injection_opts)
  defp reader_opts(opts), do: Keyword.drop(opts, @injection_opts)
end
