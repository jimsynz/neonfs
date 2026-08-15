defmodule NeonFS.Core.Volume.BlockExtent do
  @moduledoc """
  Key and entry format for a block volume's `block_index`.

  A block volume's contents are an extent map: one entry per
  `block_chunk_bytes`-sized extent of the device, keyed by the extent's
  index. Unlike a `NeonFS.Core.FileMeta`'s ordered chunk list, an extent's
  position is carried by its key rather than by the sum of its
  predecessors' sizes, so an extent may be absent — a hole — and an
  overwrite rewrites one entry instead of shifting everything after it.

  ## The key

  A key is the extent index as a fixed-width 64-bit big-endian integer, so
  the index tree's byte-wise ordering is the device's LBA ordering and a
  range scan over an LBA span is a contiguous walk. The extent index is
  `div(byte_offset, block_chunk_bytes)`; the size is per-volume
  (`NeonFS.Core.Volume.block_chunk_bytes`), so the arithmetic needs the
  volume rather than a constant.

  ## The entry

  Entries are 33 fixed bytes: a one-byte target discriminator and a
  32-byte target payload, zero-padded where the target is smaller.

  | kind | byte 0 | bytes 1..32                                            |
  |------|--------|--------------------------------------------------------|
  | hole | `0`    | zeroes                                                 |
  | chunk| `1`    | the 32-byte chunk hash                                 |
  | stripe| `2`   | 16-byte stripe UUID, 16-bit member index, 14 zero bytes|

  Both a plain chunk and a stripe member are representable from the first
  version, because temperature-driven durability conversion leaves one
  volume holding both at once — a format that could only denote a chunk
  would have to be retrofitted, which for a persistent layout means
  rewriting every extent of every device.

  Fixed size is what lets a whole group of extents be sliced out of one
  value, and it is why the stripe variant does not simply grow the entry:
  the entry is as wide as its widest target and every other target pays
  for it.

  ### Stripe ids are stored raw

  `NeonFS.Core.Stripe.id` is a `UUIDv7` in its 36-character text form. An
  entry stores its 16 raw bytes instead, which is what keeps a stripe
  target inside the 32 bytes a chunk hash already costs; the text form
  would push every entry — hole, chunk and stripe alike — to 40 bytes for
  the benefit of one variant. The conversion is confined to `encode/1` and
  `decode/1`: callers pass and receive the text form, and nothing outside
  this module sees the raw one.

  ## Extent groups are the shard unit

  `NeonFS.Core.Volume.Shard` maps a `block_index` key to a shard by its
  extent group — `div(extent_index, group_size/0)` — rather than by
  hashing the key, so a window of sequential writes touches one or two
  shards instead of scattering across all of them.

  `group_size/0` is a deployment-fixed parameter (`:neonfs_core,
  :block_extent_group_size`, default 64) and, like
  `:metadata_shard_count`, is **immutable once anything has been
  written**: changing it re-homes every key in every block volume's index.
  Its span in bytes is `group_size/0 * block_chunk_bytes`, so it varies
  per volume with the chunk size — at the default of both, 8 MiB.

  A device with a hot LBA region therefore concentrates on one shard,
  where hashing spread it for free. That is the accepted cost of keeping a
  coalesced window on a small number of shards.
  """

  @default_group_size 64

  @entry_size 33
  @hash_size 32
  @uuid_bytes 16

  @hole_kind 0
  @chunk_kind 1
  @stripe_kind 2

  @type extent_index :: non_neg_integer()
  @type target ::
          :hole
          | {:chunk, binary()}
          | {:stripe, binary(), non_neg_integer()}
  @type decode_error :: {:error, {:malformed_extent, term()}}

  @doc "The fixed width of an encoded entry, in bytes."
  @spec entry_size() :: pos_integer()
  def entry_size, do: @entry_size

  @doc "The number of extents per shard group for this deployment."
  @spec group_size() :: pos_integer()
  def group_size,
    do: Application.get_env(:neonfs_core, :block_extent_group_size, @default_group_size)

  @doc "The group `extent_index` belongs to."
  @spec group(extent_index()) :: non_neg_integer()
  def group(extent_index) when is_integer(extent_index) and extent_index >= 0,
    do: div(extent_index, group_size())

  @doc """
  The `block_index` key for `extent_index`.
  """
  @spec key(extent_index()) :: binary()
  def key(extent_index) when is_integer(extent_index) and extent_index >= 0,
    do: <<extent_index::unsigned-big-64>>

  @doc """
  The extent index a `block_index` `key` denotes.
  """
  @spec extent_index(binary()) :: extent_index()
  def extent_index(<<extent_index::unsigned-big-64>>), do: extent_index

  @doc """
  The extent index covering `byte_offset` on a volume whose extents are
  `chunk_bytes` wide.
  """
  @spec extent_index_at(non_neg_integer(), pos_integer()) :: extent_index()
  def extent_index_at(byte_offset, chunk_bytes)
      when is_integer(byte_offset) and byte_offset >= 0 and is_integer(chunk_bytes) and
             chunk_bytes > 0,
      do: div(byte_offset, chunk_bytes)

  @doc """
  Encodes an extent target as its fixed-width entry.

  A stripe id is the 36-character text UUID `NeonFS.Core.Stripe` mints.
  """
  @spec encode(target()) :: binary()
  def encode(:hole), do: <<@hole_kind::8, 0::size(@hash_size)-unit(8)>>

  def encode({:chunk, hash}) when is_binary(hash) and byte_size(hash) == @hash_size,
    do: <<@chunk_kind::8, hash::binary-size(@hash_size)>>

  def encode({:stripe, stripe_id, member_index})
      when is_binary(stripe_id) and is_integer(member_index) and member_index >= 0 and
             member_index <= 65_535 do
    raw = uuid_to_binary(stripe_id)
    padding = @hash_size - @uuid_bytes - 2

    <<@stripe_kind::8, raw::binary-size(@uuid_bytes), member_index::unsigned-big-16,
      0::size(padding)-unit(8)>>
  end

  @doc """
  Decodes an entry back into its target.

  Fails with `{:error, {:malformed_extent, reason}}` rather than raising,
  so a torn or truncated read is distinguishable from a hole.
  """
  @spec decode(binary()) :: {:ok, target()} | decode_error()
  def decode(entry) when is_binary(entry) and byte_size(entry) != @entry_size,
    do: {:error, {:malformed_extent, {:wrong_size, byte_size(entry)}}}

  def decode(<<@hole_kind::8, rest::binary-size(@hash_size)>>) do
    if rest == <<0::size(@hash_size)-unit(8)>> do
      {:ok, :hole}
    else
      {:error, {:malformed_extent, :hole_with_target}}
    end
  end

  def decode(<<@chunk_kind::8, hash::binary-size(@hash_size)>>), do: {:ok, {:chunk, hash}}

  def decode(
        <<@stripe_kind::8, raw::binary-size(@uuid_bytes), member_index::unsigned-big-16,
          _padding::binary>>
      ),
      do: {:ok, {:stripe, binary_to_uuid(raw), member_index}}

  def decode(<<kind::8, _rest::binary>>),
    do: {:error, {:malformed_extent, {:unknown_kind, kind}}}

  defp uuid_to_binary(
         <<a::binary-8, ?-, b::binary-4, ?-, c::binary-4, ?-, d::binary-4, ?-, e::binary-12>>
       ) do
    Base.decode16!(a <> b <> c <> d <> e, case: :mixed)
  end

  defp binary_to_uuid(<<a::binary-4, b::binary-2, c::binary-2, d::binary-2, e::binary-6>>) do
    [a, b, c, d, e]
    |> Enum.map_join("-", &Base.encode16(&1, case: :lower))
  end
end
