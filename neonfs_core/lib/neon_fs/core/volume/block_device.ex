defmodule NeonFS.Core.Volume.BlockDevice do
  @moduledoc """
  A block device's identity — its geometry, not its contents — stored in the
  same `block_index` as its extents.

  A device used to *be* a file, and its geometry came from that file's
  `NeonFS.Core.FileMeta`. With the extent map as the device's contents there is
  no file, so size, extent width, id and creation time need a home of their own.

  ## Why it shares the extent map's index

  Identity and contents then commit and shard together, and no fifth index kind
  is needed. A device attribute change is a metadata write like any extent
  write, rather than a consensus round — which is what putting geometry on the
  volume's Ra registry record would have cost, and it would have hard-coded one
  device per volume when `{volume, path}` keying exists precisely to avoid that.

  ## The key cannot be mistaken for an extent

  `key/0` is not eight bytes wide, and every extent key is
  (`BlockExtent.extent_key?/1`). So the header is excluded from iteration by a
  check on the key's *shape*, which cannot be defeated by arithmetic:

  - a **sentinel index** like `-1` reads as an extent everywhere it is not
    specifically excluded, and an off-by-one in a range bound clobbers the
    device header;
  - **index 0, extents from 1** puts an offset in every LBA-to-extent
    conversion — exactly the arithmetic that produces one-extent corruption —
    and makes the on-disk numbering disagree with the device's own addressing.

  The cost is that `block_index`'s key space is a union, so anything that ranges
  or sorts over its keys has to cope with a key that is not a number.
  `BlockIndex.range/3` and `BlockIndex.referenced_targets/2` both filter it —
  the second is not optional, since without it GC tries to resolve a device
  header as a chunk target.

  ## Its shard is stated, not derived

  `block_index` shards by extent group, `div(extent_index, group_size)`. The
  header has no extent index, so there is no arithmetic to fall out of; `shard/0`
  names the group it lives in and `NeonFS.Core.Volume.Shard` uses that.

  Group 0, so a device's header shares a commit with the extents at the start of
  its address space. Nothing depends on that beyond it being fixed: what matters
  is that the answer is written down rather than being whatever
  `extent_index/1` happens to do to a six-byte binary.

  ## Its value is a term, not a fixed-width entry

  Extent entries are fixed width because there is one per extent and there are
  a great many; the header is one per device and carries fields of differing
  kinds. It is encoded as ETF, the same shape `NeonFS.Core.Volume.MetadataValue`
  uses, so adding a field later is a struct entry rather than a format change.
  """

  @enforce_keys [:id, :size_bytes, :chunk_bytes, :created_at]
  defstruct [:id, :size_bytes, :chunk_bytes, :created_at]

  @type t :: %__MODULE__{
          id: binary(),
          size_bytes: non_neg_integer(),
          chunk_bytes: pos_integer(),
          created_at: DateTime.t()
        }

  @type decode_error :: {:error, {:malformed_device_header, term()}}

  # Deliberately not eight bytes. `BlockExtent.key/1` produces exactly eight,
  # so no extent index — including one far beyond any real device — can
  # collide with this.
  @key "device"

  # The header has no extent index to derive a group from, so its group is
  # declared here rather than computed.
  @shard 0

  @doc "The `block_index` key the device header lives at."
  @spec key() :: binary()
  def key, do: @key

  @doc "The extent group the device header shares a commit with."
  @spec shard() :: non_neg_integer()
  def shard, do: @shard

  @doc """
  Builds a device header.

  `chunk_bytes` is the width of the device's extents and is fixed for its life:
  it is what turns a byte offset into an extent index, so changing it would
  re-address every extent the device already holds.
  """
  @spec new(keyword()) :: t()
  def new(attrs) do
    %__MODULE__{
      id: Keyword.fetch!(attrs, :id),
      size_bytes: Keyword.fetch!(attrs, :size_bytes),
      chunk_bytes: Keyword.fetch!(attrs, :chunk_bytes),
      created_at: Keyword.get_lazy(attrs, :created_at, &DateTime.utc_now/0)
    }
  end

  @doc "Encodes the header for storage."
  @spec encode(t()) :: binary()
  def encode(%__MODULE__{} = device), do: :erlang.term_to_binary(Map.from_struct(device))

  @doc """
  Decodes a stored header.

  Refuses anything that is not a header rather than returning a partly-filled
  struct: a device whose geometry is guessed addresses the wrong extents, and
  `:safe` keeps a corrupt entry from creating atoms.
  """
  @spec decode(binary()) :: {:ok, t()} | decode_error()
  def decode(encoded) when is_binary(encoded) do
    case :erlang.binary_to_term(encoded, [:safe]) do
      %{id: _, size_bytes: _, chunk_bytes: _, created_at: _} = attrs ->
        {:ok, struct!(__MODULE__, attrs)}

      other ->
        {:error, {:malformed_device_header, other}}
    end
  rescue
    error -> {:error, {:malformed_device_header, error}}
  end
end
