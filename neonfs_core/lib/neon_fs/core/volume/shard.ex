defmodule NeonFS.Core.Volume.Shard do
  @moduledoc """
  Maps a metadata key to one of a volume's fixed root shards.

  A volume's metadata root is split into `count/0` independent
  copy-on-write shards, each its own CAS pointer in the bootstrap layer.
  `for_key/2` assigns a key to a shard so that writes to distinct keys
  land on independent root pointers and don't serialise through one CAS
  point.

  ## Two mappings, chosen by index kind

  For `:file_index`, `:chunk_index` and `:stripe_index` the shard is the
  top bits of the key's hash — those keys have no useful ordering, so
  spreading them is free.

  `:block_index` maps by extent group instead —
  `NeonFS.Core.Volume.BlockExtent.group/1` of the key's extent index. Its
  keys *are* ordered: they are device offsets, and the writes that matter
  arrive as runs of adjacent ones. Hashing would scatter a coalesced
  window of sequential writes across every shard, so a batch that should
  publish one or two roots would publish all of them — forfeiting most of
  the reason the extent map exists. The cost is that a device with a hot
  LBA region concentrates on one shard; `BlockExtent` records why that is
  accepted.

  The count is a deployment-fixed parameter (`:neonfs_core,
  :metadata_shard_count`, default 64): the key→shard
  mapping must be stable for the life of a deployment, since changing it
  would re-home every key. The same holds for `BlockExtent.group_size/0`,
  which is the other half of the `:block_index` mapping. Growing aggregate
  throughput is a matter of the per-`{volume, shard}` commit pipeline, not
  of changing either number. Dynamic shard→node placement / rebalancing is
  a separate concern.

  The `neonfs_core` unit tests pin the count to 1 (their metadata mock is
  a single store that doesn't model per-shard trees); the integration
  suite and production run at the default.
  """

  alias NeonFS.Core.Volume.BlockDevice
  alias NeonFS.Core.Volume.BlockExtent

  @default_count 64

  @type index_kind :: :file_index | :chunk_index | :stripe_index | :block_index

  @doc "The number of root shards per volume for this deployment."
  @spec count() :: pos_integer()
  def count, do: Application.get_env(:neonfs_core, :metadata_shard_count, @default_count)

  @doc "Every shard index, ascending — for full-volume scans / provisioning."
  @spec all() :: [non_neg_integer()]
  def all, do: Enum.to_list(0..(count() - 1))

  @doc """
  The shard `key` belongs to in its `index_kind` index.

  For `:block_index` that is the key's extent group, modulo `count/0` — or
  `BlockDevice.shard/0` for the one key in that index which is a device header
  rather than an extent; for every other kind it is the top 32 bits of the
  key's SHA-256 digest, modulo `count/0`.
  """
  @spec for_key(index_kind(), binary()) :: non_neg_integer()
  def for_key(:block_index, key) when is_binary(key) do
    if BlockExtent.extent_key?(key) do
      key
      |> BlockExtent.extent_index()
      |> BlockExtent.group()
      |> rem(count())
    else
      # `block_index` also holds a device header, which has no extent index and
      # so no group to derive. `BlockDevice.shard/0` states the group it lives
      # in; without this clause `extent_index/1` would raise on its key.
      rem(BlockDevice.shard(), count())
    end
  end

  def for_key(index_kind, key) when is_atom(index_kind) and is_binary(key) do
    <<top::unsigned-32, _rest::binary>> = :crypto.hash(:sha256, key)
    rem(top, count())
  end
end
