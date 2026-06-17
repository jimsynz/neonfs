defmodule NeonFS.Core.Volume.Shard do
  @moduledoc """
  Maps a metadata key to one of a volume's fixed root shards (#1307).

  A volume's metadata root is split into `count/0` independent
  copy-on-write shards, each its own CAS pointer in the bootstrap layer.
  `for_key/1` assigns a key to a shard by the top bits of its hash, so
  writes to distinct keys land on independent root pointers and don't
  serialise through one CAS point.

  The count is a deployment-fixed parameter (`:neonfs_core,
  :metadata_shard_count`, default 64): the key→shard
  mapping must be stable for the life of a deployment, since changing it
  would re-home every key. Growing aggregate throughput is a matter of
  the per-`{volume, shard}` commit pipeline (#1308), not of changing this
  number. Dynamic shard→node placement / rebalancing is a separate
  concern (#1306).

  The `neonfs_core` unit tests pin the count to 1 (their metadata mock is
  a single store that doesn't model per-shard trees); the integration
  suite and production run at the default.
  """

  @default_count 64

  @doc "The number of root shards per volume for this deployment."
  @spec count() :: pos_integer()
  def count, do: Application.get_env(:neonfs_core, :metadata_shard_count, @default_count)

  @doc "Every shard index, ascending — for full-volume scans / provisioning."
  @spec all() :: [non_neg_integer()]
  def all, do: Enum.to_list(0..(count() - 1))

  @doc """
  The shard a metadata `key` belongs to: the top 32 bits of its SHA-256
  digest, modulo `count/0`.
  """
  @spec for_key(binary()) :: non_neg_integer()
  def for_key(key) when is_binary(key) do
    <<top::unsigned-32, _rest::binary>> = :crypto.hash(:sha256, key)
    rem(top, count())
  end
end
