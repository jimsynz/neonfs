defmodule NeonFS.Core.Volume.Shard do
  @moduledoc """
  Maps a metadata key to one of a volume's fixed root shards (#1307).

  A volume's metadata root is split into `count/0` independent
  copy-on-write shards, each its own CAS pointer in the bootstrap layer.
  `for_key/1` assigns a key to a shard by the top bits of its hash, so
  writes to distinct keys land on independent root pointers and don't
  serialise through one CAS point.

  The count is fixed: the key→shard mapping must be stable, since
  changing it would re-home every key. Growing aggregate throughput is a
  matter of the per-`{volume, shard}` commit pipeline (#1308), not of
  changing this number. Dynamic shard→node placement / rebalancing is a
  separate concern (#1306).

  > This slice (#1307) lands the shard *plumbing* at `count = 1`, which
  > is behaviour-identical to the pre-sharding single root, so the change
  > is a pure structural refactor verifiable against the existing suite.
  > Flipping the count to 64 (and the provisioning / N-way-merge work that
  > only matters at N>1) is the activation follow-on.
  """

  @count 1

  @doc "The fixed number of root shards per volume."
  @spec count() :: pos_integer()
  def count, do: @count

  @doc "Every shard index, ascending — for full-volume scans / provisioning."
  @spec all() :: [non_neg_integer()]
  def all, do: Enum.to_list(0..(@count - 1))

  @doc """
  The shard a metadata `key` belongs to: the top 32 bits of its SHA-256
  digest, modulo `count/0`.
  """
  @spec for_key(binary()) :: non_neg_integer()
  def for_key(key) when is_binary(key) do
    <<top::unsigned-32, _rest::binary>> = :crypto.hash(:sha256, key)
    rem(top, @count)
  end
end
