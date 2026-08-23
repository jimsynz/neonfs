defmodule NeonFS.Integration.BlockBackingTest do
  @moduledoc """
  Crash consistency for the extent-map block device store.

  A block device makes two promises a filesystem does not have to make as
  loudly: an acknowledged flush must survive the loss of the node that
  took the write, and no sector may ever read back content it held before
  a later write. Both are asserted here across a whole-node restart,
  because that is the failure a device attached to a guest actually meets
  — the node it was talking to goes away and the guest keeps reading.

  The device is written from `node1` and read back from `node2` after
  `node1` restarts, so a read that succeeds proves the chunks reached
  another replica rather than merely surviving locally. Durability is
  `factor: 2, min_copies: 2` for that reason: the integration suite's
  default durability is single-copy, under which nothing here could be
  distinguished from a local read.

  Extents are published the way an interface node publishes them — the
  chunk placed on a node's blob store, then `commit_written/4` verifying
  that claim and publishing the map. There is no other device write path.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.BlockBacking
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag :integration

  @volume "block-backing"
  @device "/dev.img"
  @volume_opts %{
    durability: %{type: :replicate, factor: 2, min_copies: 2},
    compression: %{algorithm: :none, level: 0, min_size: 0}
  }

  @chunk 131_072
  @block 4096
  @device_chunks 8

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, volumes: [{@volume, @volume_opts}])
    :ok
  end

  test "a flushed write survives the restart of the node that took it, with no stale sector",
       %{cluster: cluster} do
    {:ok, device} =
      block_rpc(cluster, :node1, :create_device, [@volume, @device, @device_chunks * @chunk])

    stale = :binary.copy(<<0xA1>>, @block)
    fresh = :binary.copy(<<0xB2>>, @block)
    untouched = :binary.copy(<<0xC3>>, @block)

    overwritten_at = 2 * @chunk
    untouched_at = 5 * @chunk

    {:ok, _} = publish(cluster, device, 2, extent(stale))
    {:ok, _} = publish(cluster, device, 5, extent(untouched))

    :ok = block_rpc(cluster, :node1, :flush, [@volume, device.path])

    {:ok, _} = publish(cluster, device, 2, extent(fresh))

    :ok = block_rpc(cluster, :node1, :flush, [@volume, device.path])

    {:ok, cluster} = PeerCluster.restart_node(cluster, :node1)
    stabilise_after_restart(cluster)

    assert {:ok, ^fresh} =
             block_rpc(cluster, :node2, :read, [@volume, device.path, overwritten_at, @block])

    assert {:ok, ^untouched} =
             block_rpc(cluster, :node2, :read, [@volume, device.path, untouched_at, @block])

    assert {:ok, zeroes} =
             block_rpc(cluster, :node2, :read, [@volume, device.path, 7 * @chunk, @block])

    assert zeroes == :binary.copy(<<0>>, @block)

    {:ok, info} = block_rpc(cluster, :node2, :device_info, [@volume, device.path])
    assert info.size == @device_chunks * @chunk
    assert info.id == device.id
  end

  test "zeroing a range is durable and does not resurrect the bytes it replaced",
       %{cluster: cluster} do
    {:ok, device} =
      block_rpc(cluster, :node1, :create_device, [@volume, @device, @device_chunks * @chunk])

    kept = :binary.copy(<<0xD4>>, @chunk)

    {:ok, _} = publish(cluster, device, 0, kept)
    {:ok, _} = publish(cluster, device, 1, kept)
    :ok = block_rpc(cluster, :node1, :flush, [@volume, device.path])

    # Discarding an extent drops it from the map. Nothing is written in its
    # place, which is exactly why the bytes it held must not come back.
    assert {:ok, _published} =
             block_rpc(cluster, :node1, :commit_written, [@volume, device.path, [{0, :hole}]])

    :ok = block_rpc(cluster, :node1, :flush, [@volume, device.path])

    {:ok, cluster} = PeerCluster.restart_node(cluster, :node1)
    stabilise_after_restart(cluster)

    assert {:ok, zeroed} = block_rpc(cluster, :node2, :read, [@volume, device.path, 0, @block])
    assert zeroed == :binary.copy(<<0>>, @block)

    assert {:ok, kept} =
             block_rpc(cluster, :node2, :read, [@volume, device.path, @chunk, @block])

    assert kept == :binary.copy(<<0xD4>>, @block)
  end

  # An extent's worth of one byte, since a device write is whole extents.
  defp extent(block), do: :binary.copy(block, div(@chunk, byte_size(block)))

  # Places the chunk on node1's blob store — what an interface node does over
  # the data plane — and publishes the extent that names it.
  defp publish(cluster, device, index, bytes) do
    hash = :crypto.hash(:sha256, bytes)
    node1 = PeerCluster.get_node!(cluster, :node1).node

    {:ok, ^hash, _info} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.BlobStore, :write_chunk, [
        bytes,
        "default",
        "hot"
      ])

    block_rpc(cluster, :node1, :commit_written, [
      @volume,
      device.path,
      [{index, hash}],
      [
        locations: %{hash => [%{node: node1, drive_id: "default", tier: :hot}]},
        chunk_codecs: %{hash => %{compression: :none, crypto: nil}}
      ]
    ])
  end

  defp block_rpc(cluster, node, function, args) do
    PeerCluster.rpc(cluster, node, BlockBacking, function, args)
  end
end
