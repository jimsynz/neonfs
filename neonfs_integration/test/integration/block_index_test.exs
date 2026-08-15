defmodule NeonFS.Integration.BlockIndexTest do
  @moduledoc """
  Crash consistency for the block volume's extent map.

  The map's whole ordering contract is that chunks land first and the map
  commits second, so the only state a crash can leave is chunks nothing
  points at. The inverse — a map entry naming data that was never written
  — is the one outcome that is never acceptable, because a read of it has
  no correct answer to give.

  These tests assert both halves across a whole-node restart, which is the
  failure a device attached to a guest actually meets. The map is
  committed from `node1` and read back from `node2` after `node1`
  restarts, so a resolved extent proves the commit reached another replica
  rather than merely surviving locally.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.BlockIndex
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag :integration

  @volume "block-index"
  @volume_opts %{
    durability: %{type: :replicate, factor: 2, min_copies: 2},
    compression: %{algorithm: :none, level: 0, min_size: 0}
  }

  @chunk 131_072

  setup %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, volumes: [{@volume, @volume_opts}])
    :ok
  end

  test "a committed extent resolves to its bytes after the committing node restarts",
       %{cluster: cluster} do
    payload = :binary.copy(<<0xA7>>, @chunk)
    hash = store_chunk(cluster, "/extent-a.bin", payload)

    assert {:ok, _roots} = block_index(cluster, :node1, :commit, [@volume, [{3, {:chunk, hash}}]])

    {:ok, cluster} = PeerCluster.restart_node(cluster, :node1)
    stabilise_after_restart(cluster)

    assert {:ok, {:chunk, ^hash}} = block_index(cluster, :node2, :get, [@volume, 3])
    assert {:ok, ^payload} = block_index(cluster, :node2, :read_extent, [@volume, 3])
  end

  test "a chunk written but never committed leaves no extent behind",
       %{cluster: cluster} do
    payload = :binary.copy(<<0xB8>>, @chunk)
    _hash = store_chunk(cluster, "/extent-b.bin", payload)

    {:ok, cluster} = PeerCluster.restart_node(cluster, :node1)
    stabilise_after_restart(cluster)

    # The crash window is exactly this: the chunk is durable and nothing
    # points at it. The extent must read as a hole, not as the orphan.
    assert {:ok, :hole} = block_index(cluster, :node2, :get, [@volume, 9])
    assert {:ok, zeroes} = block_index(cluster, :node2, :read_extent, [@volume, 9])
    assert zeroes == :binary.copy(<<0>>, @chunk)
  end

  test "every extent the map holds names a chunk that reads back",
       %{cluster: cluster} do
    extents =
      for index <- [0, 1, 64, 65, 200] do
        payload = :binary.copy(<<index>>, @chunk)
        {index, payload, store_chunk(cluster, "/extent-#{index}.bin", payload)}
      end

    commits = Enum.map(extents, fn {index, _payload, hash} -> {index, {:chunk, hash}} end)

    assert {:ok, roots} = block_index(cluster, :node1, :commit, [@volume, commits])

    # The batch straddles extent groups, so it publishes more than one
    # shard root in the single round — the property the group mapping and
    # the batched commit exist together to deliver.
    assert map_size(roots) > 1

    {:ok, cluster} = PeerCluster.restart_node(cluster, :node1)
    stabilise_after_restart(cluster)

    for {index, payload, _hash} <- extents do
      assert {:ok, ^payload} = block_index(cluster, :node2, :read_extent, [@volume, index])
    end
  end

  test "discard drops the extents and they read back as zeroes",
       %{cluster: cluster} do
    payload = :binary.copy(<<0xC9>>, @chunk)
    hash = store_chunk(cluster, "/extent-c.bin", payload)

    assert {:ok, _} =
             block_index(cluster, :node1, :commit, [
               @volume,
               [{10, {:chunk, hash}}, {11, {:chunk, hash}}]
             ])

    assert {:ok, ^payload} = block_index(cluster, :node1, :read_extent, [@volume, 10])

    assert {:ok, _} = block_index(cluster, :node1, :discard, [@volume, 10, 11])

    {:ok, cluster} = PeerCluster.restart_node(cluster, :node1)
    stabilise_after_restart(cluster)

    for index <- [10, 11] do
      assert {:ok, :hole} = block_index(cluster, :node2, :get, [@volume, index])
      assert {:ok, zeroes} = block_index(cluster, :node2, :read_extent, [@volume, index])
      assert zeroes == :binary.copy(<<0>>, @chunk)
    end
  end

  # `BlockIndex` deliberately does not write chunk data — the caller lands
  # the chunk over the data plane and then commits the map. A file write
  # is the shortest route to a durable chunk here, and its `FileMeta`
  # hands back the hash the extent needs to name.
  defp store_chunk(cluster, path, payload) do
    {:ok, meta} =
      PeerCluster.rpc(
        cluster,
        :node1,
        NeonFS.Core,
        :write_file_streamed,
        [@volume, path, [payload], [chunk_strategy: {:fixed, @chunk}]],
        120_000
      )

    # One extent, one chunk — a split payload would make the read-back
    # comparison below assert something other than what it claims to.
    assert [hash] = meta.chunks
    hash
  end

  defp block_index(cluster, node, function, args) do
    PeerCluster.rpc(cluster, node, BlockIndex, function, args, 120_000)
  end
end
