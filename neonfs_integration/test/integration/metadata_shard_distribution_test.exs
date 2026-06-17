defmodule NeonFS.Integration.MetadataShardDistributionTest do
  @moduledoc """
  End-to-end verification that volume metadata roots are sharded at the
  live count (#1312, activation of #1307).

  A volume is provisioned with one root per shard; a burst of creates
  into a single directory spreads its `file:` / `dirent:` keys across
  shards, so more than one shard's root pointer advances independently,
  and a directory listing still returns every child by merging the
  per-shard range scans.
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.Volume.Shard
  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :per_test
  @moduletag :integration

  @file_count 40

  setup %{cluster: cluster} do
    init_single_node_cluster(cluster,
      volumes: [
        {"shard-vol",
         %{
           durability: %{type: :replicate, factor: 1, min_copies: 1},
           compression: %{algorithm: :none}
         }}
      ]
    )

    %{}
  end

  test "a volume has one root per shard and a create burst spreads across them",
       %{cluster: cluster} do
    shard_count = PeerCluster.rpc(cluster, :node1, Shard, :count, [])
    assert shard_count > 1, "this test only proves anything when sharding is active"

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, ["shard-vol"])

    # Burst of files into one directory.
    for i <- 1..@file_count do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core, :write_file_at, [
          "shard-vol",
          "/burst/file_#{i}.bin",
          0,
          :crypto.strong_rand_bytes(32),
          []
        ])
    end

    shards = volume_shards(cluster, volume.id)

    # One root per shard is registered.
    assert map_size(shards) == shard_count

    # The burst advanced more than one shard's root — the keys really did
    # distribute, not funnel through a single CAS point.
    distinct_roots =
      shards
      |> Map.values()
      |> Enum.map(& &1.root_chunk_hash)
      |> Enum.uniq()

    assert length(distinct_roots) > 1,
           "expected the create burst to advance multiple shard roots, " <>
             "got #{length(distinct_roots)} distinct root hashes across #{map_size(shards)} shards"

    # Listing merges the per-shard scans back into the full directory.
    {:ok, entries} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core, :list_dir, ["shard-vol", "/burst"])

    names = entries |> Enum.map(&Path.basename(&1.path)) |> MapSet.new()
    expected = for i <- 1..@file_count, into: MapSet.new(), do: "file_#{i}.bin"
    assert MapSet.equal?(names, expected)
  end

  defp volume_shards(cluster, volume_id) do
    {:ok, roots} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
        &MetadataStateMachine.get_volume_roots/1
      ])

    Map.get(roots, volume_id, %{})
  end
end
