defmodule NeonFS.Integration.SnapshotMultiNodeTest do
  @moduledoc """
  Multi-node fan-out test for the snapshot keyspace foundation
  (#960 / epic #959). The single-node round-trip lives in
  `snapshot_test.exs`.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.Snapshot

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "snap-multi")

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "snap-multi-vol",
        %{}
      ])

    %{volume_id: volume.id}
  end

  describe "multi-node cluster" do
    test "a snapshot taken on one node is visible from every node", %{
      cluster: cluster,
      volume_id: volume_id
    } do
      {:ok, snap} =
        PeerCluster.rpc(cluster, :node1, Snapshot, :create, [volume_id, [name: "fanout"]])

      for node <- [:node1, :node2, :node3] do
        assert {:ok, [stored]} = PeerCluster.rpc(cluster, node, Snapshot, :list, [volume_id]),
               "node #{node} did not see the snapshot"

        assert stored.id == snap.id
        assert stored.root_chunk_hash == snap.root_chunk_hash
        assert stored.name == "fanout"
      end

      assert :ok = PeerCluster.rpc(cluster, :node2, Snapshot, :delete, [volume_id, snap.id])

      for node <- [:node1, :node2, :node3] do
        assert {:ok, []} = PeerCluster.rpc(cluster, node, Snapshot, :list, [volume_id]),
               "node #{node} did not observe the delete"
      end
    end
  end
end
