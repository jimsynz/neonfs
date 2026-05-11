defmodule NeonFS.Integration.SnapshotTest do
  @moduledoc """
  Multi-node integration tests for the snapshot keyspace foundation
  (#960 / epic #959). Exercises `NeonFS.Core.Snapshot` create/get/list/delete
  through a real peer cluster.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.Snapshot

  @moduletag timeout: 180_000
  @moduletag :integration

  describe "single-node cluster" do
    @describetag nodes: 1
    @describetag cluster_mode: :shared

    setup_all %{cluster: cluster} do
      :ok = init_single_node_cluster(cluster, name: "snap-single")

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "snap-vol",
          %{}
        ])

      %{volume_id: volume.id}
    end

    test "create / list / get / delete round-trip", %{cluster: cluster, volume_id: volume_id} do
      assert {:ok, []} = PeerCluster.rpc(cluster, :node1, Snapshot, :list, [volume_id])

      {:ok, snap} =
        PeerCluster.rpc(cluster, :node1, Snapshot, :create, [volume_id, [name: "v1"]])

      assert snap.volume_id == volume_id
      assert snap.name == "v1"
      assert is_binary(snap.root_chunk_hash)
      assert byte_size(snap.root_chunk_hash) > 0

      assert {:ok, [^snap]} = PeerCluster.rpc(cluster, :node1, Snapshot, :list, [volume_id])
      assert {:ok, ^snap} = PeerCluster.rpc(cluster, :node1, Snapshot, :get, [volume_id, snap.id])

      assert :ok = PeerCluster.rpc(cluster, :node1, Snapshot, :delete, [volume_id, snap.id])
      assert {:ok, []} = PeerCluster.rpc(cluster, :node1, Snapshot, :list, [volume_id])

      assert {:error, :not_found} =
               PeerCluster.rpc(cluster, :node1, Snapshot, :get, [volume_id, snap.id])
    end

    test "refuses to snapshot a volume that doesn't exist", %{cluster: cluster} do
      assert {:error, :volume_not_found} =
               PeerCluster.rpc(cluster, :node1, Snapshot, :create, ["no-such-volume", []])
    end
  end

  describe "multi-node cluster" do
    @describetag nodes: 3

    setup_all %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster, name: "snap-multi")

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "snap-multi-vol",
          %{}
        ])

      %{volume_id: volume.id}
    end

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
