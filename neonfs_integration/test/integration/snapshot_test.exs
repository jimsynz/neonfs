defmodule NeonFS.Integration.SnapshotTest do
  @moduledoc """
  Single-node integration test for the snapshot keyspace foundation
  (#960 / epic #959). The multi-node fan-out test lives in
  `snapshot_multi_node_test.exs` because each test file pins its own
  peer-cluster shape via module-level `@moduletag`.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.Snapshot

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    :ok = init_single_node_cluster(cluster, name: "snap-single")

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "snap-vol",
        %{}
      ])

    %{volume_id: volume.id}
  end

  describe "single-node cluster" do
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
end
