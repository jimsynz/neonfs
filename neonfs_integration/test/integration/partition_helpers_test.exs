defmodule NeonFS.Integration.PartitionHelpersTest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 300_000
  @moduletag nodes: 3

  describe "disconnect and reconnect" do
    test "disconnect_nodes removes visibility between two nodes", %{cluster: cluster} do
      node2_atom = PeerCluster.get_node!(cluster, :node2).node

      # Verify connected first
      visible_from_1 = PeerCluster.visible_nodes(cluster, :node1)
      assert node2_atom in visible_from_1

      # Disconnect
      :ok = PeerCluster.disconnect_nodes(cluster, :node1, :node2)

      # Both sides should stop seeing each other
      assert_eventually timeout: 10_000 do
        node2_atom not in PeerCluster.visible_nodes(cluster, :node1)
      end

      node1_atom = PeerCluster.get_node!(cluster, :node1).node

      assert_eventually timeout: 10_000 do
        node1_atom not in PeerCluster.visible_nodes(cluster, :node2)
      end
    end

    test "reconnect_nodes restores visibility between two nodes", %{cluster: cluster} do
      node2_atom = PeerCluster.get_node!(cluster, :node2).node

      # Disconnect first
      :ok = PeerCluster.disconnect_nodes(cluster, :node1, :node2)

      assert_eventually timeout: 10_000 do
        node2_atom not in PeerCluster.visible_nodes(cluster, :node1)
      end

      # Reconnect
      :ok = PeerCluster.reconnect_nodes(cluster, :node1, :node2)

      assert_eventually timeout: 10_000 do
        node2_atom in PeerCluster.visible_nodes(cluster, :node1)
      end
    end

    test "disconnect is idempotent", %{cluster: cluster} do
      :ok = PeerCluster.disconnect_nodes(cluster, :node1, :node2)
      # Second call should not raise
      :ok = PeerCluster.disconnect_nodes(cluster, :node1, :node2)
    end

    test "reconnect is idempotent", %{cluster: cluster} do
      # Already connected — should not raise
      :ok = PeerCluster.reconnect_nodes(cluster, :node1, :node2)
    end
  end

  describe "partition_cluster" do
    test "partitions a 3-node cluster into {1} and {2,3}", %{cluster: cluster} do
      :ok = PeerCluster.partition_cluster(cluster, [[:node1], [:node2, :node3]])

      # Verify partition from all perspectives
      assert_partitioned(cluster, [:node1], [:node2, :node3])

      # Nodes within the same group should still see each other
      assert_connected(cluster, [:node2, :node3])
    end
  end

  describe "heal_partition" do
    test "restores full mesh after partition", %{cluster: cluster} do
      :ok = PeerCluster.partition_cluster(cluster, [[:node1], [:node2, :node3]])

      # Wait for partition to take effect
      assert_partitioned(cluster, [:node1], [:node2, :node3])

      # Heal
      :ok = PeerCluster.heal_partition(cluster)

      # Wait for full mesh
      :ok = wait_for_partition_healed(cluster, timeout: 30_000)

      # All nodes should see each other
      assert_connected(cluster, [:node1, :node2, :node3])
    end
  end
end
