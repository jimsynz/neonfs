defmodule NeonFS.Integration.FailureTest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 300_000
  # Phase 5 quorum-replicated metadata enables multi-node failure tolerance
  @moduletag nodes: 3
  setup %{cluster: cluster} do
    :ok = init_cluster_with_data(cluster)
    %{cluster: cluster}
  end

  describe "single node failure" do
    test "cluster remains operational when one node crashes", %{cluster: cluster} do
      # Stop node3 (simulates crash)
      :ok = PeerCluster.stop_node(cluster, :node3)

      # Wait for cluster to detect the failure and still be operational.
      # Use default 30s RPC timeout — with node3 dead, quorum reads take longer.
      assert_eventually timeout: 60_000 do
        # Data should still be readable (quorum of 2 still available)
        case PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
               "test-volume",
               "/test.txt"
             ]) do
          {:ok, "test data"} -> true
          _ -> false
        end
      end

      # Should still be able to write
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/new.txt",
          "new data"
        ])

      assert {:ok, _} = result
    end

    test "data written during outage is preserved on surviving nodes", %{cluster: cluster} do
      # Write data, then stop node3
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/before-crash.txt",
          "before"
        ])

      :ok = PeerCluster.stop_node(cluster, :node3)

      # Wait until node3 is no longer reachable from node1's perspective
      node3_info = PeerCluster.get_node!(cluster, :node3)

      assert_eventually timeout: 5_000 do
        nodes = PeerCluster.rpc(cluster, :node1, Node, :list, [])
        node3_info.node not in nodes
      end

      # Write more data while node3 is down
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/during-outage.txt",
          "during"
        ])

      # Verify both files are readable from surviving nodes.
      # Use default 30s RPC timeout — with node3 dead, each quorum read waits
      # up to 5.5s for the dead replica timeout, and read_file chains 2-3 reads.
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
               "test-volume",
               "/before-crash.txt"
             ]) do
          {:ok, "before"} -> true
          _ -> false
        end
      end

      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
               "test-volume",
               "/during-outage.txt"
             ]) do
          {:ok, "during"} -> true
          _ -> false
        end
      end
    end
  end

  describe "quorum loss" do
    test "cluster becomes read-only when quorum lost", %{cluster: cluster} do
      # Stop two nodes (lose quorum)
      :ok = PeerCluster.stop_node(cluster, :node2)
      :ok = PeerCluster.stop_node(cluster, :node3)

      # Wait for failure detection - writes should eventually fail
      # Note: Ra commands timeout after 5 seconds, GenServer.call after 10 seconds
      # RPC timeout is 30 seconds, so we need at least one full cycle
      assert_eventually timeout: 60_000 do
        result =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
            "test-volume",
            "/should-fail.txt",
            "data"
          ])

        match?({:error, _}, result)
      end
    end
  end

  defp init_cluster_with_data(cluster) do
    :ok = init_multi_node_cluster(cluster, volumes: [{"test-volume", %{}}])

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, ["test-volume"])

    # Subscribe on node2, write on node1, wait for replication event
    {:ok, _} =
      subscribe_then_act(
        cluster,
        :node2,
        volume.id,
        fn ->
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
            "test-volume",
            "/test.txt",
            "test data"
          ])
        end,
        timeout: 15_000
      )

    :ok
  end
end
