defmodule NeonFS.Integration.FailureTest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 180_000
  @moduletag nodes: 3

  setup %{cluster: cluster} do
    :ok = init_cluster_with_data(cluster)
    %{cluster: cluster}
  end

  describe "single node failure" do
    test "cluster remains operational when one node crashes", %{cluster: cluster} do
      # Stop node3 (simulates crash)
      :ok = PeerCluster.stop_node(cluster, :node3)

      # Wait for cluster to detect the failure and still be operational
      assert_eventually timeout: 10_000 do
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

      # Note: Node restart not supported with simple child_spec approach.
      # Verifying data on surviving nodes instead.

      # Verify both files are readable from surviving nodes
      {:ok, content1} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "test-volume",
          "/before-crash.txt"
        ])

      assert content1 == "before"

      {:ok, content2} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "test-volume",
          "/during-outage.txt"
        ])

      assert content2 == "during"
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
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

    {:ok, %{"token" => token}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

    node1_info = PeerCluster.get_node!(cluster, :node1)
    node1_str = Atom.to_string(node1_info.node)

    {:ok, _} =
      PeerCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :join_cluster, [token, node1_str])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node3, NeonFS.CLI.Handler, :join_cluster, [token, node1_str])

    # Wait for cluster to stabilize
    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
            {:ok, _status} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "test-volume",
        %{}
      ])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
        "test-volume",
        "/test.txt",
        "test data"
      ])

    # Wait for initial replication to complete
    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :read_file, [
                 "test-volume",
                 "/test.txt"
               ]) do
            {:ok, "test data"} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )

    :ok
  end
end
