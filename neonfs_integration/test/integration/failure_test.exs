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
    init_multi_node_cluster(cluster)

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

    # Wait for initial data to be readable from node2 (proves quorum replication works)
    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
                 "test-volume",
                 "/test.txt"
               ]) do
            {:ok, "test data"} -> true
            _ -> false
          end
        end,
        timeout: 60_000
      )

    :ok
  end

  defp init_multi_node_cluster(cluster) do
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

    {:ok, %{"token" => token}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

    node1_info = PeerCluster.get_node!(cluster, :node1)
    node1_str = Atom.to_string(node1_info.node)

    join_nodes_sequentially(cluster, token, node1_str)
    wait_for_full_mesh(cluster)
    rebuild_quorum_rings(cluster)

    :ok
  end

  defp join_nodes_sequentially(cluster, token, node1_str) do
    for node_name <- [:node2, :node3] do
      {:ok, _} =
        PeerCluster.rpc(cluster, node_name, NeonFS.CLI.Handler, :join_cluster, [
          token,
          node1_str
        ])

      wait_for_cluster_stable(cluster)
    end
  end

  defp wait_for_cluster_stable(cluster) do
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
  end

  defp wait_for_full_mesh(cluster) do
    peer_nodes = Enum.map([:node1, :node2, :node3], &PeerCluster.get_node!(cluster, &1).node)

    assert_eventually timeout: 30_000 do
      Enum.all?(peer_nodes, fn peer ->
        node_list = :rpc.call(peer, Node, :list, [])
        other_peers = Enum.filter(node_list, &(&1 in peer_nodes))

        has_metadata_store =
          case :rpc.call(peer, Process, :whereis, [NeonFS.Core.MetadataStore]) do
            pid when is_pid(pid) -> true
            _ -> false
          end

        length(other_peers) >= 2 and has_metadata_store
      end)
    end
  end

  defp rebuild_quorum_rings(cluster) do
    for node_name <- [:node1, :node2, :node3] do
      PeerCluster.rpc(cluster, node_name, NeonFS.Core.Supervisor, :rebuild_quorum_ring, [])
    end
  end
end
