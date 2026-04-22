defmodule NeonFS.Integration.ClusterFormationMultiNodeTest do
  # Read-only smoke tests on a healthy 3-node cluster. The describe block was
  # split out of ClusterFormationTest so it can use cluster_mode: :shared —
  # the three tests mutate nothing, so paying for three cluster bootstraps
  # was pure overhead (#421).
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 60_000
  @moduletag cluster_mode: :shared
  @moduletag nodes: 3

  test "all nodes can see each other", %{cluster: cluster} do
    node_names = Enum.map(cluster.nodes, & &1.name)
    assert length(node_names) == 3

    # Each node should see the others via Erlang distribution
    for node_name <- node_names do
      # Wait until node sees all other nodes
      assert_eventually timeout: 5_000 do
        node_list = PeerCluster.rpc(cluster, node_name, Node, :list, [])

        other_nodes =
          cluster.nodes
          |> Enum.reject(&(&1.name == node_name))
          |> Enum.map(& &1.node)

        Enum.all?(other_nodes, &(&1 in node_list))
      end
    end
  end

  test "nodes can communicate via RPC", %{cluster: cluster} do
    # Verify basic RPC works between nodes
    for node_name <- [:node1, :node2, :node3] do
      result = PeerCluster.rpc(cluster, node_name, Node, :self, [])
      node_info = PeerCluster.get_node!(cluster, node_name)
      assert result == node_info.node
    end
  end

  test "application is running on all nodes", %{cluster: cluster} do
    for node_name <- [:node1, :node2, :node3] do
      result = PeerCluster.rpc(cluster, node_name, Application, :started_applications, [])
      app_names = Enum.map(result, fn {name, _, _} -> name end)
      assert :neonfs_core in app_names
    end
  end
end
