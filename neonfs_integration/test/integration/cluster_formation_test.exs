defmodule NeonFS.Integration.ClusterFormationTest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 60_000
  @moduletag :tmp_dir

  describe "multi-node cluster" do
    @describetag nodes: 3

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

  describe "single-node cluster (with Ra)" do
    @describetag nodes: 1

    test "Ra is running", %{cluster: cluster} do
      # Ra should be enabled for single-node cluster
      # :ra_system.fetch/1 returns the system config map directly (not wrapped in :ok)
      result = PeerCluster.rpc(cluster, :node1, :ra_system, :fetch, [:default])
      assert is_map(result)
      assert result.name == :default
    end

    test "cluster can be initialized", %{cluster: cluster} do
      result = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])
      assert {:ok, _init_info} = result

      # Verify cluster status
      {:ok, status} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, [])
      assert status.status == :running
    end

    test "volumes can be created and listed", %{cluster: cluster} do
      # Initialize cluster first
      {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

      # Create a volume
      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, ["test-volume", %{}])

      assert volume.name == "test-volume"

      # List volumes
      {:ok, volumes} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :list_volumes, [])
      assert Enum.any?(volumes, &(&1.name == "test-volume"))
    end
  end
end
