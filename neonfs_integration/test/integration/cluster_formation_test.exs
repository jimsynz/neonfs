defmodule NeonFS.Integration.ClusterFormationTest do
  # Single-node `cluster init` / volume creation tests — each mutates state,
  # so cluster lifecycle stays `:per_test` (the default). The read-only
  # multi-node distribution smoke tests live in
  # `cluster_formation_multi_node_test.exs` with `cluster_mode: :shared`.
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 60_000
  @moduletag :tmp_dir

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
