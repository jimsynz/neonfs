defmodule NeonFS.Integration.CLITest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 60_000
  @moduletag nodes: 1

  @cli_path Path.expand("../../../neonfs-cli/target/release/neonfs-cli", __DIR__)

  describe "CLI communication with peer nodes" do
    test "CLI can query cluster status", %{cluster: cluster} do
      # Skip if CLI binary doesn't exist
      unless File.exists?(@cli_path) do
        flunk(
          "CLI binary not found at #{@cli_path}. Build it with: cd neonfs-cli && cargo build --release"
        )
      end

      # Initialize the cluster
      {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

      # Wait for initialization
      assert_eventually do
        case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
          {:ok, _} -> true
          _ -> false
        end
      end

      # Get cluster info for CLI
      node_info = PeerCluster.get_node!(cluster, :node1)

      # Run the CLI binary against the peer node
      result = run_cli(cluster.cookie, node_info.node, ["cluster", "status"])

      assert {:ok, output} = result
      assert output =~ "running"
    end

    test "CLI can create and list volumes", %{cluster: cluster} do
      # Skip if CLI binary doesn't exist
      unless File.exists?(@cli_path) do
        flunk(
          "CLI binary not found at #{@cli_path}. Build it with: cd neonfs-cli && cargo build --release"
        )
      end

      {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

      node_info = PeerCluster.get_node!(cluster, :node1)

      # Create volume via CLI
      assert {:ok, _} =
               run_cli(cluster.cookie, node_info.node, ["volume", "create", "cli-test-volume"])

      # Wait for volume to be created, then list via CLI
      assert_eventually do
        case run_cli(cluster.cookie, node_info.node, ["volume", "list"]) do
          {:ok, output} -> output =~ "cli-test-volume"
          _ -> false
        end
      end
    end
  end

  describe "cluster operations without CLI binary" do
    test "cluster status can be queried via RPC", %{cluster: cluster} do
      {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

      {:ok, status} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, [])

      assert is_map(status)
      assert Map.has_key?(status, :status)
      assert status.status == :running
    end

    test "volumes can be created and listed via RPC", %{cluster: cluster} do
      {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

      # Create volume
      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "rpc-test-volume",
          %{}
        ])

      assert volume.name == "rpc-test-volume"

      # List volumes
      {:ok, volumes} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :list_volumes, [])

      assert Enum.any?(volumes, &(&1.name == "rpc-test-volume"))
    end

    test "volumes can be deleted via RPC", %{cluster: cluster} do
      {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

      # Create volume
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, ["delete-me", %{}])

      # Delete volume
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :delete_volume, ["delete-me"])

      # Verify deleted
      {:ok, volumes} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :list_volumes, [])

      refute Enum.any?(volumes, &(&1.name == "delete-me"))
    end
  end

  defp run_cli(cookie, node, args) do
    cookie_str = Atom.to_string(cookie)
    node_str = Atom.to_string(node)

    env = [
      {"NEONFS_COOKIE", cookie_str},
      {"NEONFS_NODE", node_str}
    ]

    case System.cmd(@cli_path, args, stderr_to_stdout: true, env: env) do
      {output, 0} -> {:ok, output}
      {output, code} -> {:error, {code, output}}
    end
  end
end
