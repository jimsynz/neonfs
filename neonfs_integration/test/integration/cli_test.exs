defmodule NeonFS.Integration.CLITest do
  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 60_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  @cli_path Path.expand("../../../neonfs-cli/target/release/neonfs-cli", __DIR__)

  setup_all %{cluster: cluster} do
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])
    :ok = wait_for_cluster_stable(cluster)
    %{}
  end

  describe "CLI communication with peer nodes" do
    test "CLI can query cluster status", %{cluster: cluster} do
      # Skip if CLI binary doesn't exist
      unless File.exists?(@cli_path) do
        flunk(
          "CLI binary not found at #{@cli_path}. Build it with: cd neonfs-cli && cargo build --release"
        )
      end

      # Run the CLI binary against the peer node
      result = run_cli(cluster, :node1, ["cluster", "status"])

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

      # Create volume via CLI
      assert {:ok, _} =
               run_cli(cluster, :node1, ["volume", "create", "cli-test-volume"])

      # Wait for volume to be created, then list via CLI
      assert_eventually do
        case run_cli(cluster, :node1, ["volume", "list"]) do
          {:ok, output} -> output =~ "cli-test-volume"
          _ -> false
        end
      end
    end
  end

  describe "cluster operations without CLI binary" do
    test "cluster status can be queried via RPC", %{cluster: cluster} do
      {:ok, status} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, [])

      assert is_map(status)
      assert Map.has_key?(status, :status)
      assert status.status == :running
    end

    test "volumes can be created and listed via RPC", %{cluster: cluster} do
      # Create volume
      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "rpc-test-volume",
          %{"durability" => "replicate:1"}
        ])

      assert volume.name == "rpc-test-volume"

      # List volumes
      {:ok, volumes} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :list_volumes, [])

      assert Enum.any?(volumes, &(&1.name == "rpc-test-volume"))
    end

    test "volumes can be deleted via RPC", %{cluster: cluster} do
      # Create volume
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "delete-me",
          %{"durability" => "replicate:1"}
        ])

      # Delete volume
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :delete_volume, ["delete-me"])

      # Verify deleted
      {:ok, volumes} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :list_volumes, [])

      refute Enum.any?(volumes, &(&1.name == "delete-me"))
    end
  end

  defp run_cli(cluster, node_name, args) do
    node_info = PeerCluster.get_node!(cluster, node_name)
    cookie_str = Atom.to_string(cluster.cookie)
    node_str = Atom.to_string(node_info.node)

    env = [
      {"NEONFS_COOKIE", cookie_str},
      {"NEONFS_NODE", node_str},
      {"NEONFS_DIST_PORT", Integer.to_string(node_info.dist_port)}
    ]

    case System.cmd(@cli_path, args, stderr_to_stdout: true, env: env) do
      {output, 0} -> {:ok, output}
      {output, code} -> {:error, {code, output}}
    end
  end
end
