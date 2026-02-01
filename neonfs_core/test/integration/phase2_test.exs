defmodule NeonFS.Integration.Phase2Test do
  use ExUnit.Case, async: false

  alias NeonFS.TestCluster

  @moduletag :integration
  @moduletag timeout: 300_000

  setup do
    {:ok, cluster} = TestCluster.start(nodes: 3)
    on_exit(fn -> TestCluster.stop(cluster) end)
    %{cluster: cluster}
  end

  test "cluster survives single node failure", %{cluster: cluster} do
    # Init cluster on node1
    :ok = TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

    # Create join token from node1
    {:ok, token} = TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [])

    # Join nodes 2 and 3 to the cluster
    :ok = TestCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :cluster_join, [token, :node1])
    :ok = TestCluster.rpc(cluster, :node3, NeonFS.CLI.Handler, :cluster_join, [token, :node1])

    # Give cluster time to stabilize
    Process.sleep(5_000)

    # Create volume with replication factor 3
    :ok =
      TestCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "test",
        %{durability: %{type: :replicate, factor: 3}}
      ])

    # Write test data (100KB)
    test_data = :crypto.strong_rand_bytes(100_000)

    {:ok, _file_id} =
      TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
        "test",
        "/data.bin",
        test_data
      ])

    # Give replication time to complete
    Process.sleep(5_000)

    # Verify data is available on all nodes
    for node <- [:node1, :node2, :node3] do
      {:ok, data} =
        TestCluster.rpc(cluster, node, NeonFS.TestHelpers, :read_file, ["test", "/data.bin"])

      assert data == test_data, "Data mismatch on #{node}"
    end

    # Kill node 3 (simulate crash)
    {:ok, cluster} = TestCluster.kill_node(cluster, :node3)

    # Give cluster time to detect the failure
    Process.sleep(5_000)

    # Verify data is still readable from surviving nodes (quorum still available)
    {:ok, data} =
      TestCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, ["test", "/data.bin"])

    assert data == test_data

    {:ok, data} =
      TestCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, ["test", "/data.bin"])

    assert data == test_data

    # Restart node 3
    {:ok, _cluster} = TestCluster.restart_node(cluster, :node3)

    # Give node time to rejoin and sync
    Process.sleep(10_000)

    # Verify node 3 can read the data after rejoining
    {:ok, data} =
      TestCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :read_file, ["test", "/data.bin"])

    assert data == test_data
  end
end
