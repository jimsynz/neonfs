defmodule NeonFS.Integration.ReplicationTest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 120_000
  @moduletag nodes: 3

  setup %{cluster: cluster} do
    # Initialize cluster and create volume
    :ok = init_cluster_with_volume(cluster)
    %{cluster: cluster}
  end

  describe "write replication" do
    test "data is replicated to all nodes", %{cluster: cluster} do
      # Write data via node1
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/test.txt",
          "hello world"
        ])

      assert {:ok, _file_id} = result

      # Wait for replication - data should be readable from all nodes
      for node_name <- [:node1, :node2, :node3] do
        assert_eventually timeout: 10_000 do
          case PeerCluster.rpc(cluster, node_name, NeonFS.TestHelpers, :read_file, [
                 "test-volume",
                 "/test.txt"
               ]) do
            {:ok, "hello world"} -> true
            _ -> false
          end
        end
      end
    end

    test "large file chunking and replication", %{cluster: cluster} do
      # Write 1MB of data
      data = :crypto.strong_rand_bytes(1_024 * 1_024)

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/large.bin",
          data
        ])

      # Wait for replication to complete - verify from node2
      assert_eventually timeout: 30_000 do
        case PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
               "test-volume",
               "/large.bin"
             ]) do
          {:ok, ^data} -> true
          _ -> false
        end
      end
    end

    test "multiple files can be written and read", %{cluster: cluster} do
      # Write several files
      files = [
        {"/file1.txt", "content one"},
        {"/file2.txt", "content two"},
        {"/file3.txt", "content three"}
      ]

      for {path, content} <- files do
        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
            "test-volume",
            path,
            content
          ])
      end

      # Verify all files are readable from node2
      for {path, expected_content} <- files do
        assert_eventually timeout: 10_000 do
          case PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
                 "test-volume",
                 path
               ]) do
            {:ok, ^expected_content} -> true
            _ -> false
          end
        end
      end
    end
  end

  defp init_cluster_with_volume(cluster) do
    # Initialize cluster on node1
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

    # Create invite token
    {:ok, %{"token" => token}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

    node1_info = PeerCluster.get_node!(cluster, :node1)
    node1_str = Atom.to_string(node1_info.node)

    # Join node2 and node3
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

    # Create a test volume with 3-way replication
    {:ok, _volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "test-volume",
        %{}
      ])

    :ok
  end
end
