defmodule NeonFS.Integration.ReplicationTest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 300_000
  # Phase 5 quorum-replicated metadata enables multi-node read/write
  @moduletag nodes: 3

  # Short RPC timeout for use inside retry loops — allows multiple attempts
  # within the assert_eventually window instead of blocking on a single slow call
  @retry_rpc_timeout 10_000

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

      # Data readable from the writer node (local)
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node1,
               NeonFS.TestHelpers,
               :read_file,
               [
                 "test-volume",
                 "/test.txt"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, "hello world"} -> true
          _ -> false
        end
      end

      # Data readable from a different node (cross-node via quorum + remote chunk fetch)
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.TestHelpers,
               :read_file,
               [
                 "test-volume",
                 "/test.txt"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, "hello world"} -> true
          _ -> false
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
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.TestHelpers,
               :read_file,
               [
                 "test-volume",
                 "/large.bin"
               ],
               @retry_rpc_timeout
             ) do
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
        assert_eventually timeout: 60_000 do
          case PeerCluster.rpc(
                 cluster,
                 :node2,
                 NeonFS.TestHelpers,
                 :read_file,
                 [
                   "test-volume",
                   path
                 ],
                 @retry_rpc_timeout
               ) do
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

    # Join nodes sequentially with waits between — Ra rejects concurrent cluster changes
    {:ok, _} =
      PeerCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :join_cluster, [token, node1_str])

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
      PeerCluster.rpc(cluster, :node3, NeonFS.CLI.Handler, :join_cluster, [token, node1_str])

    # Wait for cluster to stabilise with all 3 nodes
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

    # Wait for ALL peer nodes to see each other AND have MetadataStore running.
    # discover_core_nodes() filters Node.list() by MetadataStore presence,
    # so the ring will be incomplete if MetadataStore isn't ready on all peers.
    peer_nodes = Enum.map([:node1, :node2, :node3], &PeerCluster.get_node!(cluster, &1).node)

    assert_eventually timeout: 30_000 do
      Enum.all?(peer_nodes, fn peer ->
        node_list = :rpc.call(peer, Node, :list, [])
        other_peers = Enum.filter(node_list, &(&1 in peer_nodes))
        all_connected = length(other_peers) >= 2

        has_metadata_store =
          case :rpc.call(peer, Process, :whereis, [NeonFS.Core.MetadataStore]) do
            pid when is_pid(pid) -> true
            _ -> false
          end

        all_connected and has_metadata_store
      end)
    end

    # Rebuild quorum ring on all nodes now that full membership is confirmed
    for node_name <- [:node1, :node2, :node3] do
      PeerCluster.rpc(cluster, node_name, NeonFS.Core.Supervisor, :rebuild_quorum_ring, [])
    end

    # Create a test volume
    {:ok, _volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        "test-volume",
        %{}
      ])

    :ok
  end
end
