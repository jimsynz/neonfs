defmodule NeonFS.Integration.QuorumTest do
  @moduledoc """
  Phase 5 integration tests for quorum-replicated metadata.

  Tests the leaderless quorum system in a multi-node cluster:
  - Write/read consistency across nodes
  - Node failure tolerance (quorum of 2 with 3 nodes)
  - Read repair of stale replicas
  - Full write/read/delete cycle on multi-node cluster
  """
  use NeonFS.Integration.ClusterCase, async: false

  alias NeonFS.Core.MetadataRing

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag nodes: 3

  # Short RPC timeout for use inside retry loops — allows multiple attempts
  # within the assert_eventually window instead of blocking on a single slow call
  @retry_rpc_timeout 10_000

  describe "multi-node quorum consistency" do
    test "write on one node, read from another", %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster, "consistency-vol")

      test_data = :crypto.strong_rand_bytes(4096)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "consistency-vol",
          "/quorum.bin",
          test_data
        ])

      assert file.size == byte_size(test_data)

      # Read from node2 (metadata via quorum, chunk data fetched via RPC from node1)
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.TestHelpers,
               :read_file,
               [
                 "consistency-vol",
                 "/quorum.bin"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, ^test_data} -> true
          _ -> false
        end
      end

      # Verify file metadata accessible from node3 via quorum
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node3,
               NeonFS.TestHelpers,
               :get_file,
               [
                 "consistency-vol",
                 "/quorum.bin"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, f} -> f.id == file.id
          _ -> false
        end
      end
    end

    test "multiple files readable from all nodes", %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster, "multi-vol")

      files = [
        {"/file1.txt", "content one"},
        {"/file2.txt", "content two"},
        {"/docs/readme.md", "# README"}
      ]

      for {path, content} <- files do
        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
            "multi-vol",
            path,
            content
          ])
      end

      # Verify all files readable from node2
      for {path, expected} <- files do
        assert_eventually timeout: 60_000 do
          case PeerCluster.rpc(
                 cluster,
                 :node2,
                 NeonFS.TestHelpers,
                 :read_file,
                 [
                   "multi-vol",
                   path
                 ],
                 @retry_rpc_timeout
               ) do
            {:ok, ^expected} -> true
            _ -> false
          end
        end
      end
    end
  end

  describe "node failure during quorum operation" do
    test "quorum of 2 still works when one node fails", %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster, "failure-vol")

      test_data = "data before failure"

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "failure-vol",
          "/before.txt",
          test_data
        ])

      # Wait for data to be readable from node2 before killing node3
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.TestHelpers,
               :read_file,
               [
                 "failure-vol",
                 "/before.txt"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, ^test_data} -> true
          _ -> false
        end
      end

      # Stop node3
      :ok = PeerCluster.stop_node(cluster, :node3)

      node3_info = PeerCluster.get_node!(cluster, :node3)

      assert_eventually timeout: 5_000 do
        nodes = PeerCluster.rpc(cluster, :node1, Node, :list, [])
        node3_info.node not in nodes
      end

      # Do NOT rebuild the quorum ring — the existing 3-node ring still works
      # because quorum reads need R=2 responses and 2 of 3 replicas are alive.
      # Rebuilding would change segment-to-key mappings, making old data unfindable.

      # Reads of existing data should still work (quorum of 2 from node1+node2)
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node1,
               NeonFS.TestHelpers,
               :read_file,
               [
                 "failure-vol",
                 "/before.txt"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, ^test_data} -> true
          _ -> false
        end
      end
    end
  end

  describe "read repair" do
    test "stale replica repaired after quorum read", %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster, "repair-vol")

      # Get the quorum ring to find segment and replicas for a test key
      quorum_opts =
        PeerCluster.rpc(cluster, :node1, :persistent_term, :get, [
          {NeonFS.Core.FileIndex, :quorum_opts}
        ])

      ring = Keyword.fetch!(quorum_opts, :ring)
      test_key = "test:read_repair_verification"
      {segment_id, _replicas} = MetadataRing.locate(ring, test_key)

      # Write v1 to MetadataStore on all 3 nodes
      for node_name <- [:node1, :node2, :node3] do
        :ok =
          PeerCluster.rpc(cluster, node_name, NeonFS.Core.MetadataStore, :write, [
            segment_id,
            test_key,
            %{"version" => "v1", "data" => "original"}
          ])
      end

      # Write v2 to node1 only (newer HLC timestamp)
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.MetadataStore, :write, [
          segment_id,
          test_key,
          %{"version" => "v2", "data" => "updated"}
        ])

      # Now: node1 has v2@t2, node2/node3 have v1@t1
      # Quorum read from node2 should return v2 (latest) and trigger repair
      result =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.QuorumCoordinator, :quorum_read, [
          test_key,
          quorum_opts
        ])

      assert {:ok, %{"version" => "v2", "data" => "updated"}} = result

      # Wait for read repair to update the stale replicas
      assert_eventually timeout: 30_000 do
        case PeerCluster.rpc(cluster, :node3, NeonFS.Core.MetadataStore, :read, [
               segment_id,
               test_key
             ]) do
          {:ok, %{"version" => "v2"}, _ts} -> true
          _ -> false
        end
      end
    end
  end

  describe "full write/read/delete cycle on multi-node cluster" do
    test "complete file lifecycle across nodes", %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster, "lifecycle-vol")

      test_data = :crypto.strong_rand_bytes(8192)

      # Write from node1
      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "lifecycle-vol",
          "/lifecycle.bin",
          test_data
        ])

      assert file.size == byte_size(test_data)

      # Read from node2
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.TestHelpers,
               :read_file,
               [
                 "lifecycle-vol",
                 "/lifecycle.bin"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, ^test_data} -> true
          _ -> false
        end
      end

      # Read from node3 as well
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(
               cluster,
               :node3,
               NeonFS.TestHelpers,
               :read_file,
               [
                 "lifecycle-vol",
                 "/lifecycle.bin"
               ],
               @retry_rpc_timeout
             ) do
          {:ok, ^test_data} -> true
          _ -> false
        end
      end

      # Delete from node2
      assert_eventually timeout: 30_000 do
        case PeerCluster.rpc(
               cluster,
               :node2,
               NeonFS.TestHelpers,
               :delete_file,
               [
                 "lifecycle-vol",
                 "/lifecycle.bin"
               ],
               @retry_rpc_timeout
             ) do
          :ok -> true
          _ -> false
        end
      end

      # Verify deleted from all nodes
      for node_name <- [:node1, :node2, :node3] do
        assert_eventually timeout: 30_000 do
          case PeerCluster.rpc(
                 cluster,
                 node_name,
                 NeonFS.TestHelpers,
                 :read_file,
                 [
                   "lifecycle-vol",
                   "/lifecycle.bin"
                 ],
                 @retry_rpc_timeout
               ) do
            {:error, _} -> true
            _ -> false
          end
        end
      end
    end
  end

  # ─── Helpers ──────────────────────────────────────────────────────────

  defp init_multi_node_cluster(cluster, volume_name) do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["quorum-test"])

    {:ok, %{"token" => token}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

    node1_info = PeerCluster.get_node!(cluster, :node1)
    node1_str = Atom.to_string(node1_info.node)

    # Join nodes sequentially with waits between — Ra rejects concurrent cluster changes
    {:ok, _} =
      PeerCluster.rpc(cluster, :node2, NeonFS.CLI.Handler, :join_cluster, [token, node1_str])

    # Wait for node2's Ra membership to be committed before joining node3
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
        # Check this node sees the other 2 peers (excluding the test runner)
        node_list = :rpc.call(peer, Node, :list, [])
        other_peers = Enum.filter(node_list, &(&1 in peer_nodes))
        all_connected = length(other_peers) >= 2

        # Check MetadataStore is running on this node
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

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume_name,
        %{}
      ])

    :ok
  end
end
