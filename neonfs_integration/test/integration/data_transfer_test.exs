defmodule NeonFS.Integration.DataTransferTest do
  @moduledoc """
  Integration tests for the Phase 9 data transfer plane.

  Verifies that TLS data plane operations work correctly across a multi-node
  cluster: endpoint advertisement, connection pooling, chunk transfer over TLS,
  replication via data plane, and remote reads via data plane.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.TestSupport.PeerCluster

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_data_transfer_cluster(cluster)
    %{}
  end

  describe "endpoint advertisement" do
    test "all nodes have data_endpoint in ServiceInfo after cluster init", %{cluster: cluster} do
      for node_name <- [:node1, :node2, :node3] do
        assert_eventually timeout: 15_000 do
          node_info = PeerCluster.get_node!(cluster, node_name)

          case PeerCluster.rpc(
                 cluster,
                 node_name,
                 NeonFS.Core.ServiceRegistry,
                 :get,
                 [node_info.node]
               ) do
            {:ok, service} ->
              metadata = service.metadata || %{}
              endpoint = Map.get(metadata, :data_endpoint)
              endpoint != nil and is_tuple(endpoint)

            _ ->
              false
          end
        end
      end
    end

    test "PoolManager creates pools for discovered peer endpoints", %{cluster: cluster} do
      # Wait for PoolManager to discover peer endpoints and create pools
      assert_eventually timeout: 30_000 do
        # From node1's perspective, pools should exist for node2 and node3
        node2_info = PeerCluster.get_node!(cluster, :node2)
        node3_info = PeerCluster.get_node!(cluster, :node3)

        pool2 =
          PeerCluster.rpc(cluster, :node1, NeonFS.Transport.PoolManager, :get_pool, [
            node2_info.node
          ])

        pool3 =
          PeerCluster.rpc(cluster, :node1, NeonFS.Transport.PoolManager, :get_pool, [
            node3_info.node
          ])

        match?({:ok, _}, pool2) and match?({:ok, _}, pool3)
      end
    end
  end

  describe "point-to-point chunk transfer" do
    test "put_chunk sends data from node1 to node2 via data plane", %{cluster: cluster} do
      chunk_data = "data plane integration test chunk"
      chunk_hash = :crypto.hash(:sha256, chunk_data)
      node2_info = PeerCluster.get_node!(cluster, :node2)

      # Wait for pool to be ready
      wait_for_pool(cluster, :node1, node2_info.node)

      # Send chunk from node1 to node2 via data plane
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Client.Router, :data_call, [
          node2_info.node,
          :put_chunk,
          [hash: chunk_hash, volume_id: "default", write_id: nil, tier: "hot", data: chunk_data]
        ])

      assert result == :ok

      # Verify chunk exists on node2 by reading it locally
      assert {:ok, ^chunk_data} =
               PeerCluster.rpc(cluster, :node2, NeonFS.Core.BlobStore, :read_chunk, [
                 chunk_hash,
                 "default"
               ])
    end

    test "get_chunk retrieves data from node2 to node1 via data plane", %{cluster: cluster} do
      chunk_data = "remote read integration test"
      node2_info = PeerCluster.get_node!(cluster, :node2)

      # Write chunk directly on node2
      {:ok, chunk_hash, _info} =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.BlobStore, :write_chunk, [
          chunk_data,
          "default",
          "hot"
        ])

      # Wait for pool to be ready
      wait_for_pool(cluster, :node1, node2_info.node)

      # Read chunk from node2 via data plane on node1
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Client.Router, :data_call, [
          node2_info.node,
          :get_chunk,
          [hash: chunk_hash, volume_id: "default", tier: "hot"]
        ])

      assert {:ok, ^chunk_data} = result
    end

    test "has_chunk checks chunk existence on remote node", %{cluster: cluster} do
      chunk_data = "has_chunk integration test"
      node2_info = PeerCluster.get_node!(cluster, :node2)

      # Write chunk on node2
      {:ok, chunk_hash, _info} =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.BlobStore, :write_chunk, [
          chunk_data,
          "default",
          "hot"
        ])

      wait_for_pool(cluster, :node1, node2_info.node)

      # Check chunk existence on node2 from node1
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Client.Router, :data_call, [
          node2_info.node,
          :has_chunk,
          [hash: chunk_hash]
        ])

      assert {:ok, %{tier: _tier, size: size}} = result
      assert size == byte_size(chunk_data)
    end

    test "get_chunk returns not_found for missing chunk", %{cluster: cluster} do
      node2_info = PeerCluster.get_node!(cluster, :node2)
      missing_hash = :crypto.hash(:sha256, "nonexistent chunk")

      wait_for_pool(cluster, :node1, node2_info.node)

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Client.Router, :data_call, [
          node2_info.node,
          :get_chunk,
          [hash: missing_hash, volume_id: "default", tier: "hot"]
        ])

      assert {:error, :not_found} = result
    end
  end

  describe "replicated volume write via data plane" do
    test "file write replicates chunks to remote nodes", %{cluster: cluster} do
      # Create a replicated volume
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "data-plane-vol",
          %{}
        ])

      # Wait for volume to be visible
      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
               "data-plane-vol"
             ]) do
          {:ok, _vol} -> true
          _ -> false
        end
      end

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
          "data-plane-vol"
        ])

      # Subscribe on node2, write on node1, wait for event to prove replication
      {:ok, _} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "data-plane-vol",
              "/test.txt",
              "data plane replication test content"
            ])
          end,
          timeout: 15_000
        )

      # Event proved replication — file readable from node2
      {:ok, data} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "data-plane-vol",
          "/test.txt"
        ])

      assert data == "data plane replication test content"
    end
  end

  describe "remote read via data plane" do
    test "reading a file from a node without local chunks succeeds", %{cluster: cluster} do
      # Create volume and write file from node1
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "remote-read-vol",
          %{}
        ])

      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
               "remote-read-vol"
             ]) do
          {:ok, _vol} -> true
          _ -> false
        end
      end

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
          "remote-read-vol"
        ])

      content = "remote read test: data should traverse the data plane"

      # Subscribe on node2, write on node1, wait for event to prove replication
      {:ok, _} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "remote-read-vol",
              "/remote-read.txt",
              content
            ])
          end,
          timeout: 15_000
        )

      # Event proved replication — read from node2 via data plane
      {:ok, data_node2} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "remote-read-vol",
          "/remote-read.txt"
        ])

      assert data_node2 == content

      # Also verify from node3 via quorum read
      {:ok, data_node3} =
        PeerCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :read_file, [
          "remote-read-vol",
          "/remote-read.txt"
        ])

      assert data_node3 == content
    end
  end

  describe "node failure during transfer" do
    @describetag cluster_mode: :per_test

    setup %{cluster: cluster} do
      init_data_transfer_cluster(cluster)
      %{}
    end

    test "surviving nodes continue operating after node failure", %{cluster: cluster} do
      # Create volume and initial data
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "failure-vol",
          %{}
        ])

      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
               "failure-vol"
             ]) do
          {:ok, _vol} -> true
          _ -> false
        end
      end

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
          "failure-vol"
        ])

      # Subscribe on node2, write on node1, wait for replication before failure test
      {:ok, _} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "failure-vol",
              "/before.txt",
              "before node failure"
            ])
          end,
          timeout: 15_000
        )

      # Stop node3
      :ok = PeerCluster.stop_node(cluster, :node3)

      node3_info = PeerCluster.get_node!(cluster, :node3)

      assert_eventually timeout: 5_000 do
        nodes = PeerCluster.rpc(cluster, :node1, Node, :list, [])
        node3_info.node not in nodes
      end

      # Write more data with node3 down — replication uses remaining nodes
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
               "failure-vol",
               "/after.txt",
               "after node failure"
             ]) do
          {:ok, _} -> true
          _ -> false
        end
      end

      # Data should be readable from node2
      assert_eventually timeout: 60_000 do
        case PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
               "failure-vol",
               "/after.txt"
             ]) do
          {:ok, "after node failure"} -> true
          _ -> false
        end
      end
    end
  end

  describe "erasure-coded volume write via data plane" do
    test "file write distributes stripe chunks across nodes", %{cluster: cluster} do
      # Create an erasure-coded volume (2 data + 1 parity = 3 chunks per stripe)
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "ec-data-plane-vol",
          %{"durability" => "erasure:2:1"}
        ])

      assert_eventually timeout: 10_000 do
        case PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
               "ec-data-plane-vol"
             ]) do
          {:ok, _vol} -> true
          _ -> false
        end
      end

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
          "ec-data-plane-vol"
        ])

      # Write a file — erasure coding produces stripe chunks sent via data plane
      content = :crypto.strong_rand_bytes(4096)

      # Subscribe on node2, write on node1, wait for event to prove replication
      {:ok, file} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "ec-data-plane-vol",
              "/ec-test.bin",
              content
            ])
          end,
          timeout: 15_000
        )

      # Verify the file has stripes (proves erasure coding was used)
      assert is_list(file.stripes)
      assert file.stripes != []

      # Event proved replication — read from node2
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "ec-data-plane-vol",
          "/ec-test.bin"
        ])

      assert read_data == content
    end
  end

  describe "data plane does not block Ra" do
    test "Ra metadata operations succeed during sustained data transfer", %{cluster: cluster} do
      node2_info = PeerCluster.get_node!(cluster, :node2)
      wait_for_pool(cluster, :node1, node2_info.node)

      # Check Ra leader before transfer
      ra_server = {:neonfs_meta, PeerCluster.get_node!(cluster, :node1).node}

      {:ok, _, leader_before} =
        PeerCluster.rpc(cluster, :node1, :ra, :members, [ra_server])

      # Perform sustained bulk transfer: 50 chunks via data plane
      for i <- 1..50 do
        chunk_data = "bulk transfer chunk #{i} " <> :crypto.strong_rand_bytes(1024)
        chunk_hash = :crypto.hash(:sha256, chunk_data)

        PeerCluster.rpc(cluster, :node1, NeonFS.Client.Router, :data_call, [
          node2_info.node,
          :put_chunk,
          [hash: chunk_hash, volume_id: "default", write_id: nil, tier: "hot", data: chunk_data]
        ])
      end

      # Verify Ra is still responsive — create a volume (requires Ra consensus)
      start_time = System.monotonic_time(:millisecond)

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
          "ra-stability-vol",
          %{}
        ])

      elapsed = System.monotonic_time(:millisecond) - start_time

      # Ra operation should complete within 5 seconds
      assert elapsed < 5_000,
             "Ra volume creation took #{elapsed}ms during bulk transfer, expected < 5000ms"

      # Verify no unnecessary leader elections
      {:ok, _, leader_after} =
        PeerCluster.rpc(cluster, :node1, :ra, :members, [ra_server])

      assert leader_before == leader_after,
             "Ra leader changed during bulk transfer: #{inspect(leader_before)} -> #{inspect(leader_after)}"
    end
  end

  describe "new node joins and becomes reachable" do
    @describetag cluster_mode: :per_test

    test "joining node's data plane is discovered by existing nodes", %{cluster: cluster} do
      # Initialize a 2-node cluster first
      {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

      {:ok, %{"token" => token}} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

      node1_atom = PeerCluster.get_node!(cluster, :node1) |> Map.get(:node)

      {:ok, _} =
        PeerCluster.rpc(cluster, :node2, NeonFS.Cluster.Join, :join_cluster_rpc, [
          token,
          node1_atom
        ])

      wait_for_cluster_stable(cluster)

      # Wait for 2-node mesh and data plane
      node2_info = PeerCluster.get_node!(cluster, :node2)

      assert_eventually timeout: 30_000 do
        node_list = PeerCluster.rpc(cluster, :node1, Node, :list, [])
        node2_info.node in node_list
      end

      for n <- [:node1, :node2] do
        PeerCluster.rpc(cluster, n, NeonFS.Core.Supervisor, :rebuild_quorum_ring, [])
      end

      wait_for_pool(cluster, :node1, node2_info.node)

      # Now join node3 — this should activate its data plane
      {:ok, _} =
        PeerCluster.rpc(cluster, :node3, NeonFS.Cluster.Join, :join_cluster_rpc, [
          token,
          node1_atom
        ])

      wait_for_cluster_stable(cluster)
      rebuild_quorum_rings(cluster)
      sync_data_endpoints(cluster)

      # Verify node3's data plane is reachable from node1
      node3_info = PeerCluster.get_node!(cluster, :node3)
      wait_for_pool(cluster, :node1, node3_info.node)

      # Verify chunks can be transferred to the new node
      chunk_data = "new node transfer test"
      chunk_hash = :crypto.hash(:sha256, chunk_data)

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Client.Router, :data_call, [
          node3_info.node,
          :put_chunk,
          [hash: chunk_hash, volume_id: "default", write_id: nil, tier: "hot", data: chunk_data]
        ])

      assert result == :ok

      # Verify chunk exists on node3
      assert {:ok, ^chunk_data} =
               PeerCluster.rpc(cluster, :node3, NeonFS.Core.BlobStore, :read_chunk, [
                 chunk_hash,
                 "default"
               ])
    end
  end

  ## Private helpers

  defp init_data_transfer_cluster(cluster) do
    :ok = init_multi_node_cluster(cluster)
    wait_for_data_plane(cluster)
    :ok
  end

  defp wait_for_data_plane(cluster) do
    # Pools are created eagerly during activate_data_plane (broadcast + create_peer_pools),
    # so they should be available almost immediately. Allow some time for TLS handshakes.
    assert_eventually timeout: 30_000 do
      Enum.all?([:node1, :node2, :node3], fn node_name ->
        node_has_all_peer_pools?(cluster, node_name)
      end)
    end
  end

  defp node_has_all_peer_pools?(cluster, node_name) do
    other_nodes =
      [:node1, :node2, :node3]
      |> List.delete(node_name)
      |> Enum.map(&PeerCluster.get_node!(cluster, &1).node)

    Enum.all?(other_nodes, fn peer_node ->
      match?(
        {:ok, _pool},
        PeerCluster.rpc(cluster, node_name, NeonFS.Transport.PoolManager, :get_pool, [peer_node])
      )
    end)
  end

  defp sync_data_endpoints(cluster) do
    # Ensure every node's ServiceRegistry knows about every other node's
    # data_endpoint. Needed after late joins when broadcast may not have
    # reached all peers yet.
    node_names = [:node1, :node2, :node3]

    endpoints =
      for node_name <- node_names do
        node_info = PeerCluster.get_node!(cluster, node_name)
        port = PeerCluster.rpc(cluster, node_name, NeonFS.Transport.Listener, :get_port, [])

        endpoint =
          if is_integer(port) and port > 0 do
            PeerCluster.rpc(
              cluster,
              node_name,
              NeonFS.Transport.PoolManager,
              :advertise_endpoint,
              [port]
            )
          end

        {node_name, node_info.node, endpoint}
      end

    for {_name, node, endpoint} <- endpoints, endpoint != nil do
      info = %NeonFS.Client.ServiceInfo{
        node: node,
        type: :core,
        metadata: %{data_endpoint: endpoint},
        registered_at: DateTime.utc_now()
      }

      for target_name <- node_names do
        PeerCluster.rpc(cluster, target_name, NeonFS.Core.ServiceRegistry, :register, [info])
      end
    end
  end
end
