defmodule NeonFS.Integration.PartitionTest do
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag :partition

  setup %{cluster: cluster} do
    :ok = init_cluster_with_data(cluster)
    %{cluster: cluster}
  end

  describe "majority partition" do
    test "majority (2 of 3) can still write data", %{cluster: cluster} do
      partition_majority_minority(cluster)

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "test-volume",
          "/majority-write.txt",
          "majority data"
        ])

      assert {:ok, _} = result

      assert_eventually timeout: 30_000 do
        read_matches?(cluster, :node2, "/majority-write.txt", "majority data")
      end
    end
  end

  describe "minority partition" do
    test "minority (1 of 3) cannot write", %{cluster: cluster} do
      partition_majority_minority(cluster)

      # Single write attempt with generous timeout — quorum write on the
      # minority node must fail because W=2 replicas are unreachable.
      result =
        PeerCluster.rpc(
          cluster,
          :node3,
          NeonFS.TestHelpers,
          :write_file_from_binary,
          ["test-volume", "/should-fail.txt", "data"],
          120_000
        )

      assert write_failed?(result),
             "Expected write to fail on minority partition, got: #{inspect(result)}"
    end

    test "minority can read previously written data", %{cluster: cluster} do
      partition_majority_minority(cluster)

      assert_eventually timeout: 60_000 do
        read_matches?(cluster, :node3, "/test.txt", "test data")
      end
    end
  end

  describe "partition healing" do
    test "minority receives data written during partition", %{cluster: cluster} do
      partition_majority_minority(cluster)

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "test-volume",
          "/during-partition.txt",
          "partition data"
        ])

      heal_and_sync(cluster, [:node3])

      assert_eventually timeout: 60_000 do
        read_matches?(cluster, :node3, "/during-partition.txt", "partition data")
      end
    end

    test "all nodes consistent after healing", %{cluster: cluster} do
      partition_majority_minority(cluster)

      write_numbered_files(cluster, :node1, 1..3)

      heal_and_sync(cluster, [:node1, :node2, :node3])

      assert_all_nodes_have_files(cluster, 1..3)
    end
  end

  describe "rolling restart" do
    test "data survives sequential restart of each node", %{cluster: cluster} do
      cluster = restart_and_verify(cluster, :node1)
      cluster = restart_and_verify(cluster, :node2)
      _cluster = restart_and_verify(cluster, :node3)
    end
  end

  describe "node restart recovery" do
    test "restarted node catches up via anti-entropy", %{cluster: cluster} do
      :ok = PeerCluster.stop_node(cluster, :node3)

      # Wait for the remaining nodes to detect node3 is gone
      node3_atom = PeerCluster.get_node!(cluster, :node3).node

      assert_eventually timeout: 10_000 do
        node3_atom not in PeerCluster.rpc(cluster, :node1, Node, :list, [])
      end

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "test-volume",
          "/written-while-down.txt",
          "missed data"
        ])

      {:ok, cluster} = PeerCluster.restart_node(cluster, :node3)

      stabilise_after_restart(cluster)
      trigger_anti_entropy(cluster, [:node1, :node2, :node3])

      assert_eventually timeout: 60_000 do
        read_matches?(cluster, :node3, "/written-while-down.txt", "missed data")
      end
    end
  end

  # ─── Setup helpers ───────────────────────────────────────────────────

  defp init_cluster_with_data(cluster) do
    :ok = init_multi_node_cluster(cluster, volumes: [{"test-volume", %{}}])

    {:ok, volume} =
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, ["test-volume"])

    # Write initial data, wait for replication event on node2
    {:ok, _} =
      subscribe_then_act(
        cluster,
        :node2,
        volume.id,
        fn ->
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
            "test-volume",
            "/test.txt",
            "test data"
          ])
        end,
        timeout: 15_000
      )

    # Ensure all 3 nodes can read the data (populates ETS caches and
    # triggers read repair so each node has local replicas)
    verify_all_nodes_can_read(cluster)
  end

  defp verify_all_nodes_can_read(cluster) do
    for node_name <- [:node1, :node2, :node3] do
      :ok =
        wait_until(
          fn -> read_matches?(cluster, node_name, "/test.txt", "test data") end,
          timeout: 30_000
        )
    end

    :ok
  end

  # ─── Partition helpers ───────────────────────────────────────────────

  defp partition_majority_minority(cluster) do
    :ok = PeerCluster.partition_cluster(cluster, [[:node1, :node2], [:node3]])
    assert_partitioned(cluster, [:node1, :node2], [:node3], timeout: 60_000)
  end

  defp heal_and_sync(cluster, sync_nodes) do
    :ok = PeerCluster.heal_partition(cluster)
    :ok = wait_for_partition_healed(cluster, timeout: 30_000)
    trigger_anti_entropy(cluster, sync_nodes)
  end

  defp trigger_anti_entropy(cluster, node_names) do
    for node_name <- node_names do
      PeerCluster.rpc(cluster, node_name, NeonFS.Core.AntiEntropy, :sync_now, [])
    end

    :ok
  end

  # ─── Read/write helpers ─────────────────────────────────────────────

  defp read_matches?(cluster, node_name, path, expected_content) do
    case PeerCluster.rpc(cluster, node_name, NeonFS.TestHelpers, :read_file, [
           "test-volume",
           path
         ]) do
      {:ok, ^expected_content} -> true
      _ -> false
    end
  end

  defp write_failed?(result) do
    match?({:error, _}, result) or match?({:badrpc, _}, result)
  end

  defp write_numbered_files(cluster, node_name, range) do
    for i <- range do
      {:ok, _} =
        PeerCluster.rpc(cluster, node_name, NeonFS.TestHelpers, :write_file_from_binary, [
          "test-volume",
          "/consistency-#{i}.txt",
          "data-#{i}"
        ])
    end

    :ok
  end

  defp assert_all_nodes_have_files(cluster, range) do
    for node_name <- [:node1, :node2, :node3] do
      assert_node_has_files(cluster, node_name, range)
    end
  end

  defp assert_node_has_files(cluster, node_name, range) do
    assert_eventually timeout: 60_000 do
      Enum.all?(range, fn i ->
        read_matches?(cluster, node_name, "/consistency-#{i}.txt", "data-#{i}")
      end)
    end
  end

  # ─── Restart helpers ────────────────────────────────────────────────

  defp restart_and_verify(cluster, node_name) do
    {:ok, cluster} = PeerCluster.restart_node(cluster, node_name)

    stabilise_after_restart(cluster)
    trigger_anti_entropy(cluster, [:node1, :node2, :node3])

    assert_eventually timeout: 90_000 do
      read_matches?(cluster, node_name, "/test.txt", "test data")
    end

    cluster
  end

  defp stabilise_after_restart(cluster) do
    wait_for_full_mesh(cluster)
    wait_for_ra_quorum(cluster)
    rebuild_quorum_rings(cluster)
  end

  defp wait_for_ra_quorum(cluster) do
    # Use get_state (defined in RaSupervisor, loaded on all peers) instead
    # of passing an anonymous function — funs defined in the test module
    # aren't loadable on peer nodes and would crash the Ra leader with :undef.
    for node_info <- cluster.nodes do
      :ok =
        wait_until(
          fn ->
            match?(
              {:ok, _},
              PeerCluster.rpc(
                cluster,
                node_info.name,
                NeonFS.Core.RaSupervisor,
                :get_state,
                []
              )
            )
          end,
          timeout: 30_000
        )
    end

    :ok
  end
end
