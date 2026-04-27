defmodule NeonFS.Integration.PartitionRestartTest do
  @moduledoc """
  Partition tests that mutate cluster state permanently — whole-node
  restarts, sequential rolling restarts of every node — and therefore
  require `:per_test` cluster lifecycle.

  The non-restart partition tests (majority / minority reads + writes,
  partition healing) share a single cluster in
  `partition_test.exs` with `cluster_mode: :shared` (#506).
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag :partition

  setup %{cluster: cluster} do
    :ok = init_cluster_with_data(cluster)
    %{cluster: cluster}
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

  # ─── Read helpers ───────────────────────────────────────────────────

  defp read_matches?(cluster, node_name, path, expected_content) do
    case PeerCluster.rpc(cluster, node_name, NeonFS.TestHelpers, :read_file, [
           "test-volume",
           path
         ]) do
      {:ok, ^expected_content} -> true
      _ -> false
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

  defp trigger_anti_entropy(cluster, node_names) do
    for node_name <- node_names do
      PeerCluster.rpc(cluster, node_name, NeonFS.Core.AntiEntropy, :sync_now, [])
    end

    :ok
  end
end
