defmodule NeonFS.Integration.PartitionTest do
  @moduledoc """
  Partition tests that leave the mesh in a recoverable state between
  runs — majority / minority reads and writes, plus partition-healing
  flows. All tests share a single 3-node cluster via
  `cluster_mode: :shared`; `setup` heals any partition left behind by
  the previous test so each assertion starts from a clean mesh.

  Tests that mutate cluster state permanently (whole-node restarts,
  rolling restarts) live in `partition_restart_test.exs` with
  `cluster_mode: :per_test` — sharing would break their recovery
  semantics.

  Each test writes to a unique path derived from
  `System.unique_integer/1` so state from an earlier test cannot
  collide with a later assertion. The initial `/test.txt` written
  during `setup_all` is the only shared fixture; tests treat it as
  read-only.
  """

  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 300_000
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared
  @moduletag :partition

  setup_all %{cluster: cluster} do
    :ok = init_cluster_with_data(cluster)
    %{}
  end

  setup %{cluster: cluster} do
    # Previous test may have left the mesh partitioned. `heal_partition`
    # is idempotent; calling it on an already-healed mesh is a no-op.
    :ok = PeerCluster.heal_partition(cluster)
    :ok = wait_for_partition_healed(cluster, timeout: 30_000)
    :ok
  end

  describe "majority partition" do
    test "majority (2 of 3) can still write data", %{cluster: cluster} do
      partition_majority_minority(cluster)

      path = unique_path("majority-write")

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "test-volume",
          path,
          "majority data"
        ])

      assert {:ok, _} = result

      assert_eventually timeout: 30_000 do
        read_matches?(cluster, :node2, path, "majority data")
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
          ["test-volume", unique_path("should-fail"), "data"],
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

      path = unique_path("during-partition")

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "test-volume",
          path,
          "partition data"
        ])

      :ok = PeerCluster.heal_partition(cluster)
      :ok = wait_for_partition_healed(cluster, timeout: 30_000)

      # `AntiEntropy.sync_now` only syncs `segments_per_cycle` segments
      # per call (default 100); a single pass also doesn't account for
      # races between `wait_for_partition_healed` returning and the RPC
      # layer's connection state actually catching up. Re-trigger sync
      # on every poll iteration so a missed pass doesn't fail the
      # whole test (#564).
      assert_eventually timeout: 60_000 do
        trigger_anti_entropy(cluster, [:node3])
        read_matches?(cluster, :node3, path, "partition data")
      end
    end

    test "all nodes consistent after healing", %{cluster: cluster} do
      partition_majority_minority(cluster)

      paths = write_numbered_files(cluster, :node1, 1..3)

      :ok = PeerCluster.heal_partition(cluster)
      :ok = wait_for_partition_healed(cluster, timeout: 30_000)

      assert_all_nodes_have_files(cluster, paths)
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

  defp trigger_anti_entropy(cluster, node_names) do
    for node_name <- node_names do
      PeerCluster.rpc(cluster, node_name, NeonFS.Core.AntiEntropy, :sync_now, [])
    end

    :ok
  end

  # ─── Read/write helpers ─────────────────────────────────────────────

  defp unique_path(tag) do
    "/#{tag}-#{System.unique_integer([:positive])}.txt"
  end

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
    # Return the concrete paths (and their expected content) written so
    # callers can assert against them without re-deriving the suffix.
    suffix = System.unique_integer([:positive])

    for i <- range do
      path = "/consistency-#{suffix}-#{i}.txt"
      content = "data-#{suffix}-#{i}"

      {:ok, _} =
        PeerCluster.rpc(cluster, node_name, NeonFS.TestHelpers, :write_file_from_binary, [
          "test-volume",
          path,
          content
        ])

      {path, content}
    end
  end

  defp assert_all_nodes_have_files(cluster, paths_and_contents) do
    for node_name <- [:node1, :node2, :node3] do
      assert_node_has_files(cluster, node_name, paths_and_contents)
    end
  end

  defp assert_node_has_files(cluster, node_name, paths_and_contents) do
    # Same re-trigger-on-poll pattern as the single-file test: a
    # single `sync_now` pass isn't always enough after partition
    # heal (#564).
    assert_eventually timeout: 60_000 do
      trigger_anti_entropy(cluster, [node_name])

      Enum.all?(paths_and_contents, fn {path, content} ->
        read_matches?(cluster, node_name, path, content)
      end)
    end
  end
end
