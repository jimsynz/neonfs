defmodule NeonFS.Integration.QuorumTest do
  @moduledoc """
  Phase 5 integration tests for quorum-replicated metadata.

  Tests the leaderless quorum system in a multi-node cluster:
  - Write/read consistency across nodes
  - Node failure tolerance (quorum of 2 with 3 nodes)
  - Read repair of stale replicas
  - Full write/read/delete cycle on multi-node cluster
  """
  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.Core.MetadataRing

  @moduletag timeout: 300_000
  @moduletag :integration
  @moduletag nodes: 3
  @moduletag cluster_mode: :shared

  # RPC timeout for use inside retry loops. Must be large enough for a cold-cache
  # read_file (two quorum reads + RPC chunk fetch ≈ 21s worst case). With 30s per
  # attempt and 60s assert_eventually, the first attempt populates ETS caches and
  # the second (if needed) completes quickly.
  @retry_rpc_timeout 30_000

  setup_all %{cluster: cluster} do
    :ok = init_multi_node_cluster(cluster, name: "quorum-test")
    verify_all_quorum_rings(cluster)
    sync_drive_registries(cluster)
    %{}
  end

  describe "multi-node quorum consistency" do
    @tag :pending_903
    test "write on one node, read from another", %{cluster: cluster} do
      :ok = init_quorum_cluster(cluster, "consistency-vol")

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
          "consistency-vol"
        ])

      test_data = :crypto.strong_rand_bytes(4096)

      # Subscribe on node2, write on node1, wait for event to prove replication
      {:ok, file} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "consistency-vol",
              "/quorum.bin",
              test_data
            ])
          end,
          timeout: 15_000
        )

      assert file.size == byte_size(test_data)

      # Event proved replication — read from node2
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "consistency-vol",
          "/quorum.bin"
        ])

      assert read_data == test_data

      # File metadata accessible from node3 via quorum
      {:ok, file_from_node3} =
        PeerCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :get_file, [
          "consistency-vol",
          "/quorum.bin"
        ])

      assert file_from_node3.id == file.id
    end

    @tag :pending_903
    test "multiple files readable from all nodes", %{cluster: cluster} do
      :ok = init_quorum_cluster(cluster, "multi-vol")

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, ["multi-vol"])

      files = [
        {"/file1.txt", "content one"},
        {"/file2.txt", "content two"},
        {"/docs/readme.md", "# README"}
      ]

      # Write each file and wait for replication event on node2
      for {path, content} <- files do
        {:ok, _} =
          subscribe_then_act(
            cluster,
            :node2,
            volume.id,
            fn ->
              PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
                "multi-vol",
                path,
                content
              ])
            end,
            timeout: 15_000
          )
      end

      # Events proved replication — all files readable from node2
      for {path, expected} <- files do
        {:ok, data} =
          PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
            "multi-vol",
            path
          ])

        assert data == expected
      end
    end
  end

  describe "node failure during quorum operation" do
    @describetag cluster_mode: :per_test

    setup %{cluster: cluster} do
      :ok = init_multi_node_cluster(cluster, name: "quorum-test")
      verify_all_quorum_rings(cluster)
      sync_drive_registries(cluster)
      %{}
    end

    test "quorum of 2 still works when one node fails", %{cluster: cluster} do
      :ok = init_quorum_cluster(cluster, "failure-vol")

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
          "failure-vol"
        ])

      test_data = "data before failure"

      # Subscribe on node2, write on node1, wait for replication event
      {:ok, _} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "failure-vol",
              "/before.txt",
              test_data
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
      :ok = init_quorum_cluster(cluster, "repair-vol")

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

      # Ensure wall clock advances past all v1 timestamps before generating
      # the coordinator timestamp. Without this, v1 writes and the coordinator
      # timestamp can land in the same millisecond, and the HLC node_id
      # tiebreaker makes node3's v1 appear "newer" than the coord timestamp
      # (because node3's name sorts higher than node1's).
      Process.sleep(5)

      # Generate a coordinator timestamp from node1. Since node1 already had
      # a v1 write AND the wall clock has advanced, this timestamp is guaranteed
      # newer than any v1 timestamp on any node.
      coord_ts =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.MetadataStore, :generate_timestamp, [])

      # Write v2 to node1 and node2 with the coordinator timestamp
      for node_name <- [:node1, :node2] do
        :ok =
          PeerCluster.rpc(cluster, node_name, NeonFS.Core.MetadataStore, :write, [
            segment_id,
            test_key,
            %{"version" => "v2", "data" => "updated"},
            [caller_timestamp: coord_ts]
          ])
      end

      # Now: node1 has v2@coord_ts, node2 has v2@coord_ts, node3 has v1@t1
      # Quorum read should return v2 (latest) and trigger repair on node3
      assert_eventually timeout: 10_000 do
        result =
          PeerCluster.rpc(cluster, :node2, NeonFS.Core.QuorumCoordinator, :quorum_read, [
            test_key,
            quorum_opts
          ])

        match?({:ok, %{"version" => "v2", "data" => "updated"}}, result)
      end

      # Wait for read repair to update the stale replica (node3)
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
    @tag :pending_903
    test "complete file lifecycle across nodes", %{cluster: cluster} do
      :ok = init_quorum_cluster(cluster, "lifecycle-vol")

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
          "lifecycle-vol"
        ])

      test_data = :crypto.strong_rand_bytes(8192)

      # Subscribe on node2, write on node1, wait for replication event
      {:ok, file} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "lifecycle-vol",
              "/lifecycle.bin",
              test_data
            ])
          end,
          timeout: 15_000
        )

      assert file.size == byte_size(test_data)

      # Event proved replication — read from node2
      {:ok, data_node2} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "lifecycle-vol",
          "/lifecycle.bin"
        ])

      assert data_node2 == test_data

      # Read from node3 via quorum
      {:ok, data_node3} =
        PeerCluster.rpc(cluster, :node3, NeonFS.TestHelpers, :read_file, [
          "lifecycle-vol",
          "/lifecycle.bin"
        ])

      assert data_node3 == test_data

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

  describe "cache coherence (#342)" do
    @tag :pending_903
    test "point read after remote delete does not return stale cached value",
         %{cluster: cluster} do
      :ok = init_quorum_cluster(cluster, "coherence-vol")

      {:ok, volume} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.VolumeRegistry, :get_by_name, [
          "coherence-vol"
        ])

      # Write on node1. The event proves quorum replication reached node2.
      {:ok, file} =
        subscribe_then_act(
          cluster,
          :node2,
          volume.id,
          fn ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "coherence-vol",
              "/doomed.bin",
              "payload"
            ])
          end,
          timeout: 15_000
        )

      # Prime node2's FileIndex ETS cache by reading the file through the
      # public API. get_from_quorum inserts the decoded FileMeta into
      # :file_index_by_id on success — this is the cache entry that the
      # pre-#342 code would subsequently serve without consulting the
      # quorum store.
      {:ok, ^file} =
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.FileIndex, :get, [file.volume_id, file.id])

      # Delete on node1 via the high-level helper (quorum-delete of the
      # FileMeta + the parent DirectoryEntry child). Node1's local ETS
      # clears; node2's ETS still has the stale cached FileMeta.
      :ok =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :delete_file, [
          "coherence-vol",
          "/doomed.bin"
        ])

      # Node2 must report the file gone. Pre-#342 the ETS-first check
      # returned the stale {:ok, file}; the fix routes get/1 through
      # QuorumCoordinator.quorum_read, which sees the tombstone and
      # returns :not_found.
      #
      # Quorum writes ack at W=2 but the third replica's apply is async —
      # poll briefly so we do not race the tombstone replication.
      assert_eventually timeout: 10_000 do
        PeerCluster.rpc(cluster, :node2, NeonFS.Core.FileIndex, :get, [file.volume_id, file.id]) ==
          {:error, :not_found}
      end
    end
  end

  # ─── Helpers ──────────────────────────────────────────────────────────

  defp init_quorum_cluster(cluster, volume_name) do
    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [
        volume_name,
        %{}
      ])

    :ok
  end

  defp verify_all_quorum_rings(cluster) do
    for node_info <- cluster.nodes do
      verify_quorum_ring(cluster, node_info.name)
    end
  end

  defp verify_quorum_ring(cluster, node_name) do
    opts =
      PeerCluster.rpc(cluster, node_name, :persistent_term, :get, [
        {NeonFS.Core.FileIndex, :quorum_opts}
      ])

    ring = Keyword.fetch!(opts, :ring)
    ring_size = MapSet.size(ring.node_set)
    timeout = Keyword.fetch!(opts, :timeout)

    unless ring_size == 3 do
      raise "Ring on #{node_name} has #{ring_size} nodes, expected 3: #{inspect(ring.node_set)}"
    end

    unless timeout >= 10_000 do
      raise "Timeout on #{node_name} is #{timeout}ms, expected >= 10_000"
    end
  end

  defp sync_drive_registries(cluster) do
    # Force DriveRegistry to sync remote drives immediately so ChunkFetcher
    # has drive info for scoring (avoids 30s delay and "Drive info unavailable" warnings)
    for node_name <- [:node1, :node2, :node3] do
      PeerCluster.rpc(cluster, node_name, NeonFS.Core.DriveRegistry, :sync_now, [])
    end
  end
end
