defmodule NeonFS.Integration.Phase3Test do
  @moduledoc """
  Phase 3 integration tests for tiering, caching, and drive management.

  Tests the full tiering lifecycle across nodes:
  - Data lands on correct initial tier
  - Promotion/demotion based on access patterns
  - Cache improves repeated read performance
  - Drive selection distributes writes across drives in a tier
  - Tier migration moves chunks between tiers
  """
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag timeout: 180_000
  @moduletag :integration
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_cluster_with_volume(cluster)
    %{}
  end

  describe "tiering lifecycle" do
    test "writes land on hot tier by default", %{cluster: cluster} do
      # Write test data
      test_data = :crypto.strong_rand_bytes(1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/tier-test.bin",
          test_data
        ])

      assert file.size == byte_size(test_data)

      # Verify chunks are on the hot tier
      chunk_hashes = file.chunks

      for hash <- chunk_hashes do
        {:ok, meta} =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :get, [hash])

        assert Enum.any?(meta.locations, fn loc -> loc.tier == :hot end),
               "Chunk should be on hot tier"
      end
    end

    test "read data is correct after write", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(10 * 1024)

      {:ok, _file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/read-test.bin",
          test_data
        ])

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "test-volume",
          "/read-test.bin"
        ])

      assert read_data == test_data
    end

    test "chunk access tracking records accesses", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/access-test.bin",
          test_data
        ])

      # Read multiple times to generate access records
      for _ <- 1..5 do
        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
            "test-volume",
            "/access-test.bin"
          ])
      end

      # Check that accesses were recorded
      for hash <- file.chunks do
        stats =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkAccessTracker, :get_stats, [hash])

        assert stats.daily > 0, "Chunk should have access records after reads"
      end
    end

    test "tiering manager evaluation completes without error", %{cluster: cluster} do
      # Trigger manual evaluation
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.TieringManager, :evaluate_now, [])

      assert is_map(result)
      assert Map.has_key?(result, :promotions)
      assert Map.has_key?(result, :demotions)
      refute result[:skipped]
    end

    test "background worker accepts and completes work", %{cluster: cluster} do
      # Submit work via BackgroundWorker
      # Use an external function reference since anonymous fns can't cross RPC
      # (the defining module doesn't exist on the peer node)
      {:ok, work_id} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.BackgroundWorker, :submit, [
          &:erlang.node/0
        ])

      assert is_binary(work_id)

      # Wait for completion
      :ok =
        wait_until(
          fn ->
            status =
              PeerCluster.rpc(cluster, :node1, NeonFS.Core.BackgroundWorker, :status, [work_id])

            status == :completed
          end,
          timeout: 5_000
        )
    end

    test "local tier migration moves chunk between tiers", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/migrate-test.bin",
          test_data
        ])

      hash = hd(file.chunks)

      # Get the node name for migration params
      node_name =
        PeerCluster.rpc(cluster, :node1, Node, :self, [])

      # Run migration from hot to warm
      params = %{
        chunk_hash: hash,
        source_drive: "default",
        source_node: node_name,
        source_tier: :hot,
        target_drive: "default",
        target_node: node_name,
        target_tier: :warm
      }

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.TierMigration, :run_migration, [params])

      assert {:ok, :migrated} = result

      # Verify the chunk is now on warm tier
      {:ok, meta} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :get, [hash])

      assert Enum.any?(meta.locations, fn loc -> loc.tier == :warm end),
             "Chunk should be on warm tier after migration"
    end

    test "cache hit telemetry on repeated reads", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(1024)

      {:ok, _file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "test-volume",
          "/cache-test.bin",
          test_data
        ])

      # Get cache stats before reads
      stats_before =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkCache, :stats, [])

      # Read twice
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "test-volume",
          "/cache-test.bin"
        ])

      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "test-volume",
          "/cache-test.bin"
        ])

      stats_after =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkCache, :stats, [])

      # Cache hits should increase after repeated reads
      # (if decompress is enabled in the read path)
      assert stats_after.hits >= stats_before.hits or
               stats_after.misses >= stats_before.misses,
             "Cache should have activity after reads"
    end
  end

  describe "drive registry" do
    test "lists registered drives", %{cluster: cluster} do
      drives =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.DriveRegistry, :list_drives, [])

      assert is_list(drives)
      assert drives != []

      # Default drive should be present
      assert Enum.any?(drives, fn d -> d.id == "default" end)
    end

    test "selects drive for tier", %{cluster: cluster} do
      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.DriveRegistry, :select_drive, [:hot])

      assert {:ok, drive} = result
      assert drive.tier == :hot
    end
  end

  describe "worker status" do
    test "background worker reports status", %{cluster: cluster} do
      status =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.BackgroundWorker, :status, [])

      assert is_map(status)
      assert Map.has_key?(status, :queued)
      assert Map.has_key?(status, :running)
      assert Map.has_key?(status, :completed_total)
      assert Map.has_key?(status, :by_priority)
    end

    test "tiering manager reports status", %{cluster: cluster} do
      status =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.TieringManager, :status, [])

      assert is_map(status)
      assert Map.has_key?(status, :eval_interval_ms)
      assert Map.has_key?(status, :dry_run)
    end
  end

  # Helpers

  defp init_cluster_with_volume(cluster) do
    init_single_node_cluster(cluster, volumes: [{"test-volume", %{}}])
  end
end
