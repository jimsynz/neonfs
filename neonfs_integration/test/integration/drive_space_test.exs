defmodule NeonFS.Integration.DriveSpaceTest do
  @moduledoc """
  Integration tests exercising real storage space behaviours using loopback devices.

  Tests cover ENOSPC handling, GC pressure triggers, tier eviction,
  drive evacuation, and space reclamation against small real filesystems.

  Requires root privileges for loopback device creation.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Integration.{LoopbackDevice, PeerCluster}

  import NeonFS.Integration.ClusterCase

  @moduletag :loopback
  @moduletag :requires_root
  @moduletag timeout: 180_000

  @small_device_mb 20
  @medium_device_mb 50
  @chunk_size 50 * 1024

  setup do
    wait_for_clean_epmd()
    :ok
  end

  describe "ENOSPC handling" do
    test "write returns clean error when drive is full" do
      {:ok, device} = LoopbackDevice.create(size_mb: @small_device_mb)
      on_exit(fn -> LoopbackDevice.destroy(device) end)

      cluster =
        start_cluster_with_drives([
          %{id: "hot0", path: device.path, tier: :hot, capacity: @small_device_mb * 1024 * 1024}
        ])

      :ok = init_single_node_cluster(cluster, name: "enospc", volumes: [{"vol", %{}}])

      results = write_until_error(cluster, "vol", @chunk_size)
      {successes, failures} = Enum.split_with(results, &match?({:ok, _}, &1))

      assert successes != [], "Expected at least one successful write"
      assert failures != [], "Expected at least one failed write when drive is full"
    end

    test "ENOSPC error propagates as a clear error to the write path" do
      {:ok, device} = LoopbackDevice.create(size_mb: @small_device_mb)
      on_exit(fn -> LoopbackDevice.destroy(device) end)

      cluster =
        start_cluster_with_drives([
          %{id: "hot0", path: device.path, tier: :hot, capacity: @small_device_mb * 1024 * 1024}
        ])

      :ok = init_single_node_cluster(cluster, name: "enospc-prop", volumes: [{"vol", %{}}])

      # Fill the drive
      _results = write_until_error(cluster, "vol", @chunk_size)

      # Another write should fail cleanly (not crash)
      data = :crypto.strong_rand_bytes(@chunk_size)

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "vol",
          "/overflow.bin",
          data
        ])

      assert {:error, reason} = result
      assert reason != nil, "Error reason should not be nil"
    end
  end

  describe "GC pressure detection" do
    test "capacity threshold triggers GC when pressure detection is active" do
      {:ok, device} = LoopbackDevice.create(size_mb: @medium_device_mb)
      on_exit(fn -> LoopbackDevice.destroy(device) end)

      # Use small configured capacity so the pressure ratio exceeds 85% quickly.
      # The actual filesystem is 50 MB so we won't hit ENOSPC.
      drive_capacity = 1_000_000

      cluster =
        start_cluster_with_drives([
          %{id: "hot0", path: device.path, tier: :hot, capacity: drive_capacity}
        ])

      :ok = init_single_node_cluster(cluster, name: "gc-pressure", volumes: [{"vol", %{}}])

      # Write enough data to exceed 85% of 1 MB (>850 KB = ~18 chunks of 50 KB)
      write_data(cluster, "vol", 20, @chunk_size)

      # Poke the GCScheduler to run a pressure check immediately
      gc_pid =
        PeerCluster.rpc(cluster, :node1, Process, :whereis, [NeonFS.Core.GCScheduler])

      PeerCluster.rpc(cluster, :node1, :erlang, :send, [gc_pid, :pressure_check])

      # Verify a pressure-triggered GC job was created (polls the side-effect
      # rather than using a cross-node anonymous telemetry handler which fails
      # due to BEAM fun serialisation between nodes with different bytecode)
      assert_eventually timeout: 10_000 do
        jobs =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.JobTracker, :list, [
            [type: NeonFS.Core.Job.Runners.GarbageCollection]
          ])

        is_list(jobs) and jobs != []
      end
    end
  end

  describe "GC space reclamation" do
    @tag timeout: 180_000
    test "after GC runs on a full drive, space is reclaimed and writes succeed" do
      # Use a larger device than ENOSPC tests — ext4 overhead on very small
      # devices (20MB) leaves insufficient free space even after GC reclaims chunks.
      # Use large chunks (500KB) to fill the drive with fewer files (~100 rather than
      # ~1000) so the deletion loop completes within the test timeout.
      {:ok, device} = LoopbackDevice.create(size_mb: @medium_device_mb)
      on_exit(fn -> LoopbackDevice.destroy(device) end)

      large_chunk = 500 * 1024

      cluster =
        start_cluster_with_drives([
          %{id: "hot0", path: device.path, tier: :hot, capacity: @medium_device_mb * 1024 * 1024}
        ])

      :ok = init_single_node_cluster(cluster, name: "gc-reclaim", volumes: [{"vol", %{}}])

      # Write files until the drive is full
      results = write_until_error(cluster, "vol", large_chunk)
      successes = Enum.filter(results, &match?({:ok, _}, &1))
      file_count = length(successes)
      assert file_count > 0

      # Delete all files — chunks become orphaned but still occupy disk space
      for i <- 0..(file_count - 1) do
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :delete_file, [
          "vol",
          "/file_#{i}.bin"
        ])
      end

      # Run GC to reclaim orphaned chunks
      {:ok, gc_result} =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.GarbageCollector, :collect, [])

      assert gc_result.chunks_deleted > 0, "GC should have deleted orphaned chunks"

      # Writes should succeed again now that space is freed
      data = :crypto.strong_rand_bytes(large_chunk)

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          "vol",
          "/after_gc.bin",
          data
        ])

      assert {:ok, _} = result, "Write should succeed after GC reclaimed space"
    end
  end

  describe "tier eviction" do
    test "capacity threshold triggers tier eviction when TieringManager is active" do
      {:ok, hot_device} = LoopbackDevice.create(size_mb: @medium_device_mb)
      {:ok, warm_device} = LoopbackDevice.create(size_mb: @medium_device_mb)

      on_exit(fn ->
        LoopbackDevice.destroy(hot_device)
        LoopbackDevice.destroy(warm_device)
      end)

      # Hot drive has small configured capacity so writes quickly exceed the
      # 90% eviction threshold. Warm drive is large.
      # TieringManager uses one-step demotion (hot -> warm -> cold),
      # so the target must be one tier below hot.
      hot_capacity = 500_000
      warm_capacity = @medium_device_mb * 1024 * 1024

      cluster =
        start_cluster_with_drives([
          %{id: "hot0", path: hot_device.path, tier: :hot, capacity: hot_capacity},
          %{id: "warm0", path: warm_device.path, tier: :warm, capacity: warm_capacity}
        ])

      :ok =
        init_single_node_cluster(cluster,
          name: "tier-evict",
          volumes: [
            {"vol",
             %{
               tiering: %{
                 initial_tier: :hot,
                 promotion_threshold: 10,
                 demotion_delay: 86_400
               }
             }}
          ]
        )

      # Write enough data to hot tier to exceed 90% of 500 KB (>450 KB = ~10 chunks)
      write_data(cluster, "vol", 12, @chunk_size)

      node_atom = PeerCluster.get_node!(cluster, :node1).node

      # Trigger TieringManager evaluation
      PeerCluster.rpc(cluster, :node1, NeonFS.Core.TieringManager, :evaluate_now, [])

      # Wait for the BackgroundWorker to migrate chunks to warm tier
      assert_eventually timeout: 30_000 do
        chunks =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :list_by_drive, [
            node_atom,
            "warm0"
          ])

        is_list(chunks) and chunks != []
      end
    end
  end

  describe "drive evacuation" do
    test "evacuation moves data to another drive and all files remain readable" do
      {:ok, drive_a} = LoopbackDevice.create(size_mb: @medium_device_mb)
      {:ok, drive_b} = LoopbackDevice.create(size_mb: @medium_device_mb)

      on_exit(fn ->
        LoopbackDevice.destroy(drive_a)
        LoopbackDevice.destroy(drive_b)
      end)

      drive_capacity = @medium_device_mb * 1024 * 1024

      cluster =
        start_cluster_with_drives([
          %{id: "drive_a", path: drive_a.path, tier: :hot, capacity: drive_capacity},
          %{id: "drive_b", path: drive_b.path, tier: :hot, capacity: drive_capacity}
        ])

      :ok = init_single_node_cluster(cluster, name: "evac", volumes: [{"vol", %{}}])

      # Write several files (they distribute across the two drives)
      file_data =
        for i <- 0..4 do
          data = :crypto.strong_rand_bytes(@chunk_size)
          path = "/file_#{i}.bin"

          {:ok, _} =
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
              "vol",
              path,
              data
            ])

          {path, data}
        end

      node_atom = PeerCluster.get_node!(cluster, :node1).node

      # Start evacuation of drive_a
      {:ok, _job} =
        PeerCluster.rpc(
          cluster,
          :node1,
          NeonFS.Core.DriveEvacuation,
          :start_evacuation,
          [node_atom, "drive_a", [any_tier: true]]
        )

      # Wait for evacuation to complete (120s for slow CI runners)
      assert_eventually timeout: 120_000 do
        case PeerCluster.rpc(
               cluster,
               :node1,
               NeonFS.Core.DriveEvacuation,
               :evacuation_status,
               ["drive_a"]
             ) do
          {:ok, %{status: :completed}} -> true
          _ -> false
        end
      end

      # Verify drive_a has no chunks
      chunks_a =
        PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :list_by_drive, [
          node_atom,
          "drive_a"
        ])

      assert chunks_a == [], "Drive A should have no chunks after evacuation"

      # Verify all files are still readable with correct content
      for {path, original_data} <- file_data do
        {:ok, read_data} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
            "vol",
            path
          ])

        assert read_data == original_data, "#{path} should be readable after evacuation"
      end
    end
  end

  # ─── Private helpers ──────────────────────────────────────

  defp start_cluster_with_drives(drives_config) do
    cluster =
      PeerCluster.start_cluster!(1,
        applications: [:neonfs_core],
        drives: fn _node, _data_dir -> drives_config end
      )

    PeerCluster.connect_nodes(cluster)
    :global.sync()

    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    cluster
  end

  defp write_until_error(cluster, volume_name, chunk_size) do
    Enum.reduce_while(0..999, [], fn i, acc ->
      data = :crypto.strong_rand_bytes(chunk_size)
      path = "/file_#{i}.bin"

      result =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
          volume_name,
          path,
          data
        ])

      case result do
        {:ok, _} -> {:cont, [{:ok, path} | acc]}
        {:error, _} = error -> {:halt, [error | acc]}
      end
    end)
    |> Enum.reverse()
  end

  defp write_data(cluster, volume_name, count, chunk_size) do
    for i <- 0..(count - 1) do
      data = :crypto.strong_rand_bytes(chunk_size)

      PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file, [
        volume_name,
        "/file_#{i}.bin",
        data
      ])
    end
  end

  defp wait_for_clean_epmd do
    wait_until(&no_stale_epmd_nodes?/0, timeout: 15_000, interval: 200)
  end

  defp no_stale_epmd_nodes? do
    case :erl_epmd.names() do
      {:ok, names} -> not Enum.any?(names, &stale_node_entry?/1)
      {:error, _} -> true
    end
  end

  defp stale_node_entry?({name, _port}),
    do: String.starts_with?(to_string(name), "node")
end
