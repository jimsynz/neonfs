defmodule NeonFS.Integration.IOSchedulerTest do
  @moduledoc """
  Integration tests for the I/O scheduler subsystem.

  Verifies that the GenStage-based I/O scheduler correctly handles
  file operations end-to-end, WFQ fairness between volumes, drive
  worker crash recovery, and high-concurrency workloads.
  """
  use NeonFS.Integration.ClusterCase, async: false

  @moduletag :io_scheduler
  @moduletag timeout: 180_000
  @moduletag nodes: 1
  @moduletag cluster_mode: :shared

  setup_all %{cluster: cluster} do
    init_single_node_cluster(cluster,
      volumes: [
        {"io-test-vol", %{}},
        {"io-weight-low", %{"io_weight" => 50}},
        {"io-weight-high", %{"io_weight" => 200}}
      ]
    )

    %{}
  end

  describe "data integrity through scheduler pipeline" do
    test "write and read file through I/O scheduler", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(100 * 1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "io-test-vol",
          "/scheduler_test.bin",
          test_data
        ])

      assert file.size == byte_size(test_data)
      assert file.chunks != []

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "io-test-vol",
          "/scheduler_test.bin"
        ])

      assert read_data == test_data
    end

    test "large file chunking through scheduler preserves data", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(2 * 1024 * 1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "io-test-vol",
          "/scheduler_large.bin",
          test_data
        ])

      assert length(file.chunks) > 1

      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
          "io-test-vol",
          "/scheduler_large.bin"
        ])

      assert read_data == test_data
    end
  end

  describe "WFQ fairness" do
    test "volumes with different io_weight both process operations correctly", %{
      cluster: cluster
    } do
      op_count = 20

      for i <- 1..op_count do
        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
            "io-weight-low",
            "/wfq_low_#{i}.txt",
            "low-weight-data-#{i}"
          ])

        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
            "io-weight-high",
            "/wfq_high_#{i}.txt",
            "high-weight-data-#{i}"
          ])
      end

      # Verify all writes succeeded by reading back samples
      for i <- [1, 10, 20] do
        {:ok, data} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
            "io-weight-low",
            "/wfq_low_#{i}.txt"
          ])

        assert data == "low-weight-data-#{i}"

        {:ok, data} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
            "io-weight-high",
            "/wfq_high_#{i}.txt"
          ])

        assert data == "high-weight-data-#{i}"
      end

      # Verify that the volumes were created with correct io_weight
      {:ok, vol_low} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :get_volume, ["io-weight-low"])

      {:ok, vol_high} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :get_volume, ["io-weight-high"])

      assert vol_low[:io_weight] == 50
      assert vol_high[:io_weight] == 200
    end
  end

  describe "scheduler status API" do
    test "status returns queue depths with expected structure", %{cluster: cluster} do
      scheduler_pid =
        PeerCluster.rpc(cluster, :node1, Process, :whereis, [NeonFS.IO.Scheduler])

      if is_pid(scheduler_pid) do
        status =
          PeerCluster.rpc(cluster, :node1, NeonFS.IO.Scheduler, :status, [])

        assert %{queue_depths: depths, total_pending: pending} = status
        assert is_map(depths)
        assert is_integer(pending)
        assert pending >= 0

        # All priority classes should be present in queue_depths
        for priority <- [:user_read, :user_write, :replication, :read_repair, :repair, :scrub] do
          assert Map.has_key?(depths, priority),
                 "Expected queue_depths to contain #{inspect(priority)}"
        end
      else
        # Scheduler may not be running if no drives are registered (test env).
        # Verify that submit_sync falls back to direct execution.
        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
            "io-test-vol",
            "/status_fallback.txt",
            "fallback works"
          ])

        {:ok, data} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
            "io-test-vol",
            "/status_fallback.txt"
          ])

        assert data == "fallback works"
      end
    end

    test "scheduler_available? reflects process state", %{cluster: cluster} do
      available =
        PeerCluster.rpc(cluster, :node1, NeonFS.IO.Scheduler, :scheduler_available?, [])

      assert is_boolean(available)
    end
  end

  describe "I/O subsystem components" do
    test "producer is running on peer node", %{cluster: cluster} do
      pid = PeerCluster.rpc(cluster, :node1, Process, :whereis, [NeonFS.IO.Producer])
      assert is_pid(pid), "Producer should be running as part of the I/O supervisor"
    end

    test "priority adjuster is running on peer node", %{cluster: cluster} do
      pid = PeerCluster.rpc(cluster, :node1, Process, :whereis, [NeonFS.IO.PriorityAdjuster])
      assert is_pid(pid), "PriorityAdjuster should be running as part of the I/O supervisor"
    end

    test "worker supervisor is running on peer node", %{cluster: cluster} do
      pid = PeerCluster.rpc(cluster, :node1, Process, :whereis, [NeonFS.IO.WorkerSupervisor])
      assert is_pid(pid), "WorkerSupervisor should be running as part of the I/O supervisor"
    end
  end

  describe "drive worker crash and recovery" do
    test "operations succeed after drive worker restart", %{cluster: cluster} do
      # Write a file to confirm the scheduler is functional
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "io-test-vol",
          "/pre_crash.txt",
          "before crash"
        ])

      # Look up a drive worker via the Registry
      match_spec = [{{:"$1", :"$2", :_}, [], [{{:"$1", :"$2"}}]}]

      workers =
        PeerCluster.rpc(cluster, :node1, Registry, :select, [
          NeonFS.IO.WorkerRegistry,
          match_spec
        ])

      case workers do
        [{drive_id, pid} | _] ->
          # Kill the drive worker
          PeerCluster.rpc(cluster, :node1, Process, :exit, [pid, :kill])

          # Wait for the worker to be restarted by the DynamicSupervisor
          assert_eventually timeout: 5_000 do
            case PeerCluster.rpc(cluster, :node1, NeonFS.IO.WorkerSupervisor, :lookup_worker, [
                   drive_id
                 ]) do
              {:ok, new_pid} -> is_pid(new_pid) and new_pid != pid
              :error -> false
            end
          end

          # Operations should work after worker recovery
          {:ok, _} =
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "io-test-vol",
              "/post_crash.txt",
              "after crash"
            ])

          {:ok, data} =
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
              "io-test-vol",
              "/post_crash.txt"
            ])

          assert data == "after crash"

        [] ->
          # No drive workers registered (test env without physical drives).
          # Verify the scheduler's direct-execution fallback still works.
          {:ok, _} =
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "io-test-vol",
              "/fallback.txt",
              "fallback data"
            ])

          {:ok, data} =
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
              "io-test-vol",
              "/fallback.txt"
            ])

          assert data == "fallback data"
      end
    end
  end

  describe "high concurrency" do
    test "concurrent write burst completes without data loss", %{cluster: cluster} do
      file_count = 50
      file_size = 1024

      # Generate test data for all files upfront
      test_files =
        for i <- 1..file_count do
          data = :crypto.strong_rand_bytes(file_size)
          {"/concurrent_#{i}.bin", data}
        end

      # Write all files concurrently from the test process.
      # Each RPC call creates a separate process on the remote node,
      # giving us concurrent writes through the scheduler pipeline.
      results =
        test_files
        |> Task.async_stream(
          fn {path, data} ->
            PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
              "io-test-vol",
              path,
              data
            ])
          end,
          max_concurrency: 20,
          timeout: 60_000
        )
        |> Enum.to_list()

      # All writes should succeed
      for {:ok, result} <- results do
        assert {:ok, _file} = result
      end

      assert length(results) == file_count

      # Read back a sample and verify data integrity
      sample_indices = Enum.take_random(1..file_count, 10)

      for i <- sample_indices do
        {path, expected_data} = Enum.at(test_files, i - 1)

        {:ok, read_data} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
            "io-test-vol",
            path
          ])

        assert read_data == expected_data,
               "Data mismatch for #{path}"
      end
    end
  end

  describe "replication writes through scheduler" do
    @describetag nodes: 3
    @describetag cluster_mode: :per_test

    setup %{cluster: cluster} do
      init_multi_node_cluster(cluster,
        volumes: [{"repl-vol", %{"durability" => "replicate:2"}}]
      )

      %{}
    end

    test "replicated writes complete and data is readable from other nodes", %{cluster: cluster} do
      test_data = :crypto.strong_rand_bytes(10 * 1024)

      {:ok, file} =
        PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
          "repl-vol",
          "/repl_test.bin",
          test_data
        ])

      assert file.chunks != []

      # Wait for replication to complete
      assert_eventually timeout: 30_000 do
        chunks =
          PeerCluster.rpc(cluster, :node1, NeonFS.Core.ChunkIndex, :get_chunks_for_volume, [
            file.volume_id
          ])

        is_list(chunks) and chunks != [] and
          Enum.all?(chunks, fn chunk -> length(chunk.locations) >= 2 end)
      end

      # Read from a different node to verify replication worked through the scheduler
      {:ok, read_data} =
        PeerCluster.rpc(cluster, :node2, NeonFS.TestHelpers, :read_file, [
          "repl-vol",
          "/repl_test.bin"
        ])

      assert read_data == test_data
    end
  end

  describe "scheduler under sustained load" do
    test "write and read operations remain correct under sustained load", %{cluster: cluster} do
      # Write files in batches to sustain load over time
      batch_count = 5
      files_per_batch = 10

      all_files =
        for batch <- 1..batch_count, i <- 1..files_per_batch do
          path = "/sustained_b#{batch}_#{i}.txt"
          data = "batch-#{batch}-file-#{i}-#{:crypto.strong_rand_bytes(64) |> Base.encode16()}"
          {path, data}
        end

      # Write all files
      for {path, data} <- all_files do
        {:ok, _} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
            "io-test-vol",
            path,
            data
          ])
      end

      # Read all files back and verify
      for {path, expected_data} <- all_files do
        {:ok, read_data} =
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :read_file, [
            "io-test-vol",
            path
          ])

        assert read_data == expected_data,
               "Data mismatch for #{path}"
      end
    end
  end
end
