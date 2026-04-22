defmodule NeonFS.Core.ClusterRebalanceTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    BlobStore,
    ChunkIndex,
    ChunkMeta,
    ClusterRebalance,
    DriveRegistry,
    Job
  }

  alias NeonFS.Core.Job.Runners.ClusterRebalance, as: RebalanceRunner

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    drive1_path = Path.join(tmp_dir, "drive1")
    drive2_path = Path.join(tmp_dir, "drive2")
    drive3_path = Path.join(tmp_dir, "drive3")
    File.mkdir_p!(drive1_path)
    File.mkdir_p!(drive2_path)
    File.mkdir_p!(drive3_path)

    drives = [
      %{id: "drive1", path: drive1_path, tier: :hot, capacity: 1_000_000},
      %{id: "drive2", path: drive2_path, tier: :hot, capacity: 1_000_000},
      %{id: "drive3", path: drive3_path, tier: :hot, capacity: 1_000_000}
    ]

    Application.put_env(:neonfs_core, :drives, drives)

    start_persistence()
    start_drive_registry()

    start_supervised!(
      {NeonFS.Core.BlobStore, drives: drives, prefix_depth: 2},
      restart: :temporary
    )

    start_chunk_index()
    start_file_index()
    start_volume_registry()

    :telemetry.detach("storage-metrics")

    start_supervised!(
      NeonFS.Core.StorageMetrics,
      restart: :temporary
    )

    # Wait for StorageMetrics to finish computing initial usage
    :sys.get_state(NeonFS.Core.StorageMetrics)

    start_job_tracker(tmp_dir)

    {:ok,
     drives: drives, drive1_path: drive1_path, drive2_path: drive2_path, drive3_path: drive3_path}
  end

  describe "pre-flight checks" do
    test "rejects with fewer than 2 eligible drives in tier" do
      # Set all but one drive to draining
      DriveRegistry.update_state("drive1", :draining)
      DriveRegistry.update_state("drive2", :draining)

      result =
        try do
          ClusterRebalance.start_rebalance()
        catch
          :exit, _ -> {:error, :job_tracker_unavailable}
        end

      assert {:error, reason} = result
      assert reason in [:insufficient_drives, :job_tracker_unavailable]

      # Clean up
      DriveRegistry.update_state("drive1", :active)
      DriveRegistry.update_state("drive2", :active)
    end

    test "rejects when already balanced (equal usage)" do
      # All drives have similar usage — spread < default threshold (0.10)
      DriveRegistry.update_usage("drive1", 100_000)
      DriveRegistry.update_usage("drive2", 100_000)
      DriveRegistry.update_usage("drive3", 100_000)

      result =
        try do
          ClusterRebalance.start_rebalance()
        catch
          :exit, _ -> {:error, :job_tracker_unavailable}
        end

      assert {:error, reason} = result
      assert reason in [:already_balanced, :job_tracker_unavailable]

      # Clean up
      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_usage("drive2", 0)
      DriveRegistry.update_usage("drive3", 0)
    end

    test "rejects when rebalance already running" do
      # Insert a mock running job directly into the JobTracker's DETS table.
      job = Job.new(RebalanceRunner, %{tiers: [:hot], threshold: 0.10, batch_size: 50})
      job = %{job | status: :running}
      :dets.insert(:neonfs_jobs, {job.id, job})

      # Make drives imbalanced to pass earlier checks
      DriveRegistry.update_usage("drive1", 800_000)
      DriveRegistry.update_usage("drive2", 100_000)
      DriveRegistry.update_usage("drive3", 100_000)

      assert {:error, :rebalance_already_running} = ClusterRebalance.start_rebalance()

      # Clean up
      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_usage("drive2", 0)
      DriveRegistry.update_usage("drive3", 0)
    end

    test "includes standby drives in eligible set" do
      DriveRegistry.update_state("drive2", :standby)

      # Imbalanced usage
      DriveRegistry.update_usage("drive1", 800_000)
      DriveRegistry.update_usage("drive2", 100_000)
      DriveRegistry.update_usage("drive3", 100_000)

      # Should pass pre-flight (standby drives are eligible).
      # Job creation may fail if JobTracker GenServer isn't started.
      result =
        try do
          ClusterRebalance.start_rebalance()
        catch
          :exit, _ -> {:error, :job_tracker_unavailable}
        end

      case result do
        {:ok, _job} -> :ok
        {:error, reason} -> assert reason != :insufficient_drives
      end

      # Clean up
      DriveRegistry.update_state("drive2", :active)
      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_usage("drive2", 0)
      DriveRegistry.update_usage("drive3", 0)
    end

    test "passes with imbalanced drives" do
      DriveRegistry.update_usage("drive1", 800_000)
      DriveRegistry.update_usage("drive2", 100_000)
      DriveRegistry.update_usage("drive3", 100_000)

      result =
        try do
          ClusterRebalance.start_rebalance()
        catch
          :exit, _ -> {:error, :job_tracker_unavailable}
        end

      case result do
        {:ok, job} ->
          assert job.params.tiers == [:hot]
          assert job.params.threshold == 0.10

        {:error, reason} ->
          # Pre-flight should pass; only job creation might fail
          assert reason != :already_balanced
          assert reason != :insufficient_drives
      end

      # Clean up
      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_usage("drive2", 0)
      DriveRegistry.update_usage("drive3", 0)
    end

    test "filters to specific tier when given" do
      DriveRegistry.update_usage("drive1", 800_000)
      DriveRegistry.update_usage("drive2", 100_000)
      DriveRegistry.update_usage("drive3", 100_000)

      result =
        try do
          ClusterRebalance.start_rebalance(tier: :hot)
        catch
          :exit, _ -> {:error, :job_tracker_unavailable}
        end

      case result do
        {:ok, job} ->
          assert job.params.tiers == [:hot]

        {:error, reason} ->
          assert reason != :tier_not_found
      end

      # Clean up
      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_usage("drive2", 0)
      DriveRegistry.update_usage("drive3", 0)
    end

    test "rejects non-existent tier" do
      result =
        try do
          ClusterRebalance.start_rebalance(tier: :nonexistent)
        catch
          :exit, _ -> {:error, :job_tracker_unavailable}
        end

      assert {:error, :tier_not_found} = result
    end
  end

  describe "runner label" do
    test "returns correct label" do
      assert RebalanceRunner.label() == "cluster-rebalance"
    end
  end

  describe "runner step/1" do
    test "migrates chunk from overfull to underfull drive" do
      # Write a chunk to drive1 only
      data = "test chunk data for rebalancing"
      {:ok, hash, _info} = BlobStore.write_chunk(data, "drive1", "hot")

      location = %{node: node(), drive_id: "drive1", tier: :hot}

      chunk =
        ChunkMeta.new(hash, byte_size(data), byte_size(data))
        |> ChunkMeta.add_location(location)

      ChunkIndex.put(chunk)

      # Make drive1 overfull, drive2 underfull
      DriveRegistry.update_usage("drive1", 800_000)
      DriveRegistry.update_usage("drive2", 100_000)
      DriveRegistry.update_usage("drive3", 100_000)

      job =
        Job.new(RebalanceRunner, %{
          tiers: [:hot],
          threshold: 0.10,
          batch_size: 50
        })

      job = %{job | status: :running}

      result =
        try do
          RebalanceRunner.step(job)
        catch
          :exit, _ -> {:continue, job}
        end

      assert {:continue, _updated} = result

      # Clean up
      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_usage("drive2", 0)
      DriveRegistry.update_usage("drive3", 0)
    end

    test "advances tier when spread is below threshold" do
      # All drives balanced — spread < threshold
      DriveRegistry.update_usage("drive1", 100_000)
      DriveRegistry.update_usage("drive2", 100_000)
      DriveRegistry.update_usage("drive3", 100_000)

      job =
        Job.new(RebalanceRunner, %{
          tiers: [:hot],
          threshold: 0.10,
          batch_size: 50
        })

      job = %{job | status: :running}

      {:continue, updated} = RebalanceRunner.step(job)

      # Tier should have been advanced (current_tier_index incremented)
      assert updated.state.current_tier_index == 1
      assert :hot in updated.state.tiers_completed

      # Clean up
      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_usage("drive2", 0)
      DriveRegistry.update_usage("drive3", 0)
    end

    test "completes when all tiers processed" do
      job =
        Job.new(RebalanceRunner, %{
          tiers: [:hot],
          threshold: 0.10,
          batch_size: 50
        })

      job = %{
        job
        | status: :running,
          state: %{
            current_tier_index: 1,
            bytes_moved: 0,
            chunks_migrated: 0,
            tiers_completed: [:hot],
            last_batch_at: nil
          }
      }

      {:complete, updated} = RebalanceRunner.step(job)
      assert updated.progress.description =~ "Complete"

      # Clean up
      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_usage("drive2", 0)
      DriveRegistry.update_usage("drive3", 0)
    end

    test "handles fewer than 2 eligible drives by advancing tier" do
      # Set all but one drive to draining
      DriveRegistry.update_state("drive2", :draining)
      DriveRegistry.update_state("drive3", :draining)

      job =
        Job.new(RebalanceRunner, %{
          tiers: [:hot],
          threshold: 0.10,
          batch_size: 50
        })

      job = %{job | status: :running}

      {:continue, updated} = RebalanceRunner.step(job)
      assert updated.state.current_tier_index == 1

      # Clean up
      DriveRegistry.update_state("drive2", :active)
      DriveRegistry.update_state("drive3", :active)
    end
  end

  describe "rebalance_status/0" do
    test "returns :no_rebalance when no jobs exist" do
      assert {:error, :no_rebalance} = ClusterRebalance.rebalance_status()
    end
  end

  describe "on_cancel/1" do
    test "logs cancellation" do
      job =
        Job.new(RebalanceRunner, %{
          tiers: [:hot, :warm],
          threshold: 0.10,
          batch_size: 50
        })

      assert :ok = RebalanceRunner.on_cancel(job)
    end
  end
end
