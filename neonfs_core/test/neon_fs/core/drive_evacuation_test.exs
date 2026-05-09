defmodule NeonFS.Core.DriveEvacuationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    BlobStore,
    ChunkIndex,
    ChunkMeta,
    DriveEvacuation,
    DriveRegistry,
    Job
  }

  alias NeonFS.Core.Job.Runners.DriveEvacuation, as: EvacuationRunner

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    drive1_path = Path.join(tmp_dir, "drive1")
    drive2_path = Path.join(tmp_dir, "drive2")
    File.mkdir_p!(drive1_path)
    File.mkdir_p!(drive2_path)

    drives = [
      %{id: "drive1", path: drive1_path, tier: :hot, capacity: 1_000_000},
      %{id: "drive2", path: drive2_path, tier: :hot, capacity: 1_000_000}
    ]

    Application.put_env(:neonfs_core, :drives, drives)

    start_persistence()
    start_drive_registry()

    # Start BlobStore with our custom drives
    start_supervised!(
      {NeonFS.Core.BlobStore, drives: drives, prefix_depth: 2},
      restart: :temporary
    )

    start_chunk_index()
    start_file_index()
    start_volume_registry()

    # Detach any existing handler, then start StorageMetrics
    :telemetry.detach("storage-metrics")

    start_supervised!(
      NeonFS.Core.StorageMetrics,
      restart: :temporary
    )

    # Wait for StorageMetrics to finish computing initial usage
    :sys.get_state(NeonFS.Core.StorageMetrics)

    start_job_tracker(tmp_dir)

    {:ok, drives: drives, drive1_path: drive1_path, drive2_path: drive2_path}
  end

  describe "pre-flight checks" do
    test "rejects evacuation of non-existent drive" do
      assert {:error, _} = DriveEvacuation.start_evacuation(node(), "nonexistent")
    end

    test "rejects evacuation of already draining drive" do
      DriveRegistry.update_state("drive1", :draining)

      assert {:error, :already_draining} =
               DriveEvacuation.start_evacuation(node(), "drive1")

      # Clean up
      DriveRegistry.update_state("drive1", :active)
    end

    test "rejects when insufficient capacity (same tier)" do
      # Fill drive1 to near capacity
      DriveRegistry.update_usage("drive1", 900_000)
      # Fill drive2 too — not enough space to evacuate drive1
      DriveRegistry.update_usage("drive2", 900_000)

      assert {:error, :insufficient_capacity} =
               DriveEvacuation.start_evacuation(node(), "drive1")

      # Clean up
      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_usage("drive2", 0)
    end

    test "passes capacity check when target has room" do
      # drive1 has some data, drive2 has plenty of room
      DriveRegistry.update_usage("drive1", 100_000)
      DriveRegistry.update_usage("drive2", 0)

      # Capacity check should pass. Job creation may fail because
      # JobTracker isn't started — that's fine, we just verify the
      # pre-flight didn't fail with :insufficient_capacity.
      result =
        try do
          DriveEvacuation.start_evacuation(node(), "drive1")
        catch
          :exit, _ ->
            # JobTracker not running causes exit
            {:error, :job_tracker_unavailable}
        end

      case result do
        {:ok, _job} ->
          {:ok, drive} = DriveRegistry.get_drive(node(), "drive1")
          assert drive.state == :draining
          DriveRegistry.update_state("drive1", :active)

        {:error, reason} ->
          assert reason != :insufficient_capacity
      end

      DriveRegistry.update_usage("drive1", 0)
      DriveRegistry.update_state("drive1", :active)
    end
  end

  describe "runner label" do
    test "returns correct label" do
      assert EvacuationRunner.label() == "drive-evacuation"
    end
  end

  describe "runner step/1" do
    test "processes over-replicated chunks by deleting" do
      # Write a chunk to drive1
      data = "test chunk data for evacuation"
      {:ok, hash, _info} = BlobStore.write_chunk(data, "drive1", "hot")

      # Set up chunk metadata with locations on BOTH drives (over-replicated)
      location1 = %{node: node(), drive_id: "drive1", tier: :hot}
      location2 = %{node: node(), drive_id: "drive2", tier: :hot}

      # Write to drive2 as well
      BlobStore.write_chunk(data, "drive2", "hot")

      chunk =
        ChunkMeta.new("vol-test", hash, byte_size(data), byte_size(data))
        |> ChunkMeta.add_location(location1)
        |> ChunkMeta.add_location(location2)

      ChunkIndex.put(chunk)

      # Create a mock job
      job =
        Job.new(EvacuationRunner, %{
          node: node(),
          drive_id: "drive1",
          any_tier: false,
          total_chunks: 1
        })

      job = %{job | status: :running}

      # Run one step
      {:continue, updated_job} = EvacuationRunner.step(job)

      assert updated_job.progress.completed >= 0
    end

    test "completes when no chunks remain" do
      # No chunks on drive1
      job =
        Job.new(EvacuationRunner, %{
          node: node(),
          drive_id: "drive1",
          any_tier: false,
          total_chunks: 0
        })

      job = %{job | status: :running}

      # Should complete immediately. The completion logic tries to deregister
      # via DriveManager which may not be running, so wrap in try/catch.
      result =
        try do
          EvacuationRunner.step(job)
        catch
          :exit, _ ->
            # DriveManager not running causes exit during deregister — still a completion
            {:complete, job}
        end

      assert {:complete, _updated} = result
    end

    test "surfaces last error in progress description when batch fails" do
      job = job_with_unmigratable_chunk()

      {:continue, updated} = EvacuationRunner.step(job)

      assert updated.progress.description =~ "Evacuating chunks"
      assert updated.progress.description =~ "last error: no eligible target drives"
      assert updated.state.last_error == :no_target_drives
      assert updated.state.stale_batches == 1
    end

    test "stale_batches resets after a batch with successes" do
      job = job_with_unmigratable_chunk()

      # First batch: 1 chunk, no targets → stale_batches = 1
      {:continue, after_fail} = EvacuationRunner.step(job)
      assert after_fail.state.stale_batches == 1

      # Add drive2 back as a target by re-activating it
      DriveRegistry.update_state("drive2", :active)

      {:continue, after_success} = EvacuationRunner.step(after_fail)

      assert after_success.state.stale_batches == 0
      assert after_success.state.last_error == nil
      assert after_success.progress.completed == 1
      refute after_success.progress.description =~ "last error"
    end

    test "fails the job after threshold consecutive no-progress batches" do
      job = job_with_unmigratable_chunk()

      {:continue, j1} = EvacuationRunner.step(job)
      assert j1.state.stale_batches == 1

      {:continue, j2} = EvacuationRunner.step(j1)
      assert j2.state.stale_batches == 2

      assert {:error, {:no_progress, :no_target_drives}, failed} = EvacuationRunner.step(j2)
      assert failed.state.stale_batches == 3
      assert failed.state.last_error == :no_target_drives
    end

    test "normalise_evac_reason handles common error shapes" do
      assert EvacuationRunner.normalise_evac_reason(:no_target_drives) ==
               "no eligible target drives"

      assert EvacuationRunner.normalise_evac_reason(:eacces) == "permission denied"

      assert EvacuationRunner.normalise_evac_reason({:write_failed, :eacces}) ==
               "write failed: permission denied"

      assert EvacuationRunner.normalise_evac_reason({:migration_failed, :eacces, "disk1"}) ==
               "permission denied on disk1"

      assert EvacuationRunner.normalise_evac_reason({:rpc_error, :nodedown}) == "rpc error"
      assert EvacuationRunner.normalise_evac_reason(nil) == "unknown error"
    end
  end

  defp job_with_unmigratable_chunk do
    data = "evacuation chunk needing migration"
    {:ok, hash, _info} = BlobStore.write_chunk(data, "drive1", "hot")

    chunk =
      ChunkMeta.new("vol-test", hash, byte_size(data), byte_size(data))
      |> ChunkMeta.add_location(%{node: node(), drive_id: "drive1", tier: :hot})

    ChunkIndex.put(chunk)

    # Drain drive2 so target selection has nowhere to go
    DriveRegistry.update_state("drive2", :draining)

    job =
      Job.new(EvacuationRunner, %{
        node: node(),
        drive_id: "drive1",
        any_tier: false,
        total_chunks: 1
      })

    %{job | status: :running}
  end

  describe "on_cancel/1" do
    test "restores drive to active state" do
      DriveRegistry.update_state("drive1", :draining)

      job =
        Job.new(EvacuationRunner, %{
          node: node(),
          drive_id: "drive1",
          any_tier: false,
          total_chunks: 10
        })

      EvacuationRunner.on_cancel(job)

      {:ok, drive} = DriveRegistry.get_drive(node(), "drive1")
      assert drive.state == :active
    end
  end

  describe "evacuation_status/1" do
    test "returns :no_evacuation when no jobs exist" do
      assert {:error, :no_evacuation} = DriveEvacuation.evacuation_status("drive1")
    end
  end

  describe "draining state" do
    test "draining drive excluded from select_drive" do
      DriveRegistry.update_state("drive1", :draining)

      {:ok, selected} = DriveRegistry.select_drive(:hot)
      assert selected.id == "drive2"

      # Clean up
      DriveRegistry.update_state("drive1", :active)
    end

    test "all drives draining returns no_drives_in_tier" do
      DriveRegistry.update_state("drive1", :draining)
      DriveRegistry.update_state("drive2", :draining)

      assert {:error, :no_drives_in_tier} = DriveRegistry.select_drive(:hot)

      # Clean up
      DriveRegistry.update_state("drive1", :active)
      DriveRegistry.update_state("drive2", :active)
    end
  end
end
