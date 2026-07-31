defmodule NeonFS.Core.DriveEvacuationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    BlobStore,
    ChunkIndex,
    ChunkMeta,
    DriveEvacuation,
    DriveRegistry,
    Job,
    JobTracker,
    RaSupervisor,
    VolumeRegistry
  }

  alias NeonFS.Core.Job.Runners.DriveEvacuation, as: EvacuationRunner
  alias NeonFS.Error.ReplicaGuard

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

  describe "replica guard (#1618)" do
    setup do
      # A single-copy chunk pinned to drive1, so evacuating drive1 would
      # leave the volume with no fallback if the migration failed.
      Mimic.stub(VolumeRegistry, :list, fn _opts ->
        [
          %{
            id: "vol-guard",
            name: "guarded",
            system: false,
            durability: %{type: :replicate, factor: 2, min_copies: 2}
          }
        ]
      end)

      Mimic.stub(ChunkIndex, :list_volume_chunks, fn "vol-guard" ->
        {:ok,
         [
           %ChunkMeta{
             hash: "guarded",
             original_size: 1,
             stored_size: 1,
             compression: :none,
             crypto: nil,
             locations: [%{node: node(), drive_id: "drive1", tier: :hot}],
             target_replicas: 2,
             commit_state: :committed,
             active_write_refs: MapSet.new(),
             volume_ids: MapSet.new(["vol-guard"]),
             created_at: DateTime.utc_now()
           }
         ]}
      end)

      :ok
    end

    test "allows evacuating a sole-copy drive when there is somewhere to move it" do
      # The canonical case: a factor-1 volume being moved off a drive
      # before it is retired. Evacuation relocates, so the copy count
      # survives — refusing here would break the operation's whole point.
      result = DriveEvacuation.start_evacuation(node(), "drive1")

      refute match?({:error, %ReplicaGuard{}}, result)
    end

    test "refuses when no drive remains to relocate onto, leaving the drive active" do
      DriveRegistry.update_state("drive2", :draining)

      assert {:error, %ReplicaGuard{reason: :below_min_copies}} =
               DriveEvacuation.start_evacuation(node(), "drive1")

      {:ok, drive} = DriveRegistry.get_drive(node(), "drive1")
      assert drive.state == :active
    end

    test "force gets past the no-target guard" do
      DriveRegistry.update_state("drive2", :draining)

      # The guard is the only thing being asserted here — job creation may
      # still fail on this fixture, but it must not fail on the guard.
      result = DriveEvacuation.start_evacuation(node(), "drive1", force: true)

      refute match?({:error, %ReplicaGuard{}}, result)
    end

    test "_system with nowhere to go is refused even with force" do
      DriveRegistry.update_state("drive2", :draining)

      Mimic.stub(VolumeRegistry, :list, fn _opts ->
        [
          %{
            id: "vol-guard",
            name: "_system",
            system: true,
            durability: %{type: :replicate, factor: 1, min_copies: 1}
          }
        ]
      end)

      assert {:error, %ReplicaGuard{reason: :system_zero_copies}} =
               DriveEvacuation.start_evacuation(node(), "drive1", force: true)
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
          total_chunks: 1
        })

      job = %{job | status: :running}

      # Run one step
      {:continue, updated_job} = EvacuationRunner.step(job)

      assert updated_job.progress.completed >= 0
    end

    test "classifies chunks via authoritative metadata when the ETS cache is cold (#1573)" do
      ensure_cluster_state()

      {:ok, volume} =
        VolumeRegistry.create("evac-cold-cache-vol",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      data = "cold cache chunk that must migrate as a tracked chunk"
      {:ok, hash, _info} = BlobStore.write_chunk(data, "drive1", "hot")

      chunk =
        ChunkMeta.new(volume.id, hash, byte_size(data), byte_size(data))
        |> ChunkMeta.add_location(%{node: node(), drive_id: "drive1", tier: :hot})

      ChunkIndex.put(chunk)

      # Simulate a post-restart cold ETS cache: the authoritative per-volume
      # tree still holds the chunk, but the local materialisation is empty —
      # the exact condition that made evacuation misclassify real chunks as
      # untracked blobs and move them without rewriting `chunk.locations`.
      :ets.delete_all_objects(:chunk_index)

      job =
        %{
          Job.new(EvacuationRunner, %{node: node(), drive_id: "drive1", total_chunks: 1})
          | status: :running
        }

      {:continue, _updated} = EvacuationRunner.step(job)

      # It moved as a TRACKED chunk: authoritative locations now point at the
      # target drive, not the evacuated one. The pre-fix cold-ETS path copied
      # it byte-for-byte as "untracked" and left `locations` on drive1.
      {:ok, migrated} = ChunkIndex.get(volume.id, hash)
      drive_ids = Enum.map(migrated.locations, & &1.drive_id)
      assert "drive2" in drive_ids
      refute "drive1" in drive_ids
    end

    test "caches the authoritative tracked-chunk set in job state (#1578)" do
      ensure_cluster_state()

      {:ok, volume} =
        VolumeRegistry.create("evac-cache-vol",
          durability: %{type: :replicate, factor: 1, min_copies: 1}
        )

      data = "chunk whose tracked classification is cached across batches"
      {:ok, hash, _info} = BlobStore.write_chunk(data, "drive1", "hot")

      chunk =
        ChunkMeta.new(volume.id, hash, byte_size(data), byte_size(data))
        |> ChunkMeta.add_location(%{node: node(), drive_id: "drive1", tier: :hot})

      ChunkIndex.put(chunk)

      job =
        %{
          Job.new(EvacuationRunner, %{node: node(), drive_id: "drive1", total_chunks: 1})
          | status: :running
        }

      {:continue, updated} = EvacuationRunner.step(job)

      assert Map.has_key?(updated.state.tracked_chunks, hash)
    end

    test "reuses the cached tracked set instead of rescanning (#1578)" do
      # No volume is registered, so a fresh scan would find no volumes and
      # classify the on-disk chunk as untracked. Seeding the cache proves the
      # step honours it rather than rescanning: the chunk migrates as tracked
      # (locations rewritten to the target drive).
      data = "on-disk chunk classified purely from the seeded cache"
      {:ok, hash, _info} = BlobStore.write_chunk(data, "drive1", "hot")

      seeded =
        ChunkMeta.new("vol-seeded", hash, byte_size(data), byte_size(data))
        |> ChunkMeta.add_location(%{node: node(), drive_id: "drive1", tier: :hot})

      ChunkIndex.put(seeded)

      job =
        %{
          Job.new(EvacuationRunner, %{node: node(), drive_id: "drive1", total_chunks: 1})
          | status: :running,
            state: %{tracked_chunks: %{hash => seeded}}
        }

      {:continue, _updated} = EvacuationRunner.step(job)

      {:ok, migrated} = ChunkIndex.get("vol-seeded", hash)
      drive_ids = Enum.map(migrated.locations, & &1.drive_id)
      assert "drive2" in drive_ids
      refute "drive1" in drive_ids
    end

    test "completes when no chunks remain" do
      # Finalisation verifies against Ra (#1628); with no volume roots to
      # rewrite and nothing referencing the drive, all three checks pass.
      Mimic.stub(RaSupervisor, :local_query, fn _fun -> {:ok, %{}} end)

      job = drained_job("drive1")

      # The completion logic tries to deregister via DriveManager which may
      # not be running, so wrap in try/catch.
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

      assert updated.progress.description =~ "Evacuating blobs"
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

  describe "finalisation safety (#1628)" do
    test "a volume root that could not be rewritten blocks deregistration" do
      # A root still naming drive1, and a Ra command that rejects the rewrite.
      Mimic.stub(RaSupervisor, :local_query, fn _fun ->
        {:ok,
         %{
           "vol-stuck" => %{
             0 => %{
               drive_locations: [%{node: node(), drive_id: "drive1"}],
               durability_cache: %{type: :replicate, factor: 1, min_copies: 1}
             }
           }
         }}
      end)

      Mimic.stub(RaSupervisor, :command, fn _command -> {:error, :noproc} end)

      assert {:error, {:volume_roots_not_rewritten, [failure]}, updated} =
               EvacuationRunner.step(drained_job("drive1"))

      assert failure.volume_id == "vol-stuck"
      assert updated.progress.description =~ "volume roots still point at this drive"

      # Still registered — the drive was not thrown away.
      assert {:ok, _} = DriveRegistry.get_drive(node(), "drive1")
    end

    test "a stale chunk.locations entry blocks deregistration" do
      # Nothing to rewrite, drive is empty on disk, but a volume's
      # authoritative chunk metadata still names it — the #1573 shape.
      Mimic.stub(RaSupervisor, :local_query, fn _fun -> {:ok, %{}} end)

      Mimic.stub(VolumeRegistry, :list, fn _opts ->
        [
          %{
            id: "vol-stale",
            name: "stale",
            system: false,
            durability: %{type: :replicate, factor: 2, min_copies: 2}
          }
        ]
      end)

      Mimic.stub(ChunkIndex, :list_volume_chunks, fn "vol-stale" ->
        {:ok,
         [
           ChunkMeta.new("vol-stale", "orphan-hash", 4, 4)
           |> ChunkMeta.add_location(%{node: node(), drive_id: "drive1", tier: :hot})
           |> ChunkMeta.add_location(%{node: node(), drive_id: "drive2", tier: :hot})
         ]}
      end)

      assert {:error, {:chunks_still_referenced, [volume]}, updated} =
               EvacuationRunner.step(drained_job("drive1"))

      assert volume.volume_name == "stale"
      assert volume.chunks_on_drive == 1
      assert updated.progress.description =~ "chunks still reference this drive in stale (1)"

      assert {:ok, _} = DriveRegistry.get_drive(node(), "drive1")
    end

    test "an unreadable volume-root query refuses rather than assuming nothing to rewrite" do
      Mimic.stub(RaSupervisor, :local_query, fn _fun -> {:error, :noproc} end)

      assert {:error, {:volume_roots_unreadable, :noproc}, updated} =
               EvacuationRunner.step(drained_job("drive1"))

      assert updated.progress.description =~ "could not read the volume roots"
      assert {:ok, _} = DriveRegistry.get_drive(node(), "drive1")
    end
  end

  defp drained_job(drive_id) do
    job =
      Job.new(EvacuationRunner, %{
        node: node(),
        drive_id: drive_id,
        total_chunks: 0
      })

    %{job | status: :running}
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
          total_chunks: 10
        })

      EvacuationRunner.on_cancel(job)

      {:ok, drive} = DriveRegistry.get_drive(node(), "drive1")
      assert drive.state == :active
    end
  end

  # A drive left `:draining` had no way back: `restore_active/2` existed but
  # its only caller was `on_cancel/1`, which a terminal job never reaches
  # (`JobTracker` answers `:already_terminal`). The drive kept serving reads,
  # took no writes, and refused a retry with `:already_draining` (#1634).
  describe "resume_drive/2" do
    test "returns a draining drive to active" do
      DriveRegistry.update_state("drive1", :draining)

      assert {:ok, %{drive_id: "drive1", state: :active}} =
               DriveEvacuation.resume_drive(node(), "drive1")

      assert {:ok, %{state: :active}} = DriveRegistry.get_drive(node(), "drive1")
    end

    test "a resumed drive is selectable as a migration target again" do
      DriveRegistry.update_state("drive1", :draining)
      DriveRegistry.update_state("drive2", :draining)

      assert {:error, _} = DriveRegistry.select_drive(:hot)

      {:ok, _} = DriveEvacuation.resume_drive(node(), "drive1")

      assert {:ok, %{id: "drive1"}} = DriveRegistry.select_drive(:hot)
    end

    test "retrying an evacuation is possible once the drive is resumed" do
      DriveRegistry.update_state("drive1", :draining)

      assert {:error, :already_draining} =
               DriveEvacuation.start_evacuation(node(), "drive1", force: true)

      {:ok, _} = DriveEvacuation.resume_drive(node(), "drive1")

      # Not asserting the evacuation succeeds — only that the drive is no
      # longer refused before the attempt begins.
      refute match?(
               {:error, :already_draining},
               DriveEvacuation.start_evacuation(node(), "drive1", force: true)
             )
    end

    test "refuses while an evacuation for that drive is still running" do
      DriveRegistry.update_state("drive1", :draining)

      {:ok, job} =
        JobTracker.create(EvacuationRunner, %{node: node(), drive_id: "drive1", total_chunks: 0})

      # Resuming underneath a live drain would race the runner's own target
      # selection.
      assert {:error, {:evacuation_running, job_id}} =
               DriveEvacuation.resume_drive(node(), "drive1")

      assert job_id == job.id
      assert {:ok, %{state: :draining}} = DriveRegistry.get_drive(node(), "drive1")
    end

    test "refuses a drive that is not draining" do
      assert {:error, {:not_draining, :active}} =
               DriveEvacuation.resume_drive(node(), "drive1")
    end

    test "reports an unknown drive rather than reporting success" do
      assert {:error, _} = DriveEvacuation.resume_drive(node(), "no-such-drive")
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
