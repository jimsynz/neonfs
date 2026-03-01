defmodule NeonFS.Core.Job.Runners.GarbageCollectionTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    FileIndex,
    Job,
    VolumeRegistry,
    WriteOperation
  }

  alias NeonFS.Core.Job.Runners.GarbageCollection

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_core_subsystems()
    start_stripe_index()

    on_exit(fn -> cleanup_test_dirs() end)
    :ok
  end

  describe "label/0" do
    test "returns garbage-collection" do
      assert GarbageCollection.label() == "garbage-collection"
    end
  end

  describe "step/1 without volume_id" do
    test "completes with empty index" do
      job = Job.new(GarbageCollection, %{})

      assert {:complete, updated} = GarbageCollection.step(job)
      assert updated.progress.description == "Complete"
      assert updated.state.chunks_deleted == 0
      assert updated.state.stripes_deleted == 0
      assert updated.state.chunks_protected == 0
    end

    test "reports correct counts after deleting orphaned chunks" do
      {:ok, vol} = VolumeRegistry.create("gc-runner-vol", [])
      {:ok, file} = WriteOperation.write_file(vol.id, "/orphan.txt", "orphan data")

      FileIndex.delete(file.id)

      job = Job.new(GarbageCollection, %{})

      assert {:complete, updated} = GarbageCollection.step(job)
      assert updated.state.chunks_deleted > 0
      assert updated.state.stripes_deleted == 0
    end
  end

  describe "step/1 with volume_id" do
    test "only collects orphans from the specified volume" do
      {:ok, vol_a} = VolumeRegistry.create("gc-runner-a", [])
      {:ok, vol_b} = VolumeRegistry.create("gc-runner-b", [])

      {:ok, file_a} = WriteOperation.write_file(vol_a.id, "/a.txt", "volume a data")
      {:ok, _file_b} = WriteOperation.write_file(vol_b.id, "/b.txt", "volume b data")

      # Delete file in volume A only
      FileIndex.delete(file_a.id)

      # GC scoped to volume A — should delete A's orphans
      job_a = Job.new(GarbageCollection, %{volume_id: vol_a.id})
      assert {:complete, updated_a} = GarbageCollection.step(job_a)
      assert updated_a.state.chunks_deleted > 0

      # GC scoped to volume B — nothing to delete (file_b still referenced)
      job_b = Job.new(GarbageCollection, %{volume_id: vol_b.id})
      assert {:complete, updated_b} = GarbageCollection.step(job_b)
      assert updated_b.state.chunks_deleted == 0
    end
  end
end
