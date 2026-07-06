defmodule NeonFS.Core.VolumeMaxFilesTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{VolumeRegistry, WriteOperation}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_stripe_index()
    start_volume_registry()
    ensure_chunk_access_tracker()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  defp create_volume(opts) do
    {:ok, volume} = VolumeRegistry.create("test-#{:rand.uniform(999_999)}", opts)
    volume
  end

  defp file_count(volume_id) do
    {:ok, volume} = VolumeRegistry.get(volume_id)
    volume.file_count
  end

  defp write(volume_id, path) do
    WriteOperation.write_file_streamed(volume_id, path, ["data"])
  end

  describe "max_files enforcement" do
    test "rejects creating a file beyond the quota" do
      volume = create_volume(max_files: 2)

      assert {:ok, _} = write(volume.id, "/a.txt")
      assert {:ok, _} = write(volume.id, "/b.txt")
      assert {:error, :enospc} = write(volume.id, "/c.txt")

      assert file_count(volume.id) == 2
    end

    test "a nil quota is unlimited" do
      volume = create_volume(max_files: nil)

      for i <- 1..50, do: assert({:ok, _} = write(volume.id, "/f#{i}.txt"))
      assert file_count(volume.id) == 50
    end

    test "overwriting an existing file does not consume a quota slot" do
      volume = create_volume(max_files: 2)

      assert {:ok, _} = write(volume.id, "/a.txt")
      assert {:ok, _} = write(volume.id, "/b.txt")
      assert file_count(volume.id) == 2

      # At quota, but overwriting an existing path replaces it — no new slot.
      assert {:ok, _} = write(volume.id, "/a.txt")
      assert file_count(volume.id) == 2
    end
  end

  describe "file-count accounting" do
    test "deleting a file frees a quota slot" do
      volume = create_volume(max_files: 2)

      assert {:ok, _} = write(volume.id, "/a.txt")
      assert {:ok, _} = write(volume.id, "/b.txt")
      assert {:error, :enospc} = write(volume.id, "/c.txt")

      assert :ok = NeonFS.Core.delete_file(volume.name, "/a.txt")
      assert file_count(volume.id) == 1

      # The freed slot is usable again.
      assert {:ok, _} = write(volume.id, "/c.txt")
      assert file_count(volume.id) == 2
    end

    test "renaming a file is net-zero for the count" do
      volume = create_volume(max_files: 2)

      assert {:ok, _} = write(volume.id, "/a.txt")
      assert {:ok, _} = write(volume.id, "/b.txt")
      assert file_count(volume.id) == 2

      assert :ok = NeonFS.Core.rename_file(volume.name, "/a.txt", "/renamed.txt")
      assert file_count(volume.id) == 2
    end
  end

  describe "reconcile_stats/1" do
    test "recomputes file_count from the file index, correcting drift" do
      volume = create_volume(max_files: nil)

      assert {:ok, _} = write(volume.id, "/a.txt")
      assert {:ok, _} = write(volume.id, "/b.txt")

      # Force counter drift.
      {:ok, _} = VolumeRegistry.adjust_stats(volume.id, file_count: 100)
      assert file_count(volume.id) == 102

      assert {:ok, _} = VolumeRegistry.reconcile_stats(volume.id)
      assert file_count(volume.id) == 2
    end
  end
end
