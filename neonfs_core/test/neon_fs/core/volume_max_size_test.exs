defmodule NeonFS.Core.VolumeMaxSizeTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{FileIndex, VolumeRegistry, WriteOperation}

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

  defp logical_size(volume_id) do
    {:ok, volume} = VolumeRegistry.get(volume_id)
    volume.logical_size
  end

  describe "max_size enforcement" do
    test "rejects a streamed write that would exceed the cap" do
      volume = create_volume(max_size: 100)

      assert {:error, :enospc} =
               WriteOperation.write_file_streamed(volume.id, "/big.bin", [
                 String.duplicate("x", 200)
               ])

      # The rejected write leaves no file behind.
      assert {:error, :not_found} = FileIndex.get_by_path(volume.id, "/big.bin")
      assert logical_size(volume.id) == 0
    end

    test "allows a streamed write that fits within the cap" do
      volume = create_volume(max_size: 100)

      assert {:ok, _meta} =
               WriteOperation.write_file_streamed(volume.id, "/ok.bin", [
                 String.duplicate("x", 50)
               ])

      assert logical_size(volume.id) == 50
    end

    test "a nil cap is unlimited" do
      volume = create_volume(max_size: nil)

      assert {:ok, _meta} =
               WriteOperation.write_file_streamed(volume.id, "/big.bin", [
                 String.duplicate("x", 100_000)
               ])

      assert logical_size(volume.id) == 100_000
    end

    test "rejects an offset write that would exceed the cap" do
      volume = create_volume(max_size: 100)

      assert {:ok, _meta} =
               WriteOperation.write_file_at(volume.id, "/f.bin", 0, String.duplicate("x", 80))

      assert {:error, :enospc} =
               WriteOperation.write_file_at(volume.id, "/f.bin", 80, String.duplicate("x", 40))
    end

    test "a same-size overwrite is not rejected once at the cap" do
      volume = create_volume(max_size: 100)

      assert {:ok, _} =
               WriteOperation.write_file_streamed(volume.id, "/f.bin", [
                 String.duplicate("x", 100)
               ])

      assert logical_size(volume.id) == 100

      # Re-streaming the same key replaces its bytes rather than adding to
      # them, so it must fit even though the volume is already at the cap.
      assert {:ok, _} =
               WriteOperation.write_file_streamed(volume.id, "/f.bin", [
                 String.duplicate("y", 100)
               ])
    end
  end

  describe "usage accounting" do
    test "deleting a file frees its logical bytes" do
      volume = create_volume(max_size: 100)

      assert {:ok, _} =
               WriteOperation.write_file_streamed(volume.id, "/f.bin", [
                 String.duplicate("x", 100)
               ])

      assert logical_size(volume.id) == 100
      assert :ok = NeonFS.Core.delete_file(volume.name, "/f.bin")
      assert logical_size(volume.id) == 0

      # Freed space is usable again.
      assert {:ok, _} =
               WriteOperation.write_file_streamed(volume.id, "/g.bin", [
                 String.duplicate("y", 100)
               ])
    end

    test "truncating a file down frees the trimmed bytes" do
      volume = create_volume(max_size: 100)

      assert {:ok, _} =
               WriteOperation.write_file_streamed(volume.id, "/f.bin", [
                 String.duplicate("x", 100)
               ])

      assert logical_size(volume.id) == 100
      assert {:ok, _} = NeonFS.Core.truncate_file(volume.name, "/f.bin", 30)
      assert logical_size(volume.id) == 30
    end

    test "concurrent writes to distinct files don't lose counter updates" do
      volume = create_volume(max_size: nil)

      tasks =
        for i <- 1..20 do
          Task.async(fn ->
            WriteOperation.write_file_streamed(volume.id, "/f#{i}.bin", [
              String.duplicate("x", 100)
            ])
          end)
        end

      results = Task.await_many(tasks, 30_000)
      assert Enum.all?(results, &match?({:ok, _}, &1))

      assert logical_size(volume.id) == 20 * 100
    end
  end

  describe "reconcile_stats/1" do
    test "recomputes logical_size from the file index, correcting drift" do
      volume = create_volume(max_size: nil)

      assert {:ok, _} =
               WriteOperation.write_file_streamed(volume.id, "/f.bin", [
                 String.duplicate("x", 100)
               ])

      # Simulate counter drift (e.g. a crash between the file write and the
      # increment) by forcing the counter to a wrong value.
      {:ok, _} = VolumeRegistry.adjust_stats(volume.id, logical_size: 5_000)
      assert logical_size(volume.id) == 5_100

      assert {:ok, _} = VolumeRegistry.reconcile_stats(volume.id)
      assert logical_size(volume.id) == 100
    end
  end
end
