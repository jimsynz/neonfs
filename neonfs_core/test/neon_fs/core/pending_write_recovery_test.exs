defmodule NeonFS.Core.PendingWriteRecoveryTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{PendingWriteLog, PendingWriteRecovery, VolumeRegistry, WriteOperation}

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

    Application.put_env(:neonfs_core, :meta_dir, tmp_dir)
    :ok = PendingWriteLog.open(meta_dir: tmp_dir)

    vol_name = "pwr-vol-#{:rand.uniform(999_999)}"
    {:ok, volume} = VolumeRegistry.create(vol_name, [])

    on_exit(fn ->
      PendingWriteLog.close()
      Application.delete_env(:neonfs_core, :meta_dir)
      cleanup_test_dirs()
    end)

    {:ok, volume: volume}
  end

  describe "committed writes are not orphaned" do
    test "a successful streaming write leaves no pending record", %{volume: volume} do
      stream = Stream.map(["hello ", "world"], & &1)
      {:ok, _meta} = WriteOperation.write_file_streamed(volume.id, "/committed.txt", stream)

      # After a successful write the log should be empty.
      assert [] = PendingWriteLog.list_all()
    end
  end

  describe "recovery of simulated orphan" do
    test "deletes orphaned chunks and clears the record", %{volume: volume} do
      # Simulate a crash: write a chunk under write_id, register it
      # in the pending log, but DON'T commit. This mirrors the state
      # a crashed streaming write would leave behind.
      write_id = WriteOperation.generate_write_id()
      :ok = PendingWriteLog.open_write(write_id, volume.id, "/crashed.bin")

      # Write one chunk directly via the streaming path so the
      # ChunkIndex has an uncommitted chunk with this write_id's ref.
      stream = Stream.map([:crypto.strong_rand_bytes(2048)], & &1)

      task =
        Task.async(fn ->
          WriteOperation.write_file_streamed(volume.id, "/will-commit.bin", stream)
        end)

      {:ok, _} = Task.await(task)

      # Now register a fake orphan — chunk that references write_id but
      # will never commit. Back-date started_at past the grace window.
      orphan_hash = "orphan-hash-" <> Integer.to_string(:rand.uniform(9_999_999_999))
      stale_at = DateTime.add(DateTime.utc_now(), -3600, :second)

      :dets.insert(
        :pending_writes,
        {write_id,
         %{
           write_id: write_id,
           volume_id: volume.id,
           path: "/crashed.bin",
           chunk_hashes: [orphan_hash],
           started_at: stale_at
         }}
      )

      :dets.sync(:pending_writes)

      # Sweep with a small grace — the back-dated record must be seen
      # as orphaned and cleared.
      assert :ok = PendingWriteRecovery.sweep(1)

      assert {:error, :not_found} = PendingWriteLog.get(write_id)
    end
  end

  describe "grace window" do
    test "fresh pending records are left alone", %{volume: _volume} do
      write_id = WriteOperation.generate_write_id()
      :ok = PendingWriteLog.open_write(write_id, "vol-fresh", "/fresh.bin")

      # Sweep with a generous grace — nothing should be touched.
      assert :ok = PendingWriteRecovery.sweep(3600)

      # Record still present.
      assert {:ok, _} = PendingWriteLog.get(write_id)
    end
  end
end
