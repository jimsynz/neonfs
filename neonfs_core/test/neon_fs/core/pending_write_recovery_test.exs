defmodule NeonFS.Core.PendingWriteRecoveryTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    BlobStore,
    ChunkIndex,
    ChunkMeta,
    PendingWriteLog,
    PendingWriteRecovery,
    ReadOperation,
    VolumeRegistry,
    WriteOperation
  }

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

  describe "recovery of an orphaned write" do
    # The defect this guards: `abort_chunks/1` selects by write-ref membership,
    # and `active_write_refs` are local-ETS-only and never persisted. Restarting
    # `ChunkIndex` empties them exactly as node death does, so a sweep that goes
    # by ref matches nothing and reclaims nothing — which is what this used to
    # do while still clearing the record and reporting a chunk count.
    test "reclaims the record's chunks across a ChunkIndex restart", %{volume: volume} do
      write_id = WriteOperation.generate_write_id()
      hash = stage_orphan(volume, write_id, "/crashed.bin")

      restart_chunk_index_empty()

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :write_operation, :orphan_recovered]
        ])

      assert :ok = PendingWriteRecovery.sweep(1)

      assert_receive {[:neonfs, :write_operation, :orphan_recovered], ^ref,
                      %{chunks: 1, chunks_named: 1}, %{write_id: ^write_id}}

      assert {:error, :not_found} = PendingWriteLog.get(write_id)
      assert {:error, :not_found} = ChunkIndex.get(volume.id, hash)
      refute BlobStore.chunk_exists?(hash, "default")
    end

    test "spares a chunk a committed file references", %{volume: volume} do
      data = :crypto.strong_rand_bytes(2048)

      {:ok, meta} =
        WriteOperation.write_file_streamed(volume.id, "/committed.bin", Stream.map([data], & &1))

      [hash] = meta.chunks

      # A crashed write that had deduplicated against this chunk names it in
      # its record. Reclaiming by hash must not take it.
      write_id = WriteOperation.generate_write_id()
      :ok = PendingWriteLog.open_write(write_id, volume.id, "/dedup-crashed.bin")
      :ok = PendingWriteLog.record_chunk(write_id, hash)
      backdate(write_id)

      restart_chunk_index_empty()

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :write_operation, :orphan_recovered]
        ])

      assert :ok = PendingWriteRecovery.sweep(1)

      assert_receive {[:neonfs, :write_operation, :orphan_recovered], ^ref,
                      %{chunks: 0, chunks_named: 1}, %{write_id: ^write_id}}

      assert {:error, :not_found} = PendingWriteLog.get(write_id)
      assert {:ok, ^data} = ReadOperation.read_file(volume.id, "/committed.bin")
    end

    test "a record naming a chunk that is already gone reclaims nothing", %{volume: volume} do
      write_id = WriteOperation.generate_write_id()
      :ok = PendingWriteLog.open_write(write_id, volume.id, "/vanished.bin")
      :ok = PendingWriteLog.record_chunk(write_id, :crypto.hash(:sha256, "never-stored"))
      backdate(write_id)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :write_operation, :orphan_recovered]
        ])

      assert :ok = PendingWriteRecovery.sweep(1)

      assert_receive {[:neonfs, :write_operation, :orphan_recovered], ^ref,
                      %{chunks: 0, chunks_named: 1}, %{write_id: ^write_id}}

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

  describe "crash durability" do
    test "records survive a Process.exit(:kill) on the DETS owner", %{tmp_dir: tmp_dir} do
      # The setup opens the log from the test process. Close it so the
      # child process can become the sole owner and exercise a real
      # crash path (no cooperative `terminate/2` unwind on kill -9).
      :ok = PendingWriteLog.close()

      parent = self()
      write_id = WriteOperation.generate_write_id()

      {pid, mref} =
        spawn_monitor(fn ->
          :ok = PendingWriteLog.open(meta_dir: tmp_dir)
          :ok = PendingWriteLog.open_write(write_id, "vol-crash", "/crashed.bin")
          :ok = PendingWriteLog.record_chunk(write_id, "hash-1")
          send(parent, :synced)

          receive do
          end
        end)

      assert_receive :synced, 2_000
      Process.exit(pid, :kill)
      assert_receive {:DOWN, ^mref, :process, ^pid, :killed}, 2_000

      # Re-open in the test process — the `:dets.sync/1` calls inside
      # open_write/record_chunk must have made the record durable.
      :ok = PendingWriteLog.open(meta_dir: tmp_dir)
      assert {:ok, record} = PendingWriteLog.get(write_id)
      assert record.write_id == write_id
      assert record.chunk_hashes == ["hash-1"]
    end
  end

  # Stage exactly what a streaming write leaves on disk before it commits: the
  # blob written, an uncommitted `ChunkMeta` carrying the write's ref, and the
  # hash recorded in the pending-write log. Back-dated past any grace window.
  defp stage_orphan(volume, write_id, path) do
    :ok = PendingWriteLog.open_write(write_id, volume.id, path)

    data = :crypto.strong_rand_bytes(2048)
    {:ok, hash, _info} = BlobStore.write_chunk(data, "default", "hot")

    :ok =
      ChunkIndex.put(%ChunkMeta{
        volume_ids: MapSet.new([volume.id]),
        hash: hash,
        original_size: byte_size(data),
        stored_size: byte_size(data),
        compression: :none,
        crypto: nil,
        locations: [%{node: node(), drive_id: "default", tier: :hot}],
        target_replicas: 1,
        commit_state: :uncommitted,
        active_write_refs: MapSet.new([write_id]),
        created_at: DateTime.utc_now()
      })

    :ok = PendingWriteLog.record_chunk(write_id, hash)
    backdate(write_id)

    hash
  end

  defp backdate(write_id) do
    {:ok, record} = PendingWriteLog.get(write_id)

    :dets.insert(
      :pending_writes,
      {write_id, %{record | started_at: DateTime.add(DateTime.utc_now(), -3600, :second)}}
    )

    :dets.sync(:pending_writes)
  end

  # `active_write_refs` die with the node. A restart is the cheapest faithful
  # stand-in, and the only way the sweep is exercised against the ref set it
  # will actually see at boot.
  defp restart_chunk_index_empty do
    start_chunk_index()
    assert [] = ChunkIndex.list_uncommitted()
  end
end
