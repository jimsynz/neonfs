defmodule NeonFS.Core.KeyRotationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    BlobStore,
    ChunkIndex,
    KeyManager,
    KeyRotation,
    RaServer,
    VolumeRegistry,
    WriteOperation
  }

  alias NeonFS.CLI.Handler
  alias NeonFS.Core.VolumeEncryption

  @moduletag :tmp_dir

  @test_master_key :crypto.strong_rand_bytes(32) |> Base.encode64()

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    write_cluster_json(tmp_dir, @test_master_key)

    start_persistence()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_volume_registry()
    start_background_worker()
    start_job_tracker(tmp_dir)

    start_ra()
    :ok = RaServer.init_cluster()

    on_exit(fn -> cleanup_test_dirs() end)

    :ok
  end

  describe "start_rotation/1" do
    test "starts rotation and creates new key version" do
      volume_id = create_encrypted_volume("rot-start")
      write_test_file(volume_id, "/test.txt", "Secret data")

      # Verify chunks are visible before rotation (diagnostic for CI flakiness)
      chunks = ChunkIndex.get_chunks_for_volume(volume_id)
      assert chunks != [], "expected chunks after write, got none"

      assert {:ok, info} = KeyRotation.start_rotation(volume_id)
      assert info.from_version == 1
      assert info.to_version == 2
      assert info.total_chunks >= 1
      assert is_binary(info.job_id)
    end

    test "rejects rotation for unencrypted volume" do
      {:ok, volume} = VolumeRegistry.create("plain-vol")

      assert {:error, :not_encrypted} = KeyRotation.start_rotation(volume.id)
    end

    test "rejects concurrent rotation" do
      volume_id = create_encrypted_volume("concurrent-rot")

      # Insert a fake running rotation job directly into the JobTracker ETS
      # table. A real rotation completes too quickly (single chunk) to
      # reliably test the concurrency guard.
      alias NeonFS.Core.Job
      alias NeonFS.Core.Job.Runners.KeyRotation, as: KRRunner

      now = DateTime.utc_now()

      fake_job = %Job{
        id: "fake-rotation-job",
        type: KRRunner,
        node: Node.self(),
        status: :running,
        progress: %{total: 100, completed: 0, description: "Re-encrypting chunks"},
        params: %{volume_id: volume_id, from_version: 1, to_version: 2, total_chunks: 100},
        created_at: now,
        started_at: now,
        updated_at: now
      }

      :dets.insert(:neonfs_jobs, {fake_job.id, fake_job})

      assert {:error, :rotation_in_progress} = KeyRotation.start_rotation(volume_id)
    after
      :dets.delete(:neonfs_jobs, "fake-rotation-job")
    end

    test "tracks correct total chunk count" do
      volume_id = create_encrypted_volume("chunk-count")

      # Write multiple files to create multiple chunks
      write_test_file(volume_id, "/file1.txt", "Data one")
      write_test_file(volume_id, "/file2.txt", "Data two")
      write_test_file(volume_id, "/file3.txt", "Data three")

      chunks = ChunkIndex.get_chunks_for_volume(volume_id)
      encrypted_count = Enum.count(chunks, &(&1.crypto != nil))

      assert {:ok, info} = KeyRotation.start_rotation(volume_id)
      assert info.total_chunks == encrypted_count
    end
  end

  describe "rotation_status/1" do
    test "returns rotation state when rotation is active" do
      volume_id = create_encrypted_volume("status-active")
      write_test_file(volume_id, "/test.txt", "Secret data")

      {:ok, _info} = KeyRotation.start_rotation(volume_id)

      # Check status immediately (may still be in progress)
      case KeyRotation.rotation_status(volume_id) do
        {:ok, status} ->
          assert status.from_version == 1
          assert status.to_version == 2
          assert %DateTime{} = status.started_at
          assert is_map(status.progress)

        {:error, :no_rotation} ->
          # Rotation completed very quickly — that's also valid
          :ok
      end
    end

    test "returns error when no rotation is active" do
      volume_id = create_encrypted_volume("status-none")

      assert {:error, :no_rotation} = KeyRotation.rotation_status(volume_id)
    end

    test "returns error for unknown volume" do
      assert {:error, :not_found} = KeyRotation.rotation_status("nonexistent-id")
    end
  end

  describe "worker re-encryption" do
    test "re-encrypts all chunks to new key version" do
      volume_id = create_encrypted_volume("reencrypt-all")
      write_test_file(volume_id, "/secret.txt", "Top secret data for re-encryption test")

      # Verify chunks are at version 1
      chunks_before = ChunkIndex.get_chunks_for_volume(volume_id)
      assert Enum.all?(chunks_before, &(&1.crypto != nil and &1.crypto.key_version == 1))

      # Start rotation and wait for completion
      ref = attach_job_telemetry()
      {:ok, _info} = KeyRotation.start_rotation(volume_id)
      wait_for_rotation_complete(ref)

      # All chunks should now be at version 2
      chunks_after = ChunkIndex.get_chunks_for_volume(volume_id)

      assert Enum.all?(chunks_after, fn chunk ->
               chunk.crypto != nil and chunk.crypto.key_version == 2
             end)
    end

    test "re-encrypted chunks are readable with new key" do
      volume_id = create_encrypted_volume("reencrypt-read")
      data = "Data that must survive re-encryption"
      write_test_file(volume_id, "/survive.txt", data)

      # Get chunk hashes before rotation
      chunks_before = ChunkIndex.get_chunks_for_volume(volume_id)

      ref = attach_job_telemetry()
      {:ok, _info} = KeyRotation.start_rotation(volume_id)
      wait_for_rotation_complete(ref)

      # Verify each chunk is readable with the new key
      {:ok, new_key} = KeyManager.get_volume_key(volume_id, 2)

      for chunk <- chunks_before do
        [loc | _] = chunk.locations

        # Read with new key + new nonce (from updated chunk meta)
        {:ok, updated_chunk} = ChunkIndex.get(chunk.hash)

        assert {:ok, _data} =
                 BlobStore.read_chunk(chunk.hash, loc.drive_id,
                   tier: Atom.to_string(loc.tier),
                   key: new_key,
                   nonce: updated_chunk.crypto.nonce,
                   decompress: chunk.compression != :none
                 )
      end
    end

    test "rotation clears rotation state on completion" do
      volume_id = create_encrypted_volume("clear-state")
      write_test_file(volume_id, "/test.txt", "Test data")

      ref = attach_job_telemetry()
      {:ok, _info} = KeyRotation.start_rotation(volume_id)
      wait_for_rotation_complete(ref)

      assert {:error, :no_rotation} = KeyRotation.rotation_status(volume_id)
    end

    test "progress tracking reports correct total and migrated counts" do
      volume_id = create_encrypted_volume("progress-track")
      write_test_file(volume_id, "/file1.txt", "Progress data one")
      write_test_file(volume_id, "/file2.txt", "Progress data two")

      chunks_before = ChunkIndex.get_chunks_for_volume(volume_id)
      encrypted_count = Enum.count(chunks_before, &(&1.crypto != nil))
      assert encrypted_count >= 2

      ref = attach_job_telemetry()
      {:ok, info} = KeyRotation.start_rotation(volume_id)
      assert info.total_chunks == encrypted_count

      wait_for_rotation_complete(ref)

      # After completion, all chunks should be at the new version
      chunks_after = ChunkIndex.get_chunks_for_volume(volume_id)

      assert Enum.all?(chunks_after, fn chunk ->
               chunk.crypto != nil and chunk.crypto.key_version == info.to_version
             end)
    end

    test "new nonces are generated per chunk" do
      volume_id = create_encrypted_volume("new-nonces")
      write_test_file(volume_id, "/test.txt", "Nonce test data")

      chunks_before = ChunkIndex.get_chunks_for_volume(volume_id)
      old_nonces = MapSet.new(chunks_before, & &1.crypto.nonce)

      ref = attach_job_telemetry()
      {:ok, _info} = KeyRotation.start_rotation(volume_id)
      wait_for_rotation_complete(ref)

      chunks_after = ChunkIndex.get_chunks_for_volume(volume_id)
      new_nonces = MapSet.new(chunks_after, & &1.crypto.nonce)

      # All nonces should be different from before
      assert MapSet.disjoint?(old_nonces, new_nonces)
    end
  end

  describe "reencrypt_chunk edge cases" do
    test "skips metadata update when chunk has no local replicas" do
      volume_id = create_encrypted_volume("no-local-replicas")
      write_test_file(volume_id, "/test.txt", "Data on remote node")

      chunks = ChunkIndex.get_chunks_for_volume(volume_id)
      assert [chunk | _] = chunks
      assert chunk.crypto.key_version == 1

      old_nonce = chunk.crypto.nonce

      # Rewrite the chunk's locations to point to a fake remote node so the
      # rotation runner sees no local replicas for this chunk.
      remote_chunk = %{chunk | locations: [%{node: :fake@remote, drive_id: "d0", tier: :hot}]}
      :ets.insert(:chunk_index, {remote_chunk.hash, remote_chunk})

      # Start rotation — the runner should skip this chunk (no local replicas)
      # and complete without error.
      ref = attach_job_telemetry()
      {:ok, _info} = KeyRotation.start_rotation(volume_id)
      wait_for_rotation_complete(ref)

      # The chunk metadata must NOT have been updated — nonce and key_version
      # should still be the originals, because no blob was re-encrypted.
      {:ok, after_chunk} = ChunkIndex.get(chunk.hash)
      assert after_chunk.crypto.key_version == 1
      assert after_chunk.crypto.nonce == old_nonce
    end
  end

  describe "handler integration" do
    test "rotate_volume_key handler starts rotation" do
      _volume_id = create_encrypted_volume("handler-rotate")
      write_test_file_by_name("handler-rotate", "/test.txt", "Handler test data")

      assert {:ok, info} = Handler.rotate_volume_key("handler-rotate")
      assert info.from_version == 1
      assert info.to_version == 2
    end

    test "rotation_status handler returns status" do
      volume_id = create_encrypted_volume("handler-status")
      write_test_file(volume_id, "/test.txt", "Handler status data")

      {:ok, _info} = KeyRotation.start_rotation(volume_id)

      case Handler.rotation_status("handler-status") do
        {:ok, status} ->
          assert is_map(status.progress)

        {:error, %NeonFS.Error.NotFound{}} ->
          # Completed quickly
          :ok
      end
    end

    test "rotation_status returns error for unknown volume" do
      assert {:error, %NeonFS.Error.VolumeNotFound{}} =
               Handler.rotation_status("nonexistent-vol")
    end
  end

  # Helpers

  defp create_encrypted_volume(name) do
    {:ok, volume} =
      VolumeRegistry.create(name,
        encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
        compression: %{algorithm: :none}
      )

    {:ok, _version} = KeyManager.setup_volume_encryption(volume.id)
    volume.id
  end

  defp write_test_file(volume_id, path, data) do
    {:ok, _file_meta} = WriteOperation.write_file(volume_id, path, data)
  end

  defp write_test_file_by_name(volume_name, path, data) do
    {:ok, volume} = VolumeRegistry.get_by_name(volume_name)
    write_test_file(volume.id, path, data)
  end

  defp start_background_worker do
    start_supervised!(
      {Task.Supervisor, name: NeonFS.Core.BackgroundTaskSupervisor},
      restart: :temporary
    )

    start_supervised!(
      {NeonFS.Core.BackgroundWorker, max_concurrent: 4, max_per_minute: 100},
      restart: :temporary
    )
  end

  defp attach_job_telemetry do
    :telemetry_test.attach_event_handlers(self(), [
      [:neonfs, :job, :completed],
      [:neonfs, :job, :failed]
    ])
  end

  defp wait_for_rotation_complete(ref, timeout \\ 10_000) do
    receive do
      {[:neonfs, :job, :completed], ^ref, %{}, %{}} ->
        :ok

      {[:neonfs, :job, :failed], ^ref, %{}, %{}} ->
        raise "Key rotation job failed"
    after
      timeout ->
        raise "Timed out waiting for rotation to complete"
    end
  end
end
