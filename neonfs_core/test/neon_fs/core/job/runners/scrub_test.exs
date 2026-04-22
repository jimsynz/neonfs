defmodule NeonFS.Core.Job.Runners.ScrubTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    ChunkIndex,
    Job,
    KeyManager,
    RaServer,
    VolumeEncryption,
    VolumeRegistry,
    WriteOperation
  }

  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.Job.Runners.Scrub

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_core_subsystems()
    start_stripe_index()

    on_exit(fn -> cleanup_test_dirs() end)

    {:ok, blob_dir: Application.get_env(:neonfs_core, :blob_store_base_dir)}
  end

  describe "label/0" do
    test "returns scrub" do
      assert Scrub.label() == "scrub"
    end
  end

  describe "step/1 — all chunks valid" do
    test "completes with zero corrupted" do
      {:ok, vol} = VolumeRegistry.create("scrub-valid", [])
      {:ok, _file} = WriteOperation.write_file_streamed(vol.id, "/good.txt", ["good data"])

      job = Job.new(Scrub, %{})

      # Run steps until complete
      result = run_to_completion(job)

      assert {:complete, updated} = result
      assert updated.progress.description == "Complete"
      assert updated.state.corruption_count == 0
      assert updated.state[:corrupted] == []
      assert updated.progress.completed > 0
    end

    test "updates last_verified on verified chunks" do
      {:ok, vol} = VolumeRegistry.create("scrub-verified", [])
      {:ok, _file} = WriteOperation.write_file_streamed(vol.id, "/verify.txt", ["verify data"])

      chunks_before = ChunkIndex.list_all()
      assert Enum.all?(chunks_before, &(&1.last_verified == nil))

      job = Job.new(Scrub, %{})
      {:complete, _} = run_to_completion(job)

      chunks_after = ChunkIndex.list_all()
      assert Enum.all?(chunks_after, &(&1.last_verified != nil))
    end
  end

  describe "step/1 — corruption detection" do
    test "detects a corrupted chunk" do
      {:ok, vol} = VolumeRegistry.create("scrub-corrupt", [])
      {:ok, _file} = WriteOperation.write_file_streamed(vol.id, "/bad.txt", ["original data"])

      # Find the chunk and tamper with it on disk
      [chunk | _] = ChunkIndex.list_all()
      tamper_with_chunk(chunk)

      job = Job.new(Scrub, %{})
      {:complete, updated} = run_to_completion(job)

      assert updated.state.corruption_count > 0
      assert chunk.hash in updated.state.corrupted
    end

    test "emits corruption_detected telemetry" do
      {:ok, vol} = VolumeRegistry.create("scrub-telemetry-corrupt", [])
      {:ok, _file} = WriteOperation.write_file_streamed(vol.id, "/telem.txt", ["telemetry data"])

      [chunk | _] = ChunkIndex.list_all()
      tamper_with_chunk(chunk)

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :scrub, :corruption_detected]
        ])

      job = Job.new(Scrub, %{})
      run_to_completion(job)

      assert_received {[:neonfs, :scrub, :corruption_detected], ^ref, %{}, %{hash: _, node: _}}
    end

    test "emits complete telemetry" do
      {:ok, vol} = VolumeRegistry.create("scrub-telemetry-complete", [])
      {:ok, _file} = WriteOperation.write_file_streamed(vol.id, "/done.txt", ["done data"])

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :scrub, :complete]
        ])

      job = Job.new(Scrub, %{})
      run_to_completion(job)

      assert_received {[:neonfs, :scrub, :complete], ^ref, %{total: _, corrupted: _}, %{}}
    end
  end

  describe "step/1 — volume-scoped scrub" do
    test "only scrubs the specified volume's chunks" do
      {:ok, vol_a} = VolumeRegistry.create("scrub-vol-a", [])
      {:ok, vol_b} = VolumeRegistry.create("scrub-vol-b", [])

      {:ok, _file_a} = WriteOperation.write_file_streamed(vol_a.id, "/a.txt", ["volume a data"])
      {:ok, _file_b} = WriteOperation.write_file_streamed(vol_b.id, "/b.txt", ["volume b data"])

      # Tamper with vol_a's chunk
      vol_a_chunks = ChunkIndex.get_chunks_for_volume(vol_a.id)
      Enum.each(vol_a_chunks, &tamper_with_chunk/1)

      # Scrub vol_b only — should find no corruption
      job_b = Job.new(Scrub, %{volume_id: vol_b.id})
      {:complete, updated_b} = run_to_completion(job_b)
      assert updated_b.state.corruption_count == 0

      # Scrub vol_a — should detect corruption
      job_a = Job.new(Scrub, %{volume_id: vol_a.id})
      {:complete, updated_a} = run_to_completion(job_a)
      assert updated_a.state.corruption_count > 0
    end
  end

  describe "step/1 — encrypted chunk verification" do
    @test_master_key :crypto.strong_rand_bytes(32) |> Base.encode64()

    setup %{tmp_dir: tmp_dir} do
      write_cluster_json(tmp_dir, @test_master_key)
      start_ra()
      :ok = RaServer.init_cluster()

      vol_name = "scrub-encrypted-#{:rand.uniform(999_999)}"

      {:ok, enc_volume} =
        VolumeRegistry.create(vol_name,
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        )

      {:ok, _version} = KeyManager.setup_volume_encryption(enc_volume.id)

      {:ok, enc_volume: enc_volume}
    end

    test "decrypts and verifies encrypted chunks successfully", %{enc_volume: volume} do
      {:ok, _file} =
        WriteOperation.write_file_streamed(volume.id, "/secret.txt", ["secret scrub data"])

      # Confirm the chunk is actually encrypted
      [chunk | _] = ChunkIndex.get_chunks_for_volume(volume.id)
      assert chunk.crypto != nil
      assert chunk.crypto.key_version == 1

      job = Job.new(Scrub, %{volume_id: volume.id})
      {:complete, updated} = run_to_completion(job)

      assert updated.state.corruption_count == 0
      assert updated.state.encrypted_verified > 0
      assert updated.state.key_unavailable_count == 0
    end

    test "uses historical key version after rotation", %{enc_volume: volume} do
      {:ok, _file} =
        WriteOperation.write_file_streamed(volume.id, "/pre-rotate.txt", ["before rotation"])

      # Chunks are encrypted with key version 1
      [chunk | _] = ChunkIndex.get_chunks_for_volume(volume.id)
      assert chunk.crypto.key_version == 1

      # Rotate key — current version is now 2, but chunks still reference version 1
      {:ok, 2} = KeyManager.rotate_volume_key(volume.id)

      job = Job.new(Scrub, %{volume_id: volume.id})
      {:complete, updated} = run_to_completion(job)

      # Scrub should succeed by looking up key version 1 (not current version 2)
      assert updated.state.corruption_count == 0
      assert updated.state.encrypted_verified > 0
    end

    test "tracks encrypted chunks separately in complete telemetry", %{enc_volume: volume} do
      {:ok, _file} =
        WriteOperation.write_file_streamed(volume.id, "/telemetry.txt", ["telemetry enc data"])

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :scrub, :complete]
        ])

      job = Job.new(Scrub, %{volume_id: volume.id})
      run_to_completion(job)

      assert_received {[:neonfs, :scrub, :complete], ^ref,
                       %{encrypted_verified: enc, key_unavailable: 0}, %{}}

      assert enc > 0
    end
  end

  describe "step/1 — unavailable encryption key" do
    setup do
      Application.put_env(:neonfs_core, :key_manager_mod, MockKeyManager)

      on_exit(fn ->
        Application.delete_env(:neonfs_core, :key_manager_mod)
      end)

      :ok
    end

    test "reports key_unavailable instead of corruption" do
      {:ok, vol} = VolumeRegistry.create("scrub-key-unavail", [])

      {:ok, file} =
        WriteOperation.write_file_streamed(vol.id, "/unavail.txt", ["unavailable key data"])

      # Manually add crypto metadata to simulate an encrypted chunk
      for chunk_hash <- file.chunks do
        {:ok, chunk} = ChunkIndex.get(chunk_hash)

        encrypted_chunk = %{
          chunk
          | crypto: %NeonFS.Core.ChunkCrypto{
              algorithm: :aes_256_gcm,
              nonce: :crypto.strong_rand_bytes(12),
              key_version: 1
            }
        }

        ChunkIndex.put(encrypted_chunk)
      end

      ref =
        :telemetry_test.attach_event_handlers(self(), [
          [:neonfs, :scrub, :key_unavailable]
        ])

      job = Job.new(Scrub, %{volume_id: vol.id})
      {:complete, updated} = run_to_completion(job)

      # Should NOT be counted as corruption
      assert updated.state.corruption_count == 0
      # Should be counted as key_unavailable
      assert updated.state.key_unavailable_count > 0

      assert_received {[:neonfs, :scrub, :key_unavailable], ^ref, %{},
                       %{hash: _, volume_id: _, node: _}}
    end

    test "key_unavailable chunks are not marked as verified" do
      {:ok, vol} = VolumeRegistry.create("scrub-key-no-verify", [])

      {:ok, file} =
        WriteOperation.write_file_streamed(vol.id, "/no-verify.txt", ["no verify data"])

      for chunk_hash <- file.chunks do
        {:ok, chunk} = ChunkIndex.get(chunk_hash)

        encrypted_chunk = %{
          chunk
          | crypto: %NeonFS.Core.ChunkCrypto{
              algorithm: :aes_256_gcm,
              nonce: :crypto.strong_rand_bytes(12),
              key_version: 1
            }
        }

        ChunkIndex.put(encrypted_chunk)
      end

      job = Job.new(Scrub, %{volume_id: vol.id})
      run_to_completion(job)

      # Chunks should NOT have last_verified updated
      for chunk_hash <- file.chunks do
        {:ok, chunk} = ChunkIndex.get(chunk_hash)
        assert chunk.last_verified == nil
      end
    end
  end

  # Helpers

  defp run_to_completion(job, max_steps \\ 100) do
    Enum.reduce_while(1..max_steps, job, fn _i, acc ->
      case Scrub.step(acc) do
        {:complete, updated} -> {:halt, {:complete, updated}}
        {:continue, updated} -> {:cont, updated}
      end
    end)
  end

  defp tamper_with_chunk(chunk) do
    blob_dir = Application.get_env(:neonfs_core, :blob_store_base_dir)
    {:ok, store} = Native.store_open(blob_dir, 2)
    local_loc = Enum.find(chunk.locations, &(&1.node == node()))
    tier = Atom.to_string(local_loc.tier)
    Native.store_write_chunk(store, chunk.hash, "corrupted garbage data", tier)
  end
end

defmodule MockKeyManager do
  @moduledoc false

  def get_volume_key(_volume_id, _key_version) do
    {:error, :unknown_key_version}
  end

  def get_current_key(_volume_id) do
    {:error, :unknown_key_version}
  end
end
