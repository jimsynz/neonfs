defmodule NeonFS.Core.ReadOperationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    ChunkIndex,
    KeyManager,
    RaServer,
    ReadOperation,
    StripeIndex,
    VolumeEncryption,
    VolumeRegistry,
    WriteOperation
  }

  alias NeonFS.Error.FileNotFound, as: FileNotFoundError
  alias NeonFS.Error.{Internal, VolumeNotFound}

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

    # Set up telemetry test handler
    :telemetry.attach_many(
      "read-operation-test",
      [
        [:neonfs, :read_operation, :start],
        [:neonfs, :read_operation, :stop],
        [:neonfs, :read_operation, :exception],
        [:neonfs, :read_operation, :stripe_read]
      ],
      &handle_telemetry_event/4,
      nil
    )

    on_exit(fn ->
      :telemetry.detach("read-operation-test")
      cleanup_test_dirs()
    end)

    # Create test volume with unique name
    vol_name = "test-volume-#{:rand.uniform(999_999)}"
    {:ok, volume} = VolumeRegistry.create(vol_name, [])

    {:ok, volume: volume}
  end

  describe "read_file/3" do
    test "reads entire small file (single chunk)", %{volume: volume} do
      data = "Hello, NeonFS! This is a test file."

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/test.txt", data)

      # Read it back
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/test.txt")
      assert read_data == data
    end

    test "reads entire large file (multiple chunks)", %{volume: volume} do
      # Create 2MB of data to ensure multiple chunks
      data = :crypto.strong_rand_bytes(2 * 1024 * 1024)

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/large.bin", data)

      # Read it back
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/large.bin")
      assert read_data == data
    end

    test "reads file with compression enabled", %{volume: volume} do
      # Update volume to enable compression
      {:ok, volume} =
        VolumeRegistry.update(volume.id,
          compression: %{algorithm: :zstd, level: 3, min_size: 100}
        )

      # Create compressible data (repeated pattern)
      data = String.duplicate("ABCDEFGH", 1000)

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/compressed.txt", data)

      # Read it back
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/compressed.txt")
      assert read_data == data
    end

    test "reads empty file", %{volume: volume} do
      data = ""

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/empty.txt", data)

      # Read it back
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/empty.txt")
      assert read_data == ""
    end

    test "returns error for non-existent file", %{volume: volume} do
      assert {:error, %FileNotFoundError{file_path: "/does-not-exist.txt"}} =
               ReadOperation.read_file(volume.id, "/does-not-exist.txt")
    end

    test "returns error for non-existent volume" do
      fake_volume_id = UUIDv7.generate()

      assert {:error, %VolumeNotFound{volume_id: ^fake_volume_id}} =
               ReadOperation.read_file(fake_volume_id, "/test.txt")
    end
  end

  describe "read_file/3 with offset and length" do
    test "reads with offset from beginning", %{volume: volume} do
      data = "0123456789ABCDEFGHIJ"

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/offset-test.txt", data)

      # Read from offset 10
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/offset-test.txt", offset: 10)
      assert read_data == "ABCDEFGHIJ"
    end

    test "reads with offset and length", %{volume: volume} do
      data = "0123456789ABCDEFGHIJ"

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/partial.txt", data)

      # Read 5 bytes from offset 5
      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/partial.txt", offset: 5, length: 5)

      assert read_data == "56789"
    end

    test "reads spanning chunk boundaries", %{volume: volume} do
      # Create 500KB of data with fixed chunks
      data = :crypto.strong_rand_bytes(500 * 1024)

      # Write with fixed chunk size
      {:ok, _file_meta} =
        WriteOperation.write_file(volume.id, "/chunks.bin", data,
          chunk_strategy: {:fixed, 64_000}
        )

      # Read middle portion spanning multiple chunks
      offset = 60_000
      length = 10_000

      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/chunks.bin", offset: offset, length: length)

      expected = binary_part(data, offset, length)
      assert read_data == expected
    end

    test "reads beyond file size returns available data", %{volume: volume} do
      data = "Short file"

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/short.txt", data)

      # Try to read 100 bytes
      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/short.txt", offset: 0, length: 100)

      # Should return only available data
      assert read_data == data
    end

    test "reads with offset beyond file size returns empty", %{volume: volume} do
      data = "Short file"

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/short.txt", data)

      # Read from offset beyond file size
      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/short.txt", offset: 1000, length: 10)

      assert read_data == ""
    end

    test "reads last byte of file", %{volume: volume} do
      data = "0123456789"

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/last-byte.txt", data)

      # Read last byte
      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/last-byte.txt", offset: 9, length: 1)

      assert read_data == "9"
    end

    test "reads single byte from middle", %{volume: volume} do
      data = "ABCDEFGHIJ"

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/single-byte.txt", data)

      # Read single byte from middle
      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/single-byte.txt", offset: 5, length: 1)

      assert read_data == "F"
    end
  end

  describe "read_file/3 with verification" do
    test "reads with verification enabled (always)", %{volume: volume} do
      # Update volume to enable verification
      {:ok, volume} =
        VolumeRegistry.update(volume.id,
          verification: %{on_read: :always, sampling_rate: nil}
        )

      data = "Verified data content"

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/verified.txt", data)

      # Read with verification
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/verified.txt")
      assert read_data == data
    end

    test "reads with verification disabled (never)", %{volume: volume} do
      # Update volume to disable verification
      {:ok, volume} =
        VolumeRegistry.update(volume.id,
          verification: %{on_read: :never, sampling_rate: nil}
        )

      data = "Unverified data content"

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/unverified.txt", data)

      # Read without verification
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/unverified.txt")
      assert read_data == data
    end

    test "reads with sampling verification", %{volume: volume} do
      # Update volume to enable sampling verification
      {:ok, volume} =
        VolumeRegistry.update(volume.id,
          verification: %{on_read: :sampling, sampling_rate: 0.5}
        )

      data = "Sampling verified data"

      # Write the file
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/sampling.txt", data)

      # Read multiple times - some should verify, some should not
      # We can't predict which ones, but all should succeed
      for _i <- 1..10 do
        assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/sampling.txt")
        assert read_data == data
      end
    end
  end

  describe "telemetry events" do
    test "emits start and stop events on success", %{volume: volume} do
      # Clear previous events
      Process.put(:telemetry_events, [])

      data = "Test data for telemetry"
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/telemetry.txt", data)

      # Read the file
      {:ok, read_data} = ReadOperation.read_file(volume.id, "/telemetry.txt")

      events = Process.get(:telemetry_events, []) |> Enum.reverse()

      # Should have start and stop events (excluding write operation events)
      read_events = Enum.filter(events, &match?([:neonfs, :read_operation | _], &1.event))

      start_event = Enum.find(read_events, &(&1.event == [:neonfs, :read_operation, :start]))
      assert start_event
      assert start_event.measurements.offset == 0
      assert start_event.metadata.volume_id == volume.id
      assert start_event.metadata.path == "/telemetry.txt"

      stop_event = Enum.find(read_events, &(&1.event == [:neonfs, :read_operation, :stop]))
      assert stop_event
      assert stop_event.measurements.bytes == byte_size(read_data)
      assert is_integer(stop_event.measurements.duration)
    end

    test "emits exception event on failure", %{volume: volume} do
      # Clear previous events
      Process.put(:telemetry_events, [])

      # Try to read non-existent file
      {:error, %FileNotFoundError{}} = ReadOperation.read_file(volume.id, "/not-found.txt")

      events = Process.get(:telemetry_events, []) |> Enum.reverse()

      # Should have start and exception events
      exception_event =
        Enum.find(events, &(&1.event == [:neonfs, :read_operation, :exception]))

      assert exception_event
      assert %FileNotFoundError{} = exception_event.metadata.error
      assert is_integer(exception_event.measurements.duration)
    end
  end

  describe "complex scenarios" do
    test "reads file written with deduplication", %{volume: volume} do
      # Write two files with same content (deduplication)
      data = "Duplicate content for dedup test"

      {:ok, _file1} = WriteOperation.write_file(volume.id, "/file1.txt", data)
      {:ok, _file2} = WriteOperation.write_file(volume.id, "/file2.txt", data)

      # Read both files
      assert {:ok, read_data1} = ReadOperation.read_file(volume.id, "/file1.txt")
      assert {:ok, read_data2} = ReadOperation.read_file(volume.id, "/file2.txt")

      assert read_data1 == data
      assert read_data2 == data
    end

    test "reads file after overwrite", %{volume: volume} do
      path = "/overwrite.txt"

      # Write first version
      {:ok, _file1} = WriteOperation.write_file(volume.id, path, "Version 1")

      # Write second version
      data2 = "Version 2 - longer content"
      {:ok, _file2} = WriteOperation.write_file(volume.id, path, data2)

      # Read should return the latest version
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, path)
      assert read_data == data2
    end

    test "reads multiple files in parallel", %{volume: volume} do
      # Write multiple files
      files = [
        {"/file1.txt", "Content 1"},
        {"/file2.txt", "Content 2"},
        {"/file3.txt", "Content 3"}
      ]

      for {path, data} <- files do
        {:ok, _} = WriteOperation.write_file(volume.id, path, data)
      end

      # Read all in parallel
      tasks =
        for {path, _data} <- files do
          Task.async(fn ->
            ReadOperation.read_file(volume.id, path)
          end)
        end

      results = Task.await_many(tasks)

      # All should succeed
      assert Enum.all?(results, &match?({:ok, _}, &1))

      # Verify content
      for {{_path, expected_data}, {:ok, read_data}} <- Enum.zip(files, results) do
        assert read_data == expected_data
      end
    end
  end

  describe "erasure-coded read path" do
    setup %{volume: _volume} do
      vol_name = "erasure-read-vol-#{:rand.uniform(999_999)}"

      {:ok, ec_volume} =
        VolumeRegistry.create(vol_name,
          durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1},
          compression: %{algorithm: :none}
        )

      {:ok, ec_volume: ec_volume}
    end

    test "reads entire small file (single partial stripe)", %{ec_volume: volume} do
      data = "Hello, erasure coding read test!"

      {:ok, _fm} =
        WriteOperation.write_file(volume.id, "/small_ec.txt", data, chunk_strategy: :single)

      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/small_ec.txt")
      assert read_data == data
    end

    test "reads entire multi-stripe file", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(4096)

      {:ok, _fm} =
        WriteOperation.write_file(volume.id, "/multi_ec.bin", data,
          chunk_strategy: {:fixed, 1024}
        )

      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/multi_ec.bin")
      assert read_data == data
    end

    test "reads file spanning 3 stripes (2 complete + 1 partial)", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(5120)

      {:ok, _fm} =
        WriteOperation.write_file(volume.id, "/three_ec.bin", data,
          chunk_strategy: {:fixed, 1024}
        )

      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/three_ec.bin")
      assert read_data == data
    end

    test "reads with offset within single stripe", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(2048)

      {:ok, _fm} =
        WriteOperation.write_file(volume.id, "/offset_ec.bin", data,
          chunk_strategy: {:fixed, 1024}
        )

      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/offset_ec.bin", offset: 100, length: 500)

      assert read_data == binary_part(data, 100, 500)
    end

    test "reads spanning stripe boundary", %{ec_volume: volume} do
      # 4096 bytes with 1024-byte chunks, 2 per stripe → stripe 0: bytes 0-2047, stripe 1: bytes 2048-4095
      data = :crypto.strong_rand_bytes(4096)

      {:ok, _fm} =
        WriteOperation.write_file(volume.id, "/boundary_ec.bin", data,
          chunk_strategy: {:fixed, 1024}
        )

      # Read spanning the boundary at 2048
      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/boundary_ec.bin",
                 offset: 2000,
                 length: 200
               )

      assert read_data == binary_part(data, 2000, 200)
    end

    test "reads beyond file size returns available data", %{ec_volume: volume} do
      data = "Short erasure file"

      {:ok, _fm} =
        WriteOperation.write_file(volume.id, "/short_ec.txt", data, chunk_strategy: :single)

      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/short_ec.txt", offset: 0, length: 1000)

      assert read_data == data
    end

    test "reads with offset beyond file size returns empty", %{ec_volume: volume} do
      data = "Short"

      {:ok, _fm} =
        WriteOperation.write_file(volume.id, "/past_ec.txt", data, chunk_strategy: :single)

      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/past_ec.txt", offset: 1000, length: 10)

      assert read_data == <<>>
    end

    test "degraded read reconstructs missing data chunk", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(2048)

      {:ok, fm} =
        WriteOperation.write_file(volume.id, "/degraded_ec.bin", data,
          chunk_strategy: {:fixed, 1024}
        )

      # Get the stripe and delete one data chunk from ChunkIndex
      [stripe_ref] = fm.stripes
      {:ok, stripe} = StripeIndex.get(stripe_ref.stripe_id)

      # Delete the first data chunk (index 0) from ChunkIndex
      first_data_hash = Enum.at(stripe.chunks, 0)
      ChunkIndex.delete(first_data_hash)

      # Read should still succeed via degraded reconstruction
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/degraded_ec.bin")
      assert read_data == data
    end

    test "critical stripe returns error when too many chunks missing", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(2048)

      {:ok, fm} =
        WriteOperation.write_file(volume.id, "/critical_ec.bin", data,
          chunk_strategy: {:fixed, 1024}
        )

      # Get the stripe and delete 2 chunks (more than parity_chunks=1 can handle)
      [stripe_ref] = fm.stripes
      {:ok, stripe} = StripeIndex.get(stripe_ref.stripe_id)

      # Delete first two chunks
      ChunkIndex.delete(Enum.at(stripe.chunks, 0))
      ChunkIndex.delete(Enum.at(stripe.chunks, 1))

      assert {:error, %NeonFS.Error.Unavailable{}} =
               ReadOperation.read_file(volume.id, "/critical_ec.bin")
    end

    test "reads partial stripe respects data_bytes boundary", %{ec_volume: volume} do
      # Single chunk → partial stripe. The stripe has padding beyond data_bytes.
      data = "Partial stripe data only"

      {:ok, _fm} =
        WriteOperation.write_file(volume.id, "/partial_ec.txt", data, chunk_strategy: :single)

      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/partial_ec.txt")
      # Should return only actual data, not padding
      assert read_data == data
      assert byte_size(read_data) == byte_size(data)
    end

    test "reads file after overwrite", %{ec_volume: volume} do
      {:ok, _} =
        WriteOperation.write_file(volume.id, "/overwrite_ec.txt", "Version 1",
          chunk_strategy: :single
        )

      {:ok, _} =
        WriteOperation.write_file(volume.id, "/overwrite_ec.txt", "Version 2 longer",
          chunk_strategy: :single
        )

      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/overwrite_ec.txt")
      assert read_data == "Version 2 longer"
    end
  end

  describe "encrypted read path" do
    @test_master_key :crypto.strong_rand_bytes(32) |> Base.encode64()

    setup %{tmp_dir: tmp_dir} do
      # Set up encryption infrastructure: Ra + cluster.json with master key
      write_cluster_json(tmp_dir, @test_master_key)

      start_ra()
      :ok = RaServer.init_cluster()

      # Attach decrypt telemetry handler
      :telemetry.attach(
        "read-decrypt-test",
        [:neonfs, :read, :decrypt],
        &handle_telemetry_event/4,
        nil
      )

      on_exit(fn -> :telemetry.detach("read-decrypt-test") end)

      # Create an encrypted volume
      vol_name = "encrypted-read-vol-#{:rand.uniform(999_999)}"

      {:ok, enc_volume} =
        VolumeRegistry.create(vol_name,
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        )

      {:ok, _version} = KeyManager.setup_volume_encryption(enc_volume.id)

      {:ok, enc_volume: enc_volume}
    end

    test "full round-trip: encrypted write then read", %{enc_volume: volume} do
      data = "Secret data for encrypted round-trip test"

      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/secret.txt", data)

      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/secret.txt")
      assert read_data == data
    end

    test "reads large encrypted file (multiple chunks)", %{enc_volume: volume} do
      data = :crypto.strong_rand_bytes(2 * 1024 * 1024)

      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/large_enc.bin", data)

      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/large_enc.bin")
      assert read_data == data
    end

    test "reads encrypted file with offset and length", %{enc_volume: volume} do
      data = "0123456789ABCDEFGHIJ"

      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/partial_enc.txt", data)

      assert {:ok, read_data} =
               ReadOperation.read_file(volume.id, "/partial_enc.txt", offset: 5, length: 5)

      assert read_data == "56789"
    end

    test "reads with mixed key versions after rotation", %{enc_volume: volume} do
      # Write first file at key version 1
      data_v1 = "Data encrypted with key version 1"
      {:ok, _} = WriteOperation.write_file(volume.id, "/v1.txt", data_v1)

      # Rotate key → version 2
      {:ok, 2} = KeyManager.rotate_volume_key(volume.id)

      # Write second file at key version 2
      data_v2 = "Data encrypted with key version 2"
      {:ok, _} = WriteOperation.write_file(volume.id, "/v2.txt", data_v2)

      # Both files should read correctly — each chunk uses its own key version
      assert {:ok, read_v1} = ReadOperation.read_file(volume.id, "/v1.txt")
      assert read_v1 == data_v1

      assert {:ok, read_v2} = ReadOperation.read_file(volume.id, "/v2.txt")
      assert read_v2 == data_v2
    end

    test "tampered ciphertext returns decryption error", %{enc_volume: volume} do
      data = "Data to be tampered with"
      {:ok, file_meta} = WriteOperation.write_file(volume.id, "/tamper.txt", data)

      # Get chunk hash and metadata
      [chunk_hash] = file_meta.chunks
      {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)

      # Modify the chunk's crypto nonce in metadata to simulate wrong decryption params.
      # The chunk stays on disk unchanged — the NIF will attempt decryption with the
      # wrong nonce, causing GCM authentication to fail.
      wrong_nonce = :crypto.strong_rand_bytes(12)
      wrong_crypto = %{chunk_meta.crypto | nonce: wrong_nonce}
      wrong_meta = %{chunk_meta | crypto: wrong_crypto}
      ChunkIndex.put(wrong_meta)

      assert {:error, %Internal{}} =
               ReadOperation.read_file(volume.id, "/tamper.txt")
    end

    test "unencrypted reads still work in Ra context", %{tmp_dir: _tmp_dir} do
      vol_name = "plain-read-vol-#{:rand.uniform(999_999)}"
      {:ok, plain_vol} = VolumeRegistry.create(vol_name, compression: %{algorithm: :none})

      data = "Plaintext data in Ra context"
      {:ok, _} = WriteOperation.write_file(plain_vol.id, "/plain.txt", data)

      assert {:ok, read_data} = ReadOperation.read_file(plain_vol.id, "/plain.txt")
      assert read_data == data
    end

    test "emits decrypt telemetry events", %{enc_volume: volume} do
      Process.put(:telemetry_events, [])

      data = "Telemetry decrypt test"
      {:ok, _} = WriteOperation.write_file(volume.id, "/telem_dec.txt", data)
      {:ok, _} = ReadOperation.read_file(volume.id, "/telem_dec.txt")

      events = Process.get(:telemetry_events, [])

      decrypt_events =
        Enum.filter(events, &(&1.event == [:neonfs, :read, :decrypt]))

      assert decrypt_events != []

      for event <- decrypt_events do
        assert event.metadata.volume_id == volume.id
        assert event.metadata.key_version == 1
        assert is_binary(event.metadata.chunk_hash)
      end
    end
  end

  describe "verification failure repair" do
    use Mimic

    setup :verify_on_exit!

    setup %{volume: volume} do
      # Enable verification on reads
      {:ok, volume} =
        VolumeRegistry.update(volume.id,
          verification: %{on_read: :always, sampling_rate: nil}
        )

      # Attach verification failure telemetry handler
      test_pid = self()

      :telemetry.attach(
        "verification-failure-test",
        [:neonfs, :read_operation, :verification_failed],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:verification_failed, event, measurements, metadata})
        end,
        nil
      )

      on_exit(fn -> :telemetry.detach("verification-failure-test") end)

      {:ok, volume: volume}
    end

    test "local verification failure triggers remote fetch and returns data", %{volume: volume} do
      data = "data for verification failure test"
      {:ok, file_meta} = WriteOperation.write_file(volume.id, "/verify_fail.txt", data)
      [chunk_hash] = file_meta.chunks

      call_count = :counters.new(1, [:atomics])

      # First call: simulate local verification failure
      # Second call (with exclude_nodes): return valid data from "remote"
      Mimic.stub(NeonFS.Core.ChunkFetcher, :fetch_chunk, fn hash, opts ->
        assert hash == chunk_hash
        :counters.add(call_count, 1, 1)
        count = :counters.get(call_count, 1)

        if count == 1 do
          {:error, "corrupt chunk: expected abc123, got def456"}
        else
          assert node() in Keyword.get(opts, :exclude_nodes, [])
          {:ok, data, {:remote, :fake_remote@host}}
        end
      end)

      Mimic.stub(NeonFS.Core.BackgroundWorker, :submit, fn _work_fn, opts ->
        assert Keyword.get(opts, :priority) == :high
        assert Keyword.get(opts, :label) =~ "repair_corrupt_chunk"
        {:ok, "mock_work_id"}
      end)

      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/verify_fail.txt")
      assert read_data == data
    end

    test "successful remote fetch returns data to caller", %{volume: volume} do
      data = "remote recovery data"
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/remote_ok.txt", data)

      call_count = :counters.new(1, [:atomics])

      Mimic.stub(NeonFS.Core.ChunkFetcher, :fetch_chunk, fn _hash, _opts ->
        :counters.add(call_count, 1, 1)
        count = :counters.get(call_count, 1)

        if count == 1 do
          {:error, "corrupt chunk: expected abc, got def"}
        else
          {:ok, data, {:remote, :other_node@host}}
        end
      end)

      Mimic.stub(NeonFS.Core.BackgroundWorker, :submit, fn _work_fn, _opts ->
        {:ok, "mock_work_id"}
      end)

      assert {:ok, ^data} = ReadOperation.read_file(volume.id, "/remote_ok.txt")
    end

    test "background repair is submitted after successful remote recovery", %{volume: volume} do
      data = "repair test data"
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/repair.txt", data)

      test_pid = self()
      call_count = :counters.new(1, [:atomics])

      Mimic.stub(NeonFS.Core.ChunkFetcher, :fetch_chunk, fn _hash, _opts ->
        :counters.add(call_count, 1, 1)
        count = :counters.get(call_count, 1)

        if count == 1 do
          {:error, "corrupt chunk: expected aaa, got bbb"}
        else
          {:ok, data, {:remote, :repair_node@host}}
        end
      end)

      Mimic.expect(NeonFS.IO.Scheduler, :submit_async, fn op ->
        assert is_function(op.callback, 0)
        assert op.priority == :read_repair
        assert op.type == :write
        send(test_pid, :repair_submitted)
        :ok
      end)

      assert {:ok, _} = ReadOperation.read_file(volume.id, "/repair.txt")
      assert_receive :repair_submitted
    end

    test "both local and remote failure returns :chunk_corrupted", %{volume: volume} do
      data = "double failure data"
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/double_fail.txt", data)

      Mimic.stub(NeonFS.Core.ChunkFetcher, :fetch_chunk, fn _hash, _opts ->
        {:error, "corrupt chunk: expected xxx, got yyy"}
      end)

      assert {:error, %Internal{}} =
               ReadOperation.read_file(volume.id, "/double_fail.txt")
    end

    test "telemetry event emitted on verification failure", %{volume: volume} do
      data = "telemetry verification test"
      {:ok, file_meta} = WriteOperation.write_file(volume.id, "/telem_verify.txt", data)
      [chunk_hash] = file_meta.chunks

      call_count = :counters.new(1, [:atomics])

      Mimic.stub(NeonFS.Core.ChunkFetcher, :fetch_chunk, fn _hash, _opts ->
        :counters.add(call_count, 1, 1)
        count = :counters.get(call_count, 1)

        if count == 1 do
          {:error, "corrupt chunk: expected 0000, got ffff"}
        else
          {:ok, data, {:remote, :telem_node@host}}
        end
      end)

      Mimic.stub(NeonFS.Core.BackgroundWorker, :submit, fn _work_fn, _opts ->
        {:ok, "mock_id"}
      end)

      assert {:ok, _} = ReadOperation.read_file(volume.id, "/telem_verify.txt")

      assert_receive {:verification_failed, event, measurements, metadata}
      assert event == [:neonfs, :read_operation, :verification_failed]
      assert measurements.count == 1
      assert metadata.chunk_hash == chunk_hash
      assert metadata.volume_id == volume.id
      assert is_binary(metadata.drive_id)
      assert metadata.node == node()
    end
  end

  # Telemetry event handler
  defp handle_telemetry_event(event, measurements, metadata, _config) do
    events = Process.get(:telemetry_events, [])

    Process.put(:telemetry_events, [
      %{event: event, measurements: measurements, metadata: metadata} | events
    ])
  end
end
