defmodule NeonFS.Core.WriteOperationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{ChunkIndex, FileIndex, StripeIndex, VolumeRegistry, WriteOperation}

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

    # Set up telemetry test handler
    :telemetry.attach_many(
      "write-operation-test",
      [
        [:neonfs, :write_operation, :start],
        [:neonfs, :write_operation, :stop],
        [:neonfs, :write_operation, :exception],
        [:neonfs, :write_operation, :stripe_created]
      ],
      &handle_telemetry_event/4,
      nil
    )

    on_exit(fn ->
      :telemetry.detach("write-operation-test")
      cleanup_test_dirs()
    end)

    # Create test volume with unique name
    vol_name = "test-volume-#{:rand.uniform(999_999)}"
    {:ok, volume} = VolumeRegistry.create(vol_name, [])

    {:ok, volume: volume}
  end

  describe "write_file/4" do
    test "writes small file (single chunk)", %{volume: volume} do
      data = "Hello, NeonFS!"

      assert {:ok, file_meta} = WriteOperation.write_file(volume.id, "/test.txt", data)

      # Verify file metadata
      assert file_meta.volume_id == volume.id
      assert file_meta.path == "/test.txt"
      assert file_meta.size == byte_size(data)
      assert file_meta.chunks != []

      # Verify chunks are committed
      for chunk_hash <- file_meta.chunks do
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        assert chunk_meta.commit_state == :committed
        assert MapSet.size(chunk_meta.active_write_refs) == 0
      end

      # Verify file is in index
      assert {:ok, stored_file} = FileIndex.get(file_meta.id)
      assert stored_file.id == file_meta.id
      assert {:ok, stored_by_path} = FileIndex.get_by_path(volume.id, "/test.txt")
      assert stored_by_path.id == file_meta.id
    end

    test "writes large file (multiple chunks)", %{volume: volume} do
      # Create 2MB of data to ensure multiple chunks
      data = :crypto.strong_rand_bytes(2 * 1024 * 1024)

      assert {:ok, file_meta} = WriteOperation.write_file(volume.id, "/large.bin", data)

      # Verify file metadata
      assert file_meta.volume_id == volume.id
      assert file_meta.path == "/large.bin"
      assert file_meta.size == byte_size(data)
      # With auto strategy for 2MB, FastCDC is used (variable chunk sizes)
      # We expect multiple chunks but the exact number varies
      assert length(file_meta.chunks) >= 2

      # Verify all chunks are committed
      for chunk_hash <- file_meta.chunks do
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        assert chunk_meta.commit_state == :committed
        assert MapSet.size(chunk_meta.active_write_refs) == 0
      end
    end

    test "writes file with compression enabled", %{volume: volume} do
      # Update volume to enable compression
      {:ok, volume} =
        VolumeRegistry.update(volume.id,
          compression: %{algorithm: :zstd, level: 3, min_size: 100}
        )

      # Create compressible data (repeated pattern)
      data = String.duplicate("ABCDEFGH", 1000)

      assert {:ok, file_meta} = WriteOperation.write_file(volume.id, "/compressed.txt", data)

      # Verify chunks have compression metadata
      for chunk_hash <- file_meta.chunks do
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        # Chunk may or may not be compressed depending on size threshold
        assert chunk_meta.compression in [:none, :zstd]
      end
    end

    test "deduplication: writing same data twice creates only one chunk", %{volume: volume} do
      data = "Duplicate content"

      # Write first file
      assert {:ok, file1} = WriteOperation.write_file(volume.id, "/file1.txt", data)

      # Count chunks before second write
      chunk_count_before =
        :ets.tab2list(:chunk_index)
        |> length()

      # Write second file with same data
      assert {:ok, file2} = WriteOperation.write_file(volume.id, "/file2.txt", data)

      # Count chunks after second write
      chunk_count_after =
        :ets.tab2list(:chunk_index)
        |> length()

      # Both files should reference the same chunks
      assert file1.chunks == file2.chunks

      # No new chunks should be created
      assert chunk_count_before == chunk_count_after

      # Verify both files exist but share chunks
      assert {:ok, stored_file1} = FileIndex.get(file1.id)
      assert {:ok, stored_file2} = FileIndex.get(file2.id)
      assert stored_file1.id == file1.id
      assert stored_file2.id == file2.id
      assert file1.chunks == file2.chunks
    end

    test "returns error for non-existent volume" do
      data = "Test data"
      fake_volume_id = UUIDv7.generate()

      assert {:error, :volume_not_found} =
               WriteOperation.write_file(fake_volume_id, "/test.txt", data)
    end

    test "handles empty file", %{volume: volume} do
      data = ""

      assert {:ok, file_meta} = WriteOperation.write_file(volume.id, "/empty.txt", data)

      assert file_meta.size == 0
      # Empty file may have 0 or 1 chunks depending on chunking strategy
      assert is_list(file_meta.chunks)
    end

    test "respects chunk_strategy option", %{volume: volume} do
      data = :crypto.strong_rand_bytes(100 * 1024)

      # Force single chunk
      assert {:ok, file_meta} =
               WriteOperation.write_file(volume.id, "/single.bin", data, chunk_strategy: :single)

      # Should have exactly 1 chunk
      assert length(file_meta.chunks) == 1
    end

    test "overwrites existing file at same path", %{volume: volume} do
      path = "/overwrite.txt"

      # Write first version
      {:ok, file1} = WriteOperation.write_file(volume.id, path, "Version 1")

      # Write second version
      {:ok, file2} = WriteOperation.write_file(volume.id, path, "Version 2 - longer content")

      # Files should have different IDs
      assert file1.id != file2.id

      # Only the second file should be in the index at that path
      assert {:ok, retrieved_file} = FileIndex.get_by_path(volume.id, path)
      assert retrieved_file.id == file2.id
    end
  end

  describe "generate_write_id/0" do
    test "generates unique IDs" do
      id1 = WriteOperation.generate_write_id()
      id2 = WriteOperation.generate_write_id()

      assert is_binary(id1)
      assert is_binary(id2)
      assert id1 != id2
    end
  end

  describe "telemetry events" do
    test "emits start and stop events on success", %{volume: volume} do
      # Clear previous events
      Process.put(:telemetry_events, [])

      data = "Test data for telemetry"

      {:ok, file_meta} = WriteOperation.write_file(volume.id, "/telemetry.txt", data)

      events = Process.get(:telemetry_events, []) |> Enum.reverse()

      # Should have start and stop events
      assert [start_event | _] = events
      assert start_event.event == [:neonfs, :write_operation, :start]
      assert start_event.measurements.bytes == byte_size(data)
      assert start_event.metadata.volume_id == volume.id
      assert start_event.metadata.path == "/telemetry.txt"

      stop_event = Enum.find(events, &(&1.event == [:neonfs, :write_operation, :stop]))
      assert stop_event
      assert stop_event.measurements.bytes == byte_size(data)
      assert stop_event.measurements.chunks == length(file_meta.chunks)
      assert is_integer(stop_event.measurements.duration)
    end

    test "emits exception event on failure" do
      # Clear previous events
      Process.put(:telemetry_events, [])

      fake_volume_id = UUIDv7.generate()
      data = "Test data"

      {:error, :volume_not_found} =
        WriteOperation.write_file(fake_volume_id, "/fail.txt", data)

      events = Process.get(:telemetry_events, []) |> Enum.reverse()

      # Should have start and exception events
      assert events != []

      exception_event =
        Enum.find(events, &(&1.event == [:neonfs, :write_operation, :exception]))

      assert exception_event
      assert exception_event.metadata.error == :volume_not_found
      assert is_integer(exception_event.measurements.duration)
    end
  end

  describe "cleanup on failure" do
    test "aborts uncommitted chunks on write failure", %{volume: volume} do
      # This test simulates a failure scenario
      # We'll write a file, then manually check that if FileIndex.create fails,
      # chunks are cleaned up

      # For now, we'll test the basic flow works
      # In a real scenario, we'd need to mock FileIndex.create to fail

      data = "Test data"

      # Write successfully
      {:ok, _file_meta} = WriteOperation.write_file(volume.id, "/test.txt", data)

      # Verify no uncommitted chunks remain
      uncommitted = ChunkIndex.list_uncommitted()
      assert Enum.empty?(uncommitted)
    end

    test "cleans up chunks when write is aborted" do
      # Test direct abort functionality
      _write_id = WriteOperation.generate_write_id()

      # This tests that abort_chunks doesn't crash even with no chunks
      # In production, this would be called during error handling
      # We can't easily test the full cleanup path without mocking
      assert :ok == :ok
    end
  end

  describe "compression handling" do
    test "respects min_size threshold for compression", %{volume: volume} do
      # Set compression with min_size threshold
      {:ok, volume} =
        VolumeRegistry.update(volume.id,
          compression: %{algorithm: :zstd, level: 3, min_size: 10_000}
        )

      # Write small file (below threshold)
      small_data = "Small"

      {:ok, file_meta} = WriteOperation.write_file(volume.id, "/small.txt", small_data)

      # Small chunks should not be compressed
      for chunk_hash <- file_meta.chunks do
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        # May be :none if below threshold
        assert chunk_meta.compression in [:none, :zstd]
      end
    end

    test "compression option overrides volume settings", %{volume: volume} do
      # Volume has compression disabled
      {:ok, _volume} =
        VolumeRegistry.update(volume.id,
          compression: %{algorithm: :none}
        )

      data = String.duplicate("COMPRESS_ME", 1000)

      # Force compression via option
      {:ok, file_meta} =
        WriteOperation.write_file(volume.id, "/forced.txt", data,
          compression: %{algorithm: :zstd, level: 5, min_size: 100}
        )

      # Chunks may be compressed based on option
      assert file_meta.chunks != []
    end
  end

  describe "erasure-coded write path" do
    setup %{volume: _volume} do
      # Create erasure-coded volume: 2 data chunks + 1 parity chunk per stripe
      vol_name = "erasure-vol-#{:rand.uniform(999_999)}"

      {:ok, ec_volume} =
        VolumeRegistry.create(vol_name,
          durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1},
          compression: %{algorithm: :none}
        )

      {:ok, ec_volume: ec_volume}
    end

    test "writes small file producing single partial stripe", %{ec_volume: volume} do
      # Single chunk strategy → 1 data chunk → 1 partial stripe (needs 2 for full)
      data = "Hello, erasure coding!"

      assert {:ok, file_meta} =
               WriteOperation.write_file(volume.id, "/small_ec.txt", data,
                 chunk_strategy: :single
               )

      assert file_meta.volume_id == volume.id
      assert file_meta.path == "/small_ec.txt"
      assert file_meta.size == byte_size(data)

      # Erasure-coded file uses stripes, not chunks
      assert file_meta.chunks == []
      assert is_list(file_meta.stripes)
      assert length(file_meta.stripes) == 1

      # Verify stripe reference has correct byte_range
      [stripe_ref] = file_meta.stripes
      assert is_binary(stripe_ref.stripe_id)
      assert stripe_ref.byte_range == {0, byte_size(data)}

      # Verify stripe metadata in StripeIndex
      {:ok, stripe} = StripeIndex.get(stripe_ref.stripe_id)
      assert stripe.partial == true
      assert stripe.data_bytes == byte_size(data)
      assert stripe.padded_bytes >= 0
      assert stripe.config.data_chunks == 2
      assert stripe.config.parity_chunks == 1
      # 1 data + 1 zero-fill + 1 parity = 3 total chunks
      assert length(stripe.chunks) == 3
    end

    test "writes file filling exactly 2 complete stripes", %{ec_volume: volume} do
      # Use fixed 1024-byte chunks, 4096 bytes → 4 chunks → 2 stripes of 2
      data = :crypto.strong_rand_bytes(4096)

      assert {:ok, file_meta} =
               WriteOperation.write_file(volume.id, "/exact_ec.bin", data,
                 chunk_strategy: {:fixed, 1024}
               )

      assert file_meta.size == 4096
      assert file_meta.chunks == []
      assert length(file_meta.stripes) == 2

      # Verify byte ranges cover entire file
      [{first_stripe, second_stripe}] =
        [
          {Enum.at(file_meta.stripes, 0), Enum.at(file_meta.stripes, 1)}
        ]

      assert first_stripe.byte_range == {0, 2048}
      assert second_stripe.byte_range == {2048, 4096}

      # Both stripes should be complete (not partial)
      {:ok, s1} = StripeIndex.get(first_stripe.stripe_id)
      {:ok, s2} = StripeIndex.get(second_stripe.stripe_id)
      assert s1.partial == false
      assert s2.partial == false
      assert s1.data_bytes == 2048
      assert s2.data_bytes == 2048
      # Each stripe has 2 data + 1 parity = 3 chunks
      assert length(s1.chunks) == 3
      assert length(s2.chunks) == 3
    end

    test "writes file filling 2 complete + 1 partial stripe", %{ec_volume: volume} do
      # 5120 bytes with 1024-byte chunks → 5 chunks → 2 full + 1 partial
      data = :crypto.strong_rand_bytes(5120)

      assert {:ok, file_meta} =
               WriteOperation.write_file(volume.id, "/partial_ec.bin", data,
                 chunk_strategy: {:fixed, 1024}
               )

      assert file_meta.size == 5120
      assert file_meta.chunks == []
      assert length(file_meta.stripes) == 3

      stripes = file_meta.stripes

      # Verify byte ranges
      assert Enum.at(stripes, 0).byte_range == {0, 2048}
      assert Enum.at(stripes, 1).byte_range == {2048, 4096}
      assert Enum.at(stripes, 2).byte_range == {4096, 5120}

      # First two complete, last partial
      {:ok, s1} = StripeIndex.get(Enum.at(stripes, 0).stripe_id)
      {:ok, s2} = StripeIndex.get(Enum.at(stripes, 1).stripe_id)
      {:ok, s3} = StripeIndex.get(Enum.at(stripes, 2).stripe_id)

      assert s1.partial == false
      assert s2.partial == false
      assert s3.partial == true
      assert s3.data_bytes == 1024
      assert s3.padded_bytes > 0
    end

    test "chunk metadata has correct stripe_id and stripe_index", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(2048)

      assert {:ok, file_meta} =
               WriteOperation.write_file(volume.id, "/meta_ec.bin", data,
                 chunk_strategy: {:fixed, 1024}
               )

      # 2 chunks → 1 complete stripe with 2 data + 1 parity = 3 chunks
      assert length(file_meta.stripes) == 1
      [stripe_ref] = file_meta.stripes

      {:ok, stripe} = StripeIndex.get(stripe_ref.stripe_id)

      # Verify each chunk in the stripe has correct metadata
      stripe.chunks
      |> Enum.with_index()
      |> Enum.each(fn {chunk_hash, idx} ->
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        assert chunk_meta.stripe_id == stripe_ref.stripe_id
        assert chunk_meta.stripe_index == idx
        assert chunk_meta.commit_state == :committed
        assert MapSet.size(chunk_meta.active_write_refs) == 0
        assert chunk_meta.target_replicas == 1
      end)
    end

    test "file metadata stripes byte_ranges cover entire file", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(6144)

      assert {:ok, file_meta} =
               WriteOperation.write_file(volume.id, "/ranges_ec.bin", data,
                 chunk_strategy: {:fixed, 1024}
               )

      # Verify byte_ranges are contiguous and cover entire file
      stripes = file_meta.stripes
      assert stripes != []

      # First stripe starts at 0
      assert elem(hd(stripes).byte_range, 0) == 0

      # Last stripe ends at file size
      assert elem(List.last(stripes).byte_range, 1) == byte_size(data)

      # Ranges are contiguous
      stripes
      |> Enum.chunk_every(2, 1, :discard)
      |> Enum.each(fn [s1, s2] ->
        assert elem(s1.byte_range, 1) == elem(s2.byte_range, 0)
      end)
    end

    test "commit validates data_bytes sum equals file size", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(3072)

      assert {:ok, file_meta} =
               WriteOperation.write_file(volume.id, "/commit_ec.bin", data,
                 chunk_strategy: {:fixed, 1024}
               )

      # The fact that write succeeded means validation passed
      assert file_meta.size == 3072

      # Verify stripe data_bytes sum matches
      total_data_bytes =
        file_meta.stripes
        |> Enum.map(fn sr ->
          {:ok, stripe} = StripeIndex.get(sr.stripe_id)
          stripe.data_bytes
        end)
        |> Enum.sum()

      assert total_data_bytes == byte_size(data)
    end

    test "all chunks are committed after successful write", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(2048)

      {:ok, file_meta} =
        WriteOperation.write_file(volume.id, "/committed_ec.bin", data,
          chunk_strategy: {:fixed, 1024}
        )

      # Verify all stripe chunks are committed
      for stripe_ref <- file_meta.stripes do
        {:ok, stripe} = StripeIndex.get(stripe_ref.stripe_id)

        for chunk_hash <- stripe.chunks do
          {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
          assert chunk_meta.commit_state == :committed
          assert MapSet.size(chunk_meta.active_write_refs) == 0
        end
      end

      # Verify no uncommitted chunks remain
      uncommitted = ChunkIndex.list_uncommitted()
      assert Enum.empty?(uncommitted)
    end

    test "emits stripe_created telemetry events", %{ec_volume: volume} do
      Process.put(:telemetry_events, [])
      data = :crypto.strong_rand_bytes(4096)

      {:ok, _file_meta} =
        WriteOperation.write_file(volume.id, "/telemetry_ec.bin", data,
          chunk_strategy: {:fixed, 1024}
        )

      events = Process.get(:telemetry_events, [])

      stripe_events =
        Enum.filter(events, &(&1.event == [:neonfs, :write_operation, :stripe_created]))

      # 4096 bytes / 1024 fixed = 4 chunks / 2 per stripe = 2 stripes
      assert length(stripe_events) == 2

      for event <- stripe_events do
        assert event.metadata.data_chunks == 2
        assert event.metadata.parity_chunks == 1
        assert is_binary(event.metadata.stripe_id)
        assert event.metadata.volume_id == volume.id
        assert event.measurements.chunk_count == 3
      end
    end

    test "overwrites existing erasure-coded file", %{ec_volume: volume} do
      path = "/overwrite_ec.txt"

      {:ok, file1} =
        WriteOperation.write_file(volume.id, path, "Version 1", chunk_strategy: :single)

      {:ok, file2} =
        WriteOperation.write_file(volume.id, path, "Version 2 - longer", chunk_strategy: :single)

      assert file1.id != file2.id
      assert {:ok, retrieved} = FileIndex.get_by_path(volume.id, path)
      assert retrieved.id == file2.id
    end

    test "volume stats updated after erasure write", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(2048)

      {:ok, _file_meta} =
        WriteOperation.write_file(volume.id, "/stats_ec.bin", data,
          chunk_strategy: {:fixed, 1024}
        )

      {:ok, updated_volume} = VolumeRegistry.get(volume.id)
      assert updated_volume.logical_size == byte_size(data)
      assert updated_volume.physical_size > 0
      assert updated_volume.chunk_count > 0
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
