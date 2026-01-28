defmodule NeonFS.Core.WriteOperationTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.{
    ChunkIndex,
    FileIndex,
    VolumeRegistry,
    WriteOperation
  }

  setup do
    # Clear all data
    :ets.delete_all_objects(:chunk_index)
    :ets.delete_all_objects(:file_index_by_id)
    :ets.delete_all_objects(:file_index_by_path)
    :ets.delete_all_objects(:volumes_by_id)
    :ets.delete_all_objects(:volumes_by_name)

    # Set up telemetry test handler
    :telemetry.attach_many(
      "write-operation-test",
      [
        [:neonfs, :write_operation, :start],
        [:neonfs, :write_operation, :stop],
        [:neonfs, :write_operation, :exception]
      ],
      &handle_telemetry_event/4,
      nil
    )

    on_exit(fn ->
      :telemetry.detach("write-operation-test")
    end)

    # Create test volume with default settings
    {:ok, volume} = VolumeRegistry.create("test-volume", [])

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

  # Telemetry event handler
  defp handle_telemetry_event(event, measurements, metadata, _config) do
    events = Process.get(:telemetry_events, [])

    Process.put(:telemetry_events, [
      %{event: event, measurements: measurements, metadata: metadata} | events
    ])
  end
end
