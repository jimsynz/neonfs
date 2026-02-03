defmodule NeonFS.Core.ReadOperationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{ReadOperation, VolumeRegistry, WriteOperation}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_volume_registry()

    # Set up telemetry test handler
    :telemetry.attach_many(
      "read-operation-test",
      [
        [:neonfs, :read_operation, :start],
        [:neonfs, :read_operation, :stop],
        [:neonfs, :read_operation, :exception]
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
      assert {:error, :file_not_found} =
               ReadOperation.read_file(volume.id, "/does-not-exist.txt")
    end

    test "returns error for non-existent volume" do
      fake_volume_id = UUIDv7.generate()

      assert {:error, :volume_not_found} =
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
      {:error, :file_not_found} = ReadOperation.read_file(volume.id, "/not-found.txt")

      events = Process.get(:telemetry_events, []) |> Enum.reverse()

      # Should have start and exception events
      exception_event =
        Enum.find(events, &(&1.event == [:neonfs, :read_operation, :exception]))

      assert exception_event
      assert exception_event.metadata.error == :file_not_found
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

  # Telemetry event handler
  defp handle_telemetry_event(event, measurements, metadata, _config) do
    events = Process.get(:telemetry_events, [])

    Process.put(:telemetry_events, [
      %{event: event, measurements: measurements, metadata: metadata} | events
    ])
  end
end
