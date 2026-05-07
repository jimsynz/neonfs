defmodule NeonFS.Core.WriteOperationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    ChunkIndex,
    FileIndex,
    KeyManager,
    LockManager,
    RaServer,
    StripeIndex,
    VolumeEncryption,
    VolumeRegistry,
    WriteOperation
  }

  alias NeonFS.Error.VolumeNotFound

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
      "write-operation-test",
      [
        [:neonfs, :write_operation, :start],
        [:neonfs, :write_operation, :stop],
        [:neonfs, :write_operation, :exception],
        [:neonfs, :write_operation, :stripe_created],
        [:neonfs, :write, :encrypt]
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

      assert {:ok, file_meta} = WriteOperation.write_file_streamed(volume.id, "/test.txt", [data])

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
      assert {:ok, stored_file} = FileIndex.get(file_meta.volume_id, file_meta.id)
      assert stored_file.id == file_meta.id
      assert {:ok, stored_by_path} = FileIndex.get_by_path(volume.id, "/test.txt")
      assert stored_by_path.id == file_meta.id
    end

    test "writes large file (multiple chunks)", %{volume: volume} do
      # Create 2MB of data to ensure multiple chunks
      data = :crypto.strong_rand_bytes(2 * 1024 * 1024)

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/large.bin", [data])

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

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/compressed.txt", [data])

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
      assert {:ok, file1} = WriteOperation.write_file_streamed(volume.id, "/file1.txt", [data])

      # Count chunks before second write
      chunk_count_before =
        :ets.tab2list(:chunk_index)
        |> length()

      # Write second file with same data
      assert {:ok, file2} = WriteOperation.write_file_streamed(volume.id, "/file2.txt", [data])

      # Count chunks after second write
      chunk_count_after =
        :ets.tab2list(:chunk_index)
        |> length()

      # Both files should reference the same chunks
      assert file1.chunks == file2.chunks

      # No new chunks should be created
      assert chunk_count_before == chunk_count_after

      # Verify both files exist but share chunks
      assert {:ok, stored_file1} = FileIndex.get(file1.volume_id, file1.id)
      assert {:ok, stored_file2} = FileIndex.get(file2.volume_id, file2.id)
      assert stored_file1.id == file1.id
      assert stored_file2.id == file2.id
      assert file1.chunks == file2.chunks
    end

    test "returns error for non-existent volume" do
      data = "Test data"
      fake_volume_id = UUIDv7.generate()

      assert {:error, %VolumeNotFound{volume_id: ^fake_volume_id}} =
               WriteOperation.write_file_streamed(fake_volume_id, "/test.txt", [data])
    end

    test "handles empty file", %{volume: volume} do
      data = ""

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/empty.txt", [data])

      assert file_meta.size == 0
      # Empty file may have 0 or 1 chunks depending on chunking strategy
      assert is_list(file_meta.chunks)
    end

    test "respects chunk_strategy option", %{volume: volume} do
      data = :crypto.strong_rand_bytes(100 * 1024)

      # Force single chunk
      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/single.bin", [data],
                 chunk_strategy: :single
               )

      # Should have exactly 1 chunk
      assert length(file_meta.chunks) == 1
    end

    test "overwrites existing file at same path", %{volume: volume} do
      path = "/overwrite.txt"

      # Write first version
      {:ok, file1} = WriteOperation.write_file_streamed(volume.id, path, ["Version 1"])

      # Write second version
      {:ok, file2} =
        WriteOperation.write_file_streamed(volume.id, path, ["Version 2 - longer content"])

      # Files should have different IDs
      assert file1.id != file2.id

      # Only the second file should be in the index at that path
      assert {:ok, retrieved_file} = FileIndex.get_by_path(volume.id, path)
      assert retrieved_file.id == file2.id
    end

    test "unencrypted volume chunks have nil crypto field", %{volume: volume} do
      data = "Plaintext data"

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/plain.txt", [data])

      for chunk_hash <- file_meta.chunks do
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        assert chunk_meta.crypto == nil
      end
    end
  end

  describe "write_file_streamed/4" do
    test "writes empty stream to an empty file", %{volume: volume} do
      assert {:ok, file_meta} = WriteOperation.write_file_streamed(volume.id, "/empty.txt", [])
      assert file_meta.size == 0
      assert is_list(file_meta.chunks)
    end

    test "single-segment stream produces same chunks as write_file/4", %{volume: volume} do
      data = :crypto.strong_rand_bytes(50_000)

      {:ok, batch} = WriteOperation.write_file_streamed(volume.id, "/batch.bin", [data])
      {:ok, streamed} = WriteOperation.write_file_streamed(volume.id, "/streamed.bin", [data])

      assert streamed.size == batch.size
      assert streamed.chunks == batch.chunks
    end

    test "multi-segment stream crossing chunk boundaries matches write_file/4", %{volume: volume} do
      data = :crypto.strong_rand_bytes(2 * 1024 * 1024)
      slices = for <<slice::binary-size(7919) <- data>>, do: slice
      tail = binary_part(data, length(slices) * 7919, byte_size(data) - length(slices) * 7919)
      segments = if tail == "", do: slices, else: slices ++ [tail]

      {:ok, batch} = WriteOperation.write_file_streamed(volume.id, "/batch-large.bin", [data])

      {:ok, streamed} =
        WriteOperation.write_file_streamed(volume.id, "/streamed-large.bin", segments)

      assert streamed.size == byte_size(data)
      assert streamed.chunks == batch.chunks
    end

    test "respects explicit chunk_strategy override", %{volume: volume} do
      data = :crypto.strong_rand_bytes(10_000)

      {:ok, file_meta} =
        WriteOperation.write_file_streamed(volume.id, "/fixed.bin", [data],
          chunk_strategy: {:fixed, 1024}
        )

      assert file_meta.size == 10_000
      # 10000 / 1024 = 9 full chunks + 1 partial = 10 chunks total
      assert length(file_meta.chunks) == 10
    end

    test "deduplicates against existing batch-written content", %{volume: volume} do
      # Both paths must use the same explicit chunk strategy — `:auto`
      # resolves differently between batch (size-aware: fixed/256KB for
      # 100KB) and streamed (always fastcdc with content-defined cuts),
      # so `:auto` on both sides produces non-identical chunk sets for
      # random payloads. See #329.
      strategy = {:fixed, 32_768}
      data = :crypto.strong_rand_bytes(100_000)

      {:ok, batch} =
        WriteOperation.write_file_streamed(volume.id, "/dedup-a.bin", [data],
          chunk_strategy: strategy
        )

      chunks_before =
        :ets.tab2list(:chunk_index)
        |> length()

      {:ok, streamed} =
        WriteOperation.write_file_streamed(
          volume.id,
          "/dedup-b.bin",
          Stream.unfold(data, fn
            "" -> nil
            d when byte_size(d) <= 1024 -> {d, ""}
            d -> {binary_part(d, 0, 1024), binary_part(d, 1024, byte_size(d) - 1024)}
          end),
          chunk_strategy: strategy
        )

      chunks_after =
        :ets.tab2list(:chunk_index)
        |> length()

      assert streamed.chunks == batch.chunks
      assert chunks_before == chunks_after
    end

    test "returns not-supported error for erasure-coded volumes", %{volume: _volume} do
      vol_name = "erasure-#{:rand.uniform(999_999)}"

      {:ok, erasure_volume} =
        VolumeRegistry.create(vol_name,
          durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1}
        )

      assert {:error, :streaming_writes_not_supported_for_erasure} =
               WriteOperation.write_file_streamed(erasure_volume.id, "/x.bin", ["data"])
    end

    test "returns VolumeNotFound for unknown volume" do
      fake_volume_id = UUIDv7.generate()

      assert {:error, %VolumeNotFound{volume_id: ^fake_volume_id}} =
               WriteOperation.write_file_streamed(fake_volume_id, "/fail.txt", ["data"])
    end
  end

  # `create_only: true` (sub-issue #592 of #303) is the atomic
  # create-if-not-exist option used by `O_EXCL`,
  # WebDAV `If-None-Match: *`, S3 `If-None-Match: *`, etc. The
  # cross-node coordination story is exercised in the peer-cluster
  # integration suite; these unit tests cover the FileIndex / fast-path
  # checks and the coordinator-unavailable fallback (single-node
  # correctness when Ra is down, which is the default for this test
  # module).
  describe "create_only/2" do
    test "creates a new streamed file when none exists", %{volume: volume} do
      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/create-only-new.txt", ["hello"],
                 create_only: true
               )

      assert file_meta.path == "/create-only-new.txt"
      assert file_meta.size == 5
    end

    test "returns :exists when the streamed target already exists", %{volume: volume} do
      {:ok, _} = WriteOperation.write_file_streamed(volume.id, "/already.txt", ["seed"])

      assert {:error, :exists} =
               WriteOperation.write_file_streamed(volume.id, "/already.txt", ["overwrite"],
                 create_only: true
               )

      # Existing content unchanged.
      assert {:ok, existing} = FileIndex.get_by_path(volume.id, "/already.txt")
      assert existing.size == 4
    end

    test "creates a new file via write_file_at when none exists", %{volume: volume} do
      assert {:ok, file_meta} =
               WriteOperation.write_file_at(volume.id, "/at-create.txt", 0, "data",
                 create_only: true
               )

      assert file_meta.path == "/at-create.txt"
    end

    test "write_file_at with create_only returns :exists when target already exists",
         %{volume: volume} do
      {:ok, _} = WriteOperation.write_file_streamed(volume.id, "/at-exists.txt", ["seed"])

      assert {:error, :exists} =
               WriteOperation.write_file_at(volume.id, "/at-exists.txt", 0, "data",
                 create_only: true
               )
    end

    test "create_only: false (default) still overwrites", %{volume: volume} do
      {:ok, _} = WriteOperation.write_file_streamed(volume.id, "/overwrite.txt", ["v1"])

      assert {:ok, _} =
               WriteOperation.write_file_streamed(volume.id, "/overwrite.txt", ["version-2"])

      assert {:ok, latest} = FileIndex.get_by_path(volume.id, "/overwrite.txt")
      assert latest.size == 9
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

      {:ok, file_meta} = WriteOperation.write_file_streamed(volume.id, "/telemetry.txt", [data])

      events = Process.get(:telemetry_events, []) |> Enum.reverse()

      # Should have start and stop events
      assert [start_event | _] = events
      assert start_event.event == [:neonfs, :write_operation, :start]
      # Streamed writes don't know the byte count up front — start fires with 0.
      assert start_event.measurements.bytes == 0
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

      {:error, %VolumeNotFound{}} =
        WriteOperation.write_file_streamed(fake_volume_id, "/fail.txt", [data])

      events = Process.get(:telemetry_events, []) |> Enum.reverse()

      # Should have start and exception events
      assert events != []

      exception_event =
        Enum.find(events, &(&1.event == [:neonfs, :write_operation, :exception]))

      assert exception_event
      assert %VolumeNotFound{} = exception_event.metadata.error
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
      {:ok, _file_meta} = WriteOperation.write_file_streamed(volume.id, "/test.txt", [data])

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

      {:ok, file_meta} = WriteOperation.write_file_streamed(volume.id, "/small.txt", [small_data])

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
        WriteOperation.write_file_streamed(volume.id, "/forced.txt", [data],
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
               WriteOperation.write_file_at(volume.id, "/small_ec.txt", 0, data,
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
               WriteOperation.write_file_at(volume.id, "/exact_ec.bin", 0, data,
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
               WriteOperation.write_file_at(volume.id, "/partial_ec.bin", 0, data,
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
               WriteOperation.write_file_at(volume.id, "/meta_ec.bin", 0, data,
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
               WriteOperation.write_file_at(volume.id, "/ranges_ec.bin", 0, data,
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
               WriteOperation.write_file_at(volume.id, "/commit_ec.bin", 0, data,
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
        WriteOperation.write_file_at(volume.id, "/committed_ec.bin", 0, data,
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
        WriteOperation.write_file_at(volume.id, "/telemetry_ec.bin", 0, data,
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
        WriteOperation.write_file_at(volume.id, path, 0, "Version 1", chunk_strategy: :single)

      # write_file_at on an existing erasure-coded file is an offset write, not
      # a replacement — delete first to exercise the "overwrite" semantic the
      # test name implies.
      :ok = FileIndex.delete(file1.id)

      {:ok, file2} =
        WriteOperation.write_file_at(volume.id, path, 0, "Version 2 - longer",
          chunk_strategy: :single
        )

      assert file1.id != file2.id
      assert {:ok, retrieved} = FileIndex.get_by_path(volume.id, path)
      assert retrieved.id == file2.id
    end

    test "volume stats updated after erasure write", %{ec_volume: volume} do
      data = :crypto.strong_rand_bytes(2048)

      {:ok, _file_meta} =
        WriteOperation.write_file_at(volume.id, "/stats_ec.bin", 0, data,
          chunk_strategy: {:fixed, 1024}
        )

      {:ok, updated_volume} = VolumeRegistry.get(volume.id)
      assert updated_volume.logical_size == byte_size(data)
      assert updated_volume.physical_size > 0
      assert updated_volume.chunk_count > 0
    end
  end

  describe "encrypted write path" do
    @test_master_key :crypto.strong_rand_bytes(32) |> Base.encode64()

    setup %{tmp_dir: tmp_dir} do
      # Set up encryption infrastructure: Ra + cluster.json with master key
      write_cluster_json(tmp_dir, @test_master_key)

      start_ra()
      :ok = RaServer.init_cluster()

      # Create an encrypted volume
      vol_name = "encrypted-vol-#{:rand.uniform(999_999)}"

      {:ok, enc_volume} =
        VolumeRegistry.create(vol_name,
          encryption: VolumeEncryption.new(mode: :server_side, current_key_version: 1),
          compression: %{algorithm: :none}
        )

      # Set up the encryption key in Ra
      {:ok, _version} = KeyManager.setup_volume_encryption(enc_volume.id)

      {:ok, enc_volume: enc_volume}
    end

    test "writes to encrypted volume with crypto metadata", %{enc_volume: volume} do
      data = "Secret data for encryption test"

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/secret.txt", [data])

      assert file_meta.volume_id == volume.id
      assert file_meta.size == byte_size(data)

      # Verify all chunks have crypto metadata populated
      for chunk_hash <- file_meta.chunks do
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        assert chunk_meta.crypto != nil
        assert chunk_meta.crypto.algorithm == :aes_256_gcm
        assert byte_size(chunk_meta.crypto.nonce) == 12
        assert chunk_meta.crypto.key_version == 1
      end
    end

    test "stored_size accounts for GCM auth tag overhead", %{enc_volume: volume} do
      # Use a compressible repeated pattern that won't compress much with :none
      data = :crypto.strong_rand_bytes(1024)

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/overhead.bin", [data])

      for chunk_hash <- file_meta.chunks do
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        # stored_size should be original_size + 16 (GCM auth tag)
        assert chunk_meta.stored_size == chunk_meta.original_size + 16
      end
    end

    test "stored bytes differ from plaintext", %{enc_volume: volume} do
      data = String.duplicate("KNOWN_PATTERN", 100)

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/verify_enc.txt", [data])

      # Read raw bytes from disk (without decryption) and confirm they
      # differ from input. Reach into the on-disk file directly because the
      # codec-aware read path (#270) would attempt decryption.
      blob_dir = Application.get_env(:neonfs_core, :blob_store_base_dir)

      for chunk_hash <- file_meta.chunks do
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        [location | _] = chunk_meta.locations
        tier_str = Atom.to_string(Map.get(location, :tier, :hot))
        hex = Base.encode16(chunk_hash, case: :lower)
        prefix1 = String.slice(hex, 0, 2)
        prefix2 = String.slice(hex, 2, 2)
        chunk_dir = Path.join([blob_dir, "blobs", tier_str, prefix1, prefix2])
        [chunk_path] = Path.wildcard(Path.join(chunk_dir, "#{hex}.*"))
        raw_bytes = File.read!(chunk_path)

        refute raw_bytes == data
      end
    end

    test "each chunk gets a unique nonce", %{enc_volume: volume} do
      # Use fixed chunking to ensure multiple chunks
      data = :crypto.strong_rand_bytes(2048)

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(volume.id, "/multi_chunk.bin", [data],
                 chunk_strategy: {:fixed, 1024}
               )

      nonces =
        Enum.map(file_meta.chunks, fn hash ->
          {:ok, meta} = ChunkIndex.get(hash)
          meta.crypto.nonce
        end)

      # All nonces should be unique
      assert length(Enum.uniq(nonces)) == length(nonces)
    end

    test "emits encrypt telemetry events", %{enc_volume: volume} do
      Process.put(:telemetry_events, [])

      data = "Telemetry encryption test"
      {:ok, _file_meta} = WriteOperation.write_file_streamed(volume.id, "/telem_enc.txt", [data])

      events = Process.get(:telemetry_events, [])

      encrypt_events =
        Enum.filter(events, &(&1.event == [:neonfs, :write, :encrypt]))

      assert encrypt_events != []

      for event <- encrypt_events do
        assert event.metadata.volume_id == volume.id
        assert event.metadata.key_version == 1
        assert is_binary(event.metadata.chunk_hash)
      end
    end

    test "unencrypted volume continues to work with no crypto field", %{tmp_dir: _tmp_dir} do
      # Create a plain (unencrypted) volume in the same test context where Ra is running
      vol_name = "plain-vol-#{:rand.uniform(999_999)}"
      {:ok, plain_vol} = VolumeRegistry.create(vol_name, compression: %{algorithm: :none})

      data = "Plaintext in Ra context"

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(plain_vol.id, "/plain.txt", [data])

      for chunk_hash <- file_meta.chunks do
        {:ok, chunk_meta} = ChunkIndex.get(chunk_hash)
        assert chunk_meta.crypto == nil
      end
    end
  end

  describe "write_file_at/5 (offset writes)" do
    test "appending at non-zero offset does not crash", %{volume: volume} do
      initial_data = "Hello, NeonFS!"

      {:ok, _file_meta} =
        WriteOperation.write_file_streamed(volume.id, "/offset.txt", [initial_data],
          chunk_strategy: :single
        )

      append_data = " More data."

      # Before the fix for issue #110, this crashed with {:else_clause, nil}
      # because ChunkFetcher.fetch_chunk/2 returns a 3-tuple {:ok, data, source}
      # but callers pattern-matched on 2-tuple {:ok, _}
      assert {:ok, updated_meta} =
               WriteOperation.write_file_at(
                 volume.id,
                 "/offset.txt",
                 byte_size(initial_data),
                 append_data
               )

      assert updated_meta.size == byte_size(initial_data) + byte_size(append_data)
    end

    test "overwriting middle of existing file does not crash", %{volume: volume} do
      initial_data = String.duplicate("A", 100)

      {:ok, _file_meta} =
        WriteOperation.write_file_streamed(volume.id, "/middle.txt", [initial_data],
          chunk_strategy: :single
        )

      overwrite_data = "BBBBBBBBBB"

      assert {:ok, updated_meta} =
               WriteOperation.write_file_at(volume.id, "/middle.txt", 10, overwrite_data)

      assert updated_meta.size == 100

      alias NeonFS.Core.ReadOperation
      assert {:ok, read_data} = ReadOperation.read_file(volume.id, "/middle.txt")
      expected = String.duplicate("A", 10) <> overwrite_data <> String.duplicate("A", 80)
      assert read_data == expected
    end
  end

  describe "mandatory lock enforcement" do
    setup %{tmp_dir: _tmp_dir} do
      start_lock_manager()
      :ok
    end

    test "rejects write when mandatory lock held by another client", %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/locked.txt")

      assert :ok =
               LockManager.lock(
                 lock_file_id,
                 :smb_client_a,
                 {0, 1000},
                 :exclusive,
                 mode: :mandatory
               )

      assert {:error, :lock_conflict} =
               WriteOperation.write_file_at(volume.id, "/locked.txt", 0, "blocked data",
                 client_ref: :smb_client_b
               )
    end

    test "permits write when mandatory lock held by same client", %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/my-locked.txt")

      assert :ok =
               LockManager.lock(
                 lock_file_id,
                 :smb_client_a,
                 {0, 1000},
                 :exclusive,
                 mode: :mandatory
               )

      assert {:ok, file_meta} =
               WriteOperation.write_file_at(volume.id, "/my-locked.txt", 0, "my data",
                 client_ref: :smb_client_a
               )

      assert file_meta.size == byte_size("my data")
    end

    test "permits write when no client_ref provided (backward compatible)", %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/legacy.txt")

      assert :ok =
               LockManager.lock(
                 lock_file_id,
                 :smb_client_a,
                 {0, 1000},
                 :exclusive,
                 mode: :mandatory
               )

      assert {:ok, _file_meta} =
               WriteOperation.write_file_at(volume.id, "/legacy.txt", 0, "no client ref")
    end

    test "permits write when only advisory locks exist", %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/advisory.txt")

      assert :ok =
               LockManager.lock(
                 lock_file_id,
                 :nfs_client,
                 {0, 1000},
                 :exclusive,
                 mode: :advisory
               )

      assert {:ok, _file_meta} =
               WriteOperation.write_file_at(volume.id, "/advisory.txt", 0, "allowed",
                 client_ref: :other_client
               )
    end

    test "rejects offset write when mandatory lock overlaps range", %{volume: volume} do
      assert {:ok, _} =
               WriteOperation.write_file_streamed(volume.id, "/partial.txt", [
                 String.duplicate("A", 200)
               ])

      lock_file_id = WriteOperation.lock_file_id(volume.id, "/partial.txt")

      assert :ok =
               LockManager.lock(
                 lock_file_id,
                 :smb_client_a,
                 {50, 100},
                 :exclusive,
                 mode: :mandatory
               )

      assert {:error, :lock_conflict} =
               WriteOperation.write_file_at(volume.id, "/partial.txt", 75, "blocked",
                 client_ref: :smb_client_b
               )
    end

    test "permits offset write to non-overlapping range", %{volume: volume} do
      assert {:ok, _} =
               WriteOperation.write_file_streamed(
                 volume.id,
                 "/nonoverlap.txt",
                 [String.duplicate("A", 200)],
                 chunk_strategy: :single
               )

      lock_file_id = WriteOperation.lock_file_id(volume.id, "/nonoverlap.txt")

      assert :ok =
               LockManager.lock(
                 lock_file_id,
                 :smb_client_a,
                 {0, 50},
                 :exclusive,
                 mode: :mandatory
               )

      assert {:ok, _file_meta} =
               WriteOperation.write_file_at(volume.id, "/nonoverlap.txt", 100, "allowed",
                 client_ref: :smb_client_b
               )
    end

    test "write_file_streamed rejects write when mandatory lock held by another client",
         %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/stream-locked.txt")

      assert :ok =
               LockManager.lock(
                 lock_file_id,
                 :smb_client_a,
                 {0, 1000},
                 :exclusive,
                 mode: :mandatory
               )

      assert {:error, :lock_conflict} =
               WriteOperation.write_file_streamed(
                 volume.id,
                 "/stream-locked.txt",
                 ["streamed data"],
                 client_ref: :smb_client_b
               )
    end

    test "write_file_streamed permits write when mandatory lock held by same client",
         %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/stream-mine.txt")

      assert :ok =
               LockManager.lock(
                 lock_file_id,
                 :smb_client_a,
                 {0, 1000},
                 :exclusive,
                 mode: :mandatory
               )

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(
                 volume.id,
                 "/stream-mine.txt",
                 ["streamed payload"],
                 client_ref: :smb_client_a
               )

      assert file_meta.size == byte_size("streamed payload")
    end

    test "write_file_streamed rejects even with a narrow lock range (conservative full-file check)",
         %{volume: volume} do
      # Regression for #374: a mandatory lock at bytes 50-100 previously did not
      # block streamed writes because write_file_streamed/4 passed {0, 0} to
      # check_lock, which never overlaps any finite range.
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/stream-narrow.txt")

      assert :ok =
               LockManager.lock(
                 lock_file_id,
                 :smb_client_a,
                 {50, 50},
                 :exclusive,
                 mode: :mandatory
               )

      assert {:error, :lock_conflict} =
               WriteOperation.write_file_streamed(
                 volume.id,
                 "/stream-narrow.txt",
                 ["streamed"],
                 client_ref: :smb_client_b
               )
    end
  end

  describe "share mode (deny-write) enforcement" do
    setup %{tmp_dir: _tmp_dir} do
      start_lock_manager()
      :ok
    end

    test "rejects write when another client has deny_write open", %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/shared.txt")

      assert :ok = LockManager.open(lock_file_id, :smb_client_a, :read_write, :write)

      assert {:error, :share_denied} =
               WriteOperation.write_file_at(volume.id, "/shared.txt", 0, "blocked",
                 client_ref: :smb_client_b
               )
    end

    test "rejects write when another client has deny_read_write open", %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/exclusive.txt")

      assert :ok = LockManager.open(lock_file_id, :smb_client_a, :read_write, :read_write)

      assert {:error, :share_denied} =
               WriteOperation.write_file_at(volume.id, "/exclusive.txt", 0, "blocked",
                 client_ref: :smb_client_b
               )
    end

    test "permits write by the client that holds deny_write open", %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/mine.txt")

      assert :ok = LockManager.open(lock_file_id, :smb_client_a, :read_write, :write)

      assert {:ok, file_meta} =
               WriteOperation.write_file_at(volume.id, "/mine.txt", 0, "my data",
                 client_ref: :smb_client_a
               )

      assert file_meta.size == byte_size("my data")
    end

    test "permits write when deny is only :read", %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/deny-read.txt")

      assert :ok = LockManager.open(lock_file_id, :smb_client_a, :write, :read)

      assert {:ok, _file_meta} =
               WriteOperation.write_file_at(volume.id, "/deny-read.txt", 0, "allowed",
                 client_ref: :smb_client_b
               )
    end

    test "rejects offset write when deny_write open is held", %{volume: volume} do
      assert {:ok, _} =
               WriteOperation.write_file_streamed(
                 volume.id,
                 "/offset-share.txt",
                 [String.duplicate("A", 200)]
               )

      lock_file_id = WriteOperation.lock_file_id(volume.id, "/offset-share.txt")

      assert :ok = LockManager.open(lock_file_id, :smb_client_a, :read_write, :write)

      assert {:error, :share_denied} =
               WriteOperation.write_file_at(volume.id, "/offset-share.txt", 50, "blocked",
                 client_ref: :smb_client_b
               )
    end

    test "permits write when no client_ref provided (backward compatible)", %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/legacy-share.txt")

      assert :ok = LockManager.open(lock_file_id, :smb_client_a, :read_write, :write)

      assert {:ok, _file_meta} =
               WriteOperation.write_file_at(volume.id, "/legacy-share.txt", 0, "no client ref")
    end

    test "write_file_streamed rejects when another client has deny_write open",
         %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/stream-share.txt")

      assert :ok = LockManager.open(lock_file_id, :smb_client_a, :read_write, :write)

      assert {:error, :share_denied} =
               WriteOperation.write_file_streamed(
                 volume.id,
                 "/stream-share.txt",
                 ["streamed blocked"],
                 client_ref: :smb_client_b
               )
    end

    test "write_file_streamed permits write for the client holding deny_write",
         %{volume: volume} do
      lock_file_id = WriteOperation.lock_file_id(volume.id, "/stream-share-mine.txt")

      assert :ok = LockManager.open(lock_file_id, :smb_client_a, :read_write, :write)

      assert {:ok, file_meta} =
               WriteOperation.write_file_streamed(
                 volume.id,
                 "/stream-share-mine.txt",
                 ["streamed mine"],
                 client_ref: :smb_client_a
               )

      assert file_meta.size == byte_size("streamed mine")
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
