defmodule NeonFS.Core.BlobStoreTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.BlobStore

  @tmp_dir_prefix "blob_store_test_"

  setup do
    # Create a unique temporary directory for each test
    tmp_dir = Path.join(System.tmp_dir!(), @tmp_dir_prefix <> random_string())
    File.mkdir_p!(tmp_dir)

    # Start a BlobStore for this test
    {:ok, pid} =
      start_supervised({BlobStore, base_dir: tmp_dir, prefix_depth: 2, name: test_server()})

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
    end)

    %{store: pid, tmp_dir: tmp_dir}
  end

  describe "write_chunk/3 and read_chunk/3" do
    test "basic write and read roundtrip", %{store: _store} do
      data = "hello world"

      assert {:ok, hash, info} = BlobStore.write_chunk(data, "hot", server: test_server())
      assert byte_size(hash) == 32
      assert info.original_size == byte_size(data)
      assert info.stored_size > 0
      assert info.compression == "none"

      assert {:ok, ^data} = BlobStore.read_chunk(hash, "hot", server: test_server())
    end

    test "write and read with compression", %{store: _store} do
      data = String.duplicate("hello world ", 1000)

      assert {:ok, hash, info} =
               BlobStore.write_chunk(data, "hot",
                 compression: "zstd",
                 compression_level: 3,
                 server: test_server()
               )

      assert byte_size(hash) == 32
      assert info.original_size == byte_size(data)
      assert info.stored_size < info.original_size
      assert info.compression == "zstd:3"

      # Read without decompression returns compressed data
      assert {:ok, compressed} = BlobStore.read_chunk(hash, "hot", server: test_server())
      assert byte_size(compressed) < byte_size(data)

      # Read with decompression returns original data
      assert {:ok, decompressed} =
               BlobStore.read_chunk_with_options(hash, "hot",
                 decompress: true,
                 server: test_server()
               )

      assert decompressed == data
    end

    test "read with verification succeeds for valid chunk", %{store: _store} do
      data = "test data"

      assert {:ok, hash, _info} = BlobStore.write_chunk(data, "hot", server: test_server())

      assert {:ok, ^data} =
               BlobStore.read_chunk_with_options(hash, "hot", verify: true, server: test_server())
    end

    test "write to different tiers", %{store: _store} do
      data = "test data"

      for tier <- ["hot", "warm", "cold"] do
        {:ok, hash, _info} = BlobStore.write_chunk(data, tier, server: test_server())
        {:ok, ^data} = BlobStore.read_chunk(hash, tier, server: test_server())
      end
    end

    test "reading nonexistent chunk returns error", %{store: _store} do
      fake_hash = :crypto.strong_rand_bytes(32)

      assert {:error, reason} = BlobStore.read_chunk(fake_hash, "hot", server: test_server())
      assert reason =~ "not found" or reason =~ "NotFound"
    end

    test "empty data roundtrip", %{store: _store} do
      data = ""

      assert {:ok, hash, info} = BlobStore.write_chunk(data, "hot", server: test_server())
      assert byte_size(hash) == 32
      assert info.original_size == 0

      assert {:ok, ^data} = BlobStore.read_chunk(hash, "hot", server: test_server())
    end
  end

  describe "delete_chunk/3" do
    test "deletes an existing chunk", %{store: _store} do
      data = "delete me"

      assert {:ok, hash, _info} = BlobStore.write_chunk(data, "hot", server: test_server())
      assert {:ok, ^data} = BlobStore.read_chunk(hash, "hot", server: test_server())

      assert {:ok, {}} = BlobStore.delete_chunk(hash, "hot", server: test_server())

      # Reading after delete should fail
      assert {:error, _reason} = BlobStore.read_chunk(hash, "hot", server: test_server())
    end

    test "deleting nonexistent chunk returns error", %{store: _store} do
      fake_hash = :crypto.strong_rand_bytes(32)

      assert {:error, reason} = BlobStore.delete_chunk(fake_hash, "hot", server: test_server())
      assert reason =~ "not found" or reason =~ "NotFound"
    end
  end

  describe "migrate_chunk/4" do
    test "migrates chunk from hot to cold", %{store: _store} do
      data = "migrate me"

      assert {:ok, hash, _info} = BlobStore.write_chunk(data, "hot", server: test_server())
      assert {:ok, ^data} = BlobStore.read_chunk(hash, "hot", server: test_server())

      assert {:ok, {}} = BlobStore.migrate_chunk(hash, "hot", "cold", server: test_server())

      # Should no longer exist in hot tier
      assert {:error, _} = BlobStore.read_chunk(hash, "hot", server: test_server())

      # Should exist in cold tier
      assert {:ok, ^data} = BlobStore.read_chunk(hash, "cold", server: test_server())
    end

    test "migrates chunk from cold to warm", %{store: _store} do
      data = "migrate me back"

      assert {:ok, hash, _info} = BlobStore.write_chunk(data, "cold", server: test_server())

      assert {:ok, {}} = BlobStore.migrate_chunk(hash, "cold", "warm", server: test_server())

      assert {:error, _} = BlobStore.read_chunk(hash, "cold", server: test_server())
      assert {:ok, ^data} = BlobStore.read_chunk(hash, "warm", server: test_server())
    end

    test "migrating nonexistent chunk returns error", %{store: _store} do
      fake_hash = :crypto.strong_rand_bytes(32)

      assert {:error, reason} =
               BlobStore.migrate_chunk(fake_hash, "hot", "cold", server: test_server())

      assert reason =~ "not found" or reason =~ "NotFound"
    end

    test "migration preserves data integrity", %{store: _store} do
      data = String.duplicate("important data ", 100)

      assert {:ok, hash, _info} = BlobStore.write_chunk(data, "hot", server: test_server())
      assert {:ok, {}} = BlobStore.migrate_chunk(hash, "hot", "warm", server: test_server())
      assert {:ok, {}} = BlobStore.migrate_chunk(hash, "warm", "cold", server: test_server())
      assert {:ok, {}} = BlobStore.migrate_chunk(hash, "cold", "hot", server: test_server())

      assert {:ok, ^data} = BlobStore.read_chunk(hash, "hot", server: test_server())
    end
  end

  describe "chunk_data/3" do
    test "single strategy for small data", %{store: _store} do
      data = "small data"

      assert {:ok, chunks} = BlobStore.chunk_data(data, {:single}, server: test_server())
      assert length(chunks) == 1

      [{chunk_data, hash, offset, size}] = chunks
      assert chunk_data == data
      assert byte_size(hash) == 32
      assert offset == 0
      assert size == byte_size(data)
    end

    test "auto strategy selection", %{store: _store} do
      # Small data (< 64KB) should use single strategy
      small_data = String.duplicate("x", 1000)

      assert {:ok, [_single_chunk]} =
               BlobStore.chunk_data(small_data, :auto, server: test_server())

      # Medium data (64KB - 1MB) should use fixed strategy
      medium_data = String.duplicate("x", 100_000)
      assert {:ok, chunks} = BlobStore.chunk_data(medium_data, :auto, server: test_server())
      # Fixed 256KB chunks means 100KB should be 1 chunk
      assert chunks != []

      # Large data (>= 1MB) should use FastCDC
      large_data = String.duplicate("x", 2_000_000)

      assert {:ok, [_first | _rest] = chunks} =
               BlobStore.chunk_data(large_data, :auto, server: test_server())

      assert chunks != [] and chunks != [_first]
    end

    test "fixed strategy with specified size", %{store: _store} do
      data = String.duplicate("fixed chunk test ", 1000)
      chunk_size = 1024

      assert {:ok, [_first | _rest] = chunks} =
               BlobStore.chunk_data(data, {:fixed, chunk_size}, server: test_server())

      # Verify reconstruction
      reconstructed =
        chunks
        |> Enum.sort_by(fn {_data, _hash, offset, _size} -> offset end)
        |> Enum.map_join(fn {chunk_data, _hash, _offset, _size} -> chunk_data end)

      assert reconstructed == data
    end

    test "fastcdc strategy", %{store: _store} do
      data = String.duplicate("content defined chunking ", 10_000)

      assert {:ok, chunks} =
               BlobStore.chunk_data(data, {:fastcdc, 256 * 1024}, server: test_server())

      assert chunks != []

      # Verify reconstruction
      reconstructed =
        chunks
        |> Enum.sort_by(fn {_data, _hash, offset, _size} -> offset end)
        |> Enum.map_join(fn {chunk_data, _hash, _offset, _size} -> chunk_data end)

      assert reconstructed == data

      # Verify all hashes are correct
      for {chunk_data, hash, _offset, _size} <- chunks do
        expected_hash = Native.compute_hash(chunk_data)
        assert hash == expected_hash
      end
    end

    test "empty data chunking", %{store: _store} do
      data = ""

      # Empty data may return empty list or single empty chunk depending on strategy
      assert {:ok, chunks} = BlobStore.chunk_data(data, :auto, server: test_server())

      # If chunks are returned, verify they're valid
      for {chunk_data, hash, offset, size} <- chunks do
        assert chunk_data == ""
        assert byte_size(hash) == 32
        assert offset == 0
        assert size == 0
      end
    end
  end

  describe "telemetry events" do
    test "emits write_chunk events", %{store: _store} do
      ref = telemetry_listen([:neonfs, :blob_store, :write_chunk])

      data = "telemetry test"
      assert {:ok, _hash, _info} = BlobStore.write_chunk(data, "hot", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :write_chunk, :start], _measurements,
                      metadata}

      assert metadata.tier == "hot"
      assert metadata.data_size == byte_size(data)

      assert_receive {:telemetry, [:neonfs, :blob_store, :write_chunk, :stop], measurements,
                      _metadata}

      assert measurements.duration > 0
      assert measurements.bytes_written > 0

      :telemetry.detach(ref)
    end

    test "emits read_chunk events", %{store: _store} do
      data = "read telemetry test"
      assert {:ok, hash, _info} = BlobStore.write_chunk(data, "hot", server: test_server())

      ref = telemetry_listen([:neonfs, :blob_store, :read_chunk])

      assert {:ok, _data} = BlobStore.read_chunk(hash, "hot", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :read_chunk, :start], _measurements,
                      metadata}

      assert metadata.tier == "hot"
      assert metadata.verify == false
      assert metadata.decompress == false

      assert_receive {:telemetry, [:neonfs, :blob_store, :read_chunk, :stop], measurements,
                      _metadata}

      assert measurements.duration > 0
      assert measurements.bytes_read > 0

      :telemetry.detach(ref)
    end

    test "emits delete_chunk events", %{store: _store} do
      data = "delete telemetry test"
      assert {:ok, hash, _info} = BlobStore.write_chunk(data, "hot", server: test_server())

      ref = telemetry_listen([:neonfs, :blob_store, :delete_chunk])

      assert {:ok, {}} = BlobStore.delete_chunk(hash, "hot", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :delete_chunk, :start], _measurements,
                      _metadata}

      assert_receive {:telemetry, [:neonfs, :blob_store, :delete_chunk, :stop], measurements,
                      _metadata}

      assert measurements.duration > 0

      :telemetry.detach(ref)
    end

    test "emits migrate_chunk events", %{store: _store} do
      data = "migrate telemetry test"
      assert {:ok, hash, _info} = BlobStore.write_chunk(data, "hot", server: test_server())

      ref = telemetry_listen([:neonfs, :blob_store, :migrate_chunk])

      assert {:ok, {}} = BlobStore.migrate_chunk(hash, "hot", "cold", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :migrate_chunk, :start], _measurements,
                      metadata}

      assert metadata.from_tier == "hot"
      assert metadata.to_tier == "cold"

      assert_receive {:telemetry, [:neonfs, :blob_store, :migrate_chunk, :stop], measurements,
                      _metadata}

      assert measurements.duration > 0

      :telemetry.detach(ref)
    end

    test "emits exception events on error", %{store: _store} do
      ref = telemetry_listen([:neonfs, :blob_store, :read_chunk])

      fake_hash = :crypto.strong_rand_bytes(32)
      assert {:error, _reason} = BlobStore.read_chunk(fake_hash, "hot", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :read_chunk, :start], _measurements,
                      _metadata}

      assert_receive {:telemetry, [:neonfs, :blob_store, :read_chunk, :exception], measurements,
                      metadata}

      assert measurements.duration > 0
      assert is_binary(metadata.error)

      :telemetry.detach(ref)
    end
  end

  describe "supervision" do
    test "blob store starts and registers name" do
      # Start a named BlobStore with truly unique name and ID
      unique_id = System.unique_integer([:positive, :monotonic])
      tmp_dir = Path.join(System.tmp_dir!(), "blob_store_sup_test_#{unique_id}")
      File.mkdir_p!(tmp_dir)
      name = :"test_blob_store_#{unique_id}"

      on_exit(fn -> File.rm_rf!(tmp_dir) end)

      {:ok, pid} =
        start_supervised(
          {BlobStore, base_dir: tmp_dir, prefix_depth: 2, name: name},
          id: name,
          restart: :permanent
        )

      assert Process.whereis(name) == pid
    end

    test "blob store restarts on crash when supervised" do
      # Start under test supervisor with permanent restart, unique name and ID
      unique_id = System.unique_integer([:positive, :monotonic])
      tmp_dir = Path.join(System.tmp_dir!(), "blob_store_restart_test_#{unique_id}")
      File.mkdir_p!(tmp_dir)
      name = :"restart_test_blob_store_#{unique_id}"

      on_exit(fn -> File.rm_rf!(tmp_dir) end)

      {:ok, original_pid} =
        start_supervised(
          {BlobStore, base_dir: tmp_dir, prefix_depth: 2, name: name},
          id: name,
          restart: :permanent
        )

      assert original_pid != nil

      # Kill the process
      Process.exit(original_pid, :kill)

      # Wait a bit for supervisor to restart it
      Process.sleep(100)

      # Should have a new PID
      new_pid = Process.whereis(name)
      assert new_pid != nil
      assert new_pid != original_pid
    end
  end

  ## Helper Functions

  defp random_string do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end

  defp test_server do
    :"blob_store_#{inspect(self())}"
  end

  defp telemetry_listen(event_prefix) do
    ref = make_ref()
    test_pid = self()

    :telemetry.attach_many(
      ref,
      [
        event_prefix ++ [:start],
        event_prefix ++ [:stop],
        event_prefix ++ [:exception]
      ],
      fn name, measurements, metadata, _config ->
        send(test_pid, {:telemetry, name, measurements, metadata})
      end,
      nil
    )

    ref
  end
end
