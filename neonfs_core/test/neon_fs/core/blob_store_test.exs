defmodule NeonFS.Core.BlobStoreTest do
  use ExUnit.Case, async: true

  alias NeonFS.Core.Blob.Native
  alias NeonFS.Core.BlobStore

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    # Create a unique temporary directory for each test
    File.mkdir_p!(tmp_dir)

    drives = [%{id: "default", path: tmp_dir, tier: :hot, capacity: 100_000_000}]

    # Start a BlobStore for this test
    {:ok, pid} =
      start_supervised({BlobStore, drives: drives, prefix_depth: 2, name: test_server()})

    on_exit(fn ->
      File.rm_rf!(tmp_dir)
    end)

    %{store: pid, tmp_dir: tmp_dir}
  end

  describe "write_chunk/4 and read_chunk/3" do
    test "basic write and read roundtrip", %{store: _store} do
      data = "hello world"

      assert {:ok, hash, info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      assert byte_size(hash) == 32
      assert info.original_size == byte_size(data)
      assert info.stored_size > 0
      assert info.compression == "none"

      assert {:ok, ^data} =
               BlobStore.read_chunk(hash, "default", tier: "hot", server: test_server())
    end

    test "write and read with compression", %{store: _store} do
      data = String.duplicate("hello world ", 1000)

      assert {:ok, hash, info} =
               BlobStore.write_chunk(data, "default", "hot",
                 compression: "zstd",
                 compression_level: 3,
                 server: test_server()
               )

      assert byte_size(hash) == 32
      assert info.original_size == byte_size(data)
      assert info.stored_size < info.original_size
      assert info.compression == "zstd:3"

      # Read without decompression returns compressed data
      assert {:ok, compressed} =
               BlobStore.read_chunk(hash, "default", tier: "hot", server: test_server())

      assert byte_size(compressed) < byte_size(data)

      # Read with decompression returns original data
      assert {:ok, decompressed} =
               BlobStore.read_chunk_with_options(hash, "default", "hot",
                 decompress: true,
                 server: test_server()
               )

      assert decompressed == data
    end

    test "read with verification succeeds for valid chunk", %{store: _store} do
      data = "test data"

      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      assert {:ok, ^data} =
               BlobStore.read_chunk_with_options(hash, "default", "hot",
                 verify: true,
                 server: test_server()
               )
    end

    test "write to different tiers", %{store: _store} do
      data = "test data"

      for tier <- ["hot", "warm", "cold"] do
        {:ok, hash, _info} =
          BlobStore.write_chunk(data, "default", tier, server: test_server())

        {:ok, ^data} =
          BlobStore.read_chunk(hash, "default", tier: tier, server: test_server())
      end
    end

    test "reading nonexistent chunk returns error", %{store: _store} do
      fake_hash = :crypto.strong_rand_bytes(32)

      assert {:error, reason} =
               BlobStore.read_chunk(fake_hash, "default", tier: "hot", server: test_server())

      assert reason =~ "not found" or reason =~ "NotFound"
    end

    test "empty data roundtrip", %{store: _store} do
      data = ""

      assert {:ok, hash, info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      assert byte_size(hash) == 32
      assert info.original_size == 0

      assert {:ok, ^data} =
               BlobStore.read_chunk(hash, "default", tier: "hot", server: test_server())
    end

    test "unknown drive_id returns error", %{store: _store} do
      data = "test data"

      assert {:error, reason} =
               BlobStore.write_chunk(data, "nonexistent", "hot", server: test_server())

      assert reason =~ "unknown drive_id"
    end
  end

  describe "delete_chunk/2" do
    test "deletes an existing chunk", %{store: _store} do
      data = "delete me"

      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      assert {:ok, ^data} =
               BlobStore.read_chunk(hash, "default", tier: "hot", server: test_server())

      assert {:ok, _bytes_freed} = BlobStore.delete_chunk(hash, "default", server: test_server())

      # Reading after delete should fail
      assert {:error, _reason} =
               BlobStore.read_chunk(hash, "default", tier: "hot", server: test_server())
    end

    test "deleting nonexistent chunk returns error", %{store: _store} do
      fake_hash = :crypto.strong_rand_bytes(32)

      assert {:error, reason} =
               BlobStore.delete_chunk(fake_hash, "default", server: test_server())

      assert reason =~ "not found"
    end

    test "deletes chunk regardless of tier", %{store: _store} do
      data = "find me in any tier"

      # Write to warm tier
      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "default", "warm", server: test_server())

      # Delete without specifying tier - should find it
      assert {:ok, _bytes_freed} = BlobStore.delete_chunk(hash, "default", server: test_server())

      # Verify it's gone
      assert {:error, _} =
               BlobStore.read_chunk(hash, "default", tier: "warm", server: test_server())
    end
  end

  describe "migrate_chunk/4" do
    test "migrates chunk from hot to cold", %{store: _store} do
      data = "migrate me"

      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      assert {:ok, ^data} =
               BlobStore.read_chunk(hash, "default", tier: "hot", server: test_server())

      assert {:ok, {}} =
               BlobStore.migrate_chunk(hash, "default", "hot", "cold", server: test_server())

      # Should no longer exist in hot tier
      assert {:error, _} =
               BlobStore.read_chunk(hash, "default", tier: "hot", server: test_server())

      # Should exist in cold tier
      assert {:ok, ^data} =
               BlobStore.read_chunk(hash, "default", tier: "cold", server: test_server())
    end

    test "migrates chunk from cold to warm", %{store: _store} do
      data = "migrate me back"

      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "default", "cold", server: test_server())

      assert {:ok, {}} =
               BlobStore.migrate_chunk(hash, "default", "cold", "warm", server: test_server())

      assert {:error, _} =
               BlobStore.read_chunk(hash, "default", tier: "cold", server: test_server())

      assert {:ok, ^data} =
               BlobStore.read_chunk(hash, "default", tier: "warm", server: test_server())
    end

    test "migrating nonexistent chunk returns error", %{store: _store} do
      fake_hash = :crypto.strong_rand_bytes(32)

      assert {:error, reason} =
               BlobStore.migrate_chunk(fake_hash, "default", "hot", "cold", server: test_server())

      assert reason =~ "not found" or reason =~ "NotFound"
    end

    test "migration preserves data integrity", %{store: _store} do
      data = String.duplicate("important data ", 100)

      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      assert {:ok, {}} =
               BlobStore.migrate_chunk(hash, "default", "hot", "warm", server: test_server())

      assert {:ok, {}} =
               BlobStore.migrate_chunk(hash, "default", "warm", "cold", server: test_server())

      assert {:ok, {}} =
               BlobStore.migrate_chunk(hash, "default", "cold", "hot", server: test_server())

      assert {:ok, ^data} =
               BlobStore.read_chunk(hash, "default", tier: "hot", server: test_server())
    end
  end

  describe "list_drives/0" do
    test "returns configured drives", %{store: _store} do
      assert {:ok, drives} = BlobStore.list_drives(server: test_server())
      assert is_map(drives)
      assert Map.has_key?(drives, "default")
      assert drives["default"].tier == :hot
    end
  end

  describe "multi-drive scenarios" do
    setup %{tmp_dir: tmp_dir} do
      # Create two separate drive directories
      drive1_dir = Path.join(tmp_dir, "multi_drive_test_1")
      drive2_dir = Path.join(tmp_dir, "multi_drive_test_2")
      File.mkdir_p!(drive1_dir)
      File.mkdir_p!(drive2_dir)

      drives = [
        %{id: "nvme0", path: drive1_dir, tier: :hot, capacity: 500_000_000},
        %{id: "sata0", path: drive2_dir, tier: :cold, capacity: 2_000_000_000}
      ]

      server_name = :"multi_drive_test_#{System.unique_integer([:positive, :monotonic])}"

      {:ok, _pid} =
        start_supervised(
          {BlobStore, drives: drives, prefix_depth: 2, name: server_name},
          id: server_name
        )

      on_exit(fn ->
        File.rm_rf!(drive1_dir)
        File.rm_rf!(drive2_dir)
      end)

      %{
        server: server_name,
        drive1_dir: drive1_dir,
        drive2_dir: drive2_dir
      }
    end

    test "writes to different drives end up in correct directories", ctx do
      data1 = "hot data on nvme"
      data2 = "cold data on sata"

      assert {:ok, hash1, _info} =
               BlobStore.write_chunk(data1, "nvme0", "hot", server: ctx.server)

      assert {:ok, hash2, _info} =
               BlobStore.write_chunk(data2, "sata0", "cold", server: ctx.server)

      # Read from correct drives
      assert {:ok, ^data1} =
               BlobStore.read_chunk(hash1, "nvme0", tier: "hot", server: ctx.server)

      assert {:ok, ^data2} =
               BlobStore.read_chunk(hash2, "sata0", tier: "cold", server: ctx.server)

      # Data should NOT be on the other drive
      assert {:error, _} =
               BlobStore.read_chunk(hash1, "sata0", tier: "hot", server: ctx.server)

      assert {:error, _} =
               BlobStore.read_chunk(hash2, "nvme0", tier: "cold", server: ctx.server)
    end

    test "each drive gets independent NIF handle", ctx do
      assert {:ok, drives} = BlobStore.list_drives(server: ctx.server)
      assert map_size(drives) == 2
      assert Map.has_key?(drives, "nvme0")
      assert Map.has_key?(drives, "sata0")
      assert drives["nvme0"].tier == :hot
      assert drives["sata0"].tier == :cold
    end

    test "operations on unknown drive return error", ctx do
      assert {:error, reason} =
               BlobStore.write_chunk("data", "unknown_drive", "hot", server: ctx.server)

      assert reason =~ "unknown drive_id"
    end

    test "migration within a drive works", ctx do
      data = "migrate within drive"

      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "nvme0", "hot", server: ctx.server)

      assert {:ok, {}} =
               BlobStore.migrate_chunk(hash, "nvme0", "hot", "warm", server: ctx.server)

      assert {:ok, ^data} =
               BlobStore.read_chunk(hash, "nvme0", tier: "warm", server: ctx.server)
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
    test "emits write_chunk events with drive_id", %{store: _store} do
      ref = telemetry_listen([:neonfs, :blob_store, :write_chunk])

      data = "telemetry test"
      expected_size = byte_size(data)

      assert {:ok, _hash, _info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :write_chunk, :start], _measurements,
                      %{data_size: ^expected_size} = metadata}

      assert metadata.tier == "hot"
      assert metadata.drive_id == "default"

      assert_receive {:telemetry, [:neonfs, :blob_store, :write_chunk, :stop], measurements,
                      %{data_size: ^expected_size}}

      assert measurements.duration > 0
      assert measurements.bytes_written > 0

      :telemetry.detach(ref)
    end

    test "emits read_chunk events with drive_id", %{store: _store} do
      data = "read telemetry test"

      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      expected_hash = Base.encode16(hash, case: :lower)
      ref = telemetry_listen([:neonfs, :blob_store, :read_chunk])

      assert {:ok, _data} =
               BlobStore.read_chunk(hash, "default", tier: "hot", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :read_chunk, :start], _measurements,
                      %{hash: ^expected_hash} = metadata}

      assert metadata.tier == "hot"
      assert metadata.drive_id == "default"
      assert metadata.verify == false
      assert metadata.decompress == false

      assert_receive {:telemetry, [:neonfs, :blob_store, :read_chunk, :stop], measurements,
                      %{hash: ^expected_hash}}

      assert measurements.duration > 0
      assert measurements.bytes_read > 0

      :telemetry.detach(ref)
    end

    test "emits delete_chunk events with drive_id", %{store: _store} do
      data = "delete telemetry test"

      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      expected_hash = Base.encode16(hash, case: :lower)
      ref = telemetry_listen([:neonfs, :blob_store, :delete_chunk])

      assert {:ok, _bytes_freed} = BlobStore.delete_chunk(hash, "default", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :delete_chunk, :start], _measurements,
                      %{hash: ^expected_hash} = metadata}

      assert metadata.drive_id == "default"

      assert_receive {:telemetry, [:neonfs, :blob_store, :delete_chunk, :stop], measurements,
                      %{hash: ^expected_hash}}

      assert measurements.duration > 0

      :telemetry.detach(ref)
    end

    test "emits migrate_chunk events with drive_id", %{store: _store} do
      data = "migrate telemetry test"

      assert {:ok, hash, _info} =
               BlobStore.write_chunk(data, "default", "hot", server: test_server())

      expected_hash = Base.encode16(hash, case: :lower)
      ref = telemetry_listen([:neonfs, :blob_store, :migrate_chunk])

      assert {:ok, {}} =
               BlobStore.migrate_chunk(hash, "default", "hot", "cold", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :migrate_chunk, :start], _measurements,
                      %{hash: ^expected_hash} = metadata}

      assert metadata.from_tier == "hot"
      assert metadata.to_tier == "cold"
      assert metadata.drive_id == "default"

      assert_receive {:telemetry, [:neonfs, :blob_store, :migrate_chunk, :stop], measurements,
                      %{hash: ^expected_hash}}

      assert measurements.duration > 0

      :telemetry.detach(ref)
    end

    test "emits exception events on error", %{store: _store} do
      ref = telemetry_listen([:neonfs, :blob_store, :read_chunk])

      fake_hash = :crypto.strong_rand_bytes(32)
      expected_hash = Base.encode16(fake_hash, case: :lower)

      assert {:error, _reason} =
               BlobStore.read_chunk(fake_hash, "default", tier: "hot", server: test_server())

      assert_receive {:telemetry, [:neonfs, :blob_store, :read_chunk, :start], _measurements,
                      %{hash: ^expected_hash}}

      assert_receive {:telemetry, [:neonfs, :blob_store, :read_chunk, :exception], measurements,
                      %{hash: ^expected_hash} = metadata}

      assert measurements.duration > 0
      assert is_binary(metadata.error)

      :telemetry.detach(ref)
    end
  end

  describe "startup" do
    test "starts with empty drives for later runtime addition" do
      {:ok, pid} =
        start_supervised(
          {BlobStore, drives: [], prefix_depth: 2, name: :no_drives_test},
          id: :no_drives_test
        )

      assert is_pid(pid)

      # Operations fail gracefully when no drives are present
      assert {:ok, drives} = BlobStore.list_drives(server: :no_drives_test)
      assert drives == %{}
    end

    test "blob store starts and registers name", %{tmp_dir: tmp_dir} do
      unique_id = System.unique_integer([:positive, :monotonic])
      store_dir = Path.join(tmp_dir, "blob_store_sup_test_#{unique_id}")
      File.mkdir_p!(store_dir)
      name = :"test_blob_store_#{unique_id}"

      drives = [%{id: "default", path: store_dir, tier: :hot, capacity: 0}]

      {:ok, pid} =
        start_supervised(
          {BlobStore, drives: drives, prefix_depth: 2, name: name},
          id: name,
          restart: :permanent
        )

      assert Process.whereis(name) == pid
    end

    test "blob store restarts on crash when supervised", %{tmp_dir: tmp_dir} do
      unique_id = System.unique_integer([:positive, :monotonic])
      store_dir = Path.join(tmp_dir, "blob_store_restart_test_#{unique_id}")
      File.mkdir_p!(store_dir)
      name = :"restart_test_blob_store_#{unique_id}"

      drives = [%{id: "default", path: store_dir, tier: :hot, capacity: 0}]

      {:ok, original_pid} =
        start_supervised(
          {BlobStore, drives: drives, prefix_depth: 2, name: name},
          id: name,
          restart: :permanent
        )

      assert original_pid != nil

      # Monitor and kill the process
      ref = Process.monitor(original_pid)
      Process.exit(original_pid, :kill)
      assert_receive {:DOWN, ^ref, :process, ^original_pid, :killed}, 1_000

      # Wait for supervisor to restart it with a new PID
      new_pid = wait_for_registered(name, 1_000)
      assert new_pid != original_pid
    end
  end

  ## Helper Functions

  defp test_server do
    :"blob_store_#{inspect(self())}"
  end

  defp wait_for_registered(name, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    wait_for_registered_loop(name, deadline)
  end

  defp wait_for_registered_loop(name, deadline) do
    case Process.whereis(name) do
      nil ->
        if System.monotonic_time(:millisecond) > deadline do
          flunk("Timed out waiting for #{inspect(name)} to be registered")
        end

        Process.sleep(1)
        wait_for_registered_loop(name, deadline)

      pid ->
        pid
    end
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
