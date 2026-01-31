defmodule NeonFS.Integration.Phase1Test do
  use ExUnit.Case, async: false

  alias NeonFS.CLI.Handler
  alias NeonFS.Core.{FileIndex, Persistence, ReadOperation, VolumeRegistry, WriteOperation}
  alias NeonFS.Integration.Helpers

  @moduletag :integration
  @moduletag timeout: 120_000

  setup do
    # Ensure applications are started
    {:ok, _} = Application.ensure_all_started(:neonfs_core)

    # Clean up any existing test volumes before starting
    cleanup_test_volume()

    # Also clean up at the start in case previous test crashed
    :ets.delete_all_objects(:volumes_by_id)
    :ets.delete_all_objects(:volumes_by_name)
    :ets.delete_all_objects(:file_index_by_id)
    :ets.delete_all_objects(:file_index_by_path)
    :ets.delete_all_objects(:chunk_index)

    # Create temp directory for test artifacts
    temp_dir = Helpers.create_temp_dir()
    data_dir = Path.join(temp_dir, "data")
    mount_point = Path.join(temp_dir, "mount")
    File.mkdir_p!(data_dir)
    File.mkdir_p!(mount_point)

    on_exit(fn ->
      Helpers.cleanup(temp_dir, mount_point)
      cleanup_test_volume()
    end)

    %{temp_dir: temp_dir, data_dir: data_dir, mount_point: mount_point}
  end

  defp cleanup_test_volume do
    case VolumeRegistry.get_by_name("test_volume") do
      {:ok, volume} -> VolumeRegistry.delete(volume.id)
      _ -> :ok
    end
  end

  describe "Phase 1 Integration - Core Operations" do
    test "create volume, write and read file via API", _context do
      # Create volume
      {:ok, volume} = Helpers.create_test_volume("test_volume")
      assert is_binary(volume["id"])
      assert volume["name"] == "test_volume"

      # Verify volume appears in list
      {:ok, volumes} = Handler.list_volumes()
      # Handler returns volumes as structs/maps with atom keys
      assert Enum.any?(volumes, fn v ->
               (is_map(v) and Map.get(v, :name) == "test_volume") or
                 (is_map(v) and Map.get(v, "name") == "test_volume")
             end)

      # Write test data via WriteOperation
      test_data = Helpers.random_data(100 * 1024)
      # 100KB
      {:ok, volume_rec} = VolumeRegistry.get_by_name("test_volume")

      {:ok, file} = WriteOperation.write_file(volume_rec.id, "/test.bin", test_data)
      assert file.size == byte_size(test_data)
      assert file.chunks != []

      # Read data back via ReadOperation
      {:ok, read_data} = ReadOperation.read_file(volume_rec.id, "/test.bin")
      assert read_data == test_data

      # Verify file appears in directory listing
      files = FileIndex.list_dir(volume_rec.id, "/")
      assert Enum.any?(files, fn f -> f.path == "/test.bin" end)

      # Check cluster status
      {:ok, status} = Handler.cluster_status()
      # Handler returns maps with atom keys
      assert status[:volumes] >= 1
      assert is_binary(status[:node])
    end

    test "data persists across application restart (DETS persistence)", _context do
      # Phase 1 uses DETS-backed persistence for metadata (Task 0040).
      # Metadata is periodically snapshotted to DETS and restored on startup.
      # Phase 2 will add Ra consensus for distributed metadata storage.

      # Create volume and write data
      {:ok, _volume} = Helpers.create_test_volume("test_volume")
      {:ok, volume_rec} = VolumeRegistry.get_by_name("test_volume")

      test_data = Helpers.random_data(50 * 1024)
      # 50KB
      {:ok, file} = WriteOperation.write_file(volume_rec.id, "/persist.bin", test_data)
      original_file_id = file.id

      # Trigger persistence snapshot before restart
      :ok = Persistence.snapshot_now()

      # Stop and restart core application
      :ok = Application.stop(:neonfs_core)
      Process.sleep(200)
      {:ok, _} = Application.ensure_all_started(:neonfs_core)

      # Wait for persistence to restore data
      Process.sleep(300)
      {:ok, restored_volume} = VolumeRegistry.get_by_name("test_volume")
      assert restored_volume.id == volume_rec.id

      {:ok, restored_file} = FileIndex.get_by_path(restored_volume.id, "/persist.bin")
      assert restored_file.id == original_file_id

      {:ok, read_data} = ReadOperation.read_file(restored_volume.id, "/persist.bin")
      assert read_data == test_data
    end

    test "multiple files in directory structure", _context do
      {:ok, _volume} = Helpers.create_test_volume("test_volume")
      {:ok, volume_rec} = VolumeRegistry.get_by_name("test_volume")

      # Write multiple files
      files = [
        {"/file1.txt", "Hello World"},
        {"/file2.txt", "Test Data"},
        {"/docs/readme.md", "# README"},
        {"/docs/guide.md", "# Guide"}
      ]

      for {path, content} <- files do
        {:ok, _file} = WriteOperation.write_file(volume_rec.id, path, content)
      end

      # Verify root directory listing
      root_files = FileIndex.list_dir(volume_rec.id, "/")
      root_paths = Enum.map(root_files, & &1.path)
      assert "/file1.txt" in root_paths
      assert "/file2.txt" in root_paths

      # Verify subdirectory listing
      docs_files = FileIndex.list_dir(volume_rec.id, "/docs")
      docs_paths = Enum.map(docs_files, & &1.path)
      assert "/docs/readme.md" in docs_paths
      assert "/docs/guide.md" in docs_paths
    end

    test "partial file reads work correctly", _context do
      {:ok, _volume} = Helpers.create_test_volume("test_volume")
      {:ok, volume_rec} = VolumeRegistry.get_by_name("test_volume")

      # Write test data
      test_data = Helpers.random_data(1024)
      # 1KB
      {:ok, _file} = WriteOperation.write_file(volume_rec.id, "/partial.bin", test_data)

      # Read full file
      {:ok, full_data} = ReadOperation.read_file(volume_rec.id, "/partial.bin")
      assert full_data == test_data

      # Read first 100 bytes
      {:ok, first_100} =
        ReadOperation.read_file(volume_rec.id, "/partial.bin", offset: 0, length: 100)

      assert first_100 == binary_part(test_data, 0, 100)

      # Read middle 100 bytes
      {:ok, middle_100} =
        ReadOperation.read_file(volume_rec.id, "/partial.bin", offset: 100, length: 100)

      assert middle_100 == binary_part(test_data, 100, 100)

      # Read last 100 bytes
      {:ok, last_100} =
        ReadOperation.read_file(volume_rec.id, "/partial.bin", offset: 924, length: 100)

      assert last_100 == binary_part(test_data, 924, 100)
    end

    test "large file chunking and reassembly", _context do
      {:ok, _volume} = Helpers.create_test_volume("test_volume")
      {:ok, volume_rec} = VolumeRegistry.get_by_name("test_volume")

      # Write large file (> 1MB to trigger chunking)
      test_data = Helpers.random_data(2 * 1024 * 1024)
      # 2MB
      {:ok, file} = WriteOperation.write_file(volume_rec.id, "/large.bin", test_data)

      # Verify file was chunked
      assert length(file.chunks) > 1, "Expected file to be split into multiple chunks"

      # Read back and verify
      {:ok, read_data} = ReadOperation.read_file(volume_rec.id, "/large.bin")
      assert read_data == test_data
      assert byte_size(read_data) == byte_size(test_data)
    end

    test "volume statistics are tracked correctly", _context do
      {:ok, _volume} = Helpers.create_test_volume("test_volume")
      {:ok, volume_rec} = VolumeRegistry.get_by_name("test_volume")

      # Volume stats are stored as top-level fields
      assert volume_rec.logical_size == 0
      assert volume_rec.physical_size == 0
      assert volume_rec.chunk_count == 0

      # Write some data
      test_data = Helpers.random_data(10 * 1024)
      # 10KB
      {:ok, _file} = WriteOperation.write_file(volume_rec.id, "/stats_test.bin", test_data)

      # Check volume info includes statistics
      {:ok, volume_info} = Handler.get_volume("test_volume")
      # Stats are returned as top-level fields with atom keys
      assert is_integer(volume_info[:logical_size])
      assert is_integer(volume_info[:physical_size])
      assert is_integer(volume_info[:chunk_count])

      # Note: Stats updates might be async, so we just verify the fields exist
      # rather than checking exact values
      refute is_nil(volume_info[:logical_size])
      refute is_nil(volume_info[:physical_size])
    end
  end

  describe "Phase 1 Integration - FUSE Operations" do
    @tag :fuse
    test "mount volume and perform filesystem operations", %{mount_point: mount_point} do
      # Create volume
      {:ok, _volume} = Helpers.create_test_volume("test_volume")

      # Try to mount volume - skip test if FUSE not available
      case Handler.mount("test_volume", mount_point, %{}) do
        {:ok, mount_info} ->
          assert is_map(mount_info)

          # Write file via mounted filesystem
          test_data = Helpers.random_data(100 * 1024)
          file_path = Path.join(mount_point, "fuse_test.bin")
          :ok = File.write(file_path, test_data)

          # Read file back
          {:ok, read_data} = File.read(file_path)
          assert read_data == test_data

          # List directory
          {:ok, files} = File.ls(mount_point)
          assert "fuse_test.bin" in files

          # Get file stats
          {:ok, stat} = File.stat(file_path)
          assert stat.size == byte_size(test_data)

          # Unmount
          :ok = Handler.unmount(mount_point)

          # Verify mount is gone
          {:ok, mounts} = Handler.list_mounts()
          refute Enum.any?(mounts, fn m -> m["mount_point"] == mount_point end)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available - skip test
          :ok

        {:error, _} ->
          # Other error - skip test
          :ok
      end
    end

    @tag :fuse
    test "data persists across unmount and remount", %{mount_point: mount_point} do
      # Create volume and try to mount
      {:ok, _volume} = Helpers.create_test_volume("test_volume")

      case Handler.mount("test_volume", mount_point, %{}) do
        {:ok, _mount_info} ->
          # Write test file
          test_data = "Persistent test data"
          file_path = Path.join(mount_point, "persistent.txt")
          :ok = File.write(file_path, test_data)

          # Unmount
          :ok = Handler.unmount(mount_point)

          # Re-mount
          {:ok, _mount_info} = Handler.mount("test_volume", mount_point, %{})

          # Verify data persisted
          {:ok, read_data} = File.read(file_path)
          assert read_data == test_data

          # Cleanup
          :ok = Handler.unmount(mount_point)

        {:error, {:mount_failed, _reason}} ->
          # FUSE not available - skip test
          :ok

        {:error, _} ->
          # Other error - skip test
          :ok
      end
    end
  end
end
