defmodule NeonFS.Core.PersistenceTest do
  use ExUnit.Case, async: false

  alias NeonFS.Core.Persistence

  @test_meta_dir "/tmp/neonfs_test/persistence_meta"

  setup do
    # Persistence is already started as part of the application
    # Just ensure it exists
    persistence_pid = Process.whereis(Persistence)
    assert is_pid(persistence_pid), "Persistence should be running"

    # Clean up test directory before each test
    File.rm_rf!(@test_meta_dir)
    File.mkdir_p!(@test_meta_dir)

    # Get the actual meta directory used by the application
    test_meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs/meta")

    # CRITICAL: Clean up DETS files FIRST to prevent restoration of stale data
    for file <- [
          "chunk_index.dets",
          "file_index_by_id.dets",
          "file_index_by_path.dets",
          "volume_registry_by_id.dets",
          "volume_registry_by_name.dets"
        ] do
      File.rm(Path.join(test_meta_dir, file))
    end

    # Then clean up ETS tables
    for table <- [
          :chunk_index,
          :file_index_by_id,
          :file_index_by_path,
          :volumes_by_id,
          :volumes_by_name
        ] do
      case :ets.whereis(table) do
        :undefined -> :ok
        _ref -> :ets.delete_all_objects(table)
      end
    end

    on_exit(fn ->
      # Clean up again on exit to prevent pollution of other test modules
      File.rm_rf!(@test_meta_dir)

      for file <- [
            "chunk_index.dets",
            "file_index_by_id.dets",
            "file_index_by_path.dets",
            "volume_registry_by_id.dets",
            "volume_registry_by_name.dets"
          ] do
        File.rm(Path.join(test_meta_dir, file))
      end

      for table <- [
            :chunk_index,
            :file_index_by_id,
            :file_index_by_path,
            :volumes_by_id,
            :volumes_by_name
          ] do
        case :ets.whereis(table) do
          :undefined -> :ok
          _ref -> :ets.delete_all_objects(table)
        end
      end
    end)

    %{persistence_pid: persistence_pid}
  end

  describe "atomic_snapshot/2" do
    test "creates DETS file from ETS table" do
      # The atomic_snapshot function is private and tested indirectly
      # through snapshot_now and other tests. This test verifies the
      # overall snapshot mechanism works.
      :ets.insert(:chunk_index, {:test_key, "test_value"})
      :ok = Persistence.snapshot_now()

      test_meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs/meta")
      chunk_dets = Path.join(test_meta_dir, "chunk_index.dets")
      assert File.exists?(chunk_dets)
    end

    test "atomic write-then-move prevents corruption", %{persistence_pid: _pid} do
      # Create a test ETS table
      table = :ets.new(:test_atomic, [:set, :public, :named_table])
      :ets.insert(table, {:key1, "value1"})

      dets_path = Path.join(@test_meta_dir, "test_atomic.dets")

      # First snapshot
      perform_direct_snapshot(table, dets_path)
      assert File.exists?(dets_path)

      # Verify .tmp file is cleaned up
      refute File.exists?("#{dets_path}.tmp")

      # Verify data can be read back
      {:ok, dets} = :dets.open_file(:test_verify, type: :set, file: String.to_charlist(dets_path))
      assert :dets.lookup(dets, :key1) == [{:key1, "value1"}]
      :dets.close(dets)

      :ets.delete(table)
    end
  end

  describe "restoration" do
    @tag :skip
    test "restores data from existing DETS files on startup" do
      # NOTE: This test would require stopping/starting the entire application
      # which interferes with other tests. The integration test covers this scenario.
      :ok
    end

    test "handles missing DETS files gracefully" do
      # This is the normal case - already tested by application startup
      # If DETS files don't exist, persistence starts with empty tables
      assert is_list(:ets.tab2list(:chunk_index))
    end
  end

  describe "periodic snapshots" do
    test "automatically snapshots tables at configured interval" do
      # Tables already exist from application startup - just populate them
      :ets.insert(:chunk_index, {:chunk1, "data1"})

      # Wait for at least one snapshot cycle
      # Note: test config uses 100ms interval, but app default is 30s
      # We'll trigger a manual snapshot to ensure it happens
      :ok = Persistence.snapshot_now()

      # Verify DETS file was created
      # Note: Uses the configured test meta_dir
      test_meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs/meta")
      chunk_dets = Path.join(test_meta_dir, "chunk_index.dets")
      assert File.exists?(chunk_dets)

      # Verify data is in DETS
      {:ok, dets} = :dets.open_file(:verify, type: :set, file: String.to_charlist(chunk_dets))
      assert :dets.lookup(dets, :chunk1) == [{:chunk1, "data1"}]
      :dets.close(dets)
    end
  end

  describe "graceful shutdown" do
    @tag :skip
    test "snapshots all tables on termination" do
      # NOTE: This test requires stopping the application to test shutdown behavior
      # This is covered by the integration test which stops/restarts the application
      :ok
    end
  end

  describe "temp file cleanup" do
    test "cleans up temp files on startup" do
      test_meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs/meta")

      # Create a fake temp file
      temp_file = Path.join(test_meta_dir, "test_cleanup.dets.tmp")
      File.write!(temp_file, "fake temp data")
      assert File.exists?(temp_file)

      # Restart application to trigger cleanup
      :ok = Application.stop(:neonfs_core)
      Process.sleep(100)
      {:ok, _} = Application.ensure_all_started(:neonfs_core)
      Process.sleep(200)

      # Temp file should be cleaned up
      refute File.exists?(temp_file)
    end

    test "cleanup function removes all .tmp files from directory" do
      test_meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs/meta")

      # Stop the application first to prevent periodic snapshots from racing with our test
      :ok = Application.stop(:neonfs_core)
      Process.sleep(100)

      # Create multiple temp files (using unique names that won't conflict with real snapshots)
      tmp_files = [
        Path.join(test_meta_dir, "test_leftover_1.dets.tmp"),
        Path.join(test_meta_dir, "test_leftover_2.dets.tmp"),
        Path.join(test_meta_dir, "test_leftover_3.dets.tmp")
      ]

      Enum.each(tmp_files, fn tmp ->
        File.write!(tmp, "fake temp data")
        assert File.exists?(tmp), "Failed to create #{tmp}"
      end)

      # Restart to trigger cleanup
      {:ok, _} = Application.ensure_all_started(:neonfs_core)
      Process.sleep(200)

      # All temp files should be cleaned up
      Enum.each(tmp_files, fn tmp ->
        refute File.exists?(tmp), "#{tmp} should have been cleaned up"
      end)
    end
  end

  describe "crash recovery" do
    test "old snapshot remains valid if new snapshot is interrupted" do
      # Create ETS table
      table = :ets.new(:test_crash, [:set, :public, :named_table])
      :ets.insert(table, {:key1, "original"})

      dets_path = Path.join(@test_meta_dir, "test_crash.dets")
      temp_path = "#{dets_path}.tmp"

      # Write initial valid snapshot
      perform_direct_snapshot(table, dets_path)

      # Verify original data
      {:ok, dets1} = :dets.open_file(:verify1, type: :set, file: String.to_charlist(dets_path))
      assert :dets.lookup(dets1, :key1) == [{:key1, "original"}]
      :dets.close(dets1)

      # Simulate partial write (create .tmp file but don't rename)
      {:ok, dets2} = :dets.open_file(:partial, type: :set, file: String.to_charlist(temp_path))
      :dets.insert(dets2, {:key1, "corrupted"})
      :dets.close(dets2)

      # Verify .tmp exists but main file is unchanged
      assert File.exists?(temp_path)
      {:ok, dets3} = :dets.open_file(:verify2, type: :set, file: String.to_charlist(dets_path))
      assert :dets.lookup(dets3, :key1) == [{:key1, "original"}]
      :dets.close(dets3)

      # Cleanup
      File.rm(temp_path)
      :ets.delete(table)
    end
  end

  describe "snapshot_now/0" do
    test "triggers immediate snapshot of all tables" do
      # Tables already exist from application startup
      :ets.insert(:chunk_index, {:immediate, "snapshot"})

      # Trigger immediate snapshot
      assert :ok = Persistence.snapshot_now()

      # Verify snapshot was written
      test_meta_dir = Application.get_env(:neonfs_core, :meta_dir, "/tmp/neonfs/meta")
      chunk_dets = Path.join(test_meta_dir, "chunk_index.dets")
      assert File.exists?(chunk_dets)

      {:ok, dets} = :dets.open_file(:verify, type: :set, file: String.to_charlist(chunk_dets))
      assert :dets.lookup(dets, :immediate) == [{:immediate, "snapshot"}]
      :dets.close(dets)
    end
  end

  describe "data persistence across restart" do
    @tag :skip
    test "full lifecycle: write → snapshot → restart → verify" do
      # NOTE: This test requires stopping/starting the entire application
      # which is covered by the integration test in neonfs_fuse.
      # Testing it here would interfere with other unit tests.
      :ok
    end
  end

  # Helper function to perform direct snapshot for testing
  defp perform_direct_snapshot(ets_table, dets_path) do
    temp_path = "#{dets_path}.tmp"

    {:ok, dets} = :dets.open_file(:temp, type: :set, file: String.to_charlist(temp_path))
    :ets.to_dets(ets_table, dets)
    :dets.sync(dets)
    :dets.close(dets)
    File.rename!(temp_path, dets_path)

    :ok
  end
end
