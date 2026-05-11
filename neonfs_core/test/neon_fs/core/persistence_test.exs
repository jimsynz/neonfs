defmodule NeonFS.Core.PersistenceTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.Persistence

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()

    # Start persistence and volume registry (which owns the ETS tables we persist)
    start_persistence()
    start_volume_registry()

    on_exit(fn -> cleanup_test_dirs() end)

    %{meta_dir: tmp_dir}
  end

  describe "snapshot_now/0" do
    test "creates DETS files from ETS tables", %{meta_dir: meta_dir} do
      # Insert test data into volume ETS tables
      :ets.insert(:volumes_by_id, {:test_volume, "volume_data"})
      :ets.insert(:volumes_by_name, {"test-vol", :test_volume})

      # Trigger snapshot
      :ok = Persistence.snapshot_now()

      # Verify DETS files were created for volume tables
      assert File.exists?(Path.join(meta_dir, "volume_registry_by_id.dets"))
      assert File.exists?(Path.join(meta_dir, "volume_registry_by_name.dets"))

      # Metadata tables (chunk_index, file_index, stripe_index) are no longer
      # persisted via DETS — they load from BlobStore on startup (Phase 5)
      refute File.exists?(Path.join(meta_dir, "chunk_index.dets"))
      refute File.exists?(Path.join(meta_dir, "file_index_by_id.dets"))
      refute File.exists?(Path.join(meta_dir, "stripe_index.dets"))
    end

    test "persists data that can be read back", %{meta_dir: meta_dir} do
      # Insert test data into volume table
      :ets.insert(:volumes_by_id, {:vol_key, %{name: "test-volume"}})

      # Trigger snapshot
      :ok = Persistence.snapshot_now()

      # Read back from DETS
      dets_path = Path.join(meta_dir, "volume_registry_by_id.dets")

      {:ok, dets} =
        :dets.open_file(:verify_volume, type: :set, file: String.to_charlist(dets_path))

      result = :dets.lookup(dets, :vol_key)
      :dets.close(dets)

      assert result == [{:vol_key, %{name: "test-volume"}}]
    end
  end

  describe "atomic writes" do
    test "does not leave .tmp files after successful snapshot", %{meta_dir: meta_dir} do
      :ets.insert(:volumes_by_id, {:key, "value"})
      :ok = Persistence.snapshot_now()

      # Check no .tmp files remain
      tmp_files = Path.wildcard(Path.join(meta_dir, "*.tmp"))
      assert tmp_files == []
    end
  end

  # #988: between `:ets.whereis/1` (which says the table exists) and
  # `:ets.to_dets/2` (which actually reads it), the owning GenServer
  # can shut down and tear the table down. The race manifested as
  # `ArgumentError` from BEAM crashing Persistence on every 100 ms
  # snapshot tick — enough scheduler pressure to time out unrelated
  # `GenServer.call`s.
  describe "snapshot resilience to ETS teardown race" do
    test "treats a table vanishing mid-snapshot as a per-table no-op" do
      # Stop the owning registry so the ETS table is gone before the
      # snapshot walks it. `GenServer.stop/1` is synchronous — the
      # named table is auto-deleted when the owner exits, so by the
      # time the call returns the table is already gone.
      :ok = GenServer.stop(NeonFS.Core.VolumeRegistry)
      assert :undefined = :ets.whereis(:volumes_by_id)

      # The snapshot must NOT crash — the table-gone branch logs and
      # skips, returning :ok across the table set.
      assert :ok = Persistence.snapshot_now()
    end
  end
end
