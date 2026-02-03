defmodule NeonFS.Core.PersistenceTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.Persistence

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()

    # Start persistence and the subsystems that own ETS tables
    start_persistence()
    start_chunk_index()
    start_file_index()
    start_volume_registry()

    on_exit(fn -> cleanup_test_dirs() end)

    %{meta_dir: tmp_dir}
  end

  describe "snapshot_now/0" do
    test "creates DETS files from ETS tables", %{meta_dir: meta_dir} do
      # Insert test data into ETS tables
      :ets.insert(:chunk_index, {:test_chunk, "chunk_data"})
      :ets.insert(:file_index_by_id, {:test_file, "file_data"})
      :ets.insert(:volumes_by_id, {:test_volume, "volume_data"})

      # Trigger snapshot
      :ok = Persistence.snapshot_now()

      # Verify DETS files were created
      assert File.exists?(Path.join(meta_dir, "chunk_index.dets"))
      assert File.exists?(Path.join(meta_dir, "file_index_by_id.dets"))
      assert File.exists?(Path.join(meta_dir, "volume_registry_by_id.dets"))
    end

    test "persists data that can be read back", %{meta_dir: meta_dir} do
      # Insert test data
      :ets.insert(:chunk_index, {:chunk_key, %{size: 1024}})

      # Trigger snapshot
      :ok = Persistence.snapshot_now()

      # Read back from DETS
      dets_path = Path.join(meta_dir, "chunk_index.dets")

      {:ok, dets} =
        :dets.open_file(:verify_chunk, type: :set, file: String.to_charlist(dets_path))

      result = :dets.lookup(dets, :chunk_key)
      :dets.close(dets)

      assert result == [{:chunk_key, %{size: 1024}}]
    end
  end

  describe "atomic writes" do
    test "does not leave .tmp files after successful snapshot", %{meta_dir: meta_dir} do
      :ets.insert(:chunk_index, {:key, "value"})
      :ok = Persistence.snapshot_now()

      # Check no .tmp files remain
      tmp_files = Path.wildcard(Path.join(meta_dir, "*.tmp"))
      assert tmp_files == []
    end
  end
end
