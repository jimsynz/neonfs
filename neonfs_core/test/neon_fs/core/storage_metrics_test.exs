defmodule NeonFS.Core.StorageMetricsTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{ChunkIndex, ChunkMeta, DriveRegistry, StorageMetrics}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)

    start_persistence()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()

    # Start StorageMetrics
    stop_genserver(NeonFS.Core.StorageMetrics)

    start_supervised!(
      NeonFS.Core.StorageMetrics,
      restart: :temporary
    )

    # Allow init to complete
    Process.sleep(50)

    :ok
  end

  describe "cluster_capacity/0" do
    test "returns drive info from registry" do
      result = StorageMetrics.cluster_capacity()

      assert is_map(result)
      assert is_list(result.drives)
      assert result.drives != []

      [drive | _] = result.drives
      assert Map.has_key?(drive, :node)
      assert Map.has_key?(drive, :drive_id)
      assert Map.has_key?(drive, :tier)
      assert Map.has_key?(drive, :capacity_bytes)
      assert Map.has_key?(drive, :used_bytes)
      assert Map.has_key?(drive, :available_bytes)
      assert Map.has_key?(drive, :state)
    end

    test "unlimited capacity drives report :unlimited available" do
      # Default test drive has capacity 0 (unlimited)
      result = StorageMetrics.cluster_capacity()

      drive = Enum.find(result.drives, &(&1.capacity_bytes == 0))
      assert drive != nil
      assert drive.available_bytes == :unlimited
    end
  end

  describe "initial usage computation" do
    test "computes used_bytes from chunks in ChunkIndex" do
      # Insert some chunks with locations on local node
      hash1 = :crypto.strong_rand_bytes(32)
      hash2 = :crypto.strong_rand_bytes(32)

      location = %{node: node(), drive_id: "default", tier: :hot}

      chunk1 =
        ChunkMeta.new(hash1, 1024, 512)
        |> ChunkMeta.add_location(location)

      chunk2 =
        ChunkMeta.new(hash2, 2048, 1024)
        |> ChunkMeta.add_location(location)

      ChunkIndex.put(chunk1)
      ChunkIndex.put(chunk2)

      # Restart StorageMetrics to trigger recomputation
      :telemetry.detach("storage-metrics")
      stop_genserver(NeonFS.Core.StorageMetrics)

      start_supervised!(
        NeonFS.Core.StorageMetrics,
        restart: :temporary
      )

      Process.sleep(50)

      # Check that DriveRegistry was updated
      {:ok, drive} = DriveRegistry.get_drive(node(), "default")
      assert drive.used_bytes == 512 + 1024
    end
  end

  describe "available_capacity_for_tier/2" do
    test "returns :unlimited for drives with capacity 0" do
      result = StorageMetrics.available_capacity_for_tier(:hot)
      assert result == :unlimited
    end

    test "excludes specified drives" do
      result = StorageMetrics.available_capacity_for_tier(:hot, [{node(), "default"}])
      # With the only hot drive excluded, should be 0 or :unlimited depending on
      # other drives. If no other drives exist, should be 0.
      # For test env with only one drive, this returns 0
      assert result == 0
    end
  end

  describe "available_capacity_any_tier/1" do
    test "returns :unlimited when any drive has capacity 0" do
      result = StorageMetrics.available_capacity_any_tier()
      assert result == :unlimited
    end
  end

  defp stop_genserver(name) do
    case Process.whereis(name) do
      nil -> :ok
      pid -> GenServer.stop(pid, :normal, 5000)
    end

    Process.sleep(10)
  end

  describe "telemetry-driven updates" do
    test "write_chunk telemetry increments usage" do
      {:ok, drive_before} = DriveRegistry.get_drive(node(), "default")
      used_before = drive_before.used_bytes

      # Simulate write telemetry event
      :telemetry.execute(
        [:neonfs, :blob_store, :write_chunk, :stop],
        %{duration: 1000, bytes_written: 5000},
        %{drive_id: "default", tier: "hot", data_size: 5000, compression: "none"}
      )

      Process.sleep(10)

      {:ok, drive_after} = DriveRegistry.get_drive(node(), "default")
      assert drive_after.used_bytes == used_before + 5000
    end

    test "delete_chunk telemetry decrements usage" do
      # First set some usage
      DriveRegistry.update_usage("default", 10_000)

      :telemetry.execute(
        [:neonfs, :blob_store, :delete_chunk, :stop],
        %{duration: 500, bytes_freed: 3000},
        %{drive_id: "default", hash: "abc123"}
      )

      Process.sleep(10)

      {:ok, drive} = DriveRegistry.get_drive(node(), "default")
      assert drive.used_bytes == 7000
    end

    test "decrement does not go below zero" do
      DriveRegistry.update_usage("default", 100)

      :telemetry.execute(
        [:neonfs, :blob_store, :delete_chunk, :stop],
        %{duration: 500, bytes_freed: 500},
        %{drive_id: "default", hash: "abc123"}
      )

      Process.sleep(10)

      {:ok, drive} = DriveRegistry.get_drive(node(), "default")
      assert drive.used_bytes == 0
    end
  end
end
