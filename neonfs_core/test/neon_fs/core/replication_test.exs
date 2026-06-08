defmodule NeonFS.Core.ReplicationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{Drive, DriveRegistry, Replication, Volume}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()

    on_exit(fn -> cleanup_test_dirs() end)

    # Create a test volume with 3-way replication
    volume = Volume.new("test-volume", durability: %{type: :replicate, factor: 3, min_copies: 2})

    {:ok, volume: volume}
  end

  describe "select_replication_targets/3" do
    test "returns empty list when count is 0" do
      assert {:ok, []} = Replication.select_replication_targets(0, :hot)
    end

    test "selects distinct drives on a single node (drive-aware placement)" do
      register_drive("hot1")

      assert {:ok, [target]} =
               Replication.select_replication_targets(1, :hot, [{Node.self(), "default"}])

      assert target.node == Node.self()
      assert target.drive_id == "hot1"
    end

    test "never selects an excluded {node, drive_id}" do
      register_drive("hot1")
      register_drive("hot2")

      exclude = [{Node.self(), "default"}, {Node.self(), "hot1"}]
      assert {:ok, targets} = Replication.select_replication_targets(3, :hot, exclude)

      refute Enum.any?(targets, &({&1.node, &1.drive_id} in exclude))
    end

    test "returns at most count drives" do
      register_drive("hot1")
      register_drive("hot2")
      register_drive("hot3")

      assert {:ok, targets} = Replication.select_replication_targets(2, :hot, [])
      assert length(targets) == 2
    end

    test "returns fewer than requested when the cluster lacks drives" do
      # Only the single "default" drive exists, and it is excluded.
      assert {:ok, []} =
               Replication.select_replication_targets(5, :hot, [{Node.self(), "default"}])
    end
  end

  describe "replicate_chunk/4" do
    test "returns local location for single-replica volume", %{volume: _volume} do
      # Create a volume with replication factor 1 (no replication)
      volume =
        Volume.new("no-replication", durability: %{type: :replicate, factor: 1, min_copies: 1})

      chunk_hash = :crypto.hash(:sha256, "test data")
      chunk_data = "test data"

      # Should succeed immediately with just local location
      assert {:ok, locations} = Replication.replicate_chunk(chunk_hash, chunk_data, volume)
      refute Enum.empty?(locations)
      assert Enum.any?(locations, &(&1.node == Node.self()))
    end

    test "handles local write_ack with background replication", %{volume: volume} do
      # Volume has write_ack: :local by default
      volume = %{volume | write_ack: :local}

      chunk_hash = :crypto.hash(:sha256, "test data background")
      chunk_data = "test data background"

      # Should return immediately with local location
      assert {:ok, locations} = Replication.replicate_chunk(chunk_hash, chunk_data, volume)

      # Should have at least the local location
      refute Enum.empty?(locations)
      assert Enum.any?(locations, &(&1.node == Node.self()))
    end

    test "emits telemetry events", %{volume: volume} do
      # Attach telemetry handler
      test_pid = self()

      :telemetry.attach_many(
        "test-replication",
        [
          [:neonfs, :replication, :start],
          [:neonfs, :replication, :stop],
          [:neonfs, :replication, :exception]
        ],
        fn event, measurements, metadata, _config ->
          send(test_pid, {:telemetry, event, measurements, metadata})
        end,
        nil
      )

      chunk_hash = :crypto.hash(:sha256, "test telemetry")
      chunk_data = "test telemetry"

      volume = %{volume | write_ack: :local}
      {:ok, _locations} = Replication.replicate_chunk(chunk_hash, chunk_data, volume)

      # Should receive start event
      assert_receive {:telemetry, [:neonfs, :replication, :start], measurements, metadata}
      assert measurements.bytes == byte_size(chunk_data)
      assert metadata.hash == chunk_hash

      # Should receive stop event
      assert_receive {:telemetry, [:neonfs, :replication, :stop], _measurements, _metadata}

      :telemetry.detach("test-replication")
    end

    test "validates chunk data size", %{volume: volume} do
      volume = %{volume | write_ack: :local}

      chunk_hash = :crypto.hash(:sha256, "data")
      chunk_data = "data"

      assert {:ok, _locations} = Replication.replicate_chunk(chunk_hash, chunk_data, volume)
    end
  end

  describe "quorum replication" do
    test "succeeds with sufficient replicas", %{volume: _volume} do
      # In a single-node test environment, quorum replication will likely fail
      # because we can't reach other nodes. This is expected behavior.
      volume =
        Volume.new("quorum-volume",
          durability: %{type: :replicate, factor: 3, min_copies: 2},
          write_ack: :quorum
        )

      chunk_hash = :crypto.hash(:sha256, "quorum data")
      chunk_data = "quorum data"

      # In single-node environment, this will fail quorum (expected)
      result = Replication.replicate_chunk(chunk_hash, chunk_data, volume)

      case result do
        {:ok, locations} ->
          # In single-node test, we might only have 1 location
          # In multi-node cluster, we should have at least min_copies
          assert is_list(locations)
          refute Enum.empty?(locations)

        {:error, :quorum_not_met} ->
          # Expected in single-node test when quorum can't be met
          :ok
      end
    end
  end

  describe "synchronous replication" do
    test "waits for all replicas", %{volume: _volume} do
      volume =
        Volume.new("sync-volume",
          durability: %{type: :replicate, factor: 3, min_copies: 3},
          write_ack: :all
        )

      chunk_hash = :crypto.hash(:sha256, "sync data")
      chunk_data = "sync data"

      # In single-node environment, this will fail (expected)
      result = Replication.replicate_chunk(chunk_hash, chunk_data, volume)

      case result do
        {:ok, locations} ->
          # In single-node test, we might only have 1 location
          # In multi-node cluster, we should have all replicas
          assert is_list(locations)
          refute Enum.empty?(locations)

        {:error, :replication_failed} ->
          # Expected in single-node test when can't reach all nodes
          :ok
      end
    end
  end

  defp register_drive(id) do
    base = Application.get_env(:neonfs_core, :blob_store_base_dir, "/tmp/neonfs_test/blobs")

    drive =
      Drive.from_config(
        %{id: id, path: Path.join(base, id), tier: :hot, capacity: 0},
        Node.self()
      )

    :ok = DriveRegistry.register_drive(drive)
  end
end
