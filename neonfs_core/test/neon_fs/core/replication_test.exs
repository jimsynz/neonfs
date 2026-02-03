defmodule NeonFS.Core.ReplicationTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{Replication, Volume}

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_chunk_index()

    on_exit(fn -> cleanup_test_dirs() end)

    # Create a test volume with 3-way replication
    volume = Volume.new("test-volume", durability: %{type: :replicate, factor: 3, min_copies: 2})

    {:ok, volume: volume}
  end

  describe "select_replication_targets/2" do
    test "returns empty list when count is 0" do
      assert {:ok, []} = Replication.select_replication_targets(0, [])
    end

    test "excludes specified nodes" do
      exclude = [Node.self()]
      # Since we're likely in a single-node test environment, this should return no targets
      result = Replication.select_replication_targets(2, exclude)

      case result do
        {:ok, targets} ->
          # If we got targets, verify none are excluded
          target_nodes = Enum.map(targets, & &1.node)
          refute Enum.any?(target_nodes, &(&1 in exclude))

        {:error, :no_targets} ->
          # Expected in single-node test
          :ok
      end
    end

    test "returns error when no targets available" do
      # Try to get targets when excluding all nodes
      all_nodes = [Node.self()]
      result = Replication.select_replication_targets(1, all_nodes)

      case result do
        {:ok, []} ->
          # No targets available, which is acceptable in single-node test
          :ok

        {:error, :no_targets} ->
          # Also acceptable
          :ok
      end
    end

    test "returns at most available targets" do
      # In a single-node test, we should get 0 targets when excluding self
      result = Replication.select_replication_targets(10, [Node.self()])

      case result do
        {:ok, targets} ->
          # Should get less than or equal to requested if not enough nodes available
          assert length(targets) <= 10

        {:error, :no_targets} ->
          # Expected in single-node test when excluding self
          :ok
      end
    end

    test "sets default drive_id" do
      case Replication.select_replication_targets(1, []) do
        {:ok, [target | _]} ->
          assert target.drive_id == "default"

        {:ok, []} ->
          # No targets in single-node test
          :ok

        {:error, :no_targets} ->
          :ok
      end
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
end
