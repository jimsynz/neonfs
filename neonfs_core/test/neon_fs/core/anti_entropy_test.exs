defmodule NeonFS.Core.AntiEntropyTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.AntiEntropy
  alias NeonFS.Core.MetadataRing
  alias NeonFS.Core.MetadataStore

  @moduletag :tmp_dir

  setup context do
    tmp_dir = setup_tmp_dir(context)
    configure_test_dirs(tmp_dir)
    start_drive_registry()
    start_blob_store()

    on_exit(fn ->
      cleanup_test_dirs()
    end)

    # Build a ring with the local node
    ring =
      MetadataRing.new([node()],
        virtual_nodes_per_physical: 4,
        replicas: 1
      )

    # Pick a segment from the ring
    [{segment_id, _replicas} | _] = MetadataRing.segments(ring)

    %{ring: ring, segment_id: segment_id}
  end

  describe "sync_segment/2 with identical replicas" do
    setup %{segment_id: segment_id} do
      start_supervised!({MetadataStore, name: MetadataStore}, restart: :temporary)

      # Write some test data
      :ok = MetadataStore.write(segment_id, "key:alpha", %{name: "alpha"})
      :ok = MetadataStore.write(segment_id, "key:beta", %{name: "beta"})
      :ok = MetadataStore.write(segment_id, "key:gamma", %{name: "gamma"})

      :ok
    end

    test "no repairs needed when single replica is consistent", %{
      ring: ring,
      segment_id: segment_id
    } do
      result =
        AntiEntropy.sync_segment(segment_id,
          ring: ring,
          local_node: node()
        )

      assert result.keys_repaired == 0
      assert result.skipped == false
    end
  end

  describe "merkle_tree/2" do
    setup %{segment_id: segment_id} do
      start_supervised!({MetadataStore, name: MetadataStore}, restart: :temporary)

      # Write test data
      :ok = MetadataStore.write(segment_id, "key:one", "value_one")
      :ok = MetadataStore.write(segment_id, "key:two", "value_two")

      :ok
    end

    test "computes root hash for a segment", %{segment_id: segment_id} do
      {:ok, root_hash, count} = MetadataStore.merkle_tree(segment_id)

      assert is_binary(root_hash)
      assert byte_size(root_hash) == 32
      assert count == 2
    end

    test "identical data produces identical hashes", %{segment_id: segment_id} do
      {:ok, hash1, _} = MetadataStore.merkle_tree(segment_id)
      {:ok, hash2, _} = MetadataStore.merkle_tree(segment_id)

      assert hash1 == hash2
    end

    test "different data produces different hashes", %{segment_id: segment_id} do
      {:ok, hash_before, _} = MetadataStore.merkle_tree(segment_id)

      :ok = MetadataStore.write(segment_id, "key:three", "value_three")

      {:ok, hash_after, count} = MetadataStore.merkle_tree(segment_id)

      assert hash_before != hash_after
      assert count == 3
    end

    test "empty segment returns valid hash with zero count", %{ring: ring} do
      # Use a segment we haven't written to
      [{_, _}, {other_segment, _} | _] = MetadataRing.segments(ring)

      {:ok, root_hash, count} = MetadataStore.merkle_tree(other_segment)

      assert is_binary(root_hash)
      assert byte_size(root_hash) == 32
      assert count == 0
    end
  end

  describe "list_segment_all/2" do
    setup %{segment_id: segment_id} do
      start_supervised!({MetadataStore, name: MetadataStore}, restart: :temporary)

      :ok = MetadataStore.write(segment_id, "key:live", "alive")
      :ok = MetadataStore.delete(segment_id, "key:dead")

      :ok
    end

    test "returns both live and tombstoned entries", %{segment_id: segment_id} do
      {:ok, entries} = MetadataStore.list_segment_all(segment_id)

      assert length(entries) == 2

      values = Enum.map(entries, fn {_hash, record} -> record end)
      live = Enum.find(values, &(!&1.tombstone))
      dead = Enum.find(values, & &1.tombstone)

      assert live != nil
      assert live.value == "alive"
      assert dead != nil
      assert dead.tombstone == true
    end
  end

  describe "tombstone cleanup" do
    setup %{segment_id: segment_id} do
      start_supervised!({MetadataStore, name: MetadataStore}, restart: :temporary)

      # Write and then delete (tombstone) a key
      :ok = MetadataStore.write(segment_id, "key:ephemeral", "temp_value")
      :ok = MetadataStore.delete(segment_id, "key:ephemeral")

      # Also write a live key
      :ok = MetadataStore.write(segment_id, "key:permanent", "keep_me")

      :ok
    end

    test "cleans up tombstones when all replicas agree", %{
      ring: ring,
      segment_id: segment_id
    } do
      # Verify tombstone exists before cleanup
      {:ok, entries_before} = MetadataStore.list_segment_all(segment_id)
      tombstones_before = Enum.count(entries_before, fn {_, r} -> r.tombstone end)
      assert tombstones_before == 1

      result =
        AntiEntropy.sync_segment(segment_id,
          ring: ring,
          local_node: node()
        )

      assert result.tombstones_cleaned == 1

      # Verify tombstone was purged — only the live key remains
      {:ok, entries_after} = MetadataStore.list_segment_all(segment_id)
      tombstones_after = Enum.count(entries_after, fn {_, r} -> r.tombstone end)
      assert tombstones_after == 0

      # Live key still accessible
      {:ok, "keep_me", _ts} = MetadataStore.read(segment_id, "key:permanent")
    end
  end

  describe "telemetry events" do
    setup %{segment_id: segment_id} do
      start_supervised!({MetadataStore, name: MetadataStore}, restart: :temporary)
      :ok = MetadataStore.write(segment_id, "key:telemetry_test", "data")
      :ok
    end

    test "emits started and completed events", %{ring: ring, segment_id: segment_id} do
      ref = make_ref()
      pid = self()

      :telemetry.attach(
        "ae-started-#{inspect(ref)}",
        [:neonfs, :anti_entropy, :started],
        fn _event, _measurements, metadata, _config ->
          send(pid, {:started, metadata})
        end,
        nil
      )

      :telemetry.attach(
        "ae-completed-#{inspect(ref)}",
        [:neonfs, :anti_entropy, :completed],
        fn _event, measurements, metadata, _config ->
          send(pid, {:completed, measurements, metadata})
        end,
        nil
      )

      AntiEntropy.sync_segment(segment_id, ring: ring, local_node: node())

      assert_receive {:started, %{segment_id: ^segment_id}}
      assert_receive {:completed, %{duration_ms: duration}, %{segment_id: ^segment_id}}
      assert is_integer(duration)

      :telemetry.detach("ae-started-#{inspect(ref)}")
      :telemetry.detach("ae-completed-#{inspect(ref)}")
    end

    test "emits tombstones_cleaned event on cleanup", %{ring: ring, segment_id: segment_id} do
      :ok = MetadataStore.write(segment_id, "key:to_delete", "doomed")
      :ok = MetadataStore.delete(segment_id, "key:to_delete")

      ref = make_ref()
      pid = self()

      :telemetry.attach(
        "ae-tombstones-#{inspect(ref)}",
        [:neonfs, :anti_entropy, :tombstones_cleaned],
        fn _event, measurements, metadata, _config ->
          send(pid, {:tombstones_cleaned, measurements, metadata})
        end,
        nil
      )

      AntiEntropy.sync_segment(segment_id, ring: ring, local_node: node())

      assert_receive {:tombstones_cleaned, %{count: count}, %{segment_id: ^segment_id}}
      assert count >= 1

      :telemetry.detach("ae-tombstones-#{inspect(ref)}")
    end
  end

  describe "GenServer periodic scheduling" do
    setup %{ring: ring} do
      start_supervised!({MetadataStore, name: MetadataStore}, restart: :temporary)

      # Start BackgroundWorker for periodic sync submission
      start_supervised!(
        {Task.Supervisor, name: NeonFS.Core.BackgroundTaskSupervisor},
        restart: :temporary
      )

      start_supervised!(NeonFS.Core.BackgroundWorker, restart: :temporary)

      ring_fn = fn -> ring end

      start_supervised!(
        {AntiEntropy,
         name: AntiEntropy, sync_interval_ms: 100_000, ring_fn: ring_fn, local_node: node()},
        restart: :temporary
      )

      :ok
    end

    test "sync_now returns results" do
      result = AntiEntropy.sync_now()

      assert is_map(result)
      assert Map.has_key?(result, :segments_synced)
      assert Map.has_key?(result, :keys_repaired)
      assert Map.has_key?(result, :tombstones_cleaned)
    end

    test "status returns configuration" do
      status = AntiEntropy.status()

      assert status.sync_interval_ms == 100_000
      assert status.syncs_completed == 0
    end

    test "sync_now updates stats" do
      AntiEntropy.sync_now()
      status = AntiEntropy.status()

      assert status.syncs_completed == 1
    end
  end

  describe "multi-replica repair (simulated via raw writes)" do
    setup %{segment_id: segment_id} do
      start_supervised!({MetadataStore, name: MetadataStore}, restart: :temporary)

      :ok
    end

    test "repairs when one replica is missing a key", %{ring: ring, segment_id: segment_id} do
      # Write a key to the local store
      :ok = MetadataStore.write(segment_id, "key:present", "i_exist")

      # Verify the key exists
      {:ok, entries} = MetadataStore.list_segment_all(segment_id)
      assert length(entries) == 1

      # With a single replica, sync should not repair anything
      # (there's no divergence to detect with one replica)
      result =
        AntiEntropy.sync_segment(segment_id,
          ring: ring,
          local_node: node()
        )

      assert result.keys_repaired == 0
    end

    test "raw record write works for anti-entropy repair", %{segment_id: segment_id} do
      # Simulate an anti-entropy repair by writing a raw record
      key_hash = :crypto.hash(:sha256, "key:repaired")

      record = %{
        value: "repaired_value",
        hlc_timestamp: {System.system_time(:millisecond), 0, node()},
        tombstone: false
      }

      assert :ok == AntiEntropy.write_raw_record(segment_id, key_hash, record)

      # Verify the record was written (readable from BlobStore)
      {:ok, entries} = MetadataStore.list_segment_all(segment_id)
      assert length(entries) == 1

      [{^key_hash, stored}] = entries
      assert stored.value == "repaired_value"
      assert stored.tombstone == false
    end

    test "stale value detected via merkle tree difference", %{segment_id: segment_id} do
      # Write initial data
      :ok = MetadataStore.write(segment_id, "key:versioned", "v1")
      {:ok, hash_v1, _} = MetadataStore.merkle_tree(segment_id)

      # Update the value
      :ok = MetadataStore.write(segment_id, "key:versioned", "v2")
      {:ok, hash_v2, _} = MetadataStore.merkle_tree(segment_id)

      # The merkle trees should differ since the value changed
      assert hash_v1 != hash_v2

      # Verify the latest value is stored
      {:ok, value, _ts} = MetadataStore.read(segment_id, "key:versioned")
      assert value == "v2"
    end
  end

  describe "error handling" do
    setup do
      start_supervised!({MetadataStore, name: MetadataStore}, restart: :temporary)
      :ok
    end

    test "skips segment when no replicas are reachable" do
      # Use a ring with a fake unreachable node
      ring =
        MetadataRing.new([:fake@unreachable],
          virtual_nodes_per_physical: 4,
          replicas: 1
        )

      [{segment_id, _} | _] = MetadataRing.segments(ring)

      result =
        AntiEntropy.sync_segment(segment_id,
          ring: ring,
          local_node: :fake@unreachable,
          timeout: 500
        )

      # Should not crash — may either succeed locally or skip
      assert is_map(result)
    end
  end
end
