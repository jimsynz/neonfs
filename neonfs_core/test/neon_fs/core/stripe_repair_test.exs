defmodule NeonFS.Core.StripeRepairTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    ChunkIndex,
    StripeIndex,
    StripeRepair,
    VolumeRegistry,
    WriteOperation
  }

  alias NeonFS.Core.StripeRepair.LockTable

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_core_subsystems()
    start_stripe_index()
    ensure_chunk_access_tracker()

    LockTable.init()

    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:neonfs, :stripe_repair, :scan],
        [:neonfs, :stripe_repair, :repair]
      ])

    on_exit(fn -> cleanup_test_dirs() end)
    {:ok, telemetry_ref: telemetry_ref}
  end

  defp create_erasure_volume(name \\ "repair-vol") do
    full_name = "#{name}-#{:rand.uniform(999_999)}"

    VolumeRegistry.create(full_name,
      durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1}
    )
  end

  describe "scan_stripes/0" do
    test "returns empty list when all stripes healthy" do
      {:ok, vol} = create_erasure_volume()
      {:ok, _file} = WriteOperation.write_file_at(vol.id, "/healthy.txt", 0, "healthy data here")

      assert [] = StripeRepair.scan_stripes()
    end

    test "detects degraded stripe when one chunk is missing" do
      {:ok, vol} = create_erasure_volume()
      {:ok, file} = WriteOperation.write_file_at(vol.id, "/degrade.txt", 0, "degrade data")

      [%{stripe_id: sid} | _] = file.stripes
      {:ok, stripe} = StripeIndex.get(vol.id, sid)

      # Delete one chunk to make stripe degraded
      [first_hash | _] = stripe.chunks
      ChunkIndex.delete(first_hash)

      results = StripeRepair.scan_stripes()
      vol_id = vol.id
      assert [{^vol_id, ^sid, :degraded, 1}] = results
    end

    test "detects critical stripe when too many chunks missing" do
      {:ok, vol} = create_erasure_volume()
      {:ok, file} = WriteOperation.write_file_at(vol.id, "/critical.txt", 0, "critical data")

      [%{stripe_id: sid} | _] = file.stripes
      {:ok, stripe} = StripeIndex.get(vol.id, sid)

      # Delete 2 chunks (> parity_chunks = 1) to make critical
      stripe.chunks
      |> Enum.take(2)
      |> Enum.each(&ChunkIndex.delete/1)

      results = StripeRepair.scan_stripes()
      vol_id = vol.id
      assert [{^vol_id, ^sid, :critical, 2}] = results
    end

    test "sorts critical before degraded" do
      {:ok, vol} = create_erasure_volume()
      {:ok, f1} = WriteOperation.write_file_at(vol.id, "/f1.txt", 0, "first file data")
      {:ok, f2} = WriteOperation.write_file_at(vol.id, "/f2.txt", 0, "second file data here")

      [%{stripe_id: sid1} | _] = f1.stripes
      [%{stripe_id: sid2} | _] = f2.stripes
      {:ok, s1} = StripeIndex.get(vol.id, sid1)
      {:ok, s2} = StripeIndex.get(vol.id, sid2)

      # Make s1 degraded (1 missing)
      ChunkIndex.delete(hd(s1.chunks))

      # Make s2 critical (2 missing)
      s2.chunks |> Enum.take(2) |> Enum.each(&ChunkIndex.delete/1)

      results = StripeRepair.scan_stripes()
      vol_id = vol.id
      assert [{^vol_id, ^sid2, :critical, 2}, {^vol_id, ^sid1, :degraded, 1}] = results
    end

    test "emits scan telemetry" do
      {:ok, vol} = create_erasure_volume()
      {:ok, _file} = WriteOperation.write_file_at(vol.id, "/telem.txt", 0, "telemetry data")

      StripeRepair.scan_stripes()

      assert_received {[:neonfs, :stripe_repair, :scan], _ref, measurements, _meta}
      assert measurements.stripes_scanned >= 1
      assert is_integer(measurements.degraded_found)
      assert is_integer(measurements.critical_found)
    end
  end

  describe "repair_stripe/1" do
    test "is a no-op for healthy stripe" do
      {:ok, vol} = create_erasure_volume()
      {:ok, file} = WriteOperation.write_file_at(vol.id, "/healthy.txt", 0, "healthy repair data")

      [%{stripe_id: sid} | _] = file.stripes

      assert :ok = StripeRepair.repair_stripe(vol.id, sid)
    end

    test "reconstructs missing data chunk" do
      {:ok, vol} = create_erasure_volume()
      {:ok, file} = WriteOperation.write_file_at(vol.id, "/repair.txt", 0, "repair this data")

      [%{stripe_id: sid} | _] = file.stripes
      {:ok, stripe} = StripeIndex.get(vol.id, sid)

      # Delete one data chunk
      [first_hash | _] = stripe.chunks
      ChunkIndex.delete(first_hash)

      # Verify it's degraded
      vol_id = vol.id
      assert [{^vol_id, ^sid, :degraded, 1}] = StripeRepair.scan_stripes()

      # Repair
      assert :ok = StripeRepair.repair_stripe(vol.id, sid)

      # After repair, scan should show healthy (or the chunk replaced)
      # The original hash might differ from the reconstructed one,
      # but the repair should succeed
      assert_received {[:neonfs, :stripe_repair, :repair], _ref, _m, %{outcome: :success}}
    end

    test "returns error for critical stripe" do
      {:ok, vol} = create_erasure_volume()
      {:ok, file} = WriteOperation.write_file_at(vol.id, "/crit.txt", 0, "critical stripe data")

      [%{stripe_id: sid} | _] = file.stripes
      {:ok, stripe} = StripeIndex.get(vol.id, sid)

      # Delete 2 chunks (more than parity_chunks=1)
      stripe.chunks |> Enum.take(2) |> Enum.each(&ChunkIndex.delete/1)

      assert {:error, :insufficient_chunks} = StripeRepair.repair_stripe(vol.id, sid)
    end

    test "returns error for non-existent stripe" do
      assert {:error, :stripe_not_found} =
               StripeRepair.repair_stripe("vol-missing", "nonexistent-id")
    end

    test "prevents concurrent repair of same stripe" do
      {:ok, vol} = create_erasure_volume()
      {:ok, file} = WriteOperation.write_file_at(vol.id, "/conc.txt", 0, "concurrent test")

      [%{stripe_id: sid} | _] = file.stripes

      # Manually acquire lock
      :ok = LockTable.acquire_lock(sid)

      assert {:error, :repair_in_progress} = StripeRepair.repair_stripe(vol.id, sid)

      # Release and try again
      LockTable.release_lock(sid)
      assert :ok = StripeRepair.repair_stripe(vol.id, sid)
    end

    test "emits repair telemetry" do
      {:ok, vol} = create_erasure_volume()

      {:ok, file} =
        WriteOperation.write_file_at(vol.id, "/telem-repair.txt", 0, "repair telem data")

      [%{stripe_id: sid} | _] = file.stripes
      {:ok, stripe} = StripeIndex.get(vol.id, sid)

      # Delete one chunk
      ChunkIndex.delete(hd(stripe.chunks))

      StripeRepair.repair_stripe(vol.id, sid)

      assert_received {[:neonfs, :stripe_repair, :repair], _ref, %{missing_chunks: 1},
                       %{stripe_id: ^sid, state: :degraded, outcome: :start}}
    end
  end

  describe "LockTable" do
    test "acquire and release" do
      assert :ok = LockTable.acquire_lock("test-stripe")
      assert {:error, :repair_in_progress} = LockTable.acquire_lock("test-stripe")
      assert LockTable.locked?("test-stripe")

      assert :ok = LockTable.release_lock("test-stripe")
      refute LockTable.locked?("test-stripe")

      # Can re-acquire after release
      assert :ok = LockTable.acquire_lock("test-stripe")
      LockTable.release_lock("test-stripe")
    end
  end
end
