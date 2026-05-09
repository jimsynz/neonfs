defmodule NeonFS.Core.GarbageCollectorTest do
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{
    BlobStore,
    ChunkIndex,
    FileIndex,
    GarbageCollector,
    StripeIndex,
    VolumeRegistry,
    WriteOperation
  }

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_core_subsystems()
    start_stripe_index()

    telemetry_ref =
      :telemetry_test.attach_event_handlers(self(), [
        [:neonfs, :garbage_collector, :collect]
      ])

    on_exit(fn -> cleanup_test_dirs() end)
    {:ok, telemetry_ref: telemetry_ref}
  end

  describe "collect/0 with replicated volumes" do
    setup do
      {:ok, vol} = VolumeRegistry.create("gc-rep-vol", [])
      {:ok, volume: vol}
    end

    test "keeps referenced chunks", %{volume: vol} do
      {:ok, _file} = WriteOperation.write_file_streamed(vol.id, "/keep.txt", ["hello world"])

      assert {:ok, result} = GarbageCollector.collect()
      assert result.chunks_deleted == 0
    end

    test "deletes unreferenced chunks after file deletion", %{volume: vol} do
      {:ok, file} = WriteOperation.write_file_streamed(vol.id, "/delete-me.txt", ["temp data"])
      chunk_hashes = file.chunks

      # Delete the file
      FileIndex.delete(file.id)

      assert {:ok, result} = GarbageCollector.collect()
      assert result.chunks_deleted > 0

      # Chunks should be gone from ChunkIndex
      Enum.each(chunk_hashes, fn hash ->
        assert {:error, :not_found} = ChunkIndex.get("vol-test", hash)
      end)
    end

    test "removes blob files from disk when deleting unreferenced chunks", %{volume: vol} do
      {:ok, file} =
        WriteOperation.write_file_streamed(vol.id, "/blob-check.txt", ["blob data here"])

      chunk_hashes = file.chunks

      # Verify blobs exist on disk before GC
      Enum.each(chunk_hashes, fn hash ->
        assert {:ok, _tier, _size} = BlobStore.chunk_info(hash)
      end)

      # Delete the file and run GC
      FileIndex.delete(file.id)
      assert {:ok, result} = GarbageCollector.collect()
      assert result.chunks_deleted > 0

      # Blob files should be gone from disk
      Enum.each(chunk_hashes, fn hash ->
        assert {:error, :not_found} = BlobStore.chunk_info(hash)
      end)
    end

    test "does not delete chunks with active_write_refs", %{volume: vol} do
      {:ok, file} =
        WriteOperation.write_file_streamed(vol.id, "/protected.txt", ["protected data"])

      [hash | _] = file.chunks

      # Add a write ref to protect the chunk
      ChunkIndex.add_write_ref(hash, "fake-write-id")

      # Delete the file
      FileIndex.delete(file.id)

      assert {:ok, result} = GarbageCollector.collect()
      assert result.chunks_protected >= 1

      # The protected chunk should still exist
      assert {:ok, _} = ChunkIndex.get("vol-test", hash)
    end

    test "emits telemetry event", %{volume: vol} do
      {:ok, _file} = WriteOperation.write_file_streamed(vol.id, "/telem.txt", ["data"])

      assert {:ok, _result} = GarbageCollector.collect()

      assert_received {[:neonfs, :garbage_collector, :collect], _ref, measurements, _meta}
      assert is_integer(measurements.duration)
      assert is_integer(measurements.chunks_deleted)
      assert is_integer(measurements.stripes_deleted)
    end
  end

  describe "collect/0 with erasure-coded volumes" do
    setup do
      {:ok, vol} =
        VolumeRegistry.create("gc-ec-vol",
          durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1}
        )

      {:ok, volume: vol}
    end

    test "keeps stripe chunks for referenced erasure file", %{volume: vol} do
      {:ok, _file} = WriteOperation.write_file_at(vol.id, "/ec-keep.txt", 0, "erasure data here")

      assert {:ok, result} = GarbageCollector.collect()
      assert result.chunks_deleted == 0
      assert result.stripes_deleted == 0
    end

    test "deletes stripe chunks after file deletion", %{volume: vol} do
      {:ok, file} = WriteOperation.write_file_at(vol.id, "/ec-delete.txt", 0, "erasure temp data")

      # Get stripe chunk hashes before deletion
      [%{stripe_id: sid} | _] = file.stripes
      {:ok, stripe} = StripeIndex.get(vol.id, sid)
      chunk_hashes = stripe.chunks

      # Delete the file
      FileIndex.delete(file.id)

      assert {:ok, result} = GarbageCollector.collect()
      assert result.chunks_deleted > 0

      # All stripe chunks (data + parity) should be deleted
      Enum.each(chunk_hashes, fn hash ->
        assert {:error, :not_found} = ChunkIndex.get("vol-test", hash)
      end)
    end

    test "cleans up orphaned stripe metadata", %{volume: vol} do
      {:ok, file} =
        WriteOperation.write_file_at(vol.id, "/ec-orphan.txt", 0, "orphan stripe data")

      [%{stripe_id: sid} | _] = file.stripes

      # Delete the file — stripe metadata becomes orphaned
      FileIndex.delete(file.id)

      assert {:ok, result} = GarbageCollector.collect()
      assert result.stripes_deleted > 0

      # Orphaned stripe should be gone
      assert {:error, :not_found} = StripeIndex.get(vol.id, sid)
    end

    test "does not delete active-write-protected stripe chunks", %{volume: vol} do
      {:ok, file} = WriteOperation.write_file_at(vol.id, "/ec-prot.txt", 0, "protect me")

      [%{stripe_id: sid} | _] = file.stripes
      {:ok, stripe} = StripeIndex.get(vol.id, sid)
      [first_hash | _] = stripe.chunks

      # Protect one chunk with active write ref
      ChunkIndex.add_write_ref(first_hash, "fake-write")

      # Delete the file
      FileIndex.delete(file.id)

      assert {:ok, result} = GarbageCollector.collect()
      assert result.chunks_protected >= 1

      # Protected chunk still exists
      assert {:ok, _} = ChunkIndex.get("vol-test", first_hash)
    end
  end

  describe "collect/0 with mixed volumes" do
    setup do
      {:ok, rep_vol} = VolumeRegistry.create("gc-mix-rep", [])

      {:ok, ec_vol} =
        VolumeRegistry.create("gc-mix-ec",
          durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1}
        )

      {:ok, rep_vol: rep_vol, ec_vol: ec_vol}
    end

    test "handles both replicated and erasure files in same pass", %{
      rep_vol: rep_vol,
      ec_vol: ec_vol
    } do
      {:ok, rep_file} = WriteOperation.write_file_at(rep_vol.id, "/rep.txt", 0, "replicated")
      {:ok, ec_file} = WriteOperation.write_file_at(ec_vol.id, "/ec.txt", 0, "erasure coded")

      # Keep both files — nothing should be deleted
      assert {:ok, result} = GarbageCollector.collect()
      assert result.chunks_deleted == 0

      # Delete only replicated file
      FileIndex.delete(rep_file.id)
      assert {:ok, result2} = GarbageCollector.collect()
      assert result2.chunks_deleted > 0

      # EC file's chunks should still exist
      [%{stripe_id: sid} | _] = ec_file.stripes
      {:ok, stripe} = StripeIndex.get(ec_vol.id, sid)

      Enum.each(stripe.chunks, fn hash ->
        assert {:ok, _} = ChunkIndex.get("vol-test", hash)
      end)
    end

    test "deletes both replicated and erasure chunks when both files deleted", %{
      rep_vol: rep_vol,
      ec_vol: ec_vol
    } do
      {:ok, rep_file} = WriteOperation.write_file_at(rep_vol.id, "/del-rep.txt", 0, "delete rep")
      {:ok, ec_file} = WriteOperation.write_file_at(ec_vol.id, "/del-ec.txt", 0, "delete ec")

      FileIndex.delete(rep_file.id)
      FileIndex.delete(ec_file.id)

      assert {:ok, result} = GarbageCollector.collect()
      assert result.chunks_deleted > 0
      assert result.stripes_deleted > 0
    end
  end

  describe "orphaned stripe cleanup" do
    test "deletes stripe not referenced by any file" do
      # Directly insert a stripe into StripeIndex without any file reference
      alias NeonFS.Core.Stripe

      stripe =
        Stripe.new(%{
          id: UUIDv7.generate(),
          volume_id: "nonexistent-volume",
          config: %{data_chunks: 2, parity_chunks: 1, chunk_size: 1024},
          chunks: [],
          partial: false,
          data_bytes: 2048,
          padded_bytes: 0
        })

      {:ok, _} = StripeIndex.put(stripe)

      assert {:ok, result} = GarbageCollector.collect()
      assert result.stripes_deleted >= 1
      assert {:error, :not_found} = StripeIndex.get(stripe.volume_id, stripe.id)
    end
  end
end
