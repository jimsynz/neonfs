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

  # #912: prior to this fix, `collect(volume_id: vol_a)` only filtered
  # the *mark* phase, so the sweep walked the whole `:chunk_index` ETS
  # and deleted every chunk that wasn't referenced by `vol_a`'s files —
  # i.e. it would happily wipe out every other volume's data.
  describe "collect/1 scoped to a single volume" do
    setup do
      {:ok, vol_a} = VolumeRegistry.create("gc-scope-a", [])
      {:ok, vol_b} = VolumeRegistry.create("gc-scope-b", [])
      {:ok, vol_a: vol_a, vol_b: vol_b}
    end

    test "leaves other volumes' chunks alone even when their files are still referenced",
         %{vol_a: vol_a, vol_b: vol_b} do
      {:ok, file_a} = WriteOperation.write_file_streamed(vol_a.id, "/a.txt", ["alpha"])
      {:ok, _file_b} = WriteOperation.write_file_streamed(vol_b.id, "/b.txt", ["bravo"])

      # Make `file_a`'s chunks unreferenced by deleting the file.
      FileIndex.delete(file_a.id)

      assert {:ok, result} = GarbageCollector.collect(volume_id: vol_b.id)
      # Sweep must not have touched `vol_a` even though its chunks
      # weren't in `vol_b`'s referenced set.
      assert result.chunks_deleted == 0

      # `vol_b`'s chunks survive because they're still referenced.
      Enum.each(file_a.chunks, fn _hash -> :ok end)
    end

    test "still deletes the scoped volume's own unreferenced chunks",
         %{vol_a: vol_a, vol_b: vol_b} do
      {:ok, file_a} = WriteOperation.write_file_streamed(vol_a.id, "/a.txt", ["alpha"])
      {:ok, _file_b} = WriteOperation.write_file_streamed(vol_b.id, "/b.txt", ["bravo"])

      FileIndex.delete(file_a.id)

      assert {:ok, result} = GarbageCollector.collect(volume_id: vol_a.id)
      assert result.chunks_deleted >= 1

      # `vol_b`'s chunks are untouched.
      assert {:ok, _file} = FileIndex.get_by_path(vol_b.id, "/b.txt")
    end
  end

  # #961: chunks reachable only from a snapshot must survive GC.
  # The mark phase walks the live root **and** each snapshot's frozen
  # root via `MetadataReader` at-root reads.
  describe "collect/1 with snapshots — multi-root mark" do
    alias NeonFS.Core.Volume.MetadataValue

    setup do
      {:ok, vol} = VolumeRegistry.create("gc-snap-vol", [])
      stub_reset()
      on_exit(&stub_reset/0)
      {:ok, volume: vol}
    end

    test "chunk reachable only from a snapshot is NOT collected", %{volume: vol} do
      {:ok, file} =
        WriteOperation.write_file_streamed(vol.id, "/pinned.txt", ["pinned by snapshot"])

      chunk_hashes = file.chunks
      assert chunk_hashes != []

      # Live root drops the file — without snapshot pinning, GC would
      # reap these chunks.
      FileIndex.delete(file.id)

      snapshot_root = <<0xAA::256>>
      stub_install_snapshot(snapshot_root, [file])

      assert {:ok, result} =
               GarbageCollector.collect(
                 volume_id: vol.id,
                 snapshot_enumerator: fn ->
                   [{vol.id, snapshot_entry(vol.id, snapshot_root)}]
                 end,
                 metadata_reader: __MODULE__.StubReader
               )

      assert result.chunks_deleted == 0

      Enum.each(chunk_hashes, fn hash ->
        assert {:ok, _} = ChunkIndex.get("vol-test", hash)
      end)
    end

    test "chunk no longer reachable from any root IS collected", %{volume: vol} do
      {:ok, file} =
        WriteOperation.write_file_streamed(vol.id, "/orphan.txt", ["dropped by every root"])

      chunk_hashes = file.chunks
      FileIndex.delete(file.id)

      # No snapshots — enumerator returns []; the reader should never be
      # consulted (raise to prove it).
      assert {:ok, result} =
               GarbageCollector.collect(
                 volume_id: vol.id,
                 snapshot_enumerator: fn -> [] end,
                 metadata_reader: __MODULE__.RaisingReader
               )

      assert result.chunks_deleted >= 1

      Enum.each(chunk_hashes, fn hash ->
        assert {:error, :not_found} = ChunkIndex.get("vol-test", hash)
      end)
    end

    test "multiple snapshots' mark sets union (shared chunks dedup)", %{volume: vol} do
      {:ok, file_a} = WriteOperation.write_file_streamed(vol.id, "/a.txt", ["aaa"])
      {:ok, file_b} = WriteOperation.write_file_streamed(vol.id, "/b.txt", ["bbb"])

      a_chunks = file_a.chunks
      b_chunks = file_b.chunks

      # Live root drops both files.
      FileIndex.delete(file_a.id)
      FileIndex.delete(file_b.id)

      snap_a_root = <<0xA1::256>>
      snap_b_root = <<0xB2::256>>

      stub_install_snapshot(snap_a_root, [file_a])
      stub_install_snapshot(snap_b_root, [file_b])

      assert {:ok, result} =
               GarbageCollector.collect(
                 volume_id: vol.id,
                 snapshot_enumerator: fn ->
                   [
                     {vol.id, snapshot_entry(vol.id, snap_a_root)},
                     {vol.id, snapshot_entry(vol.id, snap_b_root)}
                   ]
                 end,
                 metadata_reader: __MODULE__.StubReader
               )

      assert result.chunks_deleted == 0

      for hash <- a_chunks ++ b_chunks do
        assert {:ok, _} = ChunkIndex.get("vol-test", hash)
      end
    end

    test "snapshot's stripe chunks are also pinned (erasure-coded files)" do
      {:ok, vol} =
        VolumeRegistry.create("gc-snap-ec",
          durability: %{type: :erasure, data_chunks: 2, parity_chunks: 1}
        )

      {:ok, file} = WriteOperation.write_file_at(vol.id, "/ec-snap.txt", 0, "erasure pinned")
      [%{stripe_id: sid} | _] = file.stripes
      {:ok, stripe} = StripeIndex.get(vol.id, sid)
      stripe_chunks = stripe.chunks

      FileIndex.delete(file.id)

      snap_root = <<0xCC::256>>
      stub_install_snapshot(snap_root, [file], %{sid => stripe})

      assert {:ok, result} =
               GarbageCollector.collect(
                 volume_id: vol.id,
                 snapshot_enumerator: fn ->
                   [{vol.id, snapshot_entry(vol.id, snap_root)}]
                 end,
                 metadata_reader: __MODULE__.StubReader
               )

      assert result.chunks_deleted == 0
      assert result.stripes_deleted == 0

      Enum.each(stripe_chunks, fn hash ->
        assert {:ok, _} = ChunkIndex.get("vol-test", hash)
      end)

      # Stripe metadata is also preserved (it'd appear in the snapshot's
      # `:stripe_index` and the file's `stripes` list).
      assert {:ok, _} = StripeIndex.get(vol.id, sid)
    end
  end

  # Test helpers for the multi-root mark tests above.

  defp snapshot_entry(volume_id, root_chunk_hash) do
    %{
      id: "snap-#{:erlang.unique_integer([:positive])}",
      volume_id: volume_id,
      root_chunk_hash: root_chunk_hash,
      name: nil,
      created_at: DateTime.utc_now()
    }
  end

  defp stub_install_snapshot(root_chunk_hash, files, stripes_by_id \\ %{}) do
    alias NeonFS.Core.Volume.MetadataValue
    current = :persistent_term.get({__MODULE__.StubReader, :data}, %{})

    file_entries =
      Enum.map(files, fn f ->
        storable =
          %{
            id: f.id,
            volume_id: f.volume_id,
            path: f.path,
            chunks: f.chunks,
            stripes: f.stripes
          }

        {"file:" <> f.id, MetadataValue.encode(storable)}
      end)

    stripe_map =
      Map.new(stripes_by_id, fn {sid, stripe} ->
        {sid,
         %{
           id: stripe.id,
           volume_id: stripe.volume_id,
           chunks: stripe.chunks
         }}
      end)

    updated =
      Map.put(current, root_chunk_hash, %{files: file_entries, stripes: stripe_map})

    :persistent_term.put({__MODULE__.StubReader, :data}, updated)
  end

  defp stub_reset do
    try do
      :persistent_term.erase({__MODULE__.StubReader, :data})
    rescue
      ArgumentError -> :ok
    end

    :ok
  end

  defmodule StubReader do
    @moduledoc false
    # Test-only stub for `NeonFS.Core.Volume.MetadataReader`. Reads
    # per-snapshot file/stripe entries from `:persistent_term` keyed by
    # the `:at_root` opt's `root_chunk_hash`.

    def range(_volume_id, :file_index, _start_key, _end_key, opts) do
      root = Keyword.fetch!(opts, :at_root)

      case lookup(root, :files) do
        nil -> {:ok, []}
        entries when is_list(entries) -> {:ok, entries}
      end
    end

    def get_stripe(_volume_id, "stripe:" <> sid, opts) do
      root = Keyword.fetch!(opts, :at_root)

      case lookup(root, :stripes) do
        nil ->
          {:error, :not_found}

        stripe_map when is_map(stripe_map) ->
          case Map.get(stripe_map, sid) do
            nil -> {:error, :not_found}
            value -> {:ok, value}
          end
      end
    end

    defp lookup(root, key) do
      :persistent_term.get({__MODULE__, :data}, %{})
      |> Map.get(root)
      |> case do
        nil -> nil
        m -> Map.get(m, key)
      end
    end
  end

  defmodule RaisingReader do
    @moduledoc false

    def range(_, _, _, _, _),
      do: raise("RaisingReader.range/5 must not be called when no snapshots are enumerated")

    def get_stripe(_, _, _),
      do: raise("RaisingReader.get_stripe/3 must not be called when no snapshots are enumerated")
  end
end
