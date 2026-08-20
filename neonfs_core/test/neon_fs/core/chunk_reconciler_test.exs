defmodule NeonFS.Core.ChunkReconcilerTest do
  @moduledoc """
  Unit tests for `NeonFS.Core.ChunkReconciler` in its own right, rather than
  through `CommitChunks`.

  It has two callers — the file commit and the block device's batched extent
  commit — so its contract has to hold without a `FileIndex` entry anywhere in
  sight. Chunks are written straight into the local `BlobStore`, standing in
  for a remote `Router.data_call(:put_chunk, …)`, which is what lets the
  location probe answer without a second peer.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.ChunkIndex
  alias NeonFS.Core.ChunkReconciler
  alias NeonFS.Core.VolumeRegistry
  alias NeonFS.Core.WriteOperation

  @moduletag :tmp_dir

  setup %{tmp_dir: tmp_dir} do
    configure_test_dirs(tmp_dir)
    stop_ra()
    start_drive_registry()
    start_blob_store()
    start_chunk_index()
    start_file_index()
    start_stripe_index()
    start_volume_registry()
    ensure_chunk_access_tracker()

    on_exit(fn -> cleanup_test_dirs() end)

    volume_name = "reconciler-#{:rand.uniform(999_999_999)}"
    {:ok, volume} = VolumeRegistry.create(volume_name, [])

    {:ok, volume: volume, write_id: WriteOperation.generate_write_id()}
  end

  defp put_chunk(data) do
    {:ok, hash, _info} = BlobStore.write_chunk(data, "default", "hot", [])
    hash
  end

  defp local_location, do: %{node: node(), drive_id: "default", tier: :hot}

  describe "reconcile/5" do
    test "materialises metadata for chunks that are really on disk", %{
      volume: volume,
      write_id: write_id
    } do
      a = put_chunk(:binary.copy(<<0xAA>>, 1024))
      b = put_chunk(:binary.copy(<<0xBB>>, 2048))
      locations = %{a => [local_location()], b => [local_location()]}

      assert {:ok, [meta_a, meta_b]} =
               ChunkReconciler.reconcile(volume.id, [a, b], locations, %{}, write_id)

      assert meta_a.hash == a
      assert meta_b.hash == b
      assert meta_a.stored_size == 1024
      assert meta_b.stored_size == 2048
    end

    # The order is the caller's, not the index's: a file's chunk list and a
    # device's extent map are both positional, so a reordered result would
    # scramble the data it describes.
    test "returns metadata in the order the hashes were given", %{
      volume: volume,
      write_id: write_id
    } do
      hashes = for i <- 1..5, do: put_chunk(:binary.copy(<<i>>, 512 * i))
      locations = Map.new(hashes, &{&1, [local_location()]})

      assert {:ok, metas} =
               ChunkReconciler.reconcile(volume.id, hashes, locations, %{}, write_id)

      assert Enum.map(metas, & &1.hash) == hashes
    end

    # Nothing may be published pointing at a chunk no location will admit to
    # holding — that is the whole reason this step exists.
    test "refuses a chunk no location holds", %{volume: volume, write_id: write_id} do
      absent = :crypto.hash(:sha256, "never-written")
      locations = %{absent => [local_location()]}

      assert {:error, {:missing_chunk, ^absent}} =
               ChunkReconciler.reconcile(volume.id, [absent], locations, %{}, write_id)
    end

    test "refuses a hash the caller supplied no location for", %{
      volume: volume,
      write_id: write_id
    } do
      hash = put_chunk("orphan")

      assert {:error, {:unknown_chunk_location, ^hash}} =
               ChunkReconciler.reconcile(volume.id, [hash], %{}, %{}, write_id)
    end

    test "reports the first failure and does not keep going", %{
      volume: volume,
      write_id: write_id
    } do
      good = put_chunk("present")
      absent = :crypto.hash(:sha256, "also-never-written")

      locations = %{good => [local_location()], absent => [local_location()]}

      assert {:error, {:missing_chunk, ^absent}} =
               ChunkReconciler.reconcile(
                 volume.id,
                 [good, absent, absent],
                 locations,
                 %{},
                 write_id
               )
    end

    test "holds the caller's write ref and leaves the chunk uncommitted", %{
      volume: volume,
      write_id: write_id
    } do
      hash = put_chunk("held")
      locations = %{hash => [local_location()]}

      assert {:ok, [meta]} =
               ChunkReconciler.reconcile(volume.id, [hash], locations, %{}, write_id)

      assert meta.commit_state == :uncommitted
      assert MapSet.member?(meta.active_write_refs, write_id)

      assert {:ok, stored} = ChunkIndex.lookup_by_hash(hash)
      assert MapSet.member?(stored.active_write_refs, write_id)
    end

    # Deduplication across two callers: the second reconcile adopts the chunk
    # rather than recreating it, and both refs are held at once.
    test "a second writer adopts an existing chunk without displacing the first", %{
      volume: volume,
      write_id: first
    } do
      hash = put_chunk("shared")
      locations = %{hash => [local_location()]}
      second = WriteOperation.generate_write_id()

      assert {:ok, _} = ChunkReconciler.reconcile(volume.id, [hash], locations, %{}, first)
      assert {:ok, [meta]} = ChunkReconciler.reconcile(volume.id, [hash], locations, %{}, second)

      assert MapSet.member?(meta.active_write_refs, first)
      assert MapSet.member?(meta.active_write_refs, second)
    end

    test "carries the codec the writer reported", %{volume: volume, write_id: write_id} do
      hash = put_chunk("codec-bearing")
      locations = %{hash => [local_location()]}
      codecs = %{hash => %{compression: :zstd, crypto: nil, original_size: 9999}}

      assert {:ok, [meta]} =
               ChunkReconciler.reconcile(volume.id, [hash], locations, codecs, write_id)

      assert meta.compression == :zstd
      assert meta.original_size == 9999
    end

    test "an empty hash list reconciles to nothing", %{volume: volume, write_id: write_id} do
      assert {:ok, []} = ChunkReconciler.reconcile(volume.id, [], %{}, %{}, write_id)
    end

    # A writer that reports several replicas has only to be right about one of
    # them for the data to be there, so the probe short-circuits rather than
    # requiring every location to answer.
    test "accepts a chunk when only one of the reported locations holds it", %{
      volume: volume,
      write_id: write_id
    } do
      hash = put_chunk("one-of-three")
      absent_node = %{node: :nowhere@nohost, drive_id: "default", tier: :hot}
      locations = %{hash => [absent_node, local_location()]}

      assert {:ok, [meta]} =
               ChunkReconciler.reconcile(volume.id, [hash], locations, %{}, write_id)

      assert meta.hash == hash
      assert Enum.any?(meta.locations, &(&1.node == node()))
    end
  end
end
