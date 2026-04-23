defmodule NeonFS.Core.CommitChunksTest do
  @moduledoc """
  Unit tests for `NeonFS.Core.CommitChunks`, the write-side counterpart to
  `read_file_refs`.

  The tests simulate the "interface-side chunking" flow from #408 by
  writing chunks straight into the local `BlobStore` (standing in for a
  remote `Router.data_call(:put_chunk, …)`) and then driving
  `commit_chunks/4` with the resulting hashes. The reconcile and commit
  paths are exercised without needing a second peer node.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.ChunkIndex
  alias NeonFS.Core.CommitChunks
  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.VolumeRegistry

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

    volume_name = "commit-chunks-#{:rand.uniform(999_999_999)}"
    {:ok, volume} = VolumeRegistry.create(volume_name, [])

    {:ok, volume: volume}
  end

  defp put_chunk(data) do
    {:ok, hash, info} = BlobStore.write_chunk(data, "default", "hot", [])
    {hash, info.stored_size}
  end

  defp local_location, do: %{node: node(), drive_id: "default", tier: :hot}

  describe "commit_chunks/4 — happy path" do
    test "commits a file whose chunks are already on disk", %{volume: volume} do
      chunk_a = :binary.copy(<<0xAA>>, 1024)
      chunk_b = :binary.copy(<<0xBB>>, 2048)

      {hash_a, _} = put_chunk(chunk_a)
      {hash_b, _} = put_chunk(chunk_b)

      locations = %{hash_a => [local_location()], hash_b => [local_location()]}

      path = "/commit/happy.bin"

      assert {:ok, file_meta} =
               CommitChunks.commit(volume.id, path, [hash_a, hash_b],
                 total_size: byte_size(chunk_a) + byte_size(chunk_b),
                 locations: locations
               )

      assert file_meta.path == path
      assert file_meta.size == byte_size(chunk_a) + byte_size(chunk_b)
      assert file_meta.chunks == [hash_a, hash_b]

      assert {:ok, stored_a} = ChunkIndex.get(hash_a)
      assert stored_a.commit_state == :committed
      assert Enum.any?(stored_a.locations, &(&1.node == node()))

      assert {:ok, stored_b} = ChunkIndex.get(hash_b)
      assert stored_b.commit_state == :committed
    end

    test "overwrites an existing FileIndex entry at the same path", %{volume: volume} do
      chunk_1 = :binary.copy(<<1>>, 512)
      chunk_2 = :binary.copy(<<2>>, 512)

      {hash_1, _} = put_chunk(chunk_1)
      {hash_2, _} = put_chunk(chunk_2)

      path = "/commit/overwrite.bin"
      loc = %{hash_1 => [local_location()]}

      assert {:ok, _first} =
               CommitChunks.commit(volume.id, path, [hash_1],
                 total_size: byte_size(chunk_1),
                 locations: loc
               )

      loc2 = %{hash_2 => [local_location()]}

      assert {:ok, second} =
               CommitChunks.commit(volume.id, path, [hash_2],
                 total_size: byte_size(chunk_2),
                 locations: loc2
               )

      assert second.chunks == [hash_2]
      assert second.size == byte_size(chunk_2)

      assert {:ok, only} = FileIndex.get_by_path(volume.id, path)
      assert only.id == second.id
    end
  end

  describe "commit_chunks/4 — error paths" do
    test "returns {:missing_chunk, hash} when the blob is not on any reported location",
         %{volume: volume} do
      chunk = :binary.copy(<<0xCC>>, 256)
      phantom_hash = :crypto.hash(:sha256, "never-written")

      {real_hash, _} = put_chunk(chunk)

      locations = %{
        real_hash => [local_location()],
        phantom_hash => [local_location()]
      }

      assert {:error, {:missing_chunk, ^phantom_hash}} =
               CommitChunks.commit(volume.id, "/commit/missing.bin", [real_hash, phantom_hash],
                 total_size: byte_size(chunk) + 1,
                 locations: locations
               )

      # The FileIndex entry must not have been created.
      assert {:error, :not_found} = FileIndex.get_by_path(volume.id, "/commit/missing.bin")
    end

    test "returns {:unknown_chunk_location, hash} when locations map has no entry for a hash",
         %{volume: volume} do
      chunk = :binary.copy(<<0xDD>>, 128)
      {real_hash, _} = put_chunk(chunk)
      orphan_hash = :crypto.hash(:sha256, "orphan")

      # Only the real hash has a location — the orphan is unmentioned.
      locations = %{real_hash => [local_location()]}

      assert {:error, {:unknown_chunk_location, ^orphan_hash}} =
               CommitChunks.commit(volume.id, "/commit/orphan.bin", [real_hash, orphan_hash],
                 total_size: byte_size(chunk),
                 locations: locations
               )

      assert {:error, :not_found} = FileIndex.get_by_path(volume.id, "/commit/orphan.bin")
    end
  end

  describe "commit_chunks/4 — idempotence and reuse" do
    test "second commit with the same chunk hash adds a write ref and still commits",
         %{volume: volume} do
      chunk = :binary.copy(<<0xEE>>, 1024)
      {hash, _} = put_chunk(chunk)
      loc = %{hash => [local_location()]}

      assert {:ok, _first} =
               CommitChunks.commit(volume.id, "/commit/first.bin", [hash],
                 total_size: byte_size(chunk),
                 locations: loc
               )

      assert {:ok, _second} =
               CommitChunks.commit(volume.id, "/commit/second.bin", [hash],
                 total_size: byte_size(chunk),
                 locations: loc
               )

      assert {:ok, meta} = ChunkIndex.get(hash)
      assert meta.commit_state == :committed
    end
  end
end
