defmodule NeonFS.Core.SparseReadTest do
  # `truncate/3` grows a file without allocating chunks, so a declared size can
  # exceed the bytes any chunk backs. Those bytes are inside the file as far as
  # every caller is concerned, and POSIX requires them to read as zeros — the
  # read path has to synthesise them rather than stop at the last chunk.
  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core
  alias NeonFS.Core.{ChunkIndex, FileIndex, ReadOperation, VolumeRegistry, WriteOperation}

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

    {:ok, volume} = VolumeRegistry.create("sparse-#{:rand.uniform(999_999)}", [])
    {:ok, volume: volume}
  end

  describe "a file grown past its chunks" do
    test "streams the sparse tail as zeros", %{volume: volume} do
      {:ok, file} = WriteOperation.write_file_streamed(volume.id, "/sparse.img", ["abc"])
      {:ok, grown} = FileIndex.truncate(file.id, 8192)
      assert grown.size == 8192

      {:ok, %{stream: stream, file_size: size}} =
        ReadOperation.read_file_stream_by_id(volume.id, file.id, [])

      read = Enum.into(stream, <<>>)

      assert size == 8192
      assert byte_size(read) == 8192, "a read must not stop at the last chunk"
      assert binary_part(read, 0, 3) == "abc"
      assert binary_part(read, 3, 8189) == :binary.copy(<<0>>, 8189)
    end

    test "range-reads the sparse tail as zeros", %{volume: volume} do
      {:ok, _} = WriteOperation.write_file_streamed(volume.id, "/ranged.img", ["abc"])
      {:ok, _} = Core.truncate_file(volume.name, "/ranged.img", 8192)

      assert {:ok, read} =
               ReadOperation.read_file(volume.id, "/ranged.img", offset: 0, length: 4096)

      assert byte_size(read) == 4096
      assert binary_part(read, 3, 4093) == :binary.copy(<<0>>, 4093)
    end

    # The case a block device hits constantly: every sector of a freshly sized
    # image is hole, so a read wholly inside it must still return its bytes.
    test "reads a range that lies entirely inside the hole", %{volume: volume} do
      {:ok, _} = WriteOperation.write_file_streamed(volume.id, "/whole.img", ["abc"])
      {:ok, _} = Core.truncate_file(volume.name, "/whole.img", 8192)

      {:ok, %{stream: stream}} =
        Core.read_file_stream(volume.name, "/whole.img", offset: 4096, length: 4096)

      assert Enum.into(stream, <<>>) == :binary.copy(<<0>>, 4096)
    end

    test "synthesises the hole in bounded blocks", %{volume: volume} do
      {:ok, file} = WriteOperation.write_file_streamed(volume.id, "/big.img", ["abc"])
      {:ok, _} = FileIndex.truncate(file.id, 512 * 1024)

      {:ok, %{stream: stream}} = ReadOperation.read_file_stream_by_id(volume.id, file.id, [])
      blocks = Enum.to_list(stream)

      assert Enum.sum(Enum.map(blocks, &byte_size/1)) == 512 * 1024

      assert Enum.all?(blocks, &(byte_size(&1) <= 64 * 1024)),
             "a sized-but-unwritten device is entirely hole, so one binary would be an OOM"
    end

    test "reports the hole to refs callers instead of leaving them to infer it", %{volume: volume} do
      {:ok, file} = WriteOperation.write_file_streamed(volume.id, "/refs.img", ["abc"])
      {:ok, _} = FileIndex.truncate(file.id, 8192)

      assert {:ok, %{file_size: 8192, hole_bytes: 8189}} =
               ReadOperation.read_file_refs_by_id(volume.id, file.id, [])
    end
  end

  # Zero-filling a range whose chunk metadata went missing would dress
  # corruption up as valid data. A short read is the honest answer there, so the
  # synthesis is suppressed when the chunk list came back incomplete.
  test "does not zero-fill over a chunk whose metadata is missing", %{volume: volume} do
    {:ok, file} = WriteOperation.write_file_streamed(volume.id, "/corrupt.img", ["abc"])
    {:ok, _} = FileIndex.truncate(file.id, 8192)

    :ok = ChunkIndex.delete(hd(file.chunks))

    {:ok, %{stream: stream}} = ReadOperation.read_file_stream_by_id(volume.id, file.id, [])

    assert Enum.into(stream, <<>>) == <<>>,
           "a file missing chunk metadata must not read back as plausible zeros"

    assert {:ok, %{hole_bytes: 0}} = ReadOperation.read_file_refs_by_id(volume.id, file.id, [])
  end
end
