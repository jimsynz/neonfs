defmodule NeonFS.Core.ReadFileRefsTest do
  @moduledoc """
  Tests for `ReadOperation.read_file_refs/3`.

  The API returns chunk references (hash + locations + slice info) rather
  than assembled data, so interface nodes can fetch bytes directly over the
  TLS data plane.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{ReadOperation, VolumeRegistry, WriteOperation}

  alias NeonFS.Error.FileNotFound, as: FileNotFoundError
  alias NeonFS.Error.VolumeNotFound

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

    vol_name = "refs-volume-#{:rand.uniform(999_999)}"
    {:ok, volume} = VolumeRegistry.create(vol_name, [])

    {:ok, volume: volume}
  end

  describe "read_file_refs/3 — shape and metadata" do
    test "returns file size and an empty chunk list for an empty file", %{volume: volume} do
      {:ok, _} = WriteOperation.write_file(volume.id, "/empty.txt", "")

      assert {:ok, %{file_size: 0, chunks: []}} =
               ReadOperation.read_file_refs(volume.id, "/empty.txt")
    end

    test "returns a single ref for a small file that fits in one chunk", %{volume: volume} do
      data = "small file"
      {:ok, _} = WriteOperation.write_file(volume.id, "/small.txt", data)

      {:ok, result} = ReadOperation.read_file_refs(volume.id, "/small.txt")

      assert result.file_size == byte_size(data)
      assert [ref] = result.chunks

      assert is_binary(ref.hash)
      assert byte_size(ref.hash) == 32
      assert ref.original_size == byte_size(data)
      assert ref.chunk_offset == 0
      assert ref.read_start == 0
      assert ref.read_length == byte_size(data)
      assert ref.compression == :none
      assert ref.encrypted == false
      assert is_list(ref.locations)
      assert ref.locations != []

      Enum.each(ref.locations, fn loc ->
        assert is_atom(loc.node)
        assert is_binary(loc.drive_id)
        assert loc.tier in [:hot, :warm, :cold]
      end)
    end

    test "returns multiple refs for a multi-chunk file", %{volume: volume} do
      data = :crypto.strong_rand_bytes(500 * 1024)

      {:ok, file_meta} =
        WriteOperation.write_file(volume.id, "/multi.bin", data, chunk_strategy: {:fixed, 64_000})

      {:ok, result} = ReadOperation.read_file_refs(volume.id, "/multi.bin")

      assert result.file_size == byte_size(data)
      assert length(result.chunks) == length(file_meta.chunks)

      total = Enum.reduce(result.chunks, 0, &(&1.read_length + &2))
      assert total == byte_size(data)

      result.chunks
      |> Enum.reduce(0, fn ref, expected_offset ->
        assert ref.chunk_offset == expected_offset
        assert ref.read_start == 0
        assert ref.read_length == ref.original_size
        expected_offset + ref.original_size
      end)
    end
  end

  describe "read_file_refs/3 — offset and length" do
    test "slices the first chunk when offset is mid-chunk", %{volume: volume} do
      data = :crypto.strong_rand_bytes(200 * 1024)

      {:ok, _} =
        WriteOperation.write_file(volume.id, "/offset.bin", data,
          chunk_strategy: {:fixed, 64_000}
        )

      {:ok, result} =
        ReadOperation.read_file_refs(volume.id, "/offset.bin", offset: 10_000, length: 1_000)

      assert [ref] = result.chunks
      assert ref.chunk_offset == 0
      assert ref.read_start == 10_000
      assert ref.read_length == 1_000
    end

    test "returns only refs whose byte range overlaps the request", %{volume: volume} do
      data = :crypto.strong_rand_bytes(500 * 1024)

      {:ok, _} =
        WriteOperation.write_file(volume.id, "/range.bin", data, chunk_strategy: {:fixed, 64_000})

      {:ok, result} =
        ReadOperation.read_file_refs(volume.id, "/range.bin", offset: 100_000, length: 50_000)

      total = Enum.reduce(result.chunks, 0, &(&1.read_length + &2))
      assert total == 50_000

      first = List.first(result.chunks)
      last = List.last(result.chunks)

      assert first.chunk_offset + first.read_start == 100_000
      assert last.chunk_offset + last.read_start + last.read_length == 150_000
    end

    test "covers requests spanning three chunks", %{volume: volume} do
      data = :crypto.strong_rand_bytes(500 * 1024)

      {:ok, _} =
        WriteOperation.write_file(volume.id, "/three.bin", data, chunk_strategy: {:fixed, 64_000})

      offset = 32_000
      length = 128_100

      {:ok, result} =
        ReadOperation.read_file_refs(volume.id, "/three.bin", offset: offset, length: length)

      assert length(result.chunks) >= 3

      total = Enum.reduce(result.chunks, 0, &(&1.read_length + &2))
      assert total == length
    end

    test "trims the last ref when request ends before chunk end", %{volume: volume} do
      data = :crypto.strong_rand_bytes(200 * 1024)

      {:ok, _} =
        WriteOperation.write_file(volume.id, "/trim.bin", data, chunk_strategy: {:fixed, 64_000})

      {:ok, result} =
        ReadOperation.read_file_refs(volume.id, "/trim.bin", offset: 0, length: 70_000)

      total = Enum.reduce(result.chunks, 0, &(&1.read_length + &2))
      assert total == 70_000
    end

    test "returns empty chunks when offset is at or past EOF", %{volume: volume} do
      data = "short"
      {:ok, _} = WriteOperation.write_file(volume.id, "/short.txt", data)

      {:ok, result} =
        ReadOperation.read_file_refs(volume.id, "/short.txt", offset: byte_size(data))

      assert result.chunks == []

      {:ok, result2} =
        ReadOperation.read_file_refs(volume.id, "/short.txt", offset: 100)

      assert result2.chunks == []
    end

    test "returns empty chunks for zero-length read", %{volume: volume} do
      {:ok, _} = WriteOperation.write_file(volume.id, "/zero.txt", "hello")

      assert {:ok, %{chunks: []}} =
               ReadOperation.read_file_refs(volume.id, "/zero.txt", length: 0)
    end

    test "clamps length that extends past EOF", %{volume: volume} do
      data = "10 bytes!!"
      {:ok, _} = WriteOperation.write_file(volume.id, "/clamp.txt", data)

      {:ok, result} =
        ReadOperation.read_file_refs(volume.id, "/clamp.txt", offset: 0, length: 100)

      total = Enum.reduce(result.chunks, 0, &(&1.read_length + &2))
      assert total == byte_size(data)
    end
  end

  describe "read_file_refs/3 — error paths" do
    test "returns FileNotFound for missing file", %{volume: volume} do
      assert {:error, %FileNotFoundError{file_path: "/missing.txt"}} =
               ReadOperation.read_file_refs(volume.id, "/missing.txt")
    end

    test "returns VolumeNotFound for missing volume" do
      fake_volume_id = UUIDv7.generate()

      assert {:error, %VolumeNotFound{volume_id: ^fake_volume_id}} =
               ReadOperation.read_file_refs(fake_volume_id, "/anything.txt")
    end
  end

  describe "read_file_refs/3 — compression and encryption flags" do
    test "reports compression on compressed chunks", %{volume: volume} do
      {:ok, volume} =
        VolumeRegistry.update(volume.id,
          compression: %{algorithm: :zstd, level: 3, min_size: 100}
        )

      data = String.duplicate("ABCDEFGH", 1000)
      {:ok, _} = WriteOperation.write_file(volume.id, "/compressed.txt", data)

      {:ok, result} = ReadOperation.read_file_refs(volume.id, "/compressed.txt")

      assert Enum.any?(result.chunks, &(&1.compression == :zstd))
    end
  end

  describe "read_file_refs/3 — slice invariants for the read path" do
    test "refs for a full-file read match chunks assembled by read_file", %{volume: volume} do
      data = :crypto.strong_rand_bytes(300 * 1024)

      {:ok, _} =
        WriteOperation.write_file(volume.id, "/invariant.bin", data,
          chunk_strategy: {:fixed, 64_000}
        )

      {:ok, %{chunks: refs, file_size: file_size}} =
        ReadOperation.read_file_refs(volume.id, "/invariant.bin")

      assert file_size == byte_size(data)

      # Whole-file refs should span every byte of the file: offsets are
      # contiguous and read_length covers each chunk in full.
      refs
      |> Enum.reduce(0, fn ref, expected_offset ->
        assert ref.chunk_offset == expected_offset
        assert ref.read_start == 0
        assert ref.read_length == ref.original_size
        expected_offset + ref.original_size
      end)
      |> then(fn total -> assert total == byte_size(data) end)
    end

    test "sliced-range refs carry identical hashes and read boundaries to their full-range counterparts",
         %{volume: volume} do
      data = :crypto.strong_rand_bytes(300 * 1024)

      {:ok, _} =
        WriteOperation.write_file(volume.id, "/slice.bin", data, chunk_strategy: {:fixed, 64_000})

      {:ok, %{chunks: whole}} = ReadOperation.read_file_refs(volume.id, "/slice.bin")

      offset = 50_000
      length = 150_000
      end_byte = offset + length

      {:ok, %{chunks: sliced}} =
        ReadOperation.read_file_refs(volume.id, "/slice.bin", offset: offset, length: length)

      # Every sliced ref must match an existing whole-file ref by hash +
      # chunk_offset, guaranteeing the data plane will fetch the same bytes.
      whole_by_hash = Map.new(whole, fn r -> {r.hash, r} end)

      Enum.each(sliced, fn ref ->
        whole_ref = Map.fetch!(whole_by_hash, ref.hash)
        assert ref.chunk_offset == whole_ref.chunk_offset
        assert ref.original_size == whole_ref.original_size

        # read slice stays within the chunk
        assert ref.read_start >= 0
        assert ref.read_start + ref.read_length <= ref.original_size

        # read slice aligns with the requested file range
        file_start = ref.chunk_offset + ref.read_start
        file_end = file_start + ref.read_length
        assert file_start >= offset
        assert file_end <= end_byte
      end)

      total = Enum.reduce(sliced, 0, &(&1.read_length + &2))
      assert total == length
    end
  end
end
