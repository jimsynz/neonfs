defmodule NeonFS.Core.BlockWriteContentionTest do
  @moduledoc """
  Concurrent writers to distinct chunks of one block device.

  The partial-write commit is a compare-and-swap, and comparing the whole
  chunk list made every change anywhere in a file invalidate every concurrent
  writer — so disjointness bought nothing and a crowded device thrashed,
  each loser redoing a read, re-chunk, re-hash, re-encrypt and re-store it
  then discarded. Comparing only the span a writer read makes disjoint
  writers not collide at all.

  This is a correctness test: every writer commits and nothing is lost.
  Its runtime is not the assertion — timing belongs in the rig's bench — but
  a run that takes anything like its timeout is the symptom returning.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{BlockBacking, ChunkIndex, FileIndex}

  @moduletag :tmp_dir

  @chunk BlockBacking.chunk_bytes()
  @writers 32

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

    name = "blkcon-#{:rand.uniform(999_999)}"
    {:ok, volume} = NeonFS.Core.create_volume(name, type: :block, max_size: @writers * @chunk)
    {:ok, device} = BlockBacking.open_device(name, BlockBacking.device_path())

    {:ok, volume_name: name, volume_id: volume.id, device: device}
  end

  test "#{@writers} writers to distinct chunks all commit, and none is lost", %{
    volume_name: volume_name,
    device: device
  } do
    # A distinct byte in each chunk, so a chunk carrying another writer's
    # payload is as detectable as one carrying the original zeroes.
    payloads = Map.new(0..(@writers - 1), &{&1, :binary.copy(<<&1 + 1>>, @chunk)})

    results =
      payloads
      |> Task.async_stream(
        fn {index, payload} ->
          BlockBacking.write(volume_name, device.file_id, index * @chunk, payload)
        end,
        max_concurrency: @writers,
        timeout: 120_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    for result <- results, do: assert({:ok, _cost} = result)

    for {index, payload} <- payloads do
      assert {:ok, read} = BlockBacking.read(volume_name, device.file_id, index * @chunk, @chunk)

      assert read == payload,
             "chunk #{index} did not read back the payload its writer committed"
    end
  end

  test "the device's chunk list keeps its length and every hash is indexed", %{
    volume_name: volume_name,
    volume_id: volume_id,
    device: device
  } do
    0..(@writers - 1)
    |> Task.async_stream(
      fn index ->
        BlockBacking.write(
          volume_name,
          device.file_id,
          index * @chunk,
          :binary.copy(<<index + 1>>, @chunk)
        )
      end,
      max_concurrency: @writers,
      timeout: 120_000
    )
    |> Stream.run()

    assert {:ok, file} = FileIndex.get(volume_id, device.file_id)

    assert length(file.chunks) == @writers,
           "a splice that dropped or duplicated a chunk changes the list's length"

    assert file.size == @writers * @chunk

    # A hash spliced in from a writer whose chunks were aborted would be
    # unreadable, which the read above cannot distinguish from a stale hash
    # that happens to still resolve.
    for hash <- file.chunks do
      assert {:ok, _chunk_meta} = ChunkIndex.get(volume_id, hash)
    end
  end
end
