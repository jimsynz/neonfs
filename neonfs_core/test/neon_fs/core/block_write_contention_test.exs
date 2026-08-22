defmodule NeonFS.Core.BlockWriteContentionTest do
  @moduledoc """
  Concurrent writers to distinct extents of one block device.

  An extent is its own key, so two writers to different parts of a device
  no longer read, splice and rewrite one shared chunk list — which is what
  made a crowded device thrash, each loser redoing a read, re-chunk,
  re-hash, re-encrypt and re-store it then discarded. What they still share
  is the shard root of the extent group they land in, and the commit's
  compare-and-swap has to resolve that by retrying rather than by the last
  writer winning.

  This is a correctness test: every writer commits and nothing is lost.
  Its runtime is not the assertion — timing belongs in the rig's bench.

  ## Why the commit deadline is raised here

  Every write is its own commit through the volume's committer, which
  serialises them: a queue of 32 is 32 metadata rounds one after another,
  where the file path folded concurrent writers into shared batches on the
  way in. At the default 30 s deadline the last writer in the queue times
  out under a loaded suite, which fails this test for a reason it is not
  about. The deadline is raised so the assertion stays "nothing is lost";
  what the queueing costs belongs to the coalescing window and the bench.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{BlockBacking, BlockIndex, ChunkIndex}

  @moduletag :tmp_dir
  @moduletag timeout: 300_000

  @chunk BlockBacking.chunk_bytes()
  @writers 32

  setup %{tmp_dir: tmp_dir} do
    Application.put_env(:neonfs_core, :volume_commit_timeout_ms, 240_000)
    {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir)

    on_exit(fn ->
      Application.delete_env(:neonfs_core, :volume_commit_timeout_ms)
      stop_ra()
      cleanup_test_dirs()
    end)

    name = "blkcon-#{:rand.uniform(999_999)}"
    {:ok, volume} = NeonFS.Core.create_volume(name, type: :block, max_size: @writers * @chunk)
    {:ok, device} = BlockBacking.open_device(name, BlockBacking.device_path())

    {:ok, volume_name: name, volume_id: volume.id, device: device}
  end

  test "#{@writers} writers to distinct extents all commit, and none is lost", %{
    volume_name: volume_name,
    device: device
  } do
    # A distinct byte in each extent, so an extent carrying another writer's
    # payload is as detectable as one that is still a hole.
    payloads = Map.new(0..(@writers - 1), &{&1, :binary.copy(<<&1 + 1>>, @chunk)})

    results =
      payloads
      |> Task.async_stream(
        fn {index, payload} ->
          BlockBacking.write(volume_name, device.path, index * @chunk, payload)
        end,
        max_concurrency: @writers,
        timeout: 240_000
      )
      |> Enum.map(fn {:ok, result} -> result end)

    for result <- results, do: assert({:ok, _cost} = result)

    for {index, payload} <- payloads do
      assert {:ok, read} = BlockBacking.read(volume_name, device.path, index * @chunk, @chunk)

      assert read == payload,
             "extent #{index} did not read back the payload its writer committed"
    end
  end

  # Two 4 KiB writes into one 128 KiB extent are both read-modify-writes of
  # the whole extent, computed from the same starting point. Without the
  # commit comparing what the read saw, whichever lands second discards the
  # other's block — which is what `fio --verify` at any queue depth does to
  # a device whose extents are wider than its writes.
  test "two writers to distinct blocks of one extent both survive", %{
    volume_name: volume_name,
    device: device
  } do
    block = 4096
    first = :binary.copy(<<0xA1>>, block)
    second = :binary.copy(<<0xB2>>, block)

    [{0, first}, {block, second}]
    |> Task.async_stream(
      fn {offset, payload} -> BlockBacking.write(volume_name, device.path, offset, payload) end,
      max_concurrency: 2,
      timeout: 120_000
    )
    |> Enum.each(fn {:ok, result} -> assert {:ok, _cost} = result end)

    assert {:ok, ^first} = BlockBacking.read(volume_name, device.path, 0, block)
    assert {:ok, ^second} = BlockBacking.read(volume_name, device.path, block, block)
  end

  test "the map gains one entry per writer, and every hash it names is indexed", %{
    volume_name: volume_name,
    volume_id: volume_id,
    device: device
  } do
    0..(@writers - 1)
    |> Task.async_stream(
      fn index ->
        BlockBacking.write(
          volume_name,
          device.path,
          index * @chunk,
          :binary.copy(<<index + 1>>, @chunk)
        )
      end,
      max_concurrency: @writers,
      timeout: 240_000
    )
    |> Stream.run()

    assert {:ok, extents} = BlockIndex.range(volume_name, 0, @writers - 1)

    assert length(extents) == @writers,
           "a commit that lost a concurrent writer's entry changes the map's size"

    # A hash published by a writer whose chunks were aborted would be
    # unreadable, which the read above cannot distinguish from a stale hash
    # that happens to still resolve.
    for {_index, {:chunk, hash}} <- extents do
      assert {:ok, _chunk_meta} = ChunkIndex.get(volume_id, hash)
    end
  end
end
