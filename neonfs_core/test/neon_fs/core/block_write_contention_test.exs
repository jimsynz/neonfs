defmodule NeonFS.Core.BlockWriteContentionTest do
  @moduledoc """
  Concurrent publishers of distinct extents of one block device.

  An extent is its own key, so two writers to different parts of a device
  never read, splice and rewrite one shared value — which is what made a
  crowded device thrash when a device was a file, each loser redoing a read,
  re-chunk, re-hash, re-encrypt and re-store it then discarded. What they
  still share is the shard root of the extent group they land in, and the
  commit's compare-and-swap has to resolve that by retrying rather than by
  the last writer winning.

  The bytes are placed the way an interface node places them and published
  through `commit_written/4`, because that is the only device write path
  there is.

  This is a correctness test: every writer commits and nothing is lost. Its
  runtime is not the assertion — timing belongs in the rig's bench.

  ## Why the commit deadline is raised here

  Every publication is its own commit through the volume's committer, which
  serialises them: a queue of 32 is 32 metadata rounds one after another, so
  the last writer waits for all of them. The deadline is raised because that
  wait is the shape of the test rather than a fault in it — a runner slow
  enough to push the tail past the default would fail this test for a reason
  it is not about. What the queueing costs belongs to the coalescing window
  and the bench.
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{BlockBacking, BlockIndex, ChunkIndex}

  @moduletag timeout: 300_000

  @chunk BlockBacking.chunk_bytes()
  @writers 32

  setup_all do
    Application.put_env(:neonfs_core, :volume_commit_timeout_ms, 240_000)
    on_exit(fn -> Application.delete_env(:neonfs_core, :volume_commit_timeout_ms) end)
    {:ok, _cluster_id, _dir} = start_shared_provisioned_cluster("block_contention")
    :ok
  end

  setup do
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

    payloads
    |> Task.async_stream(
      fn {index, payload} ->
        {index, write_block_extent(volume_name, device.path, index, payload)}
      end,
      max_concurrency: @writers,
      timeout: 240_000
    )
    |> Enum.each(fn {:ok, {index, result}} ->
      assert {:ok, _hash} = result, "writer #{index} did not commit: #{inspect(result)}"
    end)

    for {index, payload} <- payloads do
      assert {:ok, read} = BlockBacking.read(volume_name, device.path, index * @chunk, @chunk)

      assert read == payload,
             "extent #{index} did not read back the payload its writer committed"
    end
  end

  test "the map gains one entry per writer, and every hash it names is indexed", %{
    volume_name: volume_name,
    volume_id: volume_id,
    device: device
  } do
    0..(@writers - 1)
    |> Task.async_stream(
      fn index ->
        {index,
         write_block_extent(volume_name, device.path, index, :binary.copy(<<index + 1>>, @chunk))}
      end,
      max_concurrency: @writers,
      timeout: 240_000
    )
    |> Enum.each(fn {:ok, {index, result}} ->
      assert {:ok, _hash} = result, "writer #{index} did not commit: #{inspect(result)}"
    end)

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
