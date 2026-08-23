defmodule NeonFS.Core.BlockBackingBenchTest do
  @moduledoc """
  Opt-in metadata-commit harness for the block device's extent map.

  A device write is one publication through `commit_written/4`: the
  interface node has already placed the chunk, and what is left is verifying
  that claim and publishing the map. The rate that happens at is what bounds
  a device's IOPS, and it is the number a coalescing window exists to
  improve — one commit for many extents instead of one each.

  Write amplification is deliberately **not** measured here any more. It is
  arithmetic on the extent geometry and it happens on the interface node: a
  request rewrites one whole extent per extent it touches, which is a
  property of the geometry rather than something to discover. What is worth
  measuring is what varies, and this is it.

  Single node, one drive, no cluster — the ceiling measured here is the
  local path's, and a peer cluster's quorum commit is slower. Treat it as an
  upper bound rather than a projection.

  Not run by default — tagged `:benchmark` and excluded by
  `test/test_helper.exs`. Run with:

      mix test test/neon_fs/core/block_backing_bench_test.exs --include benchmark
  """

  use ExUnit.Case, async: false
  use NeonFS.TestCase

  alias NeonFS.Core.{BlockBacking, BlockIndex}

  @moduletag :benchmark
  @moduletag :tmp_dir
  @moduletag timeout: 600_000

  @chunk BlockBacking.chunk_bytes()

  @device_bytes 64 * @chunk
  @commits_per_size 200

  # An extent is its own key, so the metadata cost of a commit should not
  # grow with the device the way rewriting a whole chunk list did. This table
  # is what says whether it does.
  @scaling_device_sizes [64 * @chunk, 512 * @chunk, 4096 * @chunk]

  setup %{tmp_dir: tmp_dir} do
    Application.put_env(:neonfs_core, :volume_commit_timeout_ms, 240_000)
    {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir)

    on_exit(fn ->
      Application.delete_env(:neonfs_core, :volume_commit_timeout_ms)
      stop_ra()
      cleanup_test_dirs()
    end)

    {:ok, prefix: "block-bench-#{:rand.uniform(999_999)}"}
  end

  test "device creation cost and metadata-commit throughput", %{prefix: prefix} do
    creation = measure_creation(prefix)

    scaling =
      for device_bytes <- @scaling_device_sizes do
        measure_commits(prefix, device_bytes)
      end

    report(creation, scaling)

    for row <- scaling do
      assert row.commits == @commits_per_size
      assert row.commits_per_second > 0
    end
  end

  defp measure_creation(prefix) do
    volume = new_volume(prefix)

    started = System.monotonic_time()
    {:ok, device} = BlockBacking.create_device(volume, "/bench-create.img", @device_bytes)
    elapsed = System.monotonic_time() - started

    {:ok, extents} = BlockIndex.range(volume, 0, div(@device_bytes, device.chunk_bytes) - 1)

    %{
      size: @device_bytes,
      extent_count: length(extents),
      ms: System.convert_time_unit(elapsed, :native, :millisecond)
    }
  end

  # A volume holds one device, so each row measures against its own.
  defp measure_commits(prefix, device_bytes) do
    volume = new_volume(prefix)
    {:ok, device} = BlockBacking.create_device(volume, "/bench.img", device_bytes)

    extents = div(device_bytes, @chunk)
    payload = :crypto.strong_rand_bytes(@chunk)
    indices = for _ <- 1..@commits_per_size, do: :rand.uniform(extents) - 1

    started = System.monotonic_time()

    for index <- indices do
      {:ok, _hash} = write_block_extent(volume, device.path, index, payload)
    end

    elapsed = System.monotonic_time() - started
    seconds = System.convert_time_unit(elapsed, :native, :microsecond) / 1_000_000

    %{
      device_bytes: device_bytes,
      extents: extents,
      commits: @commits_per_size,
      commits_per_second: @commits_per_size / seconds
    }
  end

  defp new_volume(prefix) do
    name = "#{prefix}-#{System.unique_integer([:positive])}"
    {:ok, _record} = create_provisioned_volume(name)
    name
  end

  defp report(creation, scaling) do
    IO.puts("""

    == block device: creation ==
    size=#{div(creation.size, 1_048_576)} MiB \
    extents_written=#{creation.extent_count} \
    elapsed=#{creation.ms} ms

    == block device: extent commits vs device size (#{@commits_per_size} commits each) ==
    device size   extents         commits/s\
    """)

    for row <- scaling do
      IO.puts(
        "#{pad(format_bytes(row.device_bytes), 13)} " <>
          "#{pad(row.extents, 15)} " <>
          "#{Float.round(row.commits_per_second, 1)}"
      )
    end

    IO.puts("")
  end

  defp pad(value, width), do: String.pad_trailing(to_string(value), width)

  defp format_bytes(bytes) when bytes >= 1_048_576, do: "#{div(bytes, 1_048_576)} MiB"
  defp format_bytes(bytes) when bytes >= 1024, do: "#{div(bytes, 1024)} KiB"
  defp format_bytes(bytes), do: "#{bytes} B"
end
