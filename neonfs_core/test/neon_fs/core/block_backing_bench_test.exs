defmodule NeonFS.Core.BlockBackingBenchTest do
  @moduledoc """
  Opt-in write-amplification and metadata-commit harness for the block
  device store.

  It reports, per guest write size, the bytes the chunk layer actually
  rewrote and the rate at which the metadata layer accepted the commits.
  A guest write costs a whole extent rewrite plus one metadata commit, so
  the numbers here are what a coalescing window has to improve on, and what
  the figures measured before the extent map are compared against.

  Single node, one drive, no cluster — the metadata-commit ceiling
  measured here is the local path's, and a peer cluster's quorum commit is
  slower. Treat it as an upper bound rather than a projection.

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
  @block 4096

  @device_bytes 64 * @chunk
  @writes_per_size 200

  # An extent is its own key, so the metadata cost per write should not grow
  # with the device the way rewriting a whole chunk list did — the scaling
  # table is what says whether it does.
  @scaling_device_sizes [64 * @chunk, 512 * @chunk, 4096 * @chunk]

  setup %{tmp_dir: tmp_dir} do
    {:ok, _cluster_id} = start_provisioned_cluster(tmp_dir)

    on_exit(fn ->
      stop_ra()
      cleanup_test_dirs()
    end)

    volume_name = "block-bench-#{:rand.uniform(999_999)}"
    {:ok, _volume} = create_provisioned_volume(volume_name)

    {:ok, volume_name: volume_name}
  end

  test "random-write amplification and metadata-commit throughput", %{volume_name: volume_name} do
    creation = measure_creation(volume_name)

    rows =
      for size <- [@block, 4 * @block, 16 * @block, @chunk] do
        measure_writes(volume_name, size)
      end

    chunk_aligned = measure_writes(volume_name, @chunk, align: @chunk)

    scaling =
      for device_bytes <- @scaling_device_sizes do
        measure_writes(volume_name, @block, device_bytes: device_bytes)
      end

    report(creation, rows ++ [chunk_aligned], scaling)

    for row <- rows do
      assert row.chunk_bytes >= row.guest_bytes
      assert row.writes == @writes_per_size
    end
  end

  defp measure_creation(volume_name) do
    started = System.monotonic_time()
    {:ok, device} = BlockBacking.create_device(volume_name, "/bench-create.img", @device_bytes)
    elapsed = System.monotonic_time() - started

    {:ok, extents} = BlockIndex.range(volume_name, 0, div(@device_bytes, device.chunk_bytes) - 1)

    %{
      size: @device_bytes,
      extent_count: length(extents),
      ms: System.convert_time_unit(elapsed, :native, :millisecond)
    }
  end

  defp measure_writes(volume_name, write_bytes, opts \\ []) do
    align = Keyword.get(opts, :align, @block)
    device_bytes = Keyword.get(opts, :device_bytes, @device_bytes)

    # A volume holds one device, so each row measures against its own.
    volume = "#{volume_name}-#{System.unique_integer([:positive])}"
    {:ok, _record} = create_provisioned_volume(volume)
    {:ok, device} = BlockBacking.create_device(volume, "/bench.img", device_bytes)

    handler = "block-bench-#{System.unique_integer([:positive])}"
    test_pid = self()

    :telemetry.attach(
      handler,
      [:neonfs, :block, :write],
      fn _event, measurements, _meta, _config -> send(test_pid, {:write, measurements}) end,
      nil
    )

    on_exit(fn -> :telemetry.detach(handler) end)

    payload = :crypto.strong_rand_bytes(write_bytes)
    offsets = random_offsets(device_bytes, write_bytes, align)

    started = System.monotonic_time()

    for offset <- offsets do
      {:ok, _cost} = BlockBacking.write(volume, device.path, offset, payload)
    end

    elapsed = System.monotonic_time() - started

    :telemetry.detach(handler)

    totals = drain_measurements(%{writes: 0, guest_bytes: 0, chunk_bytes: 0, chunks: 0})
    seconds = System.convert_time_unit(elapsed, :native, :microsecond) / 1_000_000

    Map.merge(totals, %{
      write_bytes: write_bytes,
      align: align,
      device_bytes: device_bytes,
      commits_per_second: totals.writes / seconds,
      guest_mib_per_second: totals.guest_bytes / 1_048_576 / seconds
    })
  end

  defp random_offsets(device_bytes, write_bytes, align) do
    slots = div(device_bytes - write_bytes, align)

    for _ <- 1..@writes_per_size do
      :rand.uniform(slots + 1) * align - align
    end
  end

  defp drain_measurements(acc) do
    receive do
      {:write, measurements} ->
        drain_measurements(%{
          writes: acc.writes + 1,
          guest_bytes: acc.guest_bytes + measurements.guest_bytes,
          chunk_bytes: acc.chunk_bytes + measurements.chunk_bytes,
          chunks: acc.chunks + measurements.chunks_rewritten
        })
    after
      0 -> acc
    end
  end

  defp report(creation, rows, scaling) do
    IO.puts("""

    == block backing: device creation ==
    size=#{div(creation.size, 1_048_576)} MiB \
    extents_written=#{creation.extent_count} \
    elapsed=#{creation.ms} ms

    == block backing: random-write amplification (#{@writes_per_size} writes each) ==
    guest write   aligned to   amplification   chunks/write   commits/s   guest MiB/s\
    """)

    for row <- rows do
      IO.puts(
        "#{pad(format_bytes(row.write_bytes), 13)} " <>
          "#{pad(format_bytes(row.align), 12)} " <>
          "#{pad(Float.round(row.chunk_bytes / row.guest_bytes, 1), 15)} " <>
          "#{pad(Float.round(row.chunks / row.writes, 2), 14)} " <>
          "#{pad(Float.round(row.commits_per_second, 1), 11)} " <>
          "#{Float.round(row.guest_mib_per_second, 2)}"
      )
    end

    IO.puts("""

    == block backing: 4 KiB random writes vs device size (#{@writes_per_size} writes each) ==
    device size   extents         commits/s   guest MiB/s\
    """)

    for row <- scaling do
      IO.puts(
        "#{pad(format_bytes(row.device_bytes), 13)} " <>
          "#{pad(div(row.device_bytes, @chunk), 15)} " <>
          "#{pad(Float.round(row.commits_per_second, 1), 11)} " <>
          "#{Float.round(row.guest_mib_per_second, 2)}"
      )
    end

    IO.puts("")
  end

  defp pad(value, width), do: String.pad_trailing(to_string(value), width)

  defp format_bytes(bytes) when bytes >= 1_048_576, do: "#{div(bytes, 1_048_576)} MiB"
  defp format_bytes(bytes) when bytes >= 1024, do: "#{div(bytes, 1024)} KiB"
  defp format_bytes(bytes), do: "#{bytes} B"
end
