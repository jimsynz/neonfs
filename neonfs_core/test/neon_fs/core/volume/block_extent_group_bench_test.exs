defmodule NeonFS.Core.Volume.BlockExtentGroupBenchTest do
  @moduledoc """
  Evidence for the extent group size, which was "chosen now and validated by
  benchmark, not designed around".

  The group size decides which shard an extent lands on: `Shard.for_key/2`
  maps a `:block_index` key to `div(extent_index, group_size) rem
  shard_count`. That mapping is what lets a coalesced window of sequential
  writes publish one or two roots instead of one per extent, and it is
  **immutable once anything has been written** — changing it re-homes every
  key in every block volume's index. So the window for evidence closes the
  first time a device is written through the extent map.

  This measures the mapping rather than a write path. Shards touched per batch
  is a pure function of the keys, so it needs no cluster, no coalescing window
  and no extent write path — none of which exist yet, and waiting for them
  would mean measuring after the value stopped being changeable. What it
  cannot answer is the latency half (commit rounds per MiB against a real
  committer); that still waits on the window.

  ## What it shows at the shipped default

      group  span     sequential   random   hot region
      32     4.0      2            38       2
      64     8.0      1            40       1
      128    16.0     1            45       1

  64 is the smallest group that keeps a 64-extent (8 MiB) window on **one**
  shard. Below it the window fans out — 2 shards at 32, 8 at 8 — which is
  precisely the amplification the extent map exists to avoid. Above it there is
  nothing further to win for this window size, while the hot-region span grows,
  so 64 is the knee rather than a point on a slope.

  The random column is the control: ~40 distinct shards out of 64 is what
  hashing would give (64 draws from 64 buckets ≈ 40.4 distinct), so scattered
  workloads pay nothing for the grouping. The hot-region column is the cost
  `BlockExtent`'s moduledoc accepts — writes inside one group's span land on
  one shard — now measured rather than asserted.

  **The right group size tracks the coalescing window.** 64 is right because a
  window is 64 extents; if the window lands at a different size, this file is
  where that gets rechecked.

  Not run by default (`:benchmark`):

      mix test test/neon_fs/core/volume/block_extent_group_bench_test.exs --include benchmark
  """
  use ExUnit.Case, async: false

  alias NeonFS.Core.Volume.BlockExtent
  alias NeonFS.Core.Volume.Shard

  @moduletag :benchmark

  # 64 is the shipped default; the neighbours either side say whether it sits
  # on a cliff or a plateau.
  @group_sizes [8, 16, 32, 64, 128, 256]
  @shard_count 64

  # One window's worth of guest writes, at the default 128 KiB chunk: 64
  # extents is 8 MiB.
  @batch 64
  # A 4 GiB device at 128 KiB extents.
  @device_extents 32_768
  # "Hot" is one group's span at the default, so the concentration measured is
  # real rather than an artefact of the range being wider than a group.
  @hot_extents 64

  setup do
    original = Application.get_env(:neonfs_core, :metadata_shard_count)
    Application.put_env(:neonfs_core, :metadata_shard_count, @shard_count)

    on_exit(fn ->
      if original do
        Application.put_env(:neonfs_core, :metadata_shard_count, original)
      else
        Application.delete_env(:neonfs_core, :metadata_shard_count)
      end
    end)

    :ok
  end

  test "shards touched per batch, by group size and workload" do
    rows =
      for group_size <- @group_sizes do
        with_group_size(group_size, fn ->
          %{
            group_size: group_size,
            span_mib: group_size * 131_072 / 1_048_576,
            sequential: shards_touched(sequential_batch()),
            random: shards_touched(random_batch()),
            hot: shards_touched(hot_batch())
          }
        end)
      end

    IO.puts("")
    IO.puts("==== extent group size: shards touched per #{@batch}-extent batch ====")
    IO.puts("  shard_count=#{@shard_count}, device=#{@device_extents} extents (4 GiB @ 128 KiB)")
    IO.puts("")
    IO.puts("  group  span(MiB)  sequential  random  hot region")

    for r <- rows do
      IO.puts(
        "  #{pad(r.group_size, 5)}  #{pad(Float.round(r.span_mib, 1), 9)}  " <>
          "#{pad(r.sequential, 10)}  #{pad(r.random, 6)}  #{r.hot}"
      )
    end

    IO.puts("")
    IO.puts("  sequential: lower is better — it is what the grouping is for.")
    IO.puts("  random:     ~#{@batch} means no worse than hashing would have been.")
    IO.puts("  hot region: 1 is the accepted concentration cost, now measured.")
    IO.puts("=====================================================================")
    IO.puts("")

    default = Enum.find(rows, &(&1.group_size == 64))

    # The property the grouping exists for: a coalesced window of adjacent
    # extents must not fan out across shards.
    assert default.sequential == 1

    # Smaller groups do fan it out, which is what makes 64 a knee rather than
    # an arbitrary pick.
    assert Enum.find(rows, &(&1.group_size == 32)).sequential > 1

    # And the accepted cost is real: a hot region within one group's span
    # lands on exactly one shard.
    assert default.hot == 1
  end

  defp with_group_size(size, fun) do
    original = Application.get_env(:neonfs_core, :block_extent_group_size)
    Application.put_env(:neonfs_core, :block_extent_group_size, size)
    result = fun.()

    if original do
      Application.put_env(:neonfs_core, :block_extent_group_size, original)
    else
      Application.delete_env(:neonfs_core, :block_extent_group_size)
    end

    result
  end

  # A window of adjacent extents, as a coalesced sequential write produces.
  defp sequential_batch, do: Enum.to_list(0..(@batch - 1))

  # Scattered across the whole device. Seeded, so the number is reproducible
  # rather than a fresh sample every run.
  defp random_batch do
    :rand.seed(:exsss, {1, 2, 3})
    Enum.map(1..@batch, fn _ -> :rand.uniform(@device_extents) - 1 end)
  end

  # Concentrated in one small range, well inside the device.
  defp hot_batch do
    base = div(@device_extents, 2)
    :rand.seed(:exsss, {4, 5, 6})
    Enum.map(1..@batch, fn _ -> base + :rand.uniform(@hot_extents) - 1 end)
  end

  defp shards_touched(extents) do
    extents
    |> Enum.map(&Shard.for_key(:block_index, BlockExtent.key(&1)))
    |> Enum.uniq()
    |> length()
  end

  defp pad(value, width), do: String.pad_trailing("#{value}", width)
end
