defmodule NeonFS.Core.Blob.NativeThroughputBenchTest do
  @moduledoc """
  Opt-in blob-NIF throughput + dirty-scheduler saturation benchmark (#1481).

  #1197 mandates measuring the blob NIFs before committing to the full
  tokio async rework (#1480). This harness drives `store_write_chunk` /
  `store_read_chunk` directly against `NeonFS.Core.Blob.Native` — no
  cluster, no metadata layer — at varying caller concurrency against a
  single drive and across multiple drives, and reports:

    * throughput (ops/sec and MiB/sec),
    * write/read latency percentiles (p50/p95/p99/max),
    * dirty-IO scheduler utilisation over the run, via
      `:erlang.statistics(:scheduler_wall_time_all)`.

  Not run by default — tagged `:benchmark` and excluded by
  `test/test_helper.exs`. Run with:

      mix test test/neon_fs/core/blob/native_throughput_bench_test.exs --include benchmark

  ## Quantifying #1479 (Mutex removal)

  #1479 dropped the per-drive `Mutex<BlobStore>` that serialised every NIF
  call against a drive. Its win shows up as the **single-drive scaling
  factor** the harness prints: throughput at `2×N_dirty` callers divided
  by throughput at one caller. Pre-#1479 the lock pinned this near `1.0`
  (concurrent callers queued on one mutex); post-#1479 concurrent reads
  and writes of different chunks run in parallel on the dirty schedulers,
  so it rises with available IO parallelism. To capture the literal
  before/after, run this same file on the commit preceding #1479 and
  compare the single-drive rows.
  """

  use ExUnit.Case, async: false

  alias NeonFS.Core.Blob.Native

  @moduletag :benchmark
  @moduletag :tmp_dir
  @moduletag timeout: 600_000

  @chunk_size 131_072
  @ops_per_cell 400
  @tier "hot"
  @prefix_depth 2

  test "blob NIF throughput + dirty-scheduler saturation under concurrency", %{tmp_dir: tmp_dir} do
    dirty_io = :erlang.system_info(:dirty_io_schedulers)
    concurrencies = Enum.uniq([1, dirty_io, dirty_io * 2])
    drive_counts = [1, 4]

    previous = :erlang.system_flag(:scheduler_wall_time, true)

    try do
      cells =
        for drive_count <- drive_counts, concurrency <- concurrencies do
          cell = run_cell(tmp_dir, drive_count, concurrency)

          assert cell.write.ops == @ops_per_cell
          assert cell.read.ops == @ops_per_cell

          cell
        end

      report(cells, dirty_io)
    after
      :erlang.system_flag(:scheduler_wall_time, previous)
    end
  end

  defp run_cell(tmp_dir, drive_count, concurrency) do
    stores = open_stores(tmp_dir, drive_count, concurrency)

    write =
      measure(concurrency, 0..(@ops_per_cell - 1), fn i ->
        drive_idx = rem(i, drive_count)
        data = :crypto.strong_rand_bytes(@chunk_size)
        hash = Native.compute_hash(data)

        {latency, {:ok, {}}} =
          timed(fn -> Native.store_write_chunk(elem(stores, drive_idx), hash, data, @tier) end)

        {latency, {drive_idx, hash}}
      end)

    read =
      measure(concurrency, write.results, fn {drive_idx, hash} ->
        {latency, {:ok, _data}} =
          timed(fn -> Native.store_read_chunk(elem(stores, drive_idx), hash, @tier) end)

        {latency, :ok}
      end)

    %{
      drive_count: drive_count,
      concurrency: concurrency,
      write: summarise(write),
      read: summarise(read)
    }
  end

  defp open_stores(tmp_dir, drive_count, concurrency) do
    for d <- 0..(drive_count - 1) do
      dir = Path.join(tmp_dir, "d#{drive_count}_c#{concurrency}_drive#{d}")
      File.mkdir_p!(dir)
      {:ok, store} = Native.store_open(dir, @prefix_depth)
      store
    end
    |> List.to_tuple()
  end

  defp measure(concurrency, items, fun) do
    before_sched = :erlang.statistics(:scheduler_wall_time_all)
    started_at = System.monotonic_time(:microsecond)

    pairs =
      items
      |> Task.async_stream(fun, max_concurrency: concurrency, ordered: false, timeout: 120_000)
      |> Enum.map(fn {:ok, pair} -> pair end)

    elapsed_us = System.monotonic_time(:microsecond) - started_at
    after_sched = :erlang.statistics(:scheduler_wall_time_all)

    %{
      elapsed_us: elapsed_us,
      latencies_us: Enum.map(pairs, &elem(&1, 0)),
      results: Enum.map(pairs, &elem(&1, 1)),
      dirty_io_util: dirty_io_util(before_sched, after_sched)
    }
  end

  defp timed(fun) do
    t0 = System.monotonic_time(:microsecond)
    result = fun.()
    {System.monotonic_time(:microsecond) - t0, result}
  end

  defp summarise(%{elapsed_us: elapsed_us, latencies_us: latencies, dirty_io_util: util}) do
    sorted = Enum.sort(latencies)
    ops = length(sorted)
    bytes = ops * @chunk_size

    %{
      ops: ops,
      elapsed_s: elapsed_us / 1_000_000,
      ops_per_sec: ops * 1_000_000 / elapsed_us,
      mib_per_sec: bytes * 1_000_000 / elapsed_us / 1_048_576,
      p50_ms: percentile(sorted, 50) / 1000,
      p95_ms: percentile(sorted, 95) / 1000,
      p99_ms: percentile(sorted, 99) / 1000,
      max_ms: (List.last(sorted) || 0) / 1000,
      dirty_io_util: util
    }
  end

  # Nearest-rank percentile over an ascending list of microsecond latencies.
  defp percentile([], _p), do: 0

  defp percentile(sorted, p) do
    n = length(sorted)
    idx = max(0, min(n - 1, round(p / 100 * n) - 1))
    Enum.at(sorted, idx)
  end

  # Fraction of wall time the dirty-IO schedulers were busy during the run.
  # `scheduler_wall_time_all` lists normal, then dirty-CPU, then dirty-IO
  # schedulers when sorted by id; the blob NIFs run on the dirty-IO set.
  defp dirty_io_util(before_sched, after_sched) do
    normal = :erlang.system_info(:schedulers)
    dirty_cpu = :erlang.system_info(:dirty_cpu_schedulers)
    dirty_io = :erlang.system_info(:dirty_io_schedulers)

    before_by_id = Map.new(before_sched, fn {id, active, total} -> {id, {active, total}} end)

    dirty_io_ids =
      after_sched
      |> Enum.map(&elem(&1, 0))
      |> Enum.sort()
      |> Enum.drop(normal + dirty_cpu)
      |> Enum.take(dirty_io)
      |> MapSet.new()

    {active, total} =
      after_sched
      |> Enum.filter(fn {id, _, _} -> MapSet.member?(dirty_io_ids, id) end)
      |> Enum.reduce({0, 0}, fn {id, active1, total1}, {active_acc, total_acc} ->
        {active0, total0} = Map.fetch!(before_by_id, id)
        {active_acc + (active1 - active0), total_acc + (total1 - total0)}
      end)

    if total > 0, do: active / total, else: 0.0
  end

  defp report(cells, dirty_io) do
    IO.puts("")
    IO.puts("==== blob NIF throughput + dirty-scheduler saturation (#1481) ====")

    IO.puts(
      "  chunk_size=#{div(@chunk_size, 1024)}KiB ops/cell=#{@ops_per_cell} tier=#{@tier} " <>
        "dirty_io_schedulers=#{dirty_io}"
    )

    IO.puts("")

    for cell <- cells do
      IO.puts("  drives=#{cell.drive_count} concurrency=#{cell.concurrency}")
      IO.puts(format_phase("write", cell.write))
      IO.puts(format_phase("read ", cell.read))
    end

    report_single_drive_scaling(cells)
    IO.puts("==================================================================")
    IO.puts("")
  end

  defp format_phase(label, s) do
    "    #{label}: #{pad(Float.round(s.ops_per_sec, 0), 8)} ops/s | " <>
      "#{pad(Float.round(s.mib_per_sec, 1), 7)} MiB/s | " <>
      "p50 #{ms(s.p50_ms)} p95 #{ms(s.p95_ms)} p99 #{ms(s.p99_ms)} max #{ms(s.max_ms)} | " <>
      "dirtyIO #{Float.round(s.dirty_io_util * 100, 1)}%"
  end

  # The single-drive scaling factor is the #1479 headline: how much
  # single-drive throughput rises from one caller to max concurrency now
  # that the per-drive Mutex is gone.
  defp report_single_drive_scaling(cells) do
    single = Enum.filter(cells, &(&1.drive_count == 1))

    with base when not is_nil(base) <- Enum.min_by(single, & &1.concurrency, fn -> nil end),
         top when not is_nil(top) <- Enum.max_by(single, & &1.concurrency, fn -> nil end),
         true <- base.concurrency != top.concurrency do
      IO.puts("")

      IO.puts(
        "  single-drive scaling (#1479 win): concurrency #{base.concurrency} → #{top.concurrency}"
      )

      IO.puts(
        "    write ×#{scaling(base.write, top.write)}  read ×#{scaling(base.read, top.read)} " <>
          "(≈1.0 would mean the drive is still serialised)"
      )
    else
      _ -> :ok
    end
  end

  defp scaling(base, top), do: Float.round(top.ops_per_sec / base.ops_per_sec, 2)

  defp ms(v), do: "#{Float.round(v, 2)}ms"

  defp pad(number, width), do: String.pad_leading(to_string(number), width)
end
