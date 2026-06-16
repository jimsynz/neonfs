defmodule NeonFS.Integration.MetadataThroughputBenchTest do
  @moduledoc """
  Opt-in benchmark establishing a baseline for metadata write throughput
  (#1292), so the later #1291 slices — per-entry directory entries
  (#1294), transaction-batched commit (#1295), and root sharding (#1296)
  — can be measured against a before/after.

  Not run by default — tagged `:benchmark` and excluded by
  `test/test_helper.exs` unless explicitly included. Run with:

      mix test test/integration/metadata_throughput_bench_test.exs --include benchmark

  Two workloads are driven against a single-node Ra-backed cluster under
  concurrency:

    * **single-directory burst** — `@file_count` files into one directory.
      This is the #1287 hot-directory case. Since #1294 each child is its
      own `dirent:` key (no shared-blob read-modify-write), so the residual
      cost is the single `volume_root` CAS flip per create that #1295/#1296
      target.
    * **spread** — `@file_count` files each in its own directory, so there
      is no hot-directory contention.

  For each workload it reports total wall time, creates/sec, and — via
  `NeonFS.TestSupport.MetadataBench` telemetry counters on the Ra leader —
  the number of `volume_root` flips and CAS stale-pointer retries per
  create. The per-create root-flip count is the structural cost #1291
  targets.
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.TestSupport.MetadataBench

  @moduletag :benchmark
  @moduletag timeout: 600_000
  @moduletag nodes: 1

  @file_count 50
  @concurrency 20
  @file_size 256

  setup %{cluster: cluster} do
    init_single_node_cluster(cluster, volumes: [{"bench-vol", %{}}])
    %{}
  end

  test "metadata write throughput: single-directory burst vs spread", %{cluster: cluster} do
    burst = run_workload(cluster, :burst, fn i -> "/burst/file_#{i}.bin" end)
    spread = run_workload(cluster, :spread, fn i -> "/spread/dir_#{i}/file.bin" end)

    IO.puts("")
    IO.puts("==== metadata write-throughput baseline (#1292) ====")
    IO.puts("  files=#{@file_count} concurrency=#{@concurrency} file_size=#{@file_size}B")
    IO.puts(format_workload("single-directory burst", burst))
    IO.puts(format_workload("spread (one dir per file)", spread))
    IO.puts("=====================================================")
    IO.puts("")
  end

  defp run_workload(cluster, _label, path_fn) do
    files =
      for i <- 1..@file_count do
        {path_fn.(i), :crypto.strong_rand_bytes(@file_size)}
      end

    :ok = PeerCluster.rpc(cluster, :node1, MetadataBench, :attach, [])

    started_at = System.monotonic_time(:microsecond)

    results =
      files
      |> Task.async_stream(
        fn {path, data} ->
          PeerCluster.rpc(cluster, :node1, NeonFS.TestHelpers, :write_file_from_binary, [
            "bench-vol",
            path,
            data
          ])
        end,
        max_concurrency: @concurrency,
        timeout: 120_000
      )
      |> Enum.to_list()

    elapsed_us = System.monotonic_time(:microsecond) - started_at

    counts = PeerCluster.rpc(cluster, :node1, MetadataBench, :snapshot, [])
    :ok = PeerCluster.rpc(cluster, :node1, MetadataBench, :detach, [])

    for {res, {path, _}} <- Enum.zip(results, files) do
      assert match?({:ok, {:ok, _file}}, res),
             "write for #{path} did not succeed: #{inspect(res)}"
    end

    %{
      elapsed_s: elapsed_us / 1_000_000,
      creates_per_sec: @file_count * 1_000_000 / elapsed_us,
      root_updates: counts.root_updates,
      cas_retries: counts.cas_retries
    }
  end

  defp format_workload(label, w) do
    [
      "  #{label}:",
      "    wall time:           #{Float.round(w.elapsed_s, 3)} s",
      "    throughput:          #{Float.round(w.creates_per_sec, 1)} creates/sec",
      "    root flips/create:   #{Float.round(w.root_updates / @file_count, 2)} " <>
        "(#{w.root_updates} total)",
      "    CAS retries/create:  #{Float.round(w.cas_retries / @file_count, 2)} " <>
        "(#{w.cas_retries} total)"
    ]
    |> Enum.join("\n")
  end
end
