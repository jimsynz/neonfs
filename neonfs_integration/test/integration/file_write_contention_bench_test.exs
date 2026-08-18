defmodule NeonFS.Integration.FileWriteContentionBenchTest do
  @moduledoc """
  Opt-in benchmark measuring what concurrent writers to **distinct parts of one
  file** cost on a *file* volume — the limit #1910 left behind when it narrowed
  the partial-write compare-and-swap for block volumes only.

  A partial write is a read-modify-write of a file's chunk list, and outside
  block volumes the commit still compares the writer's whole snapshot. Any
  change anywhere in the file therefore invalidates every concurrent writer,
  however disjoint their spans, and each retry redoes a read, a re-chunk, a
  re-hash, a re-encrypt and a store before discarding all of it.

  The workload is deliberately the best case for narrowing: `@writers`
  concurrent writes, each to its own chunk of one file, none of them
  overlapping. If disjointness bought anything, this would be flat.

  Both volumes are single-node and identical apart from their type, so the
  contrast is the compare's width and nothing else:

    * **file volume** — whole-list compare (the limit under measurement);
    * **block volume** — span-scoped compare (#1910), as the control.

  Not run by default (`:benchmark`). Run with:

      mix test test/integration/file_write_contention_bench_test.exs --include benchmark
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.TestSupport.PeerCluster

  @moduletag :benchmark
  @moduletag timeout: 900_000
  @moduletag nodes: 1

  # One write per chunk, so every writer targets a distinct region and the
  # only thing they share is the chunk list itself.
  @writers 32
  @chunk_bytes 131_072
  @write_bytes 4096
  @file_size @writers * @chunk_bytes

  setup %{cluster: cluster} do
    :ok = init_single_node_cluster(cluster, name: "write-contend")
    %{}
  end

  test "concurrent disjoint writers to one file all succeed", %{cluster: cluster} do
    file_vol = create_volume(cluster, "contend-file", %{})

    file = measure(cluster, file_vol, "/contended.bin")

    IO.puts("")
    IO.puts("==== concurrent disjoint writers to one file ====")
    IO.puts("  writers=#{@writers} (one per #{@chunk_bytes}-byte chunk), #{@write_bytes}B each")
    IO.puts(format(:"file volume (whole-list compare)", file))
    IO.puts("  ---")
    IO.puts("  The gap is the limit: on a file volume, disjointness buys nothing,")
    IO.puts("  because the compare is wider than the write.")
    IO.puts("=================================================")
    IO.puts("")

    assert file.succeeded == @writers
  end

  defp create_volume(cluster, name, opts) do
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [name, opts])
    name
  end

  # Each writer owns chunk `i`, writing at its start. On a file volume the
  # file has to exist first: a partial write is a read-modify-write, and the
  # thing being measured is the modify, not the create.
  defp measure(cluster, volume, path) do
    path = path || device_path(cluster, volume)

    {:ok, _} =
      PeerCluster.rpc(
        cluster,
        :node1,
        NeonFS.Core,
        :write_file_streamed,
        [volume, path, [:binary.copy(<<0>>, @file_size)]],
        300_000
      )

    payload = :binary.copy(<<0xAB>>, @write_bytes)
    started_at = System.monotonic_time(:microsecond)

    results =
      0..(@writers - 1)
      |> Task.async_stream(
        fn i ->
          PeerCluster.rpc(
            cluster,
            :node1,
            NeonFS.Core,
            :write_file_at,
            [volume, path, i * @chunk_bytes, payload],
            300_000
          )
        end,
        max_concurrency: @writers,
        timeout: 300_000
      )
      |> Enum.to_list()

    elapsed_us = System.monotonic_time(:microsecond) - started_at
    succeeded = Enum.count(results, &match?({:ok, {:ok, _}}, &1))

    for r <- results, not match?({:ok, {:ok, _}}, r) do
      IO.puts("    write failed: #{inspect(r, limit: :infinity, printable_limit: 200)}")
    end

    %{
      elapsed_s: elapsed_us / 1_000_000,
      succeeded: succeeded,
      failed: @writers - succeeded,
      writes_per_sec: succeeded * 1_000_000 / elapsed_us
    }
  end

  defp device_path(_cluster, _volume), do: "/dev.img"

  defp format(label, m) do
    [
      "  #{label}:",
      "    succeeded:   #{m.succeeded}/#{@writers} (#{m.failed} failed)",
      "    elapsed:     #{Float.round(m.elapsed_s, 2)} s",
      "    throughput:  #{Float.round(m.writes_per_sec, 1)} writes/sec"
    ]
    |> Enum.join("\n")
  end
end
