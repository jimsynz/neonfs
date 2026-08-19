defmodule NeonFS.Integration.FileWriteContentionBenchTest do
  @moduledoc """
  Opt-in benchmark putting a number on what concurrent writers to **distinct
  parts of one file** cost on a file volume — the limit #1910 left behind when
  it narrowed the partial-write compare-and-swap for block volumes only.

  A partial write is a read-modify-write of a file's chunk list, and outside
  block volumes the commit compares the writer's whole snapshot. Any change
  anywhere in the file therefore invalidates every concurrent writer, however
  disjoint their spans, and each retry redoes a read, a re-chunk, a re-hash, a
  re-encrypt and a store before discarding all of it.

  The workload is the best case for narrowing: every writer targets its own
  chunk and none of them overlap. If disjointness bought anything here, this
  would be flat.

  ## What it measured, and the defect it found

  32 writers, single-node, on a 4 MiB file. Across runs the elapsed time is
  around 1-2.5 s and the success count is **not stable** — 27/32, 26/32 and
  14/32 were all observed on one machine without changing anything:

      succeeded:   27/32 (5 failed)
      elapsed:     0.9 s

  The failures are the finding rather than noise, and their variability is part
  of it: how many writers survive depends on how the races interleave. Concurrent disjoint writes do
  not merely thrash, some of them **fail** — `:chunk_not_found`,
  `{:local_read_failed, "chunk not found: …"}` and
  `{:add_write_ref_failed, :not_found}` — because a chunk a committed file
  references can be deleted out from under it by another writer's abort. That
  is tracked separately; this benchmark is its reproducer.

  So the limit is not "concurrent disjoint writes to one file are slow". It is
  that they are slow **and a proportion of them fail**, and the second half is
  a defect rather than a cost.

  ## What it now guards

  The first two shapes mean a chunk under a committed file list is gone, and
  the abort path no longer deletes those, so this asserts that neither
  `:chunk_not_found` nor `:local_read_failed` appears at all.

  Still no assertion on the success count. `{:add_write_ref_failed, :not_found}`
  is a lost dedup race — deduplicating against a chunk whose writer then
  legitimately aborted — and it is a throughput problem with its own retry
  budget. Asserting a count here would couple a data-loss regression test to
  that budget and to runner speed, which is how this file becomes a flake in
  the data path.

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

  test "concurrent disjoint writers to one file", %{cluster: cluster} do
    file_vol = create_volume(cluster, "contend-file", %{})

    file = measure(cluster, file_vol, "/contended.bin")

    IO.puts("")
    IO.puts("==== concurrent disjoint writers to one file ====")
    IO.puts("  writers=#{@writers} (one per #{@chunk_bytes}-byte chunk), #{@write_bytes}B each")
    IO.puts(format(:"file volume (whole-list compare)", file))
    IO.puts("  ---")
    IO.puts("  Disjointness buys nothing here: the compare is wider than the write.")
    IO.puts("  Any failures above are lost dedup races, not lost data.")
    IO.puts("=================================================")
    IO.puts("")

    assert Enum.filter(file.failures, &mentions?(&1, :chunk_not_found)) == []
    assert Enum.filter(file.failures, &mentions?(&1, :local_read_failed)) == []
  end

  defp create_volume(cluster, name, opts) do
    {:ok, _} = PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [name, opts])
    name
  end

  # Each writer owns chunk `i`, writing at its start. On a file volume the
  # file has to exist first: a partial write is a read-modify-write, and the
  # thing being measured is the modify, not the create.
  defp measure(cluster, volume, path) do
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
    failures = Enum.reject(results, &match?({:ok, {:ok, _}}, &1))

    for r <- failures do
      IO.puts("    write failed: #{inspect(r, limit: :infinity, printable_limit: 200)}")
    end

    %{
      elapsed_s: elapsed_us / 1_000_000,
      succeeded: succeeded,
      failed: @writers - succeeded,
      failures: failures,
      writes_per_sec: succeeded * 1_000_000 / elapsed_us
    }
  end

  # The shapes worth failing on arrive wrapped to varying depths — an
  # `async_stream` tuple around an RPC result around an error tuple — so match
  # on the atom appearing anywhere in the term rather than on one nesting.
  defp mentions?(term, atom) when is_tuple(term), do: mentions?(Tuple.to_list(term), atom)
  defp mentions?(term, atom) when is_list(term), do: Enum.any?(term, &mentions?(&1, atom))
  defp mentions?(term, atom) when is_map(term), do: mentions?(Map.to_list(term), atom)
  defp mentions?(term, atom), do: term == atom

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
