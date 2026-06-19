defmodule NeonFS.Integration.MetadataContentionBenchTest do
  @moduledoc """
  Opt-in benchmark measuring **cross-node metadata CAS contention** — the
  YAGNI gate for shard placement / owner-routing (#1306).

  The #1292 throughput benchmark runs single-node, so all concurrency is
  serialised by one node's per-`{volume, shard}` `ShardCommitter` (#1308)
  and never produces cross-node `:cas_update_volume_root` conflicts. The
  question #1306 hangs on is the *cross-node* case: when writers on
  several core nodes hit the same volume concurrently, each node's
  committer independently CAS-flips the Ra bootstrap pointer for a shard,
  and the losers retry on `{:stale_pointer, …}`.

  To make every node a competing CAS writer the volume is `replicate:3`,
  so its metadata root is replicated to all three nodes and each writer
  CAS-flips locally (rather than remote-dispatching to a single replica
  holder via `with_remote_fallback`, which would re-serialise them).

  Two workloads at the **live shard count** (no count toggling — that
  would re-home the system volume's keys and confound the run), each a
  burst of `@file_count` creates into one `replicate:3` volume:

    * **cross-node** — writes spread round-robin across all nodes.
    * **single-node** — all writes via one node (the `ShardCommitter`
      serialises these; baseline for "no cross-node CAS races").

  The contrast in `cas_retries/create` is the signal: if 64-way sharding
  already dilutes concurrent same-volume writes across distinct shards,
  cross-node retries stay near the single-node baseline and owner-routing
  (#1306) is low value for spread workloads. (A separate observation: at
  `metadata_shard_count: 1` — one shard — the same 3-node burst saturates
  the single root pointer so hard that commits exceed the 15 s
  `FileIndex` call timeout, i.e. unsharded cross-node contention is
  pathological. That's the worst case sharding exists to prevent.)

  Reads `root_updates` + `cas_retries` from `MetadataBench` on node1 — the
  stale-CAS telemetry fires in the replicated `apply`, so one node's
  counter is the cluster-wide stale-command count.

  Not run by default (`:benchmark`). Run on the test rig with:

      mix test test/integration/metadata_contention_bench_test.exs --include benchmark
  """

  use NeonFS.TestSupport.ClusterCase, async: false

  alias NeonFS.TestSupport.MetadataBench

  @moduletag :benchmark
  @moduletag timeout: 900_000
  @moduletag nodes: 3

  @file_count 90
  @concurrency 24
  @file_size 256

  setup %{cluster: cluster} do
    init_multi_node_cluster(cluster,
      name: "contend",
      volumes: [
        {"contend-cross", %{"durability" => "replicate:3", "allow_under_replicated" => true}},
        {"contend-single", %{"durability" => "replicate:3", "allow_under_replicated" => true}}
      ]
    )

    %{}
  end

  test "cross-node vs single-node metadata CAS contention (#1306 gate)", %{cluster: cluster} do
    all_nodes = Enum.map(cluster.nodes, & &1.name)

    cross = workload(cluster, all_nodes, "contend-cross")
    single = workload(cluster, [:node1], "contend-single")

    IO.puts("")
    IO.puts("==== cross-node metadata CAS contention (#1306 gate) ====")

    IO.puts(
      "  nodes=#{length(all_nodes)} files=#{@file_count} concurrency=#{@concurrency} (live shard count)"
    )

    IO.puts(format_workload("cross-node (spread across nodes)", cross))
    IO.puts(format_workload("single-node (one writer)", single))
    IO.puts("  ---")
    IO.puts("  If cross-node CAS retries/create ≈ the single-node baseline,")
    IO.puts("  64-way sharding already absorbs cross-node contention and")
    IO.puts("  owner-routing (#1306) is low value for spread workloads.")
    IO.puts("=========================================================")
    IO.puts("")
  end

  defp workload(cluster, write_nodes, vol) do
    files =
      for i <- 1..@file_count do
        {Enum.at(write_nodes, rem(i, length(write_nodes))), "/c/file_#{i}.bin",
         :crypto.strong_rand_bytes(@file_size)}
      end

    :ok = PeerCluster.rpc(cluster, :node1, MetadataBench, :attach, [])
    started_at = System.monotonic_time(:microsecond)

    results =
      files
      |> Task.async_stream(
        fn {node, path, data} ->
          PeerCluster.rpc(cluster, node, NeonFS.TestHelpers, :write_file_from_binary, [
            vol,
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

    succeeded = Enum.count(results, &match?({:ok, {:ok, _}}, &1))

    %{
      elapsed_s: elapsed_us / 1_000_000,
      succeeded: succeeded,
      failed: @file_count - succeeded,
      creates_per_sec: succeeded * 1_000_000 / elapsed_us,
      root_updates: counts.root_updates,
      cas_retries: counts.cas_retries
    }
  end

  defp format_workload(label, w) do
    [
      "  #{label}:",
      "    succeeded:           #{w.succeeded}/#{@file_count} (#{w.failed} failed)",
      "    throughput:          #{Float.round(w.creates_per_sec, 1)} creates/sec",
      "    root flips/create:   #{Float.round(w.root_updates / @file_count, 2)} " <>
        "(#{w.root_updates} total)",
      "    CAS retries/create:  #{Float.round(w.cas_retries / @file_count, 2)} " <>
        "(#{w.cas_retries} total)"
    ]
    |> Enum.join("\n")
  end
end
