defmodule NeonFS.Bench do
  @moduledoc """
  Benchee-based benchmark harness for a running NeonFS test-rig cluster
  (#1520, foundational slice of #1309).

  Runs on the rig host and drives real interface clients over SSH against
  a cluster booted by `neonfs-rig up` — measuring the packaged,
  distributed, TLS-data-plane path, not in-BEAM code paths.

  This foundational slice ships one vertical-slice scenario — a small-file
  create/write/read/delete lifecycle against the FUSE interface — to prove
  the harness end-to-end. The canonical cross-interface op set is #1521.

  Invoked via `neonfs-rig bench`, which exports the configuration below and
  the SSH shim location (`BENCH_RIG`). Every result artifact is written
  under an output directory stamped with the commit SHA and cluster config
  the run was produced from, so later scenarios (#1522 matrix, #1523/#1524
  regression gate, #1525 bisect) can compare across revisions.
  """

  alias NeonFS.Bench.Rig

  @spec run() :: :ok
  def run do
    config = config()
    File.mkdir_p!(config.out_dir)

    write_metadata(config)

    IO.puts("""

    ==== neonfs-rig bench (#{config.interface}) ====
      sha=#{config.sha} nodes=#{config.nodes} replicas=#{config.replicas} drives_per_node=#{config.drives_per_node}
      files=#{config.file_count} file_size=#{config.file_size}B  →  #{config.out_dir}
    """)

    suite =
      Benchee.run(
        %{
          "fuse small-file lifecycle (create+write+read+delete)" => fn ->
            Rig.ssh!(config.node, lifecycle_command(config))
          end
        },
        time: config.time,
        warmup: config.warmup,
        formatters: [
          Benchee.Formatters.Console,
          {Benchee.Formatters.JSON, file: Path.join(config.out_dir, "#{config.sha}.json")},
          {Benchee.Formatters.CSV, file: Path.join(config.out_dir, "#{config.sha}.csv")},
          {Benchee.Formatters.HTML,
           file: Path.join(config.out_dir, "#{config.sha}.html"), auto_open: false}
        ]
      )

    print_summary(suite, config)
    :ok
  end

  # A single benchmarked invocation: (re)create a scratch directory under
  # the FUSE mount, write `file_count` small files, read them all back, then
  # delete them — the whole batch in one SSH round-trip so the measurement
  # reflects filesystem throughput, not SSH latency. Runs as the `neonfs`
  # user because the FUSE mount is owned by it (no `allow_other`).
  defp lifecycle_command(config) do
    n = config.file_count
    dir = "#{config.mount}/.bench"

    script = """
    set -e
    d="#{dir}"
    rm -rf "$d"; mkdir -p "$d"
    pl=$(head -c #{config.file_size} /dev/zero | tr '\\0' a)
    i=0; while [ $i -lt #{n} ]; do printf '%s' "$pl" > "$d/f$i"; i=$((i+1)); done
    i=0; while [ $i -lt #{n} ]; do cat "$d/f$i" >/dev/null; i=$((i+1)); done
    i=0; while [ $i -lt #{n} ]; do rm "$d/f$i"; i=$((i+1)); done
    rmdir "$d"
    """

    "sudo -u neonfs bash -c '#{script}'"
  end

  defp print_summary(suite, config) do
    ops_per_batch = config.file_count * 3

    for scenario <- suite.scenarios do
      ips = scenario.run_time_data.statistics.ips

      IO.puts("""

      ---- summary: #{scenario.name} ----
        batches/sec: #{fmt(ips)}
        files/sec:   #{fmt(ips * config.file_count)} (create+write)
        ops/sec:     #{fmt(ips * ops_per_batch)} (create+read+delete over #{config.file_count} files)
      ------------------------------------
      """)
    end
  rescue
    _ -> IO.puts("  (per-batch timing above; rate summary unavailable for this benchee version)")
  end

  defp write_metadata(config) do
    meta = %{
      sha: config.sha,
      interface: config.interface,
      nodes: config.nodes,
      replicas: config.replicas,
      drives_per_node: config.drives_per_node,
      file_count: config.file_count,
      file_size: config.file_size,
      produced_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }

    File.write!(Path.join(config.out_dir, "meta.json"), Jason.encode!(meta, pretty: true))
  end

  defp config do
    sha = env("BENCH_SHA", "unknown")
    base_out = env("BENCH_OUT", Path.expand("results", __DIR__ |> Path.join("../..")))
    stamp = DateTime.utc_now() |> DateTime.to_iso8601() |> String.replace(":", "")

    %{
      sha: sha,
      node: env("BENCH_NODE", "1"),
      mount: env("BENCH_MOUNT", "/mnt/bench-fuse"),
      interface: env("BENCH_INTERFACE", "fuse"),
      nodes: int("BENCH_NODES", 1),
      replicas: int("BENCH_REPLICAS", 1),
      drives_per_node: int("BENCH_DRIVES_PER_NODE", 2),
      file_count: int("BENCH_FILE_COUNT", 200),
      file_size: int("BENCH_FILE_SIZE", 4096),
      time: int("BENCH_TIME", 5),
      warmup: int("BENCH_WARMUP", 2),
      out_dir: Path.join(base_out, "#{sha}-#{stamp}")
    }
  end

  defp env(key, default), do: System.get_env(key) || default

  defp int(key, default) do
    case System.get_env(key) do
      nil -> default
      "" -> default
      val -> String.to_integer(val)
    end
  end

  defp fmt(n) when is_number(n), do: :erlang.float_to_binary(n / 1, decimals: 1)
  defp fmt(n), do: inspect(n)
end
