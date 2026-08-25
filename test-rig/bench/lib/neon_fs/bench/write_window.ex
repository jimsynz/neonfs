defmodule NeonFS.Bench.WriteWindow do
  @moduledoc """
  What the write window costs a guest, measured against a real device.

  `NeonFS.Block.WriteWindow` buffers a guest's writes and drains every dirty
  extent in one commit. `NeonFS.Block.WriteWindowBenchTest` measures the
  mechanism — commits produced and bytes amplified, with the transport
  stubbed, so the numbers stay comparable across machines. This is the
  confirmation: guest-visible write latency and IOPS over NBD, against a
  packaged cluster, with the metadata commit's real consensus cost in it.

  ## The sweep

  `:write_window_bytes` is swept, and **0 is one of the points**. The window's
  own moduledoc records that 0 "drains every write as it arrives, which is the
  behaviour from before this existed" — so the before and the after are two
  points of one run rather than two builds, which is what made this
  comparison worth having.

  The cap is read per write (`WriteWindow.byte_cap/0` is called from the write
  path, not captured at init), so a sweep point is an `Application.put_env`
  over the release's `rpc` and takes effect on the next write. No restart, and
  therefore no re-attach: the device the guest is writing to is the same one
  across every point, which is the only way the numbers compare.

  ## The workload

  32 concurrent writers, which is the queue depth a block device produces and
  the shape `NeonFS.Core.BlockWriteContentionTest` asserts the correctness of.
  Each `fio` job gets its own region via `offset_increment`, so the writers
  land on distinct extents — colliding on one extent would measure
  compare-and-swap retries instead of queueing, and those are a different
  question.

  **Two access patterns, because one of them answers the question and the
  other does not.** The window folds repeated writes to the *same* extent:
  its own moduledoc's example is "sixteen 4 KiB writes into one 128 KiB
  extent". Sequential writing does that constantly. Random 4 KiB writes over
  a region far larger than an extent almost never revisit one before it
  drains, so each still costs a read-modify-write of a whole extent and the
  window has nothing to fold. Reporting only the random figure would say the
  window barely helps, which is true of that workload and false of the
  feature.

  `--direct=1` because the guest's page cache would otherwise absorb the
  writes and measure Linux rather than NeonFS.

  ## Output

  A table on stdout and `write_window.json` in the run's output directory,
  carrying `fio`'s own completion-latency percentiles rather than a
  wall-clock estimate wrapped around a shell command — the harness cannot see
  individual IOs, and `fio` can.

  Run via `neonfs-rig bench-write-window`, which attaches the device and
  exports the environment below. `mix run -e "NeonFS.Bench.WriteWindow.run()"`
  on its own will refuse, because `BENCH_RIG` will not be set.
  """

  alias NeonFS.Bench.Rig

  @doc """
  Sweeps `:write_window_bytes` and reports guest write latency at each point.
  """
  @spec run() :: :ok
  def run do
    config = config()
    File.mkdir_p!(config.out_dir)

    IO.puts("""

    ==== neonfs-rig bench-write-window ====
      sha=#{config.sha} node=#{config.node} device=#{config.device}
      writers=#{config.writers} bs=#{config.block_size} runtime=#{config.runtime}s
      caps=#{Enum.map_join(config.caps, ",", &cap_label/1)} patterns=#{Enum.join(config.patterns, ",")}
      →  #{config.out_dir}
    """)

    config = Map.put(config, :per_job_region, per_job_region(config))
    IO.puts("  region per writer: #{div(config.per_job_region, 1_048_576)} MiB\n")

    rows =
      for pattern <- config.patterns, cap <- config.caps do
        measure(config, pattern, cap)
      end

    report(rows)
    write_json(config, rows)

    # Leave the node on its default rather than on whatever the last sweep
    # point was: a rig kept up for something else afterwards would otherwise
    # be silently running with the window off.
    reset_cap(config)

    :ok
  end

  # Each writer needs a region of its own, and the device is whatever the rig
  # made it — so derive rather than default. A fixed 16 MiB × 32 writers
  # silently exceeds a 256 MiB device, and fio then reports on a workload
  # nobody chose.
  defp per_job_region(config) do
    case env("BENCH_WW_REGION", nil) do
      nil ->
        bytes =
          config.node
          |> Rig.ssh!("sudo blockdev --getsize64 #{config.device}")
          |> String.trim()
          |> String.to_integer()

        region = div(div(bytes, config.writers), 1_048_576) * 1_048_576

        if region < 1_048_576 do
          raise "#{config.device} is #{bytes} bytes — too small for " <>
                  "#{config.writers} writers with a region each"
        end

        region

      explicit ->
        String.to_integer(explicit)
    end
  end

  defp measure(config, pattern, cap) do
    set_cap(config, pattern, cap)

    json =
      config.node
      |> Rig.ssh!(fio_command(config, pattern))
      |> extract_json()
      |> Jason.decode!()

    write = json |> Map.fetch!("jobs") |> hd() |> Map.fetch!("write")

    %{
      pattern: pattern,
      cap: cap,
      ios: Map.fetch!(write, "total_ios"),
      iops: write |> Map.fetch!("iops") |> Float.round(1),
      bw_kib: Map.fetch!(write, "bw"),
      mean_us: write |> get_in(["clat_ns", "mean"]) |> ns_to_us(),
      p50_us: write |> percentile("50.000000") |> ns_to_us(),
      p99_us: write |> percentile("99.000000") |> ns_to_us(),
      max_us: write |> get_in(["clat_ns", "max"]) |> ns_to_us()
    }
  end

  defp percentile(write, key), do: get_in(write, ["clat_ns", "percentile", key])

  defp ns_to_us(nil), do: nil
  defp ns_to_us(ns), do: Float.round(ns / 1_000, 1)

  # `fio --output-format=json` still prints its own progress lines to stdout
  # ahead of the document on some builds, so take from the first brace rather
  # than assuming the whole capture parses.
  defp extract_json(output) do
    case :binary.match(output, "{") do
      {start, _} -> binary_part(output, start, byte_size(output) - start)
      :nomatch -> raise "fio produced no JSON:\n#{output}"
    end
  end

  defp fio_command(config, pattern) do
    """
    sudo fio --name=writewindow \
      --filename=#{config.device} \
      --rw=#{pattern} --direct=1 --bs=#{config.block_size} \
      --numjobs=#{config.writers} --iodepth=1 \
      --offset_increment=#{config.per_job_region} \
      --size=#{config.per_job_region} \
      --time_based --runtime=#{config.runtime} --ramp_time=#{config.ramp} \
      --group_reporting --output-format=json
    """
  end

  defp set_cap(config, pattern, cap) do
    IO.puts("  #{pattern}, window #{cap_label(cap)} — #{config.runtime}s of fio…")
    rpc!(config, "Application.put_env(:neonfs_block, :write_window_bytes, #{cap})")
  end

  defp reset_cap(config) do
    rpc!(config, "Application.delete_env(:neonfs_block, :write_window_bytes)")
  end

  # The release's `rpc` boots a temporary node to call the running one. This
  # cluster pins its distribution port, so the temporary node needs one of its
  # own or it dies on `eaddrinuse`; and `NeonFS.Epmd` replaces EPMD, so it also
  # needs to be told where the target is listening. Neither is discoverable
  # from inside the guest, which is why the wrapper passes both.
  defp rpc!(config, expression) do
    env =
      "RELEASE_NODE=#{config.target_node} RELEASE_COOKIE=neonfs " <>
        "NEONFS_DIST_PORT=#{config.rpc_dist_port} " <>
        "NEONFS_PEER_PORTS=#{config.target_node}:#{config.target_dist_port}"

    Rig.ssh!(config.node, "sudo env #{env} #{config.release_bin} rpc '#{expression}'")
  end

  defp report(rows) do
    IO.puts("")
    # `IOs` is in the table because the latencies here are seconds: a 20s point
    # yields a few dozen samples, and a p99 drawn from that is not a p99. Show
    # the count so nobody has to take the percentile on trust.
    header = ~w(pattern window IOs IOPS KiB/s mean_ms p50_ms p99_ms max_ms)
    widths = [11, 8, 7, 8, 8, 11, 11, 11, 11]

    IO.puts("  " <> row_line(header, widths))
    IO.puts("  " <> row_line(Enum.map(widths, &String.duplicate("-", &1 - 2)), widths))

    for r <- rows do
      IO.puts(
        "  " <>
          row_line(
            [
              r.pattern,
              cap_label(r.cap),
              r.ios,
              r.iops,
              r.bw_kib,
              ms(r.mean_us),
              ms(r.p50_us),
              ms(r.p99_us),
              ms(r.max_us)
            ],
            widths
          )
      )
    end

    IO.puts("")
    Enum.each(Enum.uniq(Enum.map(rows, & &1.pattern)), &summarise(rows, &1))
  end

  defp row_line(cells, widths) do
    cells
    |> Enum.zip(widths)
    |> Enum.map_join("", fn {cell, width} -> pad(cell, width) end)
    |> String.trim_trailing()
  end

  # Latencies here run to seconds, so milliseconds read better than the
  # microseconds fio reports in.
  defp ms(nil), do: "-"
  defp ms(us), do: Float.round(us / 1_000, 1)

  # The comparison the issue asked for, stated per pattern rather than left to
  # the reader: the 0 row is the pre-window behaviour and the rest is the
  # window doing its job — and it does very different amounts of it depending
  # on whether the workload revisits an extent.
  defp summarise(rows, pattern) do
    rows = Enum.filter(rows, &(&1.pattern == pattern))

    with %{} = off <- Enum.find(rows, &(&1.cap == 0)),
         [_ | _] = on <- Enum.reject(rows, &(&1.cap == 0)) do
      best = Enum.max_by(on, & &1.iops)

      IO.puts(
        "  #{pattern}: window #{cap_label(best.cap)} vs off — " <>
          "#{ratio(best.iops, off.iops)}× the IOPS, " <>
          "#{ratio(off.p99_us, best.p99_us)}× lower p99"
      )
    else
      _ -> IO.puts("  #{pattern}: no 0-byte point — nothing to compare against")
    end
  end

  defp ratio(_a, b) when b in [0, 0.0, nil], do: "n/a"
  defp ratio(a, b), do: Float.round(a / b, 2)

  defp pad(value, width) do
    value |> to_string() |> String.pad_trailing(width)
  end

  defp write_json(config, rows) do
    path = Path.join(config.out_dir, "write_window.json")

    payload = %{
      sha: config.sha,
      node: config.node,
      device: config.device,
      writers: config.writers,
      block_size: config.block_size,
      runtime_s: config.runtime,
      rows: rows
    }

    File.write!(path, Jason.encode_to_iodata!(payload, pretty: true))
    IO.puts("  wrote #{path}")
  end

  defp cap_label(0), do: "off"
  defp cap_label(cap) when rem(cap, 1_048_576) == 0, do: "#{div(cap, 1_048_576)} MiB"
  defp cap_label(cap) when rem(cap, 1024) == 0, do: "#{div(cap, 1024)} KiB"
  defp cap_label(cap), do: "#{cap} B"

  defp config do
    sha = env("BENCH_SHA", "unknown")
    base_out = env("BENCH_OUT", Path.expand("../../../results", __DIR__))
    stamp = DateTime.utc_now() |> DateTime.to_iso8601() |> String.replace(":", "")

    %{
      sha: sha,
      node: env("BENCH_NODE", "1"),
      device: env("BENCH_BLOCK_DEV", "/dev/nbd0"),
      release_bin: env("BENCH_RELEASE_BIN", "/usr/lib/neonfs/omnibus/bin/neonfs_omnibus"),
      writers: int("BENCH_WW_WRITERS", 32),
      block_size: env("BENCH_WW_BS", "4k"),
      target_node: env("BENCH_WW_TARGET_NODE", "neonfs@10.10.10.11"),
      target_dist_port: int("BENCH_WW_TARGET_DIST_PORT", 9100),
      rpc_dist_port: int("BENCH_WW_RPC_DIST_PORT", 9199),
      runtime: int("BENCH_WW_RUNTIME", 30),
      ramp: int("BENCH_WW_RAMP", 5),
      caps: caps(),
      patterns: env("BENCH_WW_PATTERNS", "write,randwrite") |> String.split(",", trim: true),
      out_dir: Path.join(base_out, "#{sha}-write-window-#{stamp}")
    }
  end

  defp caps do
    "BENCH_WW_CAPS"
    |> env("0,1048576")
    |> String.split(",", trim: true)
    |> Enum.map(&(&1 |> String.trim() |> String.to_integer()))
  end

  defp env(key, default) do
    case System.get_env(key) do
      nil -> default
      "" -> default
      value -> value
    end
  end

  defp int(key, default) when is_integer(default),
    do: key |> env(to_string(default)) |> String.to_integer()
end
