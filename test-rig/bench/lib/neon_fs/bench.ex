defmodule NeonFS.Bench do
  @moduledoc """
  Benchee-based benchmark harness for a running NeonFS test-rig cluster
  (#1520 foundation; #1521 canonical op set).

  Runs on the rig host and drives real interface clients over SSH against
  a cluster booted by `neonfs-rig up` — measuring the packaged,
  distributed, TLS-data-plane path, not in-BEAM code paths.

  ## Canonical operation set (#1521)

  For each **file-serving interface** the `neonfs-rig bench` wrapper has set
  up (FUSE, NFS, S3, WebDAV), the suite runs the canonical operations, each
  driven the way `test-rig/lib/acceptance.sh` exercises that interface:

    * `seq_write` — sequential write of a large file (streaming, bounded
      buffer — never whole-file buffered), reported as MB/s;
    * `seq_read` — sequential read of that large file, MB/s;
    * `small_files` — create/write/read/delete a batch of small files, the
      metadata-heavy path, reported as files/sec;
    * `stat_list` — stat/HEAD one object plus a directory/bucket listing,
      reported as latency;
    * `range_read` — a small random range read, reported as latency.

  Results are captured per `(interface, operation)` so profiles are
  directly comparable, and flow through the shared SHA-/config-stamped
  output from the foundation slice.

  The **container-runtime interfaces** (#1533) are a distinct workload driven
  the way `acceptance.sh` exercises them, against a warm daemon the wrapper
  starts (so container/daemon-spawn cost isn't in the measured op):

    * `docker` — file I/O inside a warm busybox container with the NeonFS
      volume attached (`seq_write`/`seq_read`/`small_files`; its data path is
      FUSE underneath, so `stat_list`/`range_read` would duplicate FUSE);
    * `containerd` — blob ingest (`seq_write`, a fresh unique blob each time)
      and get (`seq_read`) against a warm throwaway containerd wired to the
      NeonFS content proxy; a content store has no small-file/listing/range op.

  The wrapper exports, for each interface it prepared, an availability
  flag plus its connection details (mount path, S3 flags, WebDAV auth) and
  the SSH shim location (`BENCH_RIG`). Interfaces the wrapper could not set
  up are logged and skipped — never silently dropped.
  """

  alias NeonFS.Bench.Rig

  @big_file "neonfs_bench_big"
  @scratch_dir ".neonfs_bench"

  @spec run() :: :ok
  def run do
    config = config()
    File.mkdir_p!(config.out_dir)
    write_metadata(config)

    IO.puts("""

    ==== neonfs-rig bench #{if config.config_label != "", do: "[#{config.config_label}] ", else: ""}====
      sha=#{config.sha} nodes=#{config.nodes} replicas=#{config.replicas} drives_per_node=#{config.drives_per_node}
      compression=#{label_or(config.compression)} encryption=#{label_or(config.encryption)} tier=#{label_or(config.tier)}
      interfaces=#{Enum.map_join(config.interfaces, ",", & &1.name)}
      big_file=#{mib(config.big_size)}MiB small=#{config.file_count}×#{config.file_size}B  →  #{config.out_dir}
    """)

    jobs = build_jobs(config)

    if jobs == %{} do
      IO.puts(
        "  no interfaces available — nothing to benchmark (check `neonfs-rig up` and interface setup)"
      )
    else
      suite =
        Benchee.run(jobs,
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
    end

    :ok
  end

  # Every supported (interface, op) pair becomes one benchee job keyed
  # "iface/op". Pairs the interface can't express (e.g. s3 range_read — s3cmd
  # has no byte-range GET) are skipped and logged, never silently dropped.
  defp build_jobs(config) do
    {jobs, skipped} =
      for iface <- config.interfaces, {op, _unit} <- ops(), reduce: {%{}, []} do
        {jobs, skipped} ->
          case command(iface, op, config) do
            :unsupported ->
              {jobs, ["#{iface.name}/#{op}" | skipped]}

            cmd ->
              {Map.put(jobs, "#{iface.name}/#{op}", fn -> Rig.ssh!(config.node, cmd) end),
               skipped}
          end
      end

    unless skipped == [] do
      IO.puts("  skipped (interface can't express op): #{Enum.join(Enum.reverse(skipped), ", ")}")
    end

    jobs
  end

  defp ops do
    [
      {:seq_write, :mbps},
      {:seq_read, :mbps},
      {:small_files, :files_per_sec},
      {:stat_list, :latency},
      {:range_read, :latency}
    ]
  end

  # --- per-interface remote command builders --------------------------------
  #
  # Each returns a shell command run on node 1 via the rig SSH path, mirroring
  # how test-rig/lib/acceptance.sh drives that interface. POSIX mounts use
  # `dd` (streaming, bounded 1 MiB buffer) so large-file ops never whole-file
  # buffer. The big file is pre-staged by the wrapper, so read/stat/range ops
  # have a target regardless of job order.

  defp command(%{kind: :posix} = iface, :seq_write, config) do
    as(iface, """
    dd if=/dev/zero of=#{iface.mount}/#{@scratch_dir}/w bs=1M count=#{mib(config.big_size)} conv=fsync 2>/dev/null
    """)
  end

  defp command(%{kind: :posix} = iface, :seq_read, _config) do
    as(iface, "dd if=#{iface.mount}/#{@big_file} of=/dev/null bs=1M 2>/dev/null")
  end

  defp command(%{kind: :posix} = iface, :small_files, config) do
    n = config.file_count
    d = "#{iface.mount}/#{@scratch_dir}/sf"

    as(iface, """
    rm -rf #{d}; mkdir -p #{d}
    pl=$(head -c #{config.file_size} /dev/zero | tr '\\0' a)
    i=0; while [ $i -lt #{n} ]; do printf '%s' "$pl" > #{d}/f$i; i=$((i+1)); done
    i=0; while [ $i -lt #{n} ]; do cat #{d}/f$i >/dev/null; i=$((i+1)); done
    i=0; while [ $i -lt #{n} ]; do rm #{d}/f$i; i=$((i+1)); done
    rmdir #{d}
    """)
  end

  defp command(%{kind: :posix} = iface, :stat_list, _config) do
    as(iface, "stat #{iface.mount}/#{@big_file} >/dev/null && ls -la #{iface.mount} >/dev/null")
  end

  defp command(%{kind: :posix} = iface, :range_read, config) do
    blocks = max(mib(config.big_size) * 256 - 1, 1)

    as(
      iface,
      "dd if=#{iface.mount}/#{@big_file} of=/dev/null bs=4k count=1 skip=$((RANDOM % #{blocks})) 2>/dev/null"
    )
  end

  defp command(%{kind: :s3} = iface, :seq_write, _config) do
    "s3cmd #{iface.flags} put #{host_big_file()} s3://#{iface.bucket}/w_bench >/dev/null 2>&1"
  end

  defp command(%{kind: :s3} = iface, :seq_read, _config) do
    "s3cmd #{iface.flags} get --force s3://#{iface.bucket}/#{@big_file} - >/dev/null 2>&1"
  end

  defp command(%{kind: :s3} = iface, :small_files, config) do
    n = config.file_count

    """
    printf '%s' "$(head -c #{config.file_size} /dev/zero | tr '\\0' a)" > /tmp/#{@big_file}_sf
    i=0; while [ $i -lt #{n} ]; do s3cmd #{iface.flags} put /tmp/#{@big_file}_sf s3://#{iface.bucket}/sf_$i >/dev/null 2>&1; i=$((i+1)); done
    i=0; while [ $i -lt #{n} ]; do s3cmd #{iface.flags} get --force s3://#{iface.bucket}/sf_$i - >/dev/null 2>&1; i=$((i+1)); done
    i=0; while [ $i -lt #{n} ]; do s3cmd #{iface.flags} del s3://#{iface.bucket}/sf_$i >/dev/null 2>&1; i=$((i+1)); done
    """
  end

  defp command(%{kind: :s3} = iface, :stat_list, _config) do
    "s3cmd #{iface.flags} info s3://#{iface.bucket}/#{@big_file} >/dev/null 2>&1 && s3cmd #{iface.flags} ls s3://#{iface.bucket}/ >/dev/null 2>&1"
  end

  # s3cmd has no byte-range GET; a range read would need a SigV4-signing HTTP
  # client. Skipped (and logged) rather than run as a meaningless no-op.
  defp command(%{kind: :s3}, :range_read, _config), do: :unsupported

  defp command(%{kind: :webdav} = iface, :seq_write, _config) do
    "curl -sf #{iface.auth} -T #{host_big_file()} #{iface.base}/#{iface.vol}/w_bench -o /dev/null"
  end

  defp command(%{kind: :webdav} = iface, :seq_read, _config) do
    "curl -sf #{iface.auth} #{iface.base}/#{iface.vol}/#{@big_file} -o /dev/null"
  end

  defp command(%{kind: :webdav} = iface, :small_files, config) do
    n = config.file_count

    """
    pl=$(head -c #{config.file_size} /dev/zero | tr '\\0' a)
    i=0; while [ $i -lt #{n} ]; do printf '%s' "$pl" | curl -sf #{iface.auth} -T - #{iface.base}/#{iface.vol}/sf_$i -o /dev/null; i=$((i+1)); done
    i=0; while [ $i -lt #{n} ]; do curl -sf #{iface.auth} #{iface.base}/#{iface.vol}/sf_$i -o /dev/null; i=$((i+1)); done
    i=0; while [ $i -lt #{n} ]; do curl -sf #{iface.auth} -X DELETE #{iface.base}/#{iface.vol}/sf_$i -o /dev/null; i=$((i+1)); done
    """
  end

  defp command(%{kind: :webdav} = iface, :stat_list, _config) do
    "curl -sf #{iface.auth} -X PROPFIND -H 'Depth: 0' #{iface.base}/#{iface.vol}/#{@big_file} -o /dev/null && curl -sf #{iface.auth} -X PROPFIND -H 'Depth: 1' #{iface.base}/#{iface.vol}/ -o /dev/null"
  end

  defp command(%{kind: :webdav} = iface, :range_read, _config) do
    "curl -sf #{iface.auth} -r 0-4095 #{iface.base}/#{iface.vol}/#{@big_file} -o /dev/null"
  end

  # Docker volume (#1533): file I/O inside a warm busybox container with the
  # NeonFS volume attached at /data (the wrapper started it, so container-spawn
  # cost isn't in the measured op). The volume's data path is FUSE underneath,
  # so only the attach + write/read/metadata ops are benchmarked; stat/range
  # duplicate the FUSE numbers and are skipped.
  defp command(%{kind: :docker} = iface, :seq_write, config) do
    docker_exec(
      "dd if=/dev/zero of=/data/w bs=1M count=#{mib(config.big_size)} conv=fsync 2>/dev/null",
      iface
    )
  end

  defp command(%{kind: :docker} = iface, :seq_read, _config) do
    docker_exec("dd if=/data/#{@big_file} of=/dev/null bs=1M 2>/dev/null", iface)
  end

  defp command(%{kind: :docker} = iface, :small_files, config) do
    n = config.file_count

    docker_exec(
      """
      d=/data/.bench; rm -rf "$d"; mkdir -p "$d"
      pl=$(head -c #{config.file_size} /dev/zero | tr '\\0' a)
      i=0; while [ $i -lt #{n} ]; do printf '%s' "$pl" > "$d/f$i"; i=$((i+1)); done
      i=0; while [ $i -lt #{n} ]; do cat "$d/f$i" >/dev/null; i=$((i+1)); done
      i=0; while [ $i -lt #{n} ]; do rm "$d/f$i"; i=$((i+1)); done
      rmdir "$d"
      """,
      iface
    )
  end

  defp command(%{kind: :docker}, op, _config) when op in [:stat_list, :range_read],
    do: :unsupported

  # containerd content store (#1533): blob ingest/get throughput against a warm
  # throwaway containerd wired to the NeonFS content proxy (started by the
  # wrapper). seq_write ingests a fresh unique blob (content-addressed, so it
  # must differ each invocation to measure real ingest, not a dedup hit);
  # seq_read gets the pre-staged blob. A content store has no small-file /
  # dir-listing / range analogue.
  defp command(%{kind: :containerd} = iface, :seq_write, config) do
    piped(
      """
      dd if=/dev/urandom of=/tmp/neonfs_ctr_blob bs=1M count=#{mib(config.big_size)} 2>/dev/null
      d="sha256:$(sha256sum /tmp/neonfs_ctr_blob | awk '{print $1}')"
      ctr --address #{iface.grpc} --namespace #{iface.ns} content ingest --expected-digest "$d" "ref-$$-$RANDOM" < /tmp/neonfs_ctr_blob
      """,
      "sudo bash"
    )
  end

  defp command(%{kind: :containerd} = iface, :seq_read, _config) do
    "sudo ctr --address #{iface.grpc} --namespace #{iface.ns} content get #{iface.digest} > /dev/null 2>&1"
  end

  defp command(%{kind: :containerd}, op, _config)
       when op in [:small_files, :stat_list, :range_read],
       do: :unsupported

  defp docker_exec(script, iface), do: piped(script, "sudo docker exec -i #{iface.container} sh")

  defp host_big_file, do: "/tmp/#{@big_file}"

  # Run `script` remotely as the mount's owning user. The script is
  # base64-encoded so its own quoting (`printf '%s'`, `tr '\0' a`, …) can't
  # collide with the SSH + shell layers it passes through. FUSE mounts are
  # owned by the `neonfs` user (no allow_other); NFS is driven as root —
  # mirroring acceptance.sh.
  defp as(%{user: nil}, script), do: piped(script, "sudo bash")
  defp as(%{user: user}, script), do: piped(script, "sudo -u #{user} bash")

  defp piped(script, shell), do: "echo #{Base.encode64(script)} | base64 -d | #{shell}"

  # --- summary + metadata ---------------------------------------------------

  defp print_summary(suite, config) do
    IO.puts("\n---- summary (per interface/op) ----")

    for scenario <- Enum.sort_by(suite.scenarios, & &1.name) do
      ips = scenario.run_time_data.statistics.ips

      IO.puts(
        "  #{String.pad_trailing(scenario.name, 22)} #{rate_line(scenario.name, ips, config)}"
      )
    end

    IO.puts("------------------------------------\n")
  rescue
    _ -> IO.puts("  (per-op timing above; rate summary unavailable for this benchee version)")
  end

  defp rate_line(name, ips, config) do
    cond do
      String.ends_with?(name, "/seq_write") or String.ends_with?(name, "/seq_read") ->
        "#{fmt(ips * mib(config.big_size))} MB/s"

      String.ends_with?(name, "/small_files") ->
        "#{fmt(ips * config.file_count)} files/s"

      true ->
        "#{fmt(1000.0 / ips)} ms/op"
    end
  end

  defp write_metadata(config) do
    meta = %{
      sha: config.sha,
      config_label: config.config_label,
      interfaces: Enum.map(config.interfaces, & &1.name),
      nodes: config.nodes,
      replicas: config.replicas,
      drives_per_node: config.drives_per_node,
      compression: config.compression,
      encryption: config.encryption,
      tier: config.tier,
      big_size: config.big_size,
      file_count: config.file_count,
      file_size: config.file_size,
      produced_at: DateTime.utc_now() |> DateTime.to_iso8601()
    }

    File.write!(Path.join(config.out_dir, "meta.json"), Jason.encode!(meta, pretty: true))
  end

  # --- config ---------------------------------------------------------------

  defp config do
    sha = env("BENCH_SHA", "unknown")
    base_out = env("BENCH_OUT", Path.expand("../../results", __DIR__))
    stamp = DateTime.utc_now() |> DateTime.to_iso8601() |> String.replace(":", "")
    label = env("BENCH_CONFIG_LABEL", "")
    dir_prefix = if label == "", do: sha, else: "#{sha}-#{label}"

    %{
      sha: sha,
      config_label: label,
      node: env("BENCH_NODE", "1"),
      interfaces: interfaces(),
      nodes: int("BENCH_NODES", 1),
      replicas: int("BENCH_REPLICAS", 1),
      drives_per_node: int("BENCH_DRIVES_PER_NODE", 2),
      compression: env("BENCH_COMPRESSION", ""),
      encryption: env("BENCH_ENCRYPTION", ""),
      tier: env("BENCH_TIER", ""),
      big_size: int("BENCH_BIG_SIZE", 64 * 1_048_576),
      file_count: int("BENCH_FILE_COUNT", 100),
      file_size: int("BENCH_FILE_SIZE", 4096),
      time: int("BENCH_TIME", 5),
      warmup: int("BENCH_WARMUP", 2),
      out_dir: Path.join(base_out, "#{dir_prefix}-#{stamp}")
    }
  end

  # Each interface is present only if the wrapper exported its availability
  # flag (it sets these after successfully preparing the interface).
  defp interfaces do
    []
    |> add_if("BENCH_FUSE_MOUNT", fn m ->
      %{name: "fuse", kind: :posix, mount: m, user: "neonfs"}
    end)
    |> add_if("BENCH_NFS_MOUNT", fn m -> %{name: "nfs", kind: :posix, mount: m, user: nil} end)
    |> add_if("BENCH_S3_FLAGS", fn f ->
      %{name: "s3", kind: :s3, flags: f, bucket: env("BENCH_VOLUME", "bench")}
    end)
    |> add_if("BENCH_DAV_AUTH", fn a ->
      %{
        name: "webdav",
        kind: :webdav,
        auth: a,
        base: env("BENCH_DAV_BASE", ""),
        vol: env("BENCH_VOLUME", "bench")
      }
    end)
    |> add_if("BENCH_DOCKER_CONTAINER", fn c -> %{name: "docker", kind: :docker, container: c} end)
    |> add_if("BENCH_CONTAINERD_GRPC", fn g ->
      %{
        name: "containerd",
        kind: :containerd,
        grpc: g,
        ns: env("BENCH_CONTAINERD_NS", "bench"),
        digest: env("BENCH_CONTAINERD_DIGEST", "")
      }
    end)
    |> Enum.reverse()
  end

  defp add_if(acc, key, build) do
    case System.get_env(key) do
      nil -> acc
      "" -> acc
      val -> [build.(val) | acc]
    end
  end

  defp env(key, default), do: System.get_env(key) || default

  defp int(key, default) do
    case System.get_env(key) do
      nil -> default
      "" -> default
      val -> String.to_integer(val)
    end
  end

  defp mib(bytes), do: div(bytes, 1_048_576)

  defp fmt(n), do: :erlang.float_to_binary(n / 1, decimals: 1)

  defp label_or(""), do: "(default)"
  defp label_or(v), do: v
end
