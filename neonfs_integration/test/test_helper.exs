unless Node.alive?() do
  Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
  {:ok, _} = Node.start(:neonfs_integration_test, name_domain: :shortnames)
end

# Disable global's partition prevention — tests rapidly create/destroy peer
# clusters and global misinterprets this as overlapping partitions, proactively
# disconnecting healthy nodes mid-test. Must be set at runtime since kernel is
# already started by the time config.exs runs.
Application.put_env(:kernel, :prevent_overlapping_partitions, false)

# Build CLI (cargo skips if unchanged)
cli_dir = Path.expand("../../neonfs-cli", __DIR__)

case System.cmd("cargo", ["build", "--release"],
       cd: cli_dir,
       stderr_to_stdout: true
     ) do
  {_output, 0} ->
    :ok

  {output, code} ->
    IO.puts("\n❌ Failed to build CLI (exit code #{code}):")
    IO.puts(output)
    System.halt(1)
end

# Note: FUSE integration tests have been moved to Rust because FUSE mounting
# cannot work from within the BEAM VM (Erlang's SIGCHLD handling breaks
# fusermount's fork/waitpid). See neonfs_fuse/native/neonfs_fuse/tests/mount_integration.rs

# Exclude loopback device tests unless running as root with losetup available.
# Exclude `:profile` diagnostic tests by default (e.g. #507's app-start profiler)
# — they print diagnostic output rather than assert. Run with `--include profile`.
loopback_excludes =
  if NeonFS.Integration.LoopbackDevice.available?() do
    []
  else
    [:loopback]
  end

# Exclude `:fuse` tests on hosts without `/dev/fuse` and `fusermount3`. The
# Session integration test mounts a real FUSE filesystem; CI hosts without
# the helper would surface install errors rather than a meaningful failure.
fuse_excludes =
  if File.exists?("/dev/fuse") and System.find_executable("fusermount3") != nil do
    []
  else
    [:fuse]
  end

excludes = loopback_excludes ++ fuse_excludes ++ [:profile]

# PeerClusterTelemetry accumulates per-phase timings across every
# `PeerCluster.start_cluster!` call. We print the summary from an
# `ExUnit.after_suite` callback so it runs after all tests finish but
# while the GenServer is still alive. See #423.
{:ok, _telemetry_pid} = NeonFS.Integration.PeerClusterTelemetry.start_link()

ExUnit.after_suite(fn _results ->
  NeonFS.Integration.PeerClusterTelemetry.print_summary()
end)

ExUnit.start(capture_log: true, exclude: excludes, slowest: 10)
