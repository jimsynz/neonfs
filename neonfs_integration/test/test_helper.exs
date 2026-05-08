unless Node.alive?() do
  Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
  {:ok, _} = Node.start(:neonfs_integration_test, name_domain: :shortnames)
end

# Disable global's partition prevention — tests rapidly create/destroy peer
# clusters and global misinterprets this as overlapping partitions, proactively
# disconnecting healthy nodes mid-test. Must be set at runtime since kernel is
# already started by the time config.exs runs.
Application.put_env(:kernel, :prevent_overlapping_partitions, false)

# Build CLI (cargo skips if unchanged) — `cli_test.exs` exercises the
# Rust binary against a peer cluster.
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

# Exclude loopback device tests unless running as root with losetup available.
# Exclude `:profile` diagnostic tests by default (e.g. #507's app-start profiler)
# — they print diagnostic output rather than assert. Run with `--include profile`.
loopback_excludes =
  if NeonFS.TestSupport.LoopbackDevice.available?() do
    []
  else
    [:loopback]
  end

# Exclude containerd-dependent tests unless `containerd` and `ctr` are on PATH.
# Prep work for #554 (containerd content store integration tests).
containerd_excludes =
  if System.find_executable("containerd") && System.find_executable("ctr") do
    []
  else
    [:requires_containerd]
  end

# Exclude tests that need the OCI test-registry sidecar
# (`registry:5000/neonfs-test-image:v1`) unless we can resolve and
# dial it. Set up by the `neonfs_integration` CI job's `services:`
# block (#728); locally, run `registry:2` on port 5000 with the
# `test/fixtures/test-image.tar` fixture pushed in.
test_registry_excludes =
  case :gen_tcp.connect(~c"registry", 5000, [:binary, active: false], 500) do
    {:ok, sock} ->
      :gen_tcp.close(sock)
      []

    _ ->
      [:requires_test_registry]
  end

# `:pending_903` — cross-node integration tests that depend on
# `Volume.MetadataWriter` fanning out index-tree chunks to every
# replica drive (#903). Currently the writer only replicates the
# root segment; index-tree mutations stay on a single local drive,
# so a write on node1 followed by a read on node2 fails past
# `MetadataReader.resolve_segment` with `FileNotFound`. Remove the
# `pending_903` tag from the listed tests once #903 lands.
pending_903_excludes = [:pending_903]

excludes =
  loopback_excludes ++
    containerd_excludes ++
    test_registry_excludes ++ pending_903_excludes ++ [:profile]

# PeerClusterTelemetry accumulates per-phase timings across every
# `PeerCluster.start_cluster!` call. We print the summary from an
# `ExUnit.after_suite` callback so it runs after all tests finish but
# while the GenServer is still alive. See #423.
{:ok, _telemetry_pid} = NeonFS.TestSupport.PeerClusterTelemetry.start_link()

ExUnit.after_suite(fn _results ->
  NeonFS.TestSupport.PeerClusterTelemetry.print_summary()
end)

ExUnit.start(capture_log: true, exclude: excludes, slowest: 10)
