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
# Exclude `:profile` diagnostic tests by default (e.g. the app-start profiler)
# — they print diagnostic output rather than assert. Run with `--include profile`.
# Exclude `:benchmark` tests by default (e.g. the metadata write-throughput
# baseline) — they print throughput numbers. Run with `--include benchmark`.
loopback_excludes =
  if NeonFS.TestSupport.LoopbackDevice.available?() do
    []
  else
    [:loopback]
  end

# Exclude tests that need root unless we have it. `:requires_root` was
# already tagged on the containerd and drive-space modules but never
# excluded, so an unprivileged run failed them on the machine rather than
# on the change — `containerd` boots but cannot create its runtime state,
# and `losetup` refuses outright.
root_excludes =
  if NeonFS.TestSupport.Privileges.root?() do
    []
  else
    [:requires_root]
  end

# Exclude containerd-dependent tests unless `containerd` and `ctr` are on PATH.
# Prep work for the containerd content-store integration tests.
containerd_excludes =
  if System.find_executable("containerd") && System.find_executable("ctr") do
    []
  else
    [:requires_containerd]
  end

# Exclude tests that need the OCI test-registry sidecar
# (`registry:5000/neonfs-test-image:v1`) unless we can resolve and
# dial it. Set up by the `neonfs_integration` CI job's `services:`
# block; locally, run `registry:2` on port 5000 with the
# `test/fixtures/test-image.tar` fixture pushed in.
test_registry_excludes =
  case :gen_tcp.connect(~c"registry", 5000, [:binary, active: false], 500) do
    {:ok, sock} ->
      :gen_tcp.close(sock)
      []

    _ ->
      [:requires_test_registry]
  end

# Exclude the Samba end-to-end tests unless an `smbd` with the NeonFS VFS
# module installed alongside it is available. Samba being on PATH is not
# enough: the module has to have been built inside the same Samba source
# package to ABI- and symbol-version-match, which is what
# `packaging/build-vfs-deb.sh` and the nightly `cifs_smbd` job do. An
# archive Samba with no matching module looks fine until a tree connect.
smbd_excludes =
  if NeonFS.TestSupport.Smbd.available?() do
    []
  else
    [:requires_smbd]
  end

excludes =
  loopback_excludes ++
    root_excludes ++
    containerd_excludes ++
    test_registry_excludes ++ smbd_excludes ++ [:profile, :benchmark]

# PeerClusterTelemetry accumulates per-phase timings across every
# `PeerCluster.start_cluster!` call. We print the summary from an
# `ExUnit.after_suite` callback so it runs after all tests finish but
# while the GenServer is still alive.
{:ok, _telemetry_pid} = NeonFS.TestSupport.PeerClusterTelemetry.start_link()

ExUnit.after_suite(fn _results ->
  NeonFS.TestSupport.PeerClusterTelemetry.print_summary()
end)

ExUnit.start(capture_log: true, exclude: excludes, slowest: 10)
