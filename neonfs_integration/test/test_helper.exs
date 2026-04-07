unless Node.alive?() do
  # Ensure EPMD is running (needed for Erlang distribution)
  System.cmd("epmd", ["-daemon"], stderr_to_stdout: true)
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

# Exclude loopback device tests unless running as root with losetup available
excludes =
  if NeonFS.Integration.LoopbackDevice.available?() do
    []
  else
    [:loopback]
  end

ExUnit.start(capture_log: true, exclude: excludes)
