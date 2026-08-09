unless Node.alive?() do
  Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
  {:ok, _} = Node.start(:neonfs_docker_test, name_domain: :shortnames)
end

# Disable global's partition prevention — peer-cluster integration tests
# rapidly create/destroy peer clusters and `global` misinterprets this as
# overlapping partitions, proactively disconnecting healthy nodes mid-test.
# Must be set at runtime since kernel is already started by the time
# config.exs runs.
Application.put_env(:kernel, :prevent_overlapping_partitions, false)

# Exclude `:docker` tests on hosts without a working `docker` CLI on
# `PATH`. The integration test in `test/integration/` drives the full
# `docker volume create -d neonfs … && docker run --rm -v ...` flow
# against a real daemon; hosts without `docker` would surface install
# errors rather than meaningful failures.
#
# A working daemon is necessary but not sufficient. The tests announce the
# plugin through a spec file in `/etc/docker/plugins`, which is the only
# directory dockerd's legacy discovery reads — so it cannot be redirected
# somewhere writable without the test passing while testing nothing. Writing
# there needs root. CI runs as root and still runs these; a workstation does
# not, and got three `File.Error … permission denied` failures out of the
# pre-commit fan-out every contributor is told to run.
plugin_spec_dir = "/etc/docker/plugins"

docker_available? =
  try do
    match?({_, 0}, System.cmd("docker", ["info"], stderr_to_stdout: true))
  rescue
    ErlangError -> false
  end

# Probe by writing, not by checking ownership: root-in-a-container, a group
# grant and an ACL all differ from "is this uid 0", and the thing that matters
# is whether the spec file can be installed.
plugin_spec_writable? =
  with :ok <- File.mkdir_p(plugin_spec_dir),
       probe = Path.join(plugin_spec_dir, ".neonfs-write-probe"),
       :ok <- File.write(probe, "") do
    File.rm(probe)
    true
  else
    _ -> false
  end

docker_excludes = if docker_available? and plugin_spec_writable?, do: [], else: [:docker]

if docker_available? and not plugin_spec_writable? do
  IO.puts("""
  \nSkipping :docker tests — a daemon is reachable but #{plugin_spec_dir} is not \
  writable, so the plugin spec these tests announce themselves through cannot \
  be installed. Run as root to exercise them.
  """)
end

ExUnit.configure(exclude: docker_excludes)
ExUnit.start(capture_log: true)
