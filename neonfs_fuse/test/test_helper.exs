# Ensure the FUSE application is started
Application.ensure_all_started(:neonfs_fuse)

Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
{:ok, _} = Node.start(:neonfs_fuse_test, name_domain: :shortnames)

# Disable global's partition prevention — peer-cluster integration tests
# rapidly create/destroy peer clusters and `global` misinterprets this as
# overlapping partitions, proactively disconnecting healthy nodes mid-test.
# Must be set at runtime since kernel is already started by the time
# config.exs runs.
Application.put_env(:kernel, :prevent_overlapping_partitions, false)

# Note: real FUSE mount tests live in Rust under
# `native/neonfs_fuse/tests/` because FUSE mounting cannot work from
# within the BEAM VM — Erlang's SIGCHLD handling breaks fusermount's
# fork/waitpid. The Elixir-side integration tests in `test/integration/`
# drive the handler / session against a peer cluster without doing a
# real kernel mount.

Mimic.copy(NeonFS.Client.ChunkReader)
Mimic.copy(NeonFS.Client)

# `:xattr_tools` tests need the `attr` package's `setfattr`/`getfattr`
# CLIs (used by the real-mount xattr round-trip in #671). Skip when
# either binary is missing rather than fail — keeps the suite green
# on hosts that don't ship `attr` by default.
xattr_tools_exclusion =
  if System.find_executable("setfattr") && System.find_executable("getfattr") do
    []
  else
    [exclude: [:xattr_tools]]
  end

ExUnit.start([capture_log: true] ++ xattr_tools_exclusion)
