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

ExUnit.start(capture_log: true)
