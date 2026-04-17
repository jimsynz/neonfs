# Ensure the FUSE application is started
Application.ensure_all_started(:neonfs_fuse)

Application.put_env(:kernel, :epmd_module, NeonFS.Epmd)
{:ok, _} = Node.start(:neonfs_fuse_test, name_domain: :shortnames)

# Note: FUSE mount tests have been moved to Rust integration tests because
# FUSE mounting cannot work from within the BEAM VM (Erlang's SIGCHLD handling
# breaks fusermount's fork/waitpid). See native/neonfs_fuse/tests/mount_integration.rs
#
# Handler tests that require neonfs_core have been moved to neonfs_integration.

Mimic.copy(NeonFS.Client.ChunkReader)

ExUnit.start(capture_log: true)
