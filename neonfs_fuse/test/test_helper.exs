# Ensure applications are started
Application.ensure_all_started(:neonfs_core)
Application.ensure_all_started(:neonfs_fuse)

{:ok, _} = Node.start(:neonfs_fuse_test, name_domain: :shortnames)

# Note: FUSE mount tests have been moved to Rust integration tests because
# FUSE mounting cannot work from within the BEAM VM (Erlang's SIGCHLD handling
# breaks fusermount's fork/waitpid). See native/neonfs_fuse/tests/mount_integration.rs

ExUnit.start(capture_log: true)
