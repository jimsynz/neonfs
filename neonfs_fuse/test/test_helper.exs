# Ensure applications are started
Application.ensure_all_started(:neonfs_core)
Application.ensure_all_started(:neonfs_fuse)

# Exclude integration tests by default
# - :integration - general integration tests
# - :fuse_integration - tests requiring actual FUSE mount/unmount (needs privileges)
#
# Run integration tests with: mix test --include integration
# Run FUSE tests with: mix test --include fuse_integration
# Run all tests with: mix test --include integration --include fuse_integration
ExUnit.start(exclude: [:integration, :fuse_integration])
