# Ensure applications are started
Application.ensure_all_started(:neonfs_core)
Application.ensure_all_started(:neonfs_fuse)

# Exclude integration tests by default
# Run with: mix test --only integration
ExUnit.start(exclude: [:integration])
