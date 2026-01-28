# Exclude integration tests by default
# Run with: mix test --only integration
ExUnit.start(exclude: [:integration])
