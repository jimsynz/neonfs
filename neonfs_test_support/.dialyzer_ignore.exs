# Dialyzer ignore patterns

[
  # `ClusterCase` uses ExUnit macros (`flunk/1`, `assert_receive`,
  # `assert/1`) that only resolve at the consuming test's compile
  # time. Standalone analysis of this package can't see those
  # injection sites, so dialyzer flags the helpers as `:no_return`
  # or as referencing unknown functions. The originating package
  # (`neonfs_integration`) carries the same ignore for the same
  # reason — the modules are inherently consumer-driven.
  ~r"lib/neon_fs/test_support/cluster_case\.ex:\d+.*",
  ~r"lib/neon_fs/test_support/telemetry_forwarder\.ex:\d+.*"
]
