# Dialyzer ignore patterns

[
  # Test support module uses ExUnit macros that inject functions at compile time
  # Dialyzer doesn't see these injected functions when analysing test support
  ~r"test/support/cluster_case\.ex:\d+.*ExUnit"
]
