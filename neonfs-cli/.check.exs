# .check.exs - ex_check configuration for neonfs_core
# Run with: mix check
[
  # Serialised in CI. `mix check` runs its tools concurrently, and on a
  # concurrency-1 runner that contention is self-inflicted: a peer-cluster
  # ex_unit suite competing with dialyzer and cargo has produced post-boot Ra
  # round-trip timeouts that surface as unrelated assertion failures. Left
  # parallel locally, where wall-clock matters more than isolation.
  parallel: System.get_env("NEONFS_CHECK_PARALLEL", "true") == "true",
  skipped: true,
  tools: [
    {:audit, "mix deps.audit"},
    {:cargo_clippy, "cargo clippy --all-targets -- -D warnings"},
    {:cargo_fmt, "cargo fmt --check"},
    {:cargo_test, "cargo test"},
    {:compiler, "mix compile --warnings-as-errors"},
    {:credo, false},
    {:dialyzer, false},
    {:doctor, false},
    {:ex_doc, false},
    {:ex_unit, false},
    {:formatter, "mix format --check-formatted"},
    {:gettext, false},
    {:sobelow, false}
  ]
]
