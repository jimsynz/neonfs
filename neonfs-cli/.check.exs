# .check.exs - ex_check configuration for neonfs_core
# Run with: mix check
[
  parallel: true,
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
