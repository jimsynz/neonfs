# .check.exs - ex_check configuration for neonfs_nfs
# Run with: mix check
[
  parallel: true,
  skipped: true,
  tools: [
    # Elixir tools
    {:compiler, "mix compile --warnings-as-errors"},
    {:formatter, "mix format --check-formatted"},
    {:credo, "mix credo --strict"},
    {:dialyzer, "mix dialyzer"},
    {:doctor, "mix doctor"},
    {:ex_doc, "mix docs"},
    {:audit, "mix deps.audit"},
    {:gettext, false},
    {:sobelow, false},

    # Rust tools (only if native/neonfs_nfs exists)
    {:cargo_fmt, command: "cargo fmt --check --manifest-path native/neonfs_nfs/Cargo.toml"},
    {:cargo_clippy,
     command:
       "cargo clippy --manifest-path native/neonfs_nfs/Cargo.toml --all-targets -- -D warnings"},
    {:cargo_test, command: "cargo test --manifest-path native/neonfs_nfs/Cargo.toml"}
  ]
]
