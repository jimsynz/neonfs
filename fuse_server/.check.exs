# .check.exs - ex_check configuration for fuse_server
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

    # Rust tools
    {:cargo_fmt, command: "cargo fmt --check --manifest-path native/fuse_server/Cargo.toml"},
    {:cargo_clippy,
     command:
       "cargo clippy --manifest-path native/fuse_server/Cargo.toml --all-targets -- -D warnings"},
    {:cargo_test, command: "cargo test --manifest-path native/fuse_server/Cargo.toml"}
  ]
]
