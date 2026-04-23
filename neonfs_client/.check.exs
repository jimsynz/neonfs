# .check.exs - ex_check configuration for neonfs_client
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

    # Rust tools (only if native/neonfs_chunker exists).
    # Mirrors the neonfs_core pattern so the chunker NIF crate is
    # formatted, linted, and unit-tested as part of `mix check`.
    {:cargo_fmt,
     command: "cargo fmt --check --manifest-path native/neonfs_chunker/Cargo.toml",
     enabled: File.dir?("native/neonfs_chunker")},
    {:cargo_clippy,
     command:
       "cargo clippy --manifest-path native/neonfs_chunker/Cargo.toml --all-targets -- -D warnings",
     enabled: File.dir?("native/neonfs_chunker")},
    {:cargo_test,
     command: "cargo test --manifest-path native/neonfs_chunker/Cargo.toml",
     enabled: File.dir?("native/neonfs_chunker")}
  ]
]
