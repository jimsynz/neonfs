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
    # Advisory data comes from hex.pm via `mix hex.audit`, not from the
    # community mirror `mix deps.audit` reads. The mirror produced two
    # demonstrable false positives here: it flattened GHSA-w4f7-4cxr-rv3c
    # into one `gun` entry carrying cowboy's range as well, so every gun
    # release matched forever, and it reported a ymlr advisory hex.pm does
    # not have. `hex.audit` also covers retirements, which the mirror does
    # not. Acknowledgements live in `mix.exs` under `hex: [ignore_advisories:
    # ...]`.
    {:mix_audit, false},
    {:audit, "mix hex.audit"},
    # Elixir tools
    {:compiler, "mix compile --warnings-as-errors"},
    {:formatter, "mix format --check-formatted"},
    {:credo, "mix credo --strict"},
    {:dialyzer, "mix dialyzer"},
    {:doctor, "mix doctor"},
    {:ex_doc, "mix docs"},
    {:gettext, false},
    {:sobelow, false},

    # Rust tools (only if native/neonfs_blob exists)
    {:cargo_fmt,
     command: "cargo fmt --check --manifest-path native/neonfs_blob/Cargo.toml",
     enabled: File.dir?("native/neonfs_blob")},
    {:cargo_clippy,
     command:
       "cargo clippy --manifest-path native/neonfs_blob/Cargo.toml --all-targets -- -D warnings",
     enabled: File.dir?("native/neonfs_blob")},
    {:cargo_test,
     command: "cargo test --manifest-path native/neonfs_blob/Cargo.toml",
     enabled: File.dir?("native/neonfs_blob")}
  ]
]
