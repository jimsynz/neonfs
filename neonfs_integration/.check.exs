# .check.exs - ex_check configuration for neonfs_integration
# Run with: mix check
[
  parallel: true,
  skipped: true,
  tools: [
    # `:audit` covers `mix deps.audit` — silence the curated
    # `:mix_audit` tool so we don't double-run it (see #1027).
    {:mix_audit, false},
    {:audit, "mix deps.audit --ignore-file ../.mix_audit_ignore"},
    {:compiler, "mix compile --warnings-as-errors"},
    {:credo, "mix credo --strict"},
    {:dialyzer, "mix dialyzer"},
    {:doctor, false},
    {:formatter, "mix format --check-formatted"},
    {:gettext, false},
    {:sobelow, false}
  ]
]
