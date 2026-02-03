# .check.exs - ex_check configuration for neonfs_core
# Run with: mix check
[
  parallel: true,
  skipped: true,
  tools: [
    {:audit, "mix deps.audit"},
    {:compiler, "mix compile --warnings-as-errors"},
    {:credo, "mix credo --strict"},
    {:dialyzer, "mix dialyzer"},
    {:doctor, false},
    {:formatter, "mix format --check-formatted"},
    {:gettext, false},
    {:sobelow, false}
  ]
]
