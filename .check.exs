subprojects =
  "*/.check.exs"
  |> Path.wildcard(match_dot: true)
  |> Enum.map(&Path.dirname/1)
  |> Enum.map(&String.to_atom/1)
  |> Enum.map(&{&1, command: "mix check --no-retry", retry: "mix check --retry", cd: "#{&1}"})

[
  parallel: true,
  skipped: true,
  tools:
    [
      {:audit, "mix deps.audit"},
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
    |> Enum.concat(subprojects)
]
