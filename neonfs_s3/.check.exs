[
  parallel: true,
  skipped: true,
  tools: [
    {:compiler, "mix compile --warnings-as-errors"},
    {:formatter, "mix format --check-formatted"},
    {:credo, "mix credo --strict"},
    {:dialyzer, "mix dialyzer"},
    {:doctor, "mix doctor"},
    {:ex_doc, "mix docs"},
    # See #1412 — hackney 1.25.0 (test-only via ex_aws) has advisories
    # with no 1.x patched release. The curated audit uses the root
    # ignore-file; the built-in `:mix_audit` is disabled so the audit
    # doesn't run twice (and fail on the un-ignored copy).
    {:audit, "mix deps.audit --ignore-file ../.mix_audit_ignore"},
    {:mix_audit, false},
    {:gettext, false},
    {:sobelow, false}
  ]
]
