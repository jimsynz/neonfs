# .check.exs - ex_check configuration for neonfs_webdav
# Run with: mix check
#
# Only the advisory tooling is configured here; everything else stays on
# ex_check's defaults.
[
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
    # `--force-check`, not bare `mix dialyzer`. Dialyxir decides the PLT is
    # fresh by hashing `mix.lock` plus the app-name list, and a path
    # dependency appears in neither — so changing `neonfs_client` can never
    # invalidate the PLT, and dialyzer reports every new function in it as
    # `call_to_missing` while printing "PLT is up to date!". Forcing the
    # check re-reads the beams and updates only what moved; it costs about
    # 25s per package and is the difference between this tool being trusted
    # and being reflexively re-run.
    {:dialyzer, "mix dialyzer --force-check"}
  ]
]
