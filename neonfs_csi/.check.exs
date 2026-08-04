# .check.exs - ex_check configuration for neonfs_csi
# Run with: mix check
[
  # Serialised in CI. `mix check` runs its tools concurrently, and on a
  # concurrency-1 runner that contention is self-inflicted. Left parallel
  # locally, where wall-clock matters more than isolation.
  parallel: System.get_env("NEONFS_CHECK_PARALLEL", "true") == "true",
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
    {:audit, "mix hex.audit"}
  ]
]
