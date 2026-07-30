# .check.exs - ex_check configuration for neonfs_csi
# Run with: mix check
[
  # Serialised in CI. `mix check` runs its tools concurrently, and on a
  # concurrency-1 runner that contention is self-inflicted. Left parallel
  # locally, where wall-clock matters more than isolation.
  parallel: System.get_env("NEONFS_CHECK_PARALLEL", "true") == "true",
  tools: [
    # GHSA-w4f7-4cxr-rv3c reports a cowlib CRLF-injection flaw against both
    # cowboy and gun. The advisory mirror files it under `gun` carrying *both*
    # packages' version ranges — `< 2.4.0` (gun) and `< 2.16.0` (cowboy) — so
    # any gun release matches the cowboy range and is flagged forever. gun has
    # never published a 2.16.x; its fix for this advisory is 2.4.0, released on
    # the disclosure date.
    #
    # Every affected package here is already past its own patched version:
    # cowlib 2.18.0 (the actual flaw, patched 2.16.0), cowboy 2.17.0, and gun
    # 2.4.1 (patched 2.4.0). Bumping gun does not help — the current 2.5.0
    # still matches `< 2.16.0`.
    #
    # Remove this once the mirror splits the ranges per package. Re-check by
    # dropping the flag and running `mix deps.audit`; if it passes, the data
    # has been fixed upstream.
    {:mix_audit, "mix deps.audit --ignore-advisory-ids GHSA-w4f7-4cxr-rv3c"}
  ]
]
