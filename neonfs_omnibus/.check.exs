# .check.exs - ex_check configuration for neonfs_omnibus
# Run with: mix check
[
  tools: [
    # See #1027 — the curated `mix deps.audit` invocation needs an
    # ignore-file flag while cowlib has no patched release for
    # GHSA-g2wm-735q-3f56. Drop this override once the advisory is
    # cleared.
    {:mix_audit, "mix deps.audit --ignore-file ../.mix_audit_ignore"}
  ]
]
