# Task 0002: Configure .check.exs with Rust Tool Integration

## Status
Not Started

## Phase
1 - Foundation

## Description
Create `.check.exs` configuration files for both neonfs_core and neonfs_fuse packages that integrate Rust tooling (clippy, cargo test, cargo fmt) into the `mix check` workflow. This ensures all code quality checks run from a single command.

## Acceptance Criteria
- [ ] `neonfs_core/.check.exs` created with Rust integration
- [ ] `neonfs_fuse/.check.exs` created with Rust integration
- [ ] `mix check` runs all Elixir checks (credo, dialyzer, doctor, etc.)
- [ ] `mix check` runs `cargo fmt --check` for any native crates
- [ ] `mix check` runs `cargo clippy --all-targets -- -D warnings` for any native crates
- [ ] `mix check` runs `cargo test` for any native crates
- [ ] Rust checks only run if native/ directory exists (graceful skip otherwise)
- [ ] All checks pass on current codebase

## Configuration Template
```elixir
# .check.exs
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

    # Rust tools (only if native/ exists)
    {:cargo_fmt,
     command: "cargo fmt --check --manifest-path native/neonfs_blob/Cargo.toml",
     enabled: File.dir?("native/neonfs_blob")},
    {:cargo_clippy,
     command: "cargo clippy --manifest-path native/neonfs_blob/Cargo.toml --all-targets -- -D warnings",
     enabled: File.dir?("native/neonfs_blob")},
    {:cargo_test,
     command: "cargo test --manifest-path native/neonfs_blob/Cargo.toml",
     enabled: File.dir?("native/neonfs_blob")}
  ]
]
```

## Testing Strategy
- Run `mix check` in neonfs_core and verify all tools execute
- Introduce a clippy warning and verify it fails the check
- Introduce a format issue and verify cargo fmt --check fails
- Verify `mix check` works in neonfs_fuse (will skip Rust checks until crate exists)

## Dependencies
- task_0001_blob_crate_scaffolding (for neonfs_core Rust checks to have something to check)

## Files to Create/Modify
- `neonfs_core/.check.exs`
- `neonfs_fuse/.check.exs`

## Reference
- spec/testing.md - Static Analysis and Linting section
- ex_check documentation: https://hexdocs.pm/ex_check

## Notes
The neonfs_fuse `.check.exs` should reference `neonfs_fuse` crate (to be created in a later task). The `enabled` check allows the config to exist before the crate is created.
