# Task 0022: Create neonfs-cli Rust Crate Scaffolding

## Status
Complete

## Phase
1 - Foundation

## Description
Create the standalone Rust CLI crate that communicates with the NeonFS daemon via Erlang distribution protocol. This is a separate binary (not a NIF) that provides fast startup times for CLI operations.

## Acceptance Criteria
- [x] Crate created at project root: `cli/` or `neonfs-cli/`
- [x] Cargo.toml with dependencies: clap, erl_dist, erl_rpc, tokio, thiserror
- [x] Basic CLI structure with clap argument parsing
- [x] Subcommands defined: `cluster`, `volume`, `mount`, `node`
- [x] `--help` works for main command and subcommands
- [x] `--version` shows version
- [x] `--json` flag for JSON output mode
- [x] Project compiles with `cargo build`
- [x] Clippy and fmt pass

## Crate Structure
```
neonfs-cli/
├── Cargo.toml
└── src/
    ├── main.rs           # Entry point, clap setup
    ├── commands/
    │   ├── mod.rs
    │   ├── cluster.rs    # cluster init, status, join
    │   ├── volume.rs     # volume list, create, delete
    │   ├── mount.rs      # mount, unmount, list
    │   └── node.rs       # node status
    ├── output/
    │   ├── mod.rs
    │   ├── table.rs      # Table formatting
    │   └── json.rs       # JSON output
    └── error.rs          # CLI error types
```

## Cargo.toml Dependencies
```toml
[dependencies]
clap = { version = "4", features = ["derive"] }
tokio = { version = "1", features = ["full"] }
thiserror = "1.0"
serde = { version = "1", features = ["derive"] }
serde_json = "1"

# Erlang distribution (for daemon communication)
# Note: these may need to be added in a later task when implementing connection
```

## Testing Strategy
- `cargo build` succeeds
- `cargo test` passes
- `neonfs-cli --help` shows commands
- `neonfs-cli --version` shows version
- `neonfs-cli cluster --help` shows cluster subcommands

## Dependencies
None - independent crate

## Files to Create
- `neonfs-cli/Cargo.toml`
- `neonfs-cli/src/main.rs`
- `neonfs-cli/src/commands/mod.rs`
- `neonfs-cli/src/commands/cluster.rs`
- `neonfs-cli/src/commands/volume.rs`
- `neonfs-cli/src/commands/mount.rs`
- `neonfs-cli/src/commands/node.rs`
- `neonfs-cli/src/output/mod.rs`
- `neonfs-cli/src/output/table.rs`
- `neonfs-cli/src/output/json.rs`
- `neonfs-cli/src/error.rs`

## Reference
- spec/deployment.md - CLI Architecture section
- spec/deployment.md - CLI crate structure

## Notes
The actual daemon connection comes in the next task. This task focuses on the CLI structure and argument parsing. Commands will return placeholder output initially.
