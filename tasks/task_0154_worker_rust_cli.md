# Task 0154: Worker Rust CLI Commands

## Status
Complete

## Phase
Gap Analysis — M-11 (3/3)

## Description
Add `worker status` and `worker configure` subcommands to the Rust CLI.
These are the user-facing commands that call the handler functions added in
task 0153.

## Acceptance Criteria
- [x] `WorkerCommand` enum added with `Status` and `Configure` variants
- [x] `neonfs worker status` displays current worker configuration and runtime stats
- [x] Status output shows: max_concurrent, max_per_minute, drive_concurrency, queued, running
- [x] `neonfs worker configure` accepts flags for each setting
- [x] Flag `--max-concurrent` (positive integer)
- [x] Flag `--max-per-minute` (positive integer)
- [x] Flag `--drive-concurrency` (positive integer)
- [x] At least one flag must be specified for `configure` (error otherwise)
- [x] `configure` calls `Elixir.NeonFS.CLI.Handler.handle_worker_configure/1` via RPC
- [x] `status` calls `Elixir.NeonFS.CLI.Handler.handle_worker_status/0` via RPC
- [x] Displays updated configuration on successful configure
- [x] Displays error message on failure
- [x] `cargo clippy --all-targets -- -D warnings` passes
- [x] `cargo fmt --check` passes

## Testing Strategy
- Rust unit tests for argument parsing
- Rust unit tests for term construction
- Manual end-to-end testing: run `worker status`, then `worker configure --max-concurrent 8`,
  then `worker status` again to verify change

## Dependencies
- Task 0153 (Worker CLI handler functions)

## Files to Create/Modify
- `neonfs-cli/src/commands/worker.rs` (create — worker command module)
- `neonfs-cli/src/commands/mod.rs` (modify — register worker command)
- `neonfs-cli/src/main.rs` (modify — add worker subcommand to CLI)

## Reference
- `spec/operations.md` — runtime configuration
- `spec/gap-analysis.md` — M-11
- Existing: `neonfs-cli/src/commands/volume.rs` (command pattern)
- Existing: `neonfs-cli/src/commands/gc.rs` (simpler command pattern)
- Task 0153 (handler functions this calls)

## Notes
Follow the existing command module pattern. The `status` command is
read-only and has no flags. The `configure` command uses optional flags —
build the config map with only the flags that were explicitly provided.

Format the status output as a readable table:
```
Worker Configuration:
  Max concurrent:    4
  Max per minute:    50
  Drive concurrency: 1

Runtime Status:
  Queued:    12
  Running:   3
```
