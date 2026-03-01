# Task 0128: GC Rust CLI Command Module

## Status
Complete

## Phase
Gap Analysis — M-1 (3/6)

## Description
Create a `gc.rs` command module in the Rust CLI with two subcommands:

- `neonfs gc collect [--volume <name>]` — triggers garbage collection
- `neonfs gc status` — shows recent GC job history

The commands call `handle_gc_collect/1` and `handle_gc_status/0` on the
Elixir daemon via the existing `DaemonConnection` RPC mechanism.

## Acceptance Criteria
- [x] `neonfs-cli/src/commands/gc.rs` created with `GcCommand` enum
- [x] `GcCommand::Collect` accepts optional `--volume <name>` argument
- [x] `GcCommand::Status` takes no arguments
- [x] `Collect` calls `handle_gc_collect` with options map, displays job info
- [x] `Status` calls `handle_gc_status`, displays job list in table/JSON format
- [x] Table output for `collect` shows: job ID, volume (or "all"), and a hint to track with `neonfs job show <id>`
- [x] Table output for `status` shows: job ID, status, progress, timestamps
- [x] JSON output works for both subcommands
- [x] `gc.rs` registered in `commands/mod.rs`
- [x] `Gc` variant added to `Commands` enum in `main.rs` with dispatch
- [x] `cargo test` parsing tests pass for both subcommands
- [x] `cargo clippy --all-targets -- -D warnings` passes
- [x] `cargo fmt --check` passes

## Testing Strategy
- Rust unit tests in `gc.rs`:
  - `test_gc_collect_parsing` — verify `gc collect` parses correctly
  - `test_gc_collect_with_volume` — verify `gc collect --volume myvol` parses correctly
  - `test_gc_status_parsing` — verify `gc status` parses correctly
- `main.rs` integration test: verify `neonfs-cli gc collect` parses at top level

## Dependencies
- task_0127 (GC CLI handler functions)

## Files to Create/Modify
- `neonfs-cli/src/commands/gc.rs` (create — GC command module)
- `neonfs-cli/src/commands/mod.rs` (modify — add `pub mod gc;`)
- `neonfs-cli/src/main.rs` (modify — add `Gc` variant to `Commands`, import, dispatch)

## Reference
- `spec/gap-analysis.md` — M-1
- Existing pattern: `neonfs-cli/src/commands/drive.rs` (evacuate subcommand — job creation)
- Existing pattern: `neonfs-cli/src/commands/job.rs` (job listing display)
- Existing: `neonfs-cli/src/main.rs` (command registration)

## Notes
Follow the `drive evacuate` pattern for `gc collect`: submit the job, extract
the job ID from the response, and tell the user to track it with
`neonfs job show <id>`.

Follow the `job list` pattern for `gc status`: display a table of jobs with
their status and progress.

The options map sent to `handle_gc_collect` should use binary keys (not atoms)
matching the Elixir handler's `Map.get(opts, "volume")` pattern.
