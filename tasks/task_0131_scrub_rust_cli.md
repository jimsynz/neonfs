# Task 0131: Scrub Rust CLI Command Module

## Status
Complete

## Phase
Gap Analysis — M-1 (6/6)

## Description
Create a `scrub.rs` command module in the Rust CLI with two subcommands:

- `neonfs scrub start [--volume <name>]` — starts an integrity scan
- `neonfs scrub status` — shows recent scrub job history

The commands call `handle_scrub_start/1` and `handle_scrub_status/0` on the
Elixir daemon via the existing `DaemonConnection` RPC mechanism.

## Acceptance Criteria
- [ ] `neonfs-cli/src/commands/scrub.rs` created with `ScrubCommand` enum
- [ ] `ScrubCommand::Start` accepts optional `--volume <name>` argument
- [ ] `ScrubCommand::Status` takes no arguments
- [ ] `Start` calls `handle_scrub_start` with options map, displays job info
- [ ] `Status` calls `handle_scrub_status`, displays job list in table/JSON format
- [ ] Table output for `start` shows: job ID, volume (or "all"), and a hint to track with `neonfs job show <id>`
- [ ] Table output for `status` shows: job ID, status, progress (total/completed), corruption count, timestamps
- [ ] JSON output works for both subcommands
- [ ] `scrub.rs` registered in `commands/mod.rs`
- [ ] `Scrub` variant added to `Commands` enum in `main.rs` with dispatch
- [ ] `cargo test` parsing tests pass for both subcommands
- [ ] `cargo clippy --all-targets -- -D warnings` passes
- [ ] `cargo fmt --check` passes

## Testing Strategy
- Rust unit tests in `scrub.rs`:
  - `test_scrub_start_parsing` — verify `scrub start` parses correctly
  - `test_scrub_start_with_volume` — verify `scrub start --volume myvol` parses correctly
  - `test_scrub_status_parsing` — verify `scrub status` parses correctly
- `main.rs` integration test: verify `neonfs-cli scrub start` parses at top level

## Dependencies
- task_0130 (Scrub CLI handler functions)

## Files to Create/Modify
- `neonfs-cli/src/commands/scrub.rs` (create — scrub command module)
- `neonfs-cli/src/commands/mod.rs` (modify — add `pub mod scrub;`)
- `neonfs-cli/src/main.rs` (modify — add `Scrub` variant to `Commands`, import, dispatch)

## Reference
- `spec/gap-analysis.md` — M-1
- Existing pattern: `neonfs-cli/src/commands/gc.rs` from task_0128 (same structure)
- Existing pattern: `neonfs-cli/src/commands/drive.rs` (evacuate — job submission)

## Notes
This follows the exact same pattern as `gc.rs` (task_0128). The only
differences are the subcommand names (`start` vs `collect`), the handler
function names, and the display text.

The scrub status output should include corruption count if available from the
job's progress or state. Extract it from the job map's `progress_description`
field, which the scrub runner populates with a string like
"Verified 450/1000 chunks (2 corrupted)".
