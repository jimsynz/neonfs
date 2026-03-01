# Task 0153: Worker CLI Handler Functions

## Status
Complete

## Phase
Gap Analysis — M-11 (2/3)

## Description
Add CLI handler functions for viewing and configuring BackgroundWorker
settings at runtime. This exposes the `reconfigure/1` and `status/0`
functions from task 0152 through the CLI handler.

Also add persistence: when worker settings are changed via CLI, update
`cluster.json` so the changes survive node restarts.

## Acceptance Criteria
- [x] `handle_worker_status/0` exists in `NeonFS.CLI.Handler`
- [x] Returns a map with current worker config and runtime stats
- [x] `handle_worker_configure/1` exists in `NeonFS.CLI.Handler`
- [x] Accepts a map with optional keys: `max_concurrent`, `max_per_minute`, `drive_concurrency`
- [x] Calls `BackgroundWorker.reconfigure/1` with the new settings
- [x] Persists changes to `cluster.json` via `Cluster.State.update_worker_config/1`
- [x] Returns `{:ok, new_config}` on success
- [x] Returns `{:error, reason}` if reconfiguration fails
- [x] Validates values are positive integers before applying
- [x] Unit test: worker_status returns expected fields
- [x] Unit test: worker_configure updates settings and persists
- [x] Unit test: invalid values are rejected
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/cli/handler_test.exs` (extend):
  - Call `handle_worker_status/0`, verify returned map structure
  - Call `handle_worker_configure/1` with valid settings, verify BackgroundWorker updated
  - Call with invalid values (negative numbers, non-integers), verify rejection

## Dependencies
- Task 0152 (BackgroundWorker reconfiguration support)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — add worker handler functions)
- `neonfs_core/lib/neon_fs/cluster/state.ex` (modify — add update function for worker config if not present)
- `neonfs_core/test/neon_fs/cli/handler_test.exs` (modify — add worker tests)

## Reference
- `spec/operations.md` — runtime configuration
- `spec/gap-analysis.md` — M-11
- Existing: `neonfs_core/lib/neon_fs/cli/handler.ex` (handler patterns)
- Existing: `neonfs_core/lib/neon_fs/cluster/state.ex` (cluster.json persistence)
- Task 0152 (BackgroundWorker.reconfigure/1, BackgroundWorker.status/0)

## Notes
The `Cluster.State` module handles reading and writing `cluster.json`. The
worker config is stored under the `worker` key. The update should merge
new values into the existing worker config map, not replace it entirely.

Follow the pattern used by `load_worker_config/1` in `application.ex` for
the key mapping: `"max_concurrent"` in JSON maps to `:worker_max_concurrent`
in app env. The handler should convert between these formats.
