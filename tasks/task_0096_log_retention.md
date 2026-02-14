# Task 0096: System Volume Log Retention

## Status
Complete

## Phase
7 - System Volume

## Description
Create the `NeonFS.Core.SystemVolume.Retention` module — a background process that periodically prunes old files from the system volume's append-only directories (intent archives, security audit logs). Files are date-partitioned using ISO 8601 filenames (`YYYY-MM-DD.jsonl`), so retention is based on parsing the date from each filename and comparing against configurable age thresholds.

The retention process runs daily (or on a configurable interval) and is supervised as part of the core application. Retention periods are configurable via application environment with sensible defaults.

## Acceptance Criteria
- [ ] New `NeonFS.Core.SystemVolume.Retention` GenServer module
- [ ] `Retention` is started as a child of the core supervision tree
- [ ] Configurable retention periods via application environment:
  - `intent_log_days` (default 90)
  - `security_audit_days` (default 365)
- [ ] Configurable prune interval (default 24 hours)
- [ ] `prune/0` public function that can be called manually (for testing and operational use)
- [ ] `prune/0` lists files in `/audit/intents/` and `/audit/security/`, parses dates from filenames, deletes files older than their respective retention period
- [ ] Files with unparseable dates are skipped (logged as warning, not deleted)
- [ ] `prune/0` is a no-op if the system volume does not exist yet (returns `:ok`)
- [ ] Type specs on all public functions
- [ ] Unit tests for date parsing from filenames
- [ ] Unit tests for retention logic (write old-dated files, prune, verify deleted)
- [ ] Unit tests for edge cases (no files, unparseable names, missing system volume)

## Testing Strategy
- ExUnit test: create system volume, write files with dates spanning the retention boundary, call `prune/0`, verify old files deleted and recent files retained
- ExUnit test: write files with non-date filenames (e.g., `notes.txt`), call `prune/0`, verify they are not deleted
- ExUnit test: call `prune/0` when system volume does not exist, verify `:ok` returned
- ExUnit test: verify `prune/0` respects configured retention periods (override application env in test)
- ExUnit test: verify the GenServer schedules periodic pruning (check that `handle_info(:prune, ...)` reschedules)

## Dependencies
- task_0093 (SystemVolume access API — `list/1`, `delete/1`)
- task_0092 (VolumeRegistry `create_system_volume/0` for test setup)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/system_volume/retention.ex` (new — Retention GenServer)
- `neonfs_core/test/neon_fs/core/system_volume/retention_test.exs` (new — unit tests)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add Retention to supervision tree)

## Reference
- spec/system-volume.md — Log Retention section
- spec/system-volume.md — Path Conventions (date-partitioned JSONL files)
- Existing pattern: GenServer with `Process.send_after/3` for periodic scheduling (similar to anti-entropy in Phase 5)

## Notes
The retention module deliberately uses a simple approach: list files, parse dates from filenames, delete old ones. There is no need for a database or state tracking — the filesystem (system volume) is the source of truth.

Snapshot retention (`snapshot_count`) is listed in the spec but is a Phase 12 concern (DR snapshots). This task only implements time-based retention for the two audit log directories.

The `prune/0` function should be resilient — if listing or deleting a single file fails, it logs the error and continues with remaining files rather than aborting the entire prune cycle.
