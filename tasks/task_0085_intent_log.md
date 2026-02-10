# Task 0085: Intent Log

## Status
Not Started

## Phase
5 - Metadata Tiering

## Description
Implement the Intent Log module, which provides crash-safe transaction coordination for cross-segment metadata operations. The intent log uses Ra for exclusive intent acquisition (check-and-set on conflict keys) and supports TTL-based leases with extension for long-running operations. It solves two problems: cross-segment atomicity (e.g., file create = FileMeta + DirectoryEntry) and concurrent writer detection (only one writer per file at a time).

## Acceptance Criteria
- [ ] New `NeonFS.Core.Intent` struct with fields: id, operation, conflict_key, params, state (`:pending` | `:completed` | `:failed` | `:rolled_back`), started_at, expires_at, completed_at, error
- [ ] New `NeonFS.Core.IntentLog` module
- [ ] `IntentLog.try_acquire/1` — exclusive intent acquisition via Ra; returns `{:ok, intent_id}` or `{:error, :conflict, existing_intent}`
- [ ] `IntentLog.complete/1` — marks intent completed, releases conflict_key
- [ ] `IntentLog.fail/2` — marks intent failed with reason, releases conflict_key
- [ ] `IntentLog.extend/2` — extends intent TTL by additional_seconds (default 300s)
- [ ] `IntentLog.get/1` — retrieves intent by ID
- [ ] `IntentLog.list_active/0` — returns all active (non-expired, non-completed) intents
- [ ] `IntentLog.list_expired/0` — returns all intents past their TTL
- [ ] Conflict key types supported:
  - `{:file, file_id}` — one writer per file
  - `{:create, volume_id, parent_path, name}` — prevents duplicate creation
  - `{:dir, volume_id, parent_path}` — serialises directory modifications
  - `{:chunk_migration, chunk_hash}` — one migration per chunk
  - `{:volume_key_rotation, volume_id}` — one rotation per volume
- [ ] Expired intent takeover: if an existing intent's TTL has passed, a new `try_acquire` with the same conflict_key succeeds (expired intent marked as `:expired`)
- [ ] Default TTL: 300 seconds (5 minutes)
- [ ] Periodic cleanup: `cleanup_expired_intents/0` removes all expired intents from Ra state (called on a timer)
- [ ] Intent state transitions are atomic within Ra's `apply/3`
- [ ] Unit tests: acquire intent, verify active
- [ ] Unit tests: acquire → complete → verify released
- [ ] Unit tests: acquire → fail → verify released
- [ ] Unit tests: conflict detection (two intents, same conflict_key)
- [ ] Unit tests: expired intent takeover
- [ ] Unit tests: extend TTL
- [ ] Unit tests: cleanup removes only expired intents

## Testing Strategy
- ExUnit tests for full intent lifecycle: acquire → extend → complete
- ExUnit tests for conflict detection: second acquire with same conflict_key returns error
- ExUnit tests for expired takeover: create intent, advance time past TTL, acquire new intent succeeds
- ExUnit tests for cleanup: create mix of active and expired intents, run cleanup, verify correct ones removed
- ExUnit tests for different conflict key types (verify they don't interfere with each other)
- ExUnit tests for state transitions: pending → completed, pending → failed, pending → expired

## Dependencies
- task_0082 (MSM v5 — intent commands stored in Ra)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/intent.ex` (new — Intent struct, if not already created in task 0082)
- `neonfs_core/lib/neon_fs/core/intent_log.ex` (new — IntentLog API module)
- `neonfs_core/test/neon_fs/core/intent_log_test.exs` (new)

## Reference
- spec/metadata.md — Intent Log: Transaction Safety and Write Coordination
- spec/metadata.md — Exclusive Intent Acquisition
- spec/metadata.md — Conflict Keys by Operation
- spec/metadata.md — Intent TTL and Lease Extension
- spec/metadata.md — Intent Cleanup and Archival

## Notes
The Intent struct is defined in task 0082 as part of the MSM v5 state shape, but the IntentLog module (this task) provides the higher-level API. The IntentLog module sends Ra commands and interprets responses — it doesn't implement the Ra state machine logic directly (that's in MSM). The intent log is intentionally not a general transaction system — it only covers predefined operation types with known conflict keys. The archival of completed intents to the system volume audit log (described in the spec) is deferred to the audit logging task in Phase 6. For now, completed/failed intents are simply removed from Ra state during cleanup.
