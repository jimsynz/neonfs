# Task 0055: Tier Migration with Reactor

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
Implement tier migration as Reactor sagas for both local (between drives on the same node) and cross-node (copy via RPC) scenarios. The Reactor pattern provides clean step/undo/compensate semantics for multi-step data movement. Steps: acquire lock → copy data → verify → update metadata → remove old location → release lock. An ETS-based lock table prevents concurrent migrations of the same chunk.

## Acceptance Criteria
- [x] `reactor` hex package added to neonfs_core dependencies
- [x] `TierMigration` module with saga-pattern ordered steps (copy → verify → metadata → cleanup)
- [x] Copy step — reads chunk from source drive/node, writes to target drive/node
- [x] Copy rollback — deletes the copy on failure
- [x] Metadata step — updates chunk location in ChunkIndex
- [x] Metadata undo — implicit (rollback deletes copy, source unchanged)
- [x] Cleanup step — removes chunk from source drive/node (best-effort, orphaned chunks harmless)
- [x] Cleanup — no undo (intentional, as per spec)
- [x] ETS lock table: `acquire_lock/1` and `release_lock/1` for chunk hash
- [x] Concurrent migration of same chunk detected and rejected with `{:error, :migration_in_progress}`
- [x] Local migration: copy between drives on the same node via BlobStore
- [x] Cross-node migration: copy chunk data via RPC to remote node's BlobStore
- [x] Verification step: after copy, verify chunk hash matches on target (read with verify: true)
- [x] `run_migration/1` — entry point accepting `%{chunk_hash, source_drive, source_node, target_drive, target_node, source_tier, target_tier}`
- [x] Migration callable from BackgroundWorker via `work_fn/1`
- [x] Telemetry events for migration start, success, failure
- [x] All steps have unit tests
- [x] Integration test: local migration between tiers on same drive
- [ ] Integration test: cross-node migration (deferred to task 0056 Phase 3 integration tests)

## Testing Strategy
- Unit tests for each Reactor step independently
- Unit tests for lock acquisition and conflict detection
- Unit tests for undo/rollback paths (simulate failures at each step)
- Integration test: local tier migration with two tmp drives
- Integration test: cross-node migration in PeerCluster
- Test verification catches corrupted copy

## Dependencies
- task_0046 (multi-drive BlobStore)
- task_0047 (DriveRegistry for drive info)
- task_0053 (BackgroundWorker for scheduling)
- task_0054 (TieringManager submits migrations)

## Files to Create/Modify
- `neonfs_core/mix.exs` (modify — add `reactor` dependency)
- `neonfs_core/lib/neon_fs/core/tier_migration.ex` (new — Reactor definition)
- `neonfs_core/lib/neon_fs/core/tier_migration/copy_step.ex` (new)
- `neonfs_core/lib/neon_fs/core/tier_migration/metadata_step.ex` (new)
- `neonfs_core/lib/neon_fs/core/tier_migration/cleanup_step.ex` (new)
- `neonfs_core/test/neon_fs/core/tier_migration_test.exs` (new)
- `neonfs_core/test/neon_fs/core/tier_migration/copy_step_test.exs` (new)
- `neonfs_core/test/neon_fs/core/tier_migration/metadata_step_test.exs` (new)
- `neonfs_core/test/neon_fs/core/tier_migration/cleanup_step_test.exs` (new)

## Reference
- spec/implementation.md — Phase 3: Tier migration
- spec/architecture.md — Data movement
- https://hex2txt.fly.dev/reactor/llms.txt — Reactor usage guide

## Notes
Reactor was chosen because James is the author, so the team has deep expertise. The saga pattern maps naturally to data migration: each step has a clear action and compensation. The cleanup step intentionally has no undo — if the source copy can't be deleted, the chunk is simply orphaned and will be cleaned up by a future scrubbing pass. The ETS lock table is node-local; cross-node lock coordination is not needed because each migration has a single "owning" node that runs the Reactor.
