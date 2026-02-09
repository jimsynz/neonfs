# Task 0054: Tiering Manager (Promotion/Demotion)

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
Implement a per-node `TieringManager` GenServer that periodically evaluates chunks for promotion (cold/warm → hot) and demotion (hot → warm/cold) based on access patterns and tier capacity. Respects volume-level `tiering.promotion_threshold` and `tiering.demotion_delay` configuration. Triggers eviction under pressure when a tier exceeds 90% capacity. Submits migration work to the BackgroundWorker.

## Acceptance Criteria
- [x] `TieringManager` GenServer, one per node (not per-volume)
- [x] Periodic evaluation cycle (configurable interval, default 5 minutes)
- [x] Promotion: chunks on cold/warm tier with access count exceeding `volume.tiering.promotion_threshold` in 24h window are promoted to the next tier
- [x] Demotion: chunks on hot tier with zero accesses for longer than `volume.tiering.demotion_delay` seconds are demoted to the next tier
- [x] Eviction: when a tier exceeds 90% capacity, demote least-accessed chunks regardless of delay
- [x] Tier progression: hot ↔ warm ↔ cold (no direct hot → cold jumps)
- [x] Queries `ChunkAccessTracker` for access stats
- [x] Queries `DriveRegistry` for tier capacity and usage
- [x] Reads volume tiering config from metadata for threshold/delay values
- [x] Submits migration work to `BackgroundWorker` with `:low` priority
- [x] Skips evaluation when BackgroundWorker queue is full (> configurable threshold)
- [x] Dry-run mode: log what would be promoted/demoted without actually doing it
- [x] Telemetry events for evaluation cycle, promotions scheduled, demotions scheduled
- [x] All new code has unit tests

## Testing Strategy
- Unit tests with mocked ChunkAccessTracker, DriveRegistry, BackgroundWorker
- Test promotion trigger: chunk with high access count gets promoted
- Test demotion trigger: chunk with zero access past delay gets demoted
- Test eviction under pressure: tier at 95% capacity forces demotion
- Test that only one tier step happens at a time (no hot → cold)
- Test dry-run mode logs but doesn't submit work
- Test skip when BackgroundWorker is busy

## Dependencies
- task_0047 (DriveRegistry for capacity info)
- task_0049 (ChunkAccessTracker for access patterns)
- task_0053 (BackgroundWorker for submitting migration work)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/tiering_manager.ex` (new)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add to supervision tree)
- `neonfs_core/test/neon_fs/core/tiering_manager_test.exs` (new)

## Reference
- spec/implementation.md — Phase 3: Promotion/demotion
- spec/architecture.md — Tiering policies

## Notes
The TieringManager is per-node, not per-volume. This simplifies the supervision tree — we don't need VolumeSupervisor infrastructure yet (that's a Phase 4+ concern). The manager iterates over all volumes and their chunks on the local node during each evaluation cycle. For large chunk counts, the evaluation should be bounded (process at most N chunks per cycle) to avoid long pauses. The actual data movement is handled by the tier migration system (task 0055) — the TieringManager only decides what to move and submits work items.
