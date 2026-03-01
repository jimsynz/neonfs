# Task 0169: Dynamic Priority Adjustment

## Status
Complete

## Phase
Gap Analysis — H-1 (5/8)

## Description
Add dynamic priority adjustment to the I/O producer. Under storage pressure,
scrubbing and repair operations should be promoted to run more aggressively.
Under normal conditions, user I/O should dominate.

The spec defines priority adjustment rules:
- When storage usage exceeds 85%, promote `:scrub` and `:repair` by 1 level
- When storage usage exceeds 95%, promote `:scrub` and `:repair` to match
  `:replication` priority
- When a volume has degraded redundancy, promote `:repair` for that volume
  to match `:user_read` priority
- Adjustment is re-evaluated periodically (every 30 seconds by default)

## Acceptance Criteria
- [x] `NeonFS.IO.PriorityAdjuster` GenServer that periodically checks storage and volume health
- [x] Queries `StorageMetrics` for cluster-wide storage utilisation
- [x] Queries `VolumeRegistry` for volumes with degraded redundancy
- [x] Computes adjusted priority weights based on current conditions
- [x] Publishes adjusted weights to the producer via `GenStage.cast/2`
- [x] Producer applies weight overrides when selecting next operation to dispatch
- [x] Default check interval: 30 seconds (configurable)
- [x] Storage pressure thresholds configurable: `{0.85, :boost}`, `{0.95, :critical}`
- [x] At `:boost` level: scrub and repair weights increased by 50%
- [x] At `:critical` level: scrub and repair weights match replication weight
- [x] Degraded volume repair promoted to user_read weight for that volume only
- [x] Telemetry event emitted when priority adjustment changes (`[:neon_fs, :io, :priority_adjusted]`)
- [x] Unit test: no adjustment when storage below 85%
- [x] Unit test: boost adjustment at 85-95% storage
- [x] Unit test: critical adjustment above 95% storage
- [x] Unit test: per-volume repair promotion for degraded volumes
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/io/priority_adjuster_test.exs`:
  - Mock `StorageMetrics` to return various utilisation levels
  - Mock `VolumeRegistry` to return degraded volumes
  - Start adjuster and producer, verify weight changes propagate
  - Attach telemetry handler to verify adjustment events

## Dependencies
- Task 0166 (I/O producer — receives weight adjustments)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/io/priority_adjuster.ex` (create — periodic health check and adjustment)
- `neonfs_core/lib/neon_fs/io/producer.ex` (modify — accept and apply weight overrides)
- `neonfs_core/test/neon_fs/io/priority_adjuster_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-1
- `spec/architecture.md` lines 700–740 (dynamic priority)
- `spec/storage-tiering.md` lines 750–790 (pressure response)
- Existing: `neonfs_core/lib/neon_fs/core/storage_metrics.ex`

## Notes
The priority adjuster is a read-only observer — it never modifies queues
directly, only adjusts the weights used by the producer's dispatch logic.
This keeps the adjustment mechanism loosely coupled from the scheduling
core.

Under extreme storage pressure (>95%), the system should aggressively
reclaim space. Promoting scrub and repair ensures data integrity operations
complete before the node becomes critically full.

Per-volume repair promotion is important for erasure-coded volumes: when a
stripe has lost shards, repair must run quickly to restore redundancy
before another failure causes data loss.
