# Task 0133: GC Storage Pressure Trigger

## Status
Complete

## Phase
Gap Analysis — M-2 (2/3)

## Description
Extend `NeonFS.Core.GCScheduler` (task 0132) to also trigger garbage
collection when cluster storage usage exceeds a configurable threshold.

The spec (`spec/replication.md` lines 619–630) defines
`storage_pressure_threshold: 0.85` — GC should run when more than 85% of
cluster capacity is consumed.

The pressure check runs on a shorter interval than the scheduled GC (e.g.
every 5 minutes) using a separate `Process.send_after/3` timer. It queries
`StorageMetrics.cluster_capacity/0` to compute the usage ratio. When the
ratio exceeds the threshold and no GC job is already running, a new GC job
is created.

Drives with `:unlimited` capacity (capacity_bytes == 0) are excluded from
the pressure calculation — they represent development/test setups where
pressure detection is meaningless.

## Acceptance Criteria
- [ ] `GCScheduler.start_link/1` accepts additional opts: `:pressure_check_interval_ms` (default: 300_000 = 5 min), `:pressure_threshold` (default: 0.85)
- [ ] Pressure check runs on its own timer independent of the scheduled GC timer
- [ ] `StorageMetrics.cluster_capacity/0` is called to compute `used / capacity` ratio
- [ ] If ratio >= threshold and no GC job is running, a GC job is created
- [ ] If ratio < threshold, no action taken
- [ ] If all drives have `:unlimited` capacity, pressure check is a no-op (cannot compute ratio)
- [ ] `status/0` includes pressure-related fields: `pressure_threshold`, `pressure_check_interval_ms`, `last_pressure_check_at`, `last_pressure_ratio`
- [ ] Telemetry event `[:neonfs, :gc_scheduler, :pressure_triggered]` emitted with `%{ratio: float}` when pressure triggers GC
- [ ] Same duplicate-run check as scheduled GC (reuse existing logic)
- [ ] Module dependency `:storage_metrics_mod` is injectable for testing
- [ ] Unit test: pressure check triggers GC when ratio >= threshold
- [ ] Unit test: pressure check is a no-op when ratio < threshold
- [ ] Unit test: pressure check is a no-op when capacity is `:unlimited`
- [ ] Unit test: pressure check skips when GC job already running
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/gc_scheduler_test.exs` (extend existing):
  - Mock `StorageMetrics` via injected module to return controlled capacity values
  - Send `:pressure_check` message to the GenServer
  - Verify job creation when ratio is high
  - Verify no job creation when ratio is low or capacity is unlimited
  - Verify telemetry events

## Dependencies
- Task 0132 (GC Scheduler GenServer)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/gc_scheduler.ex` (modify — add pressure check)
- `neonfs_core/test/neon_fs/core/gc_scheduler_test.exs` (modify — add pressure tests)

## Reference
- `spec/replication.md` lines 619–630 (`storage_pressure_threshold: 0.85`)
- `spec/gap-analysis.md` — M-2
- Existing: `neonfs_core/lib/neon_fs/core/storage_metrics.ex` (`cluster_capacity/0`)
- Existing: `neonfs_core/lib/neon_fs/core/tiering_manager.ex` (eviction threshold pattern)

## Notes
The pressure ratio is calculated as:

```
ratio = total_used / total_capacity
```

Where `total_used` and `total_capacity` come from
`StorageMetrics.cluster_capacity/0`. If `total_capacity` is `:unlimited`,
the pressure check returns early without action.

The pressure check and scheduled GC share the same "is a job already
running?" guard. Extract a private function like `gc_job_running?/1` to
avoid duplication.

Consider that storage pressure may be transient — a large write followed by
GC clearing orphans. The 5-minute check interval provides natural debouncing.
No additional hysteresis is needed for the initial implementation.
