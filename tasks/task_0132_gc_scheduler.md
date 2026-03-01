# Task 0132: GC Scheduler GenServer

## Status
Complete

## Phase
Gap Analysis — M-2 (1/3)

## Description
Create `NeonFS.Core.GCScheduler`, a GenServer that periodically creates
garbage collection jobs via `JobTracker`. The spec (`spec/replication.md`
lines 619–630) describes GC running on a configurable schedule (e.g. daily
at 3 AM) and also triggering when storage pressure exceeds a threshold.

This task handles the periodic scheduling only. Storage pressure triggering
is task 0133. Configuration loading from `cluster.json` is task 0134.

The scheduler follows the same pattern as `TieringManager`: a GenServer
with `Process.send_after/3` for periodic evaluation. On each tick it
creates a GC job via `JobTracker.create/2` using the existing
`Job.Runners.GarbageCollection` runner (task 0126).

To avoid duplicate concurrent runs, the scheduler checks
`JobTracker.list(status: :running, type: Job.Runners.GarbageCollection)`
before creating a new job — if a GC job is already running, the tick is
skipped.

## Acceptance Criteria
- [x] `NeonFS.Core.GCScheduler` is a GenServer
- [x] `start_link/1` accepts keyword opts: `:interval_ms` (default: 86_400_000 = 24h), `:name`
- [x] On each tick, creates a GC job via `JobTracker.create(Job.Runners.GarbageCollection, %{})`
- [x] Skips job creation if a GC job is already running (checks `JobTracker.list/1`)
- [x] `status/0` returns current config and last run info (last_triggered_at, last_skipped_at, interval_ms)
- [x] `trigger_now/0` triggers an immediate GC job (same duplicate check applies)
- [x] Telemetry event `[:neonfs, :gc_scheduler, :triggered]` emitted when a job is created
- [x] Telemetry event `[:neonfs, :gc_scheduler, :skipped]` emitted when skipped due to running job
- [x] Module dependencies are injectable via opts for testing (`:job_tracker_mod`)
- [x] Unit test: scheduler creates a GC job on tick
- [x] Unit test: scheduler skips when a GC job is already running
- [x] Unit test: `trigger_now/0` creates a job immediately
- [x] Unit test: `status/0` returns expected fields
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/gc_scheduler_test.exs`:
  - Mock `JobTracker` via injected module to control `list/1` and `create/2` returns
  - Verify job creation on `:tick` message
  - Verify skip behaviour when mock returns a running GC job
  - Verify `trigger_now/0` calls through to job creation
  - Verify telemetry events via `Telemetry.attach/4`

## Dependencies
- Task 0126 (GC Job Runner) — already complete

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/gc_scheduler.ex` (create — GenServer)
- `neonfs_core/test/neon_fs/core/gc_scheduler_test.exs` (create — tests)

## Reference
- `spec/replication.md` lines 619–630
- `spec/gap-analysis.md` — M-2
- Existing pattern: `neonfs_core/lib/neon_fs/core/tiering_manager.ex` (periodic GenServer with `Process.send_after`)
- Existing: `neonfs_core/lib/neon_fs/core/job_tracker.ex` (job creation and listing)
- Existing: `neonfs_core/lib/neon_fs/core/job/runners/garbage_collection.ex` (GC runner)

## Notes
The spec shows a cron expression `"0 3 * * *"` but implementing a full cron
parser is out of scope. Use a simple interval in milliseconds with a sensible
default of 24 hours. The `trigger_now/0` API covers the "specific time" use
case — operators can use external cron or systemd timers to call it at 3 AM
if needed.

The scheduler runs on every core node but creates cluster-scoped GC jobs.
Since `JobTracker` stores jobs locally and `GarbageCollector.collect/0` only
processes local data, this is correct — each node independently schedules its
own GC. The duplicate check prevents overlapping runs on the same node.
