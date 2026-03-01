# Task 0135: ScrubScheduler GenServer

## Status
Complete

## Phase
Gap Analysis — M-3 (1/3)

## Description
Create `NeonFS.Core.ScrubScheduler`, a GenServer that periodically creates
integrity scrub jobs via `JobTracker`. The spec (`spec/architecture.md`
lines 370–388) describes scrubbing as mandatory background verification of
stored chunks, even when `on_read: :never`.

This follows the same pattern as `GCScheduler` (task 0132): a GenServer
with `Process.send_after/3` for periodic evaluation. On each tick it
creates a scrub job via `JobTracker.create/2` using the existing
`Job.Runners.Scrub` runner (task 0129).

To avoid duplicate concurrent runs, the scheduler checks
`JobTracker.list(status: :running, type: Job.Runners.Scrub)` before
creating a new job — if a scrub job is already running, the tick is skipped.

The default interval comes from the volume's `verification.scrub_interval`
(added in QW-5 / task 0123). The scheduler should iterate volumes and
create per-volume scrub jobs, respecting each volume's configured interval.

## Acceptance Criteria
- [ ] `NeonFS.Core.ScrubScheduler` is a GenServer
- [ ] `start_link/1` accepts keyword opts: `:check_interval_ms` (default: 3_600_000 = 1h), `:name`
- [ ] On each tick, iterates all volumes via `VolumeRegistry.list/0`
- [ ] For each volume, checks if a scrub is due based on `verification.scrub_interval` and last scrub completion time
- [ ] Creates a scrub job via `JobTracker.create(Job.Runners.Scrub, %{volume_id: id})` when due
- [ ] Skips job creation if a scrub job for that volume is already running
- [ ] `status/0` returns current config and per-volume last scrub info
- [ ] `trigger_now/1` triggers an immediate scrub for a specific volume (same duplicate check applies)
- [ ] Telemetry event `[:neonfs, :scrub_scheduler, :triggered]` emitted when a job is created
- [ ] Telemetry event `[:neonfs, :scrub_scheduler, :skipped]` emitted when skipped due to running job
- [ ] Module dependencies are injectable via opts for testing (`:job_tracker_mod`, `:volume_registry_mod`)
- [ ] Unit test: scheduler creates a scrub job when interval has elapsed
- [ ] Unit test: scheduler skips when a scrub job is already running
- [ ] Unit test: scheduler respects per-volume scrub_interval
- [ ] Unit test: `trigger_now/1` creates a job immediately
- [ ] Unit test: `status/0` returns expected fields
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/scrub_scheduler_test.exs`:
  - Mock `JobTracker` and `VolumeRegistry` via injected modules
  - Verify job creation on `:tick` when scrub is due
  - Verify skip when mock returns a running scrub job
  - Verify per-volume interval tracking
  - Verify `trigger_now/1` calls through to job creation
  - Verify telemetry events via `Telemetry.attach/4`

## Dependencies
- Task 0129 (Scrub Job Runner) — already complete
- Task 0123 (scrub_interval in verification_config) — already complete

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/scrub_scheduler.ex` (create — GenServer)
- `neonfs_core/test/neon_fs/core/scrub_scheduler_test.exs` (create — tests)

## Reference
- `spec/architecture.md` lines 370–388
- `spec/gap-analysis.md` — M-3
- Existing pattern: `neonfs_core/lib/neon_fs/core/gc_scheduler.ex` (periodic GenServer with `Process.send_after`)
- Existing: `neonfs_core/lib/neon_fs/core/job_tracker.ex` (job creation and listing)
- Existing: `neonfs_core/lib/neon_fs/core/job/runners/scrub.ex` (scrub runner)

## Notes
Unlike GCScheduler which runs on a single global interval, ScrubScheduler
needs per-volume scheduling because each volume has its own `scrub_interval`.
The scheduler's tick interval (`check_interval_ms`) is how often it checks
whether any volume is due for a scrub — this should be shorter than any
individual volume's scrub interval (default 1 hour check vs default 7 day
scrub interval).

Track last scrub completion time per volume in GenServer state. On startup,
query `JobTracker` for the most recent completed scrub job per volume to
initialise the schedule.
