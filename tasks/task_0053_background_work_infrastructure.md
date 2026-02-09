# Task 0053: Background Work Infrastructure

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
Implement a background task runner with priority queues and rate limiting for long-running maintenance operations (tier migrations, scrubbing, rebalancing). Uses `Task.Supervisor` for crash isolation. Load-aware yielding checks `:scheduler_wall_time` to back off when the node is under heavy foreground load.

## Acceptance Criteria
- [x] `BackgroundWorker` GenServer with priority queues (`:high`, `:normal`, `:low`)
- [x] `submit/2` — `(work_fn, opts)` → `{:ok, work_id}` (opts include `:priority`, `:label`)
- [x] `cancel/1` — `(work_id)` → `:ok` or `{:error, :not_found}`
- [x] `status/0` — returns `%{queued: count, running: count, completed: count, by_priority: map}`
- [x] `status/1` — `(work_id)` → `:queued` / `:running` / `:completed` / `:cancelled` / `:failed`
- [x] Concurrency limit: configurable max concurrent background tasks (default 2)
- [x] Priority ordering: high tasks dequeued before normal, normal before low
- [x] Rate limiting: configurable max tasks started per minute (default 10)
- [x] `Task.Supervisor` used for crash isolation — failed tasks don't crash the worker
- [x] Failed tasks logged with reason, not retried automatically (caller decides retry policy)
- [x] Load-aware yielding: check `:scheduler_wall_time` ratio, pause queue processing above threshold (default 80%)
- [x] Telemetry events for submit, start, complete, fail, cancel
- [x] Graceful shutdown: on terminate, cancel queued work, wait for running tasks (with timeout)
- [x] All new code has unit tests

## Testing Strategy
- Unit tests for priority queue ordering
- Unit tests for concurrency limiting (submit 5, verify only 2 run concurrently)
- Unit tests for cancellation (queued and running)
- Unit tests for crash isolation (submitted fn raises, worker continues)
- Unit tests for status reporting
- Test graceful shutdown behaviour

## Dependencies
- None (foundational infrastructure, independent of other Phase 3 tasks)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/background_worker.ex` (new)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add BackgroundWorker + Task.Supervisor to supervision tree)
- `neonfs_core/test/neon_fs/core/background_worker_test.exs` (new)

## Reference
- spec/implementation.md — Phase 3: Background processes
- spec/architecture.md — Maintenance operations

## Notes
This is intentionally simpler than a full job queue (no persistence, no retries, no distributed scheduling). It's a node-local work scheduler for maintenance operations that can be safely lost on node restart. The TieringManager and future scrubbing processes submit work here rather than running tasks directly, which provides centralised rate limiting and load awareness. The `:scheduler_wall_time` check uses `:erlang.statistics(:scheduler_wall_time_all)` to measure CPU saturation — when schedulers are busy, background work yields to foreground requests.
