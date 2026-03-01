# Task 0165: I/O Operation Types and Scheduler API Facade

## Status
Complete

## Phase
Gap Analysis — H-1 (1/8)

## Description
Define the core types for the I/O scheduler subsystem and create the public
API facade (`NeonFS.IO.Scheduler`). This establishes the vocabulary that all
subsequent I/O scheduler tasks build on.

The spec defines 6 priority classes for I/O operations: user read, user write,
replication, read repair, repair/resilver, and scrubbing. Each operation
carries a volume ID (for WFQ accounting), a target drive, and a callback to
execute when the operation is dispatched.

This task also adds the `gen_stage` dependency to `neonfs_core`.

## Acceptance Criteria
- [ ] `gen_stage` added to `neonfs_core/mix.exs` dependencies
- [ ] `NeonFS.IO.Operation` struct defined with fields: `id`, `priority`, `volume_id`, `drive_id`, `type` (`:read` | `:write`), `callback`, `submitted_at`, `metadata`
- [ ] `NeonFS.IO.Priority` module defines priority levels: `:user_read`, `:user_write`, `:replication`, `:read_repair`, `:repair`, `:scrub`
- [ ] `NeonFS.IO.Priority.weight/1` returns numeric weight for each priority (higher = more urgent)
- [ ] `NeonFS.IO.Scheduler` module provides `submit/1`, `submit/2` (with opts), and `cancel/1` public API
- [ ] `submit/1` validates the operation struct before dispatching
- [ ] `cancel/1` accepts an operation ID and returns `:ok` or `{:error, :not_found}`
- [ ] `NeonFS.IO.Scheduler.status/0` returns current queue depths per priority class
- [ ] Typespec on all public functions
- [ ] Unit test: `Operation` struct creation and validation
- [ ] Unit test: `Priority.weight/1` returns expected ordering
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/io/operation_test.exs`:
  - Construct operations with each priority class
  - Validate required fields (missing drive_id, etc.)
  - Verify priority weight ordering
- Unit tests in `neonfs_core/test/neon_fs/io/scheduler_test.exs`:
  - API smoke tests (submit returns `{:ok, id}`, cancel returns appropriate result)
  - Status returns queue depth map

## Dependencies
- None

## Files to Create/Modify
- `neonfs_core/mix.exs` (modify — add `gen_stage` dependency)
- `neonfs_core/lib/neon_fs/io/operation.ex` (create — operation struct and validation)
- `neonfs_core/lib/neon_fs/io/priority.ex` (create — priority levels and weights)
- `neonfs_core/lib/neon_fs/io/scheduler.ex` (create — public API facade)
- `neonfs_core/test/neon_fs/io/operation_test.exs` (create — tests)
- `neonfs_core/test/neon_fs/io/scheduler_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-1
- `spec/architecture.md` lines 501–740 (I/O scheduling architecture)
- `spec/storage-tiering.md` lines 637–790 (drive strategies)
- Existing: `neonfs_core/lib/neon_fs/core/background_worker.ex` (current priority system)

## Notes
The `Scheduler` facade will initially be a thin wrapper that validates and
forwards. Once the producer (task 0166) and drive workers (task 0168) are
built, the facade will route operations through the GenStage pipeline.

`BackgroundWorker` is intentionally kept for non-I/O background jobs (key
rotation, GC scheduling, etc.). The I/O scheduler handles only disk-bound
operations.

The `callback` field in `Operation` is a zero-arity function that performs
the actual I/O work. The scheduler controls *when* and *where* it executes,
not *what* it does. This keeps the scheduler generic.
