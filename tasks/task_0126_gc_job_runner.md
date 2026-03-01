# Task 0126: GC Job Runner

## Status
Complete

## Phase
Gap Analysis — M-1 (1/6)

## Description
Extend `GarbageCollector.collect/0` to accept an optional volume filter, and
create a `Job.Runners.GarbageCollection` module that wraps collection as a
tracked job via `JobTracker`.

Currently `GarbageCollector.collect/0` scans all files and chunks globally.
The CLI needs a `--volume` flag, so `collect/1` must accept an optional
`volume_id` parameter and filter `FileIndex.list_all/0` results accordingly.

The job runner follows the `KeyRotation` pattern: a single `step/1` call
runs the full collection pass (GC is not naturally batchable — the mark
phase needs a complete referenced set before sweeping). The runner reports
the result via job progress fields.

## Acceptance Criteria
- [ ] `GarbageCollector.collect/0` still works (no-arg form unchanged)
- [ ] `GarbageCollector.collect/1` accepts `opts` keyword list with optional `:volume_id`
- [ ] When `:volume_id` is given, only files belonging to that volume are scanned during the mark phase
- [ ] `Job.Runners.GarbageCollection` implements the `Job.Runner` behaviour
- [ ] `label/0` returns `"garbage-collection"`
- [ ] `step/1` calls `GarbageCollector.collect/1` with params from `job.params`
- [ ] On success, returns `{:complete, job}` with progress showing chunks_deleted, stripes_deleted, chunks_protected
- [ ] On error, returns `{:error, reason, job}`
- [ ] Unit test: runner completes a job with empty index (no chunks to collect)
- [ ] Unit test: runner with volume_id param only collects orphans from that volume
- [ ] Existing GC tests pass
- [ ] `mix format` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/job/runners/garbage_collection_test.exs`:
  - Create files and orphan chunks, run runner step, verify correct counts in job progress
  - With volume_id filter, verify only that volume's orphans are collected
  - Verify the no-arg `collect/0` still works as before
- Existing `garbage_collector_test.exs` tests must still pass

## Dependencies
- None (GarbageCollector and JobTracker already exist)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/garbage_collector.ex` (modify — add `collect/1` with opts)
- `neonfs_core/lib/neon_fs/core/job/runners/garbage_collection.ex` (create — job runner)
- `neonfs_core/test/neon_fs/core/job/runners/garbage_collection_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — M-1
- `spec/replication.md` lines 619–630
- Existing pattern: `neonfs_core/lib/neon_fs/core/job/runners/key_rotation.ex`
- Existing: `neonfs_core/lib/neon_fs/core/garbage_collector.ex`

## Notes
GC is not naturally batchable like key rotation — the mark phase must build a
complete referenced set before the sweep can begin. Therefore the runner does
the entire collection in a single `step/1` call rather than processing in
batches. This is acceptable because GC is already designed to run as a
single pass.

The `volume_id` filter works by restricting `FileIndex.list_all/0` results in
the mark phase. Files not in the target volume are excluded, so their chunks
are treated as unreferenced. This means volume-scoped GC may delete chunks
that are shared across volumes — if that becomes a concern, the mark phase
should still scan all files but only sweep chunks that belong to the target
volume's files. Start with the simpler approach and document the limitation.

The `FileIndex` stores file metadata which includes a `volume_id` field that
can be used for filtering.
