# Task 0158: Drive Space and Capacity Integration Tests

## Status
Complete

## Phase
Gap Analysis — M-13 (2/2)

## Description
Add integration tests that exercise real storage space behaviours using
loopback devices: drive filling up (ENOSPC), capacity thresholds triggering
GC or eviction, and drive evacuation.

These tests use the loopback device helpers from task 0157 to create small
real filesystems (e.g. 50 MB) that behave exactly like physical drives.
This tests the real I/O path end-to-end without mocks.

## Acceptance Criteria
- [x] Test: write data until loopback drive is full, verify ENOSPC is handled gracefully
- [x] Test: ENOSPC error propagates as a clear error to the write path (not a crash)
- [x] Test: capacity threshold (85%) triggers GC when `GCScheduler` pressure detection is active
- [x] Test: capacity threshold triggers tier eviction when `TieringManager` is active
- [x] Test: after GC runs on a full drive, space is reclaimed and writes succeed again
- [x] Test: drive evacuation moves data from a loopback drive to another drive
- [x] Test: after evacuation, all data is readable from the remaining drive
- [x] All tests tagged `@tag :loopback` and `@tag :requires_root`
- [x] Tests use `assert_eventually` with appropriate timeouts for async GC/eviction
- [x] Tests clean up loopback devices on completion (via on_exit callbacks)
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Integration tests in `neonfs_integration/test/integration/drive_space_test.exs`:
  - Start a single-node cluster with loopback devices as storage drives
  - Register the loopback mount points as drives in the BlobStore
  - Write data to fill the drive
  - Verify error handling, GC triggering, and space reclamation
  - For evacuation tests, use two loopback drives (e.g. 50 MB each)

## Dependencies
- Task 0157 (Loopback device test helpers)
- Task 0132 (GCScheduler — for pressure-triggered GC tests)

## Files to Create/Modify
- `neonfs_integration/test/integration/drive_space_test.exs` (create)

## Reference
- `spec/testing.md` — failure injection
- `spec/node-management.md`
- `spec/gap-analysis.md` — M-13
- Existing: `neonfs_core/lib/neon_fs/core/storage_metrics.ex` (capacity monitoring)
- Existing: `neonfs_core/lib/neon_fs/core/gc_scheduler.ex` (pressure detection)
- Existing: `neonfs_core/lib/neon_fs/core/drive_manager.ex` (drive evacuation)
- Task 0157 (LoopbackDevice helper)

## Notes
The cluster setup for these tests differs from normal integration tests:
instead of using the default temp directory for blob storage, configure the
node to use the loopback mount points as drive paths. This requires
configuring `DriveManager` with the loopback paths.

For the ENOSPC test, a 10 MB loopback device fills quickly with a few
chunks. Write in a loop until the error occurs, then verify the error is a
clean `{:error, :no_space}` (or ENOSPC equivalent) rather than a crash.

For the GC pressure test, configure the pressure threshold low (e.g. 0.5)
and the check interval short (e.g. 1000ms) so the test doesn't take too
long. Write enough data to exceed 50% capacity, then wait for GC to
trigger.

These tests are slow (loopback device creation takes ~1 second, writes are
real I/O) and require root. Tag them appropriately and expect them to run
only in specific CI configurations.
