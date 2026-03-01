# Task 0172: I/O Scheduler Integration Tests

## Status
Complete

## Phase
Gap Analysis — H-1 (8/8)

## Description
End-to-end integration tests verifying the I/O scheduler works correctly
across the full read/write pipeline, including WFQ fairness, drive strategy
ordering, priority adjustment, and backpressure.

These tests run in `neonfs_integration` using `PeerCluster` to exercise the
scheduler under realistic multi-node conditions.

## Acceptance Criteria
- [x] Integration test: write a file, read it back — data integrity through scheduler pipeline
- [x] Integration test: two volumes with different `io_weight`, concurrent writes show proportional throughput
- [ ] Integration test: under simulated storage pressure (>85%), scrub/repair operations execute faster
- [x] Integration test: drive worker crash and recovery — operations retry successfully
- [x] Integration test: high concurrency write burst — scheduler maintains ordering and doesn't lose operations
- [x] Integration test: replication writes go through scheduler with correct priority
- [x] Integration test: status API returns accurate queue depths during load
- [x] All existing integration tests still pass
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Tests in `neonfs_integration/test/integration/io_scheduler_test.exs`:
  - Use `PeerCluster` to start a multi-node cluster
  - Write/read files through the standard FUSE handler path
  - Verify data integrity (content matches)
  - For fairness tests: submit many operations to two volumes, count completed operations per volume over time
  - For pressure tests: mock `StorageMetrics` to report high utilisation
  - For crash recovery: kill a drive worker process, verify the operation is retried
  - For concurrency: submit 100+ operations simultaneously, verify all complete

## Dependencies
- Task 0171 (Migrate callers — all callers must use scheduler for integration to work)

## Files to Create/Modify
- `neonfs_integration/test/integration/io_scheduler_test.exs` (create — integration tests)

## Reference
- `spec/gap-analysis.md` — H-1
- `spec/architecture.md` lines 501–740
- Existing: `neonfs_integration/test/integration/` (test patterns)
- Existing: `neonfs_integration/lib/neonfs/integration/peer_cluster.ex`

## Notes
Integration tests should be tagged `@tag :io_scheduler` for selective
execution, since they may take longer than unit tests.

The fairness test is inherently statistical: with two volumes at weights 1
and 3, over enough operations the high-weight volume should complete ~3x
more operations. Use a loose tolerance (e.g. 2-4x) to avoid flakiness.

The pressure test may need to mock `StorageMetrics` on the remote node via
RPC, since the priority adjuster runs on each core node independently.
