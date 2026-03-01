# Task 0168: Drive Worker GenStage Consumer

## Status
Complete

## Phase
Gap Analysis — H-1 (4/8)

## Description
Implement `NeonFS.IO.DriveWorker`, a GenStage consumer that subscribes to
the producer and executes I/O operations for a specific physical drive. Each
drive in `DriveRegistry` gets its own worker process.

The worker uses the appropriate `DriveStrategy` (HDD or SSD) to determine
operation ordering and concurrency. It executes operation callbacks within
`Task.async_stream` for controlled parallelism.

## Acceptance Criteria
- [ ] `NeonFS.IO.DriveWorker` implements `GenStage` consumer behaviour
- [ ] One worker process per registered drive (started dynamically)
- [ ] Worker subscribes to `NeonFS.IO.Producer` with `partition` dispatcher (operations routed by `drive_id`)
- [ ] Worker initialises the correct `DriveStrategy` based on drive type detection
- [ ] `handle_events/3` passes operations to the strategy's `enqueue/2`, then dispatches via `next_batch/2`
- [ ] Operation callbacks executed in `Task.async_stream` with concurrency limited by strategy config
- [ ] Successful callback results emit `:telemetry` events (`[:neon_fs, :io, :complete]`)
- [ ] Failed callbacks emit error telemetry (`[:neon_fs, :io, :error]`) and return `{:error, reason}` to caller
- [ ] Worker tracks in-flight operation count for backpressure
- [ ] `max_demand` set based on strategy type: lower for HDD (batch_size), higher for SSD (max_concurrent)
- [ ] Unit test: worker dispatches operations using HDD strategy ordering
- [ ] Unit test: worker dispatches operations using SSD strategy ordering
- [ ] Unit test: failed callback emits error telemetry
- [ ] Unit test: worker respects concurrency limit
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/io/drive_worker_test.exs`:
  - Start a producer and worker in test
  - Submit operations with trackable callbacks (send messages to test process)
  - Verify execution order matches strategy expectations
  - Verify concurrency limiting by timing overlapping operations
  - Attach telemetry handler to verify event emission

## Dependencies
- Task 0166 (I/O producer — worker subscribes to it)
- Task 0167 (Drive strategies — worker uses them)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/io/drive_worker.ex` (create — GenStage consumer)
- `neonfs_core/test/neon_fs/io/drive_worker_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-1
- `spec/architecture.md` lines 680–740 (per-drive workers)
- GenStage documentation: consumer behaviour, partition dispatcher
- Existing: `neonfs_core/lib/neon_fs/core/drive_registry.ex` (drive list)

## Notes
The partition dispatcher in GenStage routes events to consumers based on a
key function. The producer should use `:drive_id` from the operation as the
partition key, ensuring each operation goes to the correct drive's worker.

Workers should request demand conservatively for HDD drives (small batches
to maintain sorting benefits) and aggressively for SSD drives (high
parallelism). The `min_demand` and `max_demand` options on subscription
control this.

If a drive worker crashes, its supervisor should restart it and the strategy
state is rebuilt from any re-submitted operations. Operations in flight at
crash time will fail and need to be retried by the original caller.
