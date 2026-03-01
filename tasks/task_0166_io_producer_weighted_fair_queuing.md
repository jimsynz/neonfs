# Task 0166: I/O Producer with Weighted Fair Queuing

## Status
Complete

## Phase
Gap Analysis — H-1 (2/8)

## Description
Implement the GenStage producer that receives I/O operations from the
scheduler facade and dispatches them to drive workers using Weighted Fair
Queuing (WFQ).

WFQ ensures that volumes with higher `io_weight` get proportionally more
I/O bandwidth. Each volume maintains a virtual finish time; the producer
selects the operation with the lowest virtual finish time when a worker
requests demand. Within a volume's queue, operations are ordered by priority
class weight.

The producer also maintains per-priority-class counters for the `status/0`
API.

## Acceptance Criteria
- [ ] `NeonFS.IO.Producer` implements `GenStage` producer behaviour
- [ ] Operations enqueued via `GenStage.cast/2` from the scheduler facade
- [ ] Per-volume queues with virtual finish time tracking for WFQ
- [ ] Volume `io_weight` looked up from `VolumeRegistry` (cached)
- [ ] Within a volume's queue, operations sorted by `Priority.weight/1` (highest first)
- [ ] `handle_demand/2` dispatches the operation with the lowest virtual finish time
- [ ] When multiple operations have equal virtual finish time, highest priority wins
- [ ] Per-priority-class counters maintained (incremented on enqueue, decremented on dispatch)
- [ ] `status/0` call returns `%{queue_depths: %{user_read: n, ...}, total_pending: n}`
- [ ] Operations that have been cancelled (via `cancel/1`) are skipped during dispatch
- [ ] Unit test: single volume, operations dispatched in priority order
- [ ] Unit test: two volumes with different io_weight, verify proportional dispatch
- [ ] Unit test: cancel removes operation from queue
- [ ] Unit test: status returns correct queue depths
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/io/producer_test.exs`:
  - Start producer in test, enqueue operations, assert dispatch order
  - Use a test consumer that collects dispatched operations
  - Verify WFQ fairness with 2 volumes (weight 1 vs weight 3)
  - Test cancellation mid-queue

## Dependencies
- Task 0165 (I/O operation types and scheduler API)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/io/producer.ex` (create — GenStage producer with WFQ)
- `neonfs_core/lib/neon_fs/io/scheduler.ex` (modify — wire submit/cancel to producer)
- `neonfs_core/test/neon_fs/io/producer_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-1
- `spec/architecture.md` lines 501–580 (WFQ scheduling)
- GenStage documentation: producer behaviour, `handle_demand/2`
- Existing: `neonfs_client/lib/neon_fs/core/volume.ex` (io_weight field)

## Notes
The WFQ algorithm:
1. Each volume has a `virtual_finish_time` (VFT), initialised to 0
2. When an operation is enqueued for volume V: `VFT_V = max(VFT_V, current_virtual_time) + (1 / io_weight)`
3. Dispatch selects the operation whose volume has the lowest VFT
4. `current_virtual_time` advances to the VFT of the dispatched operation

This ensures volumes with higher `io_weight` advance their VFT more slowly,
getting dispatched more frequently.

Default `io_weight` is 1. Volumes can be configured with higher weights to
get priority I/O access.
