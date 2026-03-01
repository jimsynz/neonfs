# Task 0167: Drive Strategy Behaviour and Implementations

## Status
Complete

## Phase
Gap Analysis — H-1 (3/8)

## Description
Define the `NeonFS.IO.DriveStrategy` behaviour and implement two strategies:
one for HDDs (elevator scheduling) and one for SSDs (parallel FIFO).

The spec describes drive-type-specific I/O scheduling:
- **HDD**: batch reads and writes separately, sort by chunk hash prefix to
  minimise seeking, prefer sequential access patterns
- **SSD**: parallel I/O with no seek penalty, simple FIFO ordering,
  interleave reads and writes freely

Drive type detection uses `/sys/block/<dev>/queue/rotational`: `1` = HDD,
`0` = SSD, with a configurable fallback default.

## Acceptance Criteria
- [ ] `NeonFS.IO.DriveStrategy` behaviour defined with callbacks: `init/1`, `enqueue/2`, `next_batch/2`, `type/0`
- [ ] `init/1` receives drive config and returns strategy state
- [ ] `enqueue/2` adds an operation to the strategy's internal queue
- [ ] `next_batch/2` returns up to N operations to execute, ordered by the strategy's policy
- [ ] `type/0` returns `:hdd` or `:ssd`
- [ ] `NeonFS.IO.DriveStrategy.HDD` implements elevator scheduling: batches reads then writes, sorts by chunk hash prefix within each batch
- [ ] HDD strategy has configurable `batch_size` (default 32)
- [ ] `NeonFS.IO.DriveStrategy.SSD` implements parallel FIFO: returns operations in submission order, interleaves reads and writes
- [ ] SSD strategy has configurable `max_concurrent` (default 64)
- [ ] `NeonFS.IO.DriveStrategy.detect/1` accepts a drive path and returns `:hdd` or `:ssd` based on `/sys/block/*/queue/rotational`
- [ ] `detect/1` falls back to configurable default (`:ssd`) when sysfs is unavailable
- [ ] Unit test: HDD strategy sorts by hash prefix and batches reads/writes separately
- [ ] Unit test: SSD strategy returns FIFO order
- [ ] Unit test: detection returns correct type for known sysfs values
- [ ] Unit test: detection falls back when sysfs is missing
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/io/drive_strategy_test.exs`:
  - HDD: enqueue mixed reads/writes, verify `next_batch/2` returns reads sorted by hash, then writes sorted by hash
  - SSD: enqueue operations, verify FIFO order
  - Detection: mock sysfs path or use temp file to test parsing

## Dependencies
- Task 0165 (I/O operation types — Operation struct used by strategies)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/io/drive_strategy.ex` (create — behaviour + detect/1)
- `neonfs_core/lib/neon_fs/io/drive_strategy/hdd.ex` (create — elevator scheduling)
- `neonfs_core/lib/neon_fs/io/drive_strategy/ssd.ex` (create — parallel FIFO)
- `neonfs_core/test/neon_fs/io/drive_strategy_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-1
- `spec/architecture.md` lines 580–680 (drive-specific strategies)
- `spec/storage-tiering.md` lines 637–710 (HDD elevator, SSD parallel)
- Existing: `neonfs_core/lib/neon_fs/core/drive_registry.ex` (drive discovery)

## Notes
The HDD elevator algorithm mimics the Linux `noop` + `deadline` I/O
schedulers: accumulate operations into a batch, then sort by logical address
(chunk hash prefix as a proxy) to reduce seek distance. Reads get priority
over writes within a deadline window.

The `chunk_hash_prefix` is extracted from the operation's metadata map. If
not present (e.g. for non-chunk I/O), operations sort to the end of the
batch.

For drive type detection, `/sys/block/sda/queue/rotational` is the standard
Linux interface. On macOS (development) and in containers without sysfs,
the fallback default of `:ssd` is used since most development environments
use SSDs.
