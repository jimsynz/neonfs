# Task 0171: Migrate Existing Callers to I/O Scheduler

## Status
Complete

## Phase
Gap Analysis — H-1 (7/8)

## Description
Migrate existing I/O-performing code paths to submit operations through the
new `NeonFS.IO.Scheduler` instead of calling `BlobStore` NIFs directly or
using `BackgroundWorker` for I/O jobs.

The key callers to migrate:
- `WriteOperation` — chunk writes to BlobStore
- `ReadOperation` — chunk reads from BlobStore
- `ChunkReplicator` — replication writes
- `ChunkFetcher` — remote chunk retrieval (local store after fetch)
- `StripeRepair` — repair writes
- `TierMigration` — tier movement reads and writes
- `Job.Runners.Scrub` — scrub reads

Non-I/O jobs (GC mark/sweep, key rotation, scheduling) stay on
`BackgroundWorker`.

## Acceptance Criteria
- [x] `WriteOperation.write_chunk/3` submits write via `IO.Scheduler.submit_sync/2` with `:user_write` priority
- [x] `ReadOperation.read_chunk/3` submits read via `IO.Scheduler.submit_async/1` with `:read_repair` priority
- [x] `ChunkReplicator` — skipped, only writes to remote nodes via RPC (no local BlobStore calls)
- [x] `ChunkFetcher` submits local read with `:user_read` and cache writes with `:replication` priority
- [x] `StripeRepair` submits repair writes with `:repair` priority
- [x] `TierMigration` submits reads and writes with `:repair` priority (resilver)
- [x] `Job.Runners.Scrub` submits verification reads with `:scrub` priority
- [x] All callers correctly set `drive_id` based on the target `BlobStore` handle's drive
- [x] All callers correctly set `volume_id` for WFQ accounting
- [x] `BackgroundWorker` still handles non-I/O jobs (no regression)
- [x] Fallback: when scheduler not running, callbacks execute directly (no test breakage)
- [x] Existing unit tests pass (1466 tests, 0 failures in neonfs_core)
- [x] Integration tests pass (164 tests, 0 failures)
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Modify existing tests to account for I/O scheduler indirection:
  - Tests that directly assert on BlobStore calls may need to wait for async dispatch
  - Use telemetry events (`[:neon_fs, :io, :complete]`) to synchronise assertions
- Run full `mix test` in neonfs_core to verify no regression
- Run `mix test` in neonfs_integration to verify end-to-end paths still work

## Dependencies
- Task 0170 (I/O scheduler supervision — full pipeline must be running)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — submit via scheduler)
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (modify — submit via scheduler)
- `neonfs_core/lib/neon_fs/core/chunk_replicator.ex` (modify — submit via scheduler)
- `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (modify — submit via scheduler)
- `neonfs_core/lib/neon_fs/core/stripe_repair.ex` (modify — submit via scheduler)
- `neonfs_core/lib/neon_fs/core/tier_migration.ex` (modify — submit via scheduler)
- `neonfs_core/lib/neon_fs/core/job/runners/scrub.ex` (modify — submit via scheduler)

## Reference
- `spec/gap-analysis.md` — H-1
- `spec/architecture.md` lines 501–740 (which operations go through scheduler)
- Existing callers (grep for `BlobStore.store_chunk` and `BlobStore.retrieve_chunk`)

## Notes
This is the largest task in the H-1 chain. Consider migrating one caller at
a time and running tests between each migration.

The migration should be mostly mechanical:
1. Build an `IO.Operation` struct with the appropriate priority and drive_id
2. Set the callback to the existing BlobStore NIF call
3. Submit via `IO.Scheduler.submit/1`
4. Await the result (the scheduler returns a reference that resolves when the callback completes)

The challenge is managing the async nature: operations that were previously
synchronous NIF calls now go through a queue. Callers that need the result
before proceeding must await the operation's completion.

The `submit/1` function should return `{:ok, ref}` where `ref` can be
awaited. This pattern is similar to `Task.async/1` + `Task.await/1`.
