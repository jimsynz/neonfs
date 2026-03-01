# Task 0159: Read Verification Failure Handling and Repair

## Status
Complete

## Phase
Gap Analysis — M-14

## Description
Wire read-time chunk hash verification into the repair pipeline. Currently,
when `should_verify_on_read?/1` returns true and the chunk fails hash
verification, the error simply propagates up as a read failure. No repair
is attempted, no telemetry is emitted, and no corruption is logged.

This task adds:
1. Telemetry emission on verification failure
2. Automatic retry from a remote replica when local verification fails
3. Background repair submission for the corrupt local copy
4. Structured error logging with chunk and volume context

## Acceptance Criteria
- [x] Telemetry event `[:neonfs, :read_operation, :verification_failed]` emitted on hash mismatch
- [x] Telemetry metadata includes: `chunk_hash`, `volume_id`, `drive_id`, `node`
- [x] On local verification failure, ReadOperation retries from a remote replica
- [x] Remote replica is selected via `ChunkFetcher.fetch_chunk/2` with `exclude_nodes: [node()]`
- [x] If remote copy verifies, it is served to the caller (transparent recovery)
- [x] If remote copy also fails, return `{:error, :chunk_corrupted}` to caller
- [x] On successful remote fetch, submit background repair via `BackgroundWorker` to fix local copy
- [x] Repair job replaces the corrupt local chunk with the verified remote copy
- [x] Logger.error emitted with structured metadata on corruption detection
- [x] Counter tracked via `:telemetry` for per-node verification failures
- [x] Unit test: local verification failure triggers remote fetch
- [x] Unit test: successful remote fetch returns data to caller
- [x] Unit test: background repair is submitted after successful remote recovery
- [x] Unit test: both local and remote failure returns :chunk_corrupted
- [x] Unit test: telemetry event emitted on verification failure
- [x] Existing read path tests still pass
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/read_operation_test.exs` (extend):
  - Mock BlobStore to return a chunk that fails hash verification on first call
  - Mock ChunkFetcher to return a valid chunk from remote
  - Verify the caller receives valid data (transparent recovery)
  - Verify BackgroundWorker.submit is called for repair
  - Verify telemetry events via Telemetry.attach
  - Test the case where remote also fails

## Dependencies
- None (ReadOperation, ChunkFetcher, BackgroundWorker already exist)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (modify — add repair logic on verification failure)
- `neonfs_core/test/neon_fs/core/read_operation_test.exs` (modify — add verification failure tests)

## Reference
- `spec/architecture.md` lines 353–368
- `spec/gap-analysis.md` — M-14
- Existing: `neonfs_core/lib/neon_fs/core/read_operation.ex` (`should_verify_on_read?/1`)
- Existing: `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (remote fetch)
- Existing: `neonfs_core/lib/neon_fs/core/background_worker.ex` (repair submission)

## Notes
The repair flow is:
1. Read chunk locally → hash verification fails
2. Emit telemetry + log error
3. Fetch chunk from remote replica (excluding current node)
4. Verify remote copy's hash
5. If valid: return to caller + submit background repair of local copy
6. If invalid: return `{:error, :chunk_corrupted}` (all replicas corrupt)

The background repair job should use the same `BackgroundWorker` priority
system as read repair (`:high` priority). The repair replaces the local
corrupt chunk by writing the verified remote copy to the local BlobStore.

This creates a self-healing read path: corruption is detected at read time,
transparently recovered from a replica, and the local copy is fixed in the
background. Combined with periodic scrubbing (M-3), this provides
comprehensive data integrity protection.
