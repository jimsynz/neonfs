# Task 0129: Scrub Job Runner

## Status
Complete

## Phase
Gap Analysis — M-1 (4/6)

## Description
Create `NeonFS.Core.Job.Runners.Scrub`, a job runner that performs integrity
verification of stored chunks by walking the local `ChunkIndex`, reading each
chunk from `BlobStore`, and verifying its SHA-256 hash matches the stored hash.

The runner processes chunks in batches (like `KeyRotation`), making it
resumable after restart. It tracks progress via `job.progress` and records
corruption in `job.state`.

Corrupted chunks are reported via telemetry and logged. Repair submission
(via `BackgroundWorker`) is a follow-up concern (see M-3 for scheduled
scrubbing and repair integration).

## Acceptance Criteria
- [ ] `Job.Runners.Scrub` implements the `Job.Runner` behaviour
- [ ] `label/0` returns `"scrub"`
- [ ] `step/1` processes one batch of chunks per call
- [ ] Each chunk is read from BlobStore and its SHA-256 hash verified against `ChunkMeta.hash`
- [ ] Supports optional `volume_id` in `job.params` to restrict scrubbing to one volume's chunks
- [ ] Without `volume_id`, scrubs all committed chunks on the local node
- [ ] Progress tracks total chunks, completed chunks, and corruption count
- [ ] Corrupted chunks are recorded in `job.state.corrupted` (list of hashes)
- [ ] Telemetry event `[:neonfs, :scrub, :corruption_detected]` emitted per corrupt chunk with `%{hash: hash, node: node()}`
- [ ] Telemetry event `[:neonfs, :scrub, :complete]` emitted on completion with `%{total: n, corrupted: n}`
- [ ] Returns `{:continue, job}` while batches remain, `{:complete, job}` when done
- [ ] Batch size configurable via `Application.get_env(:neonfs_core, :scrub_batch_size, 500)`
- [ ] Unit test: scrub completes with all chunks valid
- [ ] Unit test: scrub detects a corrupted chunk
- [ ] `mix format` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/job/runners/scrub_test.exs`:
  - Store a few chunks via BlobStore, run scrub, verify all pass (progress shows 0 corrupted)
  - Store a chunk, tamper with its data on disk, run scrub, verify corruption detected
  - With volume_id filter, verify only that volume's chunks are checked
  - Verify telemetry events are emitted

## Dependencies
- None (ChunkIndex, BlobStore, JobTracker already exist)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/job/runners/scrub.ex` (create — scrub runner)
- `neonfs_core/test/neon_fs/core/job/runners/scrub_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — M-1, M-3
- `spec/architecture.md` lines 370–388 (integrity verification)
- Existing pattern: `neonfs_core/lib/neon_fs/core/job/runners/key_rotation.ex` (batch processing)
- Existing: `neonfs_core/lib/neon_fs/core/chunk_index.ex` (chunk enumeration)
- Existing: `neonfs_core/lib/neon_fs/core/blob_store.ex` (chunk reading)

## Notes
The scrub runner only verifies chunks that have replicas on the local node
(`ChunkMeta.locations` filtered by `node() == loc.node`). Remote chunks are
not fetched — each node scrubs its own local data.

To verify a chunk: read the raw bytes from BlobStore (bypassing decompression
and decryption — we verify the stored form), compute SHA-256, compare against
`ChunkMeta.hash`. For encrypted volumes, the stored bytes include the
ciphertext, so the hash should match the content-addressed hash of the
**original** data. Check how `ChunkMeta.hash` is computed during writes to
ensure the verification uses the same input.

The `job.state.corrupted` list should be bounded — if corruption is widespread,
cap at 1000 entries and note the overflow in progress description.

Repair is out of scope for this task. The runner detects and reports corruption
but does not trigger read repair or stripe reconstruction. That integration is
tracked in M-3 and M-14.
