# Task 0060: Erasure-Coded Write Path

## Status
Complete

## Phase
4 - Erasure Coding

## Description
Modify `WriteOperation` to support erasure-coded volumes. When the volume's durability type is `:erasure`, data chunks are grouped into stripe-sized batches, parity chunks are computed via the Reed-Solomon NIF, all chunks are stored and distributed across nodes, and file metadata references stripes instead of individual chunks. The final stripe may be partial (padded with zeros). On commit, stripe data_bytes sums must equal file size.

## Acceptance Criteria
- [ ] `WriteOperation.write_file/4` branches on `volume.durability.type` — `:replicate` uses existing path unchanged, `:erasure` uses new stripe-based path
- [ ] Data chunks grouped into stripe-sized batches (e.g. 10 chunks per stripe for a 10+4 config)
- [ ] Each chunk within a stripe padded to equal size (the largest chunk's size becomes the stripe's `chunk_size`)
- [ ] `Native.erasure_encode/2` called to compute parity chunks for each complete stripe
- [ ] All chunks (data + parity) stored via BlobStore with appropriate drive/tier selection
- [ ] ChunkMeta entries created with `stripe_id` and `stripe_index` populated
- [ ] Stripe metadata created via StripeIndex for each stripe
- [ ] Final stripe handling: if remaining data chunks don't fill a stripe, pad with zero-filled chunks to complete it, mark stripe as `partial: true`, set `data_bytes` to actual data size and `padded_bytes` to padding size
- [ ] FileMeta created with `stripes` field (not `chunks`) — list of `%{stripe_id: id, byte_range: {start, end}}`
- [ ] On commit: validate that sum of all stripe `data_bytes` equals file size
- [ ] On abort: clean up stripe metadata and all stripe chunks via existing active_write_refs mechanism
- [ ] Write_id added to `active_write_refs` on all stripe chunks (same pattern as replicated writes)
- [ ] Telemetry events for stripe creation (include stripe_id, data_chunks, parity_chunks, partial flag)
- [ ] Unit tests for stripe accumulation with various data sizes (exact multiple of stripe capacity, partial final stripe, single small file)
- [ ] Unit tests for parity generation and chunk storage
- [ ] Unit tests for FileMeta stripe references (byte_range calculation)
- [ ] Unit tests for commit validation (data_bytes sum check)
- [ ] All existing write path tests still pass

## Testing Strategy
- Unit test: write small file (< 1 stripe) — produces single partial stripe with correct data_bytes/padded_bytes
- Unit test: write file exactly filling 2 stripes — no partial stripe, 2 complete stripes
- Unit test: write file filling 2.5 stripes — 2 complete + 1 partial
- Unit test: verify ChunkMeta entries have correct stripe_id and stripe_index values
- Unit test: verify FileMeta.stripes has correct byte_ranges covering entire file
- Unit test: verify commit validation rejects mismatched data_bytes sum
- Unit test: verify abort cleans up stripe and chunk metadata
- Run full `mix test` in neonfs_core

## Dependencies
- task_0057 (Reed-Solomon NIF for parity computation)
- task_0058 (Stripe struct)
- task_0059 (StripeIndex for storing stripe metadata)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — add erasure-coded write path branch, stripe accumulation, parity generation)
- `neonfs_core/lib/neon_fs/core/replication.ex` (modify — stripe chunk distribution to remote nodes)
- `neonfs_core/test/neon_fs/core/write_operation_test.exs` (modify — add erasure-coded write tests)

## Reference
- spec/replication.md — Write Flow (Erasure-Coded Volume), Partial Stripe Handling
- spec/data-model.md — File References for Erasure-Coded Volumes
- spec/architecture.md — Write path design

## Notes
The key difference from replicated writes: instead of replicating each chunk N times, erasure coding creates N unique chunks (data + parity) and distributes one copy of each to different locations. Chunk distribution/placement is handled in task 0062 — for this task, a basic round-robin or local storage approach is sufficient, with placement refinement coming later. The chunk_data NIF still handles the initial content-defined chunking — the stripe layer sits on top of the existing chunking, grouping those chunks into stripes. Compression (if configured on the volume) should be applied to data chunks before parity computation, since parity is computed on the stored form of the data.
