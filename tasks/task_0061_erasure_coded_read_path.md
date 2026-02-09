# Task 0061: Erasure-Coded Read Path

## Status
Complete

## Phase
4 - Erasure Coding

## Description
Modify `ReadOperation` to support reading from erasure-coded files. When a file has `stripes` set (not nil), reads are routed through stripe-based logic that determines which stripes overlap the requested byte range, assesses each stripe's health, and either reads data chunks directly (healthy) or reconstructs via Reed-Solomon decoding (degraded). Partial stripes must respect their `data_bytes` boundary to avoid returning padding.

## Acceptance Criteria
- [ ] `ReadOperation.read_file/3` branches on `file_meta.stripes` — `nil` uses existing chunk-based path, non-nil uses stripe-based path
- [ ] Stripe-based read calculates which stripes overlap the requested `(offset, length)` range using stripe byte_ranges
- [ ] For each relevant stripe, calculates the offset within the stripe and bytes needed
- [ ] Stripe state calculated at read time: `:healthy` (all N chunks available), `:degraded` (>= K data_chunks available), `:critical` (< K available)
- [ ] Healthy stripe read: fetches only the needed data chunks directly (indices calculated from offset/chunk_size), no decoding
- [ ] Degraded stripe read: fetches any K available chunks (data or parity), calls `Native.erasure_decode/4`, extracts requested byte range from reconstructed data
- [ ] Critical stripe: returns `{:error, :insufficient_chunks}`
- [ ] Partial stripe reads respect `data_bytes` boundary — reads beyond `data_bytes` return empty binary, reads spanning the boundary are truncated
- [ ] Multi-stripe reads: data from multiple stripes concatenated correctly for contiguous result
- [ ] Reads beyond file size truncated to `file_meta.size`
- [ ] Reconstructed stripe data cached via ChunkCache when `volume.caching.reconstructed_stripes` is true
- [ ] Chunk availability checked via ChunkIndex (local) and ChunkFetcher (remote)
- [ ] Telemetry events for stripe reads (include stripe_id, state, degraded chunk count)
- [ ] Unit tests for healthy read (single stripe, multi-stripe)
- [ ] Unit tests for degraded read (simulate missing chunks)
- [ ] Unit tests for critical error
- [ ] Unit tests for partial stripe reads
- [ ] Unit tests for reads spanning stripe boundaries
- [ ] All existing read path tests still pass

## Testing Strategy
- Unit test: write then read small file (single partial stripe) — verify data matches
- Unit test: write then read file spanning 3 stripes — verify offset/length combinations across stripe boundaries
- Unit test: healthy stripe read — all chunks present, no decoding, correct data returned
- Unit test: degraded stripe read — delete `parity_chunks` data chunks, verify reconstruction returns correct data
- Unit test: critical stripe read — delete more than `parity_chunks` chunks, verify `{:error, :insufficient_chunks}`
- Unit test: partial stripe — read up to data_bytes returns data, read beyond returns empty
- Unit test: read with offset starting mid-stripe — correct byte range extraction
- Unit test: caching — verify reconstructed data cached when configured, not cached when disabled
- Run full `mix test` in neonfs_core

## Dependencies
- task_0057 (Reed-Solomon NIF for degraded read reconstruction)
- task_0059 (StripeIndex to look up stripe metadata)
- task_0060 (write path must exist to create test data)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (modify — add stripe-based read path, stripe state calculation, healthy/degraded/critical handlers)
- `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (modify — may need helpers for batch chunk availability checking)
- `neonfs_core/test/neon_fs/core/read_operation_test.exs` (modify — add erasure-coded read tests)

## Reference
- spec/replication.md — Reading from Erasure-Coded Files, Reading from Stripes (Healthy and Degraded), Stripe State Calculation, Edge Cases table
- spec/data-model.md — File references for erasure-coded volumes

## Notes
The spec provides detailed pseudocode for `read_stripe/3`, `read_stripe_healthy/3`, `read_stripe_degraded/3`, and `calculate_stripe_state/1` in `replication.md` — follow these closely. The key insight for healthy reads is that you only need to fetch the specific data chunks that contain the requested byte range, not all data chunks. For degraded reads, you need any K chunks (mix of data and parity) to reconstruct all data chunks. The reconstruction via NIF returns all data chunks concatenated — extract the requested byte range from that. Caching reconstructed data is important because degraded reads are expensive (cross-node fetches + CPU for Reed-Solomon decode).
