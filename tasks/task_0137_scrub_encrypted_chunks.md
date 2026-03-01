# Task 0137: Encrypted Chunk Scrub Support

## Status
Complete

## Phase
Gap Analysis — M-3 (3/3)

## Description
The existing `Job.Runners.Scrub` (task 0129) skips encrypted chunks entirely
because it cannot verify their content hash without decrypting first. This
task wires the scrub runner into the encryption subsystem so that encrypted
chunks can be verified.

The approach: when scrubbing an encrypted chunk, use `KeyManager` to look up
the volume's current key, decrypt via `BlobStore.read_chunk/3` with the
encryption key in read options, then verify the plaintext hash. If the key
version stored with the chunk differs from the current key, use
`KeyManager.get_key_by_version/2` to get the correct key.

This requires the scrub runner to know which volume a chunk belongs to (to
look up the encryption key). The `ChunkIndex` stores chunk metadata which
includes the volume association needed.

## Acceptance Criteria
- [ ] `Job.Runners.Scrub.verify_chunk/1` no longer skips encrypted chunks
- [ ] Encrypted chunks are decrypted using `KeyManager.get_volume_key/2` before hash verification
- [ ] Key version mismatch is handled — uses `KeyManager.get_key_by_version/2` when chunk's key version differs from current
- [ ] Chunks whose volume has no encryption key (key deleted/unavailable) are reported as `:key_unavailable` (not corruption)
- [ ] Telemetry event `[:neonfs, :scrub, :key_unavailable]` emitted when a key is missing
- [ ] Scrub progress tracks encrypted chunks verified separately from unencrypted
- [ ] Unit test: encrypted chunk is decrypted and verified successfully
- [ ] Unit test: encrypted chunk with wrong key version uses correct historical key
- [ ] Unit test: chunk with unavailable key is reported correctly (not as corruption)
- [ ] Existing scrub tests still pass
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/job/runners/scrub_test.exs` (extend):
  - Set up an encrypted volume with a key via `KeyManager`
  - Store an encrypted chunk via the write path
  - Run scrub step, verify the encrypted chunk is checked (not skipped)
  - Mock a key version mismatch scenario, verify historical key lookup
  - Mock a missing key scenario, verify `:key_unavailable` result

## Dependencies
- Task 0129 (Scrub Job Runner) — already complete
- Task 0071 (KeyManager) — already complete

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/job/runners/scrub.ex` (modify — add encrypted chunk support)
- `neonfs_core/test/neon_fs/core/job/runners/scrub_test.exs` (modify — add encrypted chunk tests)

## Reference
- `spec/architecture.md` lines 370–388
- `spec/gap-analysis.md` — M-3
- Existing: `neonfs_core/lib/neon_fs/core/job/runners/scrub.ex` (current skip logic)
- Existing: `neonfs_core/lib/neon_fs/core/key_manager.ex` (key lookup)
- Existing: `neonfs_core/lib/neon_fs/core/blob_store.ex` (read with encryption options)

## Notes
The chunk-to-volume association is available via `ChunkIndex.get/1` which
returns chunk metadata including the volume_id. This is the same lookup
the scrub runner already does for each chunk.

Performance consideration: decrypting every encrypted chunk during a scrub
adds CPU load. Consider adding a `scrub_encrypted: boolean()` flag to the
volume's verification config so operators can opt out of encrypted chunk
scrubbing if the overhead is unacceptable. Default should be `true`.
