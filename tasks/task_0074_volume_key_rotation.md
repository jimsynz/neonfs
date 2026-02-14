# Task 0074: Volume Key Rotation Worker

## Status
Complete

## Phase
6 - Security

## Description
Implement background volume key rotation that re-encrypts all chunks from the old key version to a new key version. Rotation is a long-running operation: generate new key → update current version → background worker re-encrypts chunks in batches → clean up old key after retention period. Uses the BackgroundWorker infrastructure (from Phase 3) for rate-limited, priority-based scheduling. Rotation state is tracked in VolumeEncryption and exposed via CLI. Re-encryption uses the `store_reencrypt_chunk` NIF (from task 0067) which handles the entire decrypt-old → encrypt-new cycle in Rust without any chunk data crossing the NIF boundary.

## Acceptance Criteria
- [x] New `NeonFS.Core.KeyRotation` module with `start_rotation/1` and `rotation_status/1` functions
- [x] `start_rotation/1` generates new key version via KeyManager, updates volume's `current_key_version`, sets rotation state, starts background worker
- [x] Concurrent rotation detection: only one rotation per volume at a time (check rotation state before starting)
- [x] New `NeonFS.Core.KeyRotation.Worker` module processes chunks in configurable batches (default 1000)
- [x] Worker for each chunk: calls `Native.store_reencrypt_chunk/6` (old key/nonce, new key/nonce) — entire re-encryption happens in Rust, no chunk data crosses the NIF boundary
- [x] Worker generates new nonce per chunk for re-encryption, updates ChunkMeta crypto field with new nonce and key version
- [x] Progress tracking: `rotation.progress` updated after each batch with `{total_chunks, migrated_chunks}`
- [x] Rate limiting: configurable chunks-per-second limit (default 1000) to avoid overwhelming the system
- [x] BackgroundWorker integration: rotation jobs submitted at `:low` priority
- [x] Rotation state persisted in Ra (survives node restart — worker resumes from last progress)
- [x] On completion: rotation state cleared, old key version marked deprecated with timestamp
- [x] Old key retention: deprecated keys kept for configurable period (default 24h) before deletion, to handle in-flight reads
- [x] CLI handler: `handle_rotate_key/1` starts rotation, `handle_rotation_status/1` returns progress
- [x] Telemetry events: `rotation_started`, `rotation_progress`, `rotation_completed`, `rotation_failed`
- [x] Unit tests: start rotation, verify new key version created
- [x] Unit tests: worker re-encrypts chunks correctly (old key → new key)
- [x] Unit tests: concurrent rotation rejected
- [x] Unit tests: progress tracking updates correctly

## Testing Strategy
- ExUnit tests for rotation lifecycle: start → progress → complete
- ExUnit test: write file with key v1, rotate to v2, verify all chunks now at v2
- ExUnit test: read during rotation (mixed key versions) works correctly
- ExUnit test: attempt second rotation while first in progress returns error
- ExUnit test: rotation state persists across simulated restart
- ExUnit test: old key accessible during retention period, deleted after
- Verify BackgroundWorker integration (priority, rate limiting)

## Dependencies
- task_0073 (Encrypted read path — needed for reads during rotation with mixed key versions)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/key_rotation.ex` (new — rotation orchestration)
- `neonfs_core/lib/neon_fs/core/key_rotation/worker.ex` (new — batch re-encryption worker using `store_reencrypt_chunk` NIF)
- `neonfs_core/lib/neon_fs/core/cli/handler.ex` (modify — add rotation handler functions)
- `neonfs_core/test/neon_fs/core/key_rotation_test.exs` (new)

## Reference
- spec/security.md — Volume Key Rotation section
- spec/security.md — Key Rotation configuration (batch_size, rate limit, priority, retention)
- spec/security.md — Reads during rotation
- task_0067 — `store_reencrypt_chunk` NIF specification

## Notes
The `store_reencrypt_chunk` NIF (from task 0067) is the key performance optimisation here: it reads the stored ciphertext, decrypts with the old key, re-encrypts with the new key, and writes back — all in Rust, without any chunk data crossing the NIF boundary. Elixir only needs to pass the old key/nonce and new key/nonce. The chunk hash doesn't change (hash is of original plaintext), only the stored bytes and crypto metadata change. The atomic update must ensure that the new ciphertext and updated ChunkMeta are committed together — the NIF handles the storage atomically (write to temp, rename), and Elixir updates metadata in Ra after the NIF succeeds. For erasure-coded volumes, parity chunks also need re-encryption — they are just chunks like any other from the encryption perspective.
