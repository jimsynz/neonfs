# Task 0072: Encrypted Write Path

## Status
Complete

## Phase
6 - Security

## Description
Modify `WriteOperation` to support server-side encryption for volumes with `encryption.mode == :server_side`. The write pipeline passes encryption parameters (key + nonce) through to the existing BlobStore NIF, which handles compress → encrypt → write internally (see task 0067). Elixir's responsibility is: check if the volume is encrypted, fetch the current key from KeyManager, generate a unique nonce per chunk, and pass those as options to the BlobStore write call. Encryption metadata (nonce, key version) is stored in ChunkMeta via a new `crypto` field.

## Acceptance Criteria
- [ ] `WriteOperation.write_file/4` checks `Volume.encrypted?/1` and branches accordingly
- [ ] For encrypted volumes: fetch current key via `KeyManager.get_current_key/1`, returns `{unwrapped_key, key_version}`
- [ ] Unique 96-bit random nonce generated per chunk via `:crypto.strong_rand_bytes(12)`
- [ ] BlobStore write call passes key and nonce as additional params — Rust handles compress → encrypt → store internally
- [ ] No separate `encrypt_chunk` NIF call — encryption happens inside the existing store write NIF (single NIF boundary crossing)
- [ ] ChunkMeta struct extended with `crypto` field (nil for unencrypted, `%ChunkCrypto{}` for encrypted)
- [ ] ChunkMeta `stored_size` reflects ciphertext size (larger than compressed size due to 16-byte GCM auth tag)
- [ ] ChunkCrypto populated with: `algorithm: :aes_256_gcm`, `nonce: <<12 bytes>>`, `key_version: N`
- [ ] MetadataStateMachine stores crypto metadata alongside chunk metadata
- [ ] Unencrypted volumes continue to work exactly as before (no key/nonce params passed to NIF)
- [ ] Abort/cleanup handles encrypted chunks correctly (delete ciphertext from blob store)
- [ ] Telemetry events: `[:neonfs, :write, :encrypt]` with chunk_hash, volume_id, key_version
- [ ] Unit tests: write to encrypted volume, verify ChunkMeta has crypto field populated
- [ ] Unit tests: write to unencrypted volume, verify ChunkMeta crypto is nil
- [ ] Unit tests: verify stored bytes are not plaintext (encrypted data differs from input)

## Testing Strategy
- ExUnit tests writing to encrypted vs unencrypted volumes
- Verify ChunkMeta crypto fields are correctly populated
- Read raw bytes from BlobStore (without decryption params) and confirm they don't match plaintext
- Verify stored_size accounts for auth tag overhead (16 bytes for GCM)
- Test abort path cleans up encrypted chunks
- All existing write path tests pass unchanged (unencrypted volumes)

## Dependencies
- task_0067 (Encryption support in BlobStore NIFs — extended write NIF with key/nonce params)
- task_0071 (KeyManager — `get_current_key/1`)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — pass key/nonce to BlobStore write call for encrypted volumes)
- `neonfs_core/lib/neon_fs/core/chunk_meta.ex` (modify — add `crypto` field)
- `neonfs_core/lib/neon_fs/core/metadata_state_machine.ex` (modify — store crypto in chunk metadata)
- `neonfs_core/test/neon_fs/core/write_operation_test.exs` (modify — add encrypted write tests)

## Reference
- spec/security.md — Mode 2: Server-side encryption
- spec/architecture.md — Transformation pipeline (hash → compress → encrypt → store)
- spec/security.md — Key Versioning (chunk metadata tracks key version)
- Existing pattern: `BlobStore.write_chunk/4` already passes compression options to the NIF

## Notes
The hash is always computed on original (uncompressed, unencrypted) data to preserve deduplication detection. With server-side encryption using per-volume keys and random nonces, two identical chunks produce different ciphertext, so storage-level dedup won't save space for encrypted volumes — but logical dedup (same hash = same content) still works for metadata. The write path should check if a chunk hash already exists and skip re-encryption if the chunk is already stored with the current key version. The key insight is that Elixir never touches ciphertext — it passes the plaintext data and encryption params to the NIF, and Rust handles compress → encrypt → write in a single call.
