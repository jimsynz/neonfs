# Task 0073: Encrypted Read Path and Key Version Lookup

## Status
Complete

## Phase
6 - Security

## Description
Modify `ReadOperation` to support reading encrypted chunks. The read pipeline passes decryption parameters (key + nonce) through to the existing BlobStore NIF, which handles read → decrypt → decompress → verify internally (see task 0067). Elixir's responsibility is: check each chunk's `ChunkCrypto` metadata, look up the correct key version from KeyManager, and pass the key and nonce as options to the BlobStore read call. The key version is determined from the chunk's metadata, allowing reads to work correctly even when chunks are encrypted with different key versions (e.g., during key rotation).

## Acceptance Criteria
- [ ] `ReadOperation.read_file/3` checks each chunk's `crypto` field to determine if decryption is needed
- [ ] Key version lookup: read `crypto.key_version` from ChunkMeta, call `KeyManager.get_volume_key/2` to get the correct unwrapped key
- [ ] BlobStore read call passes key and nonce as additional params — Rust handles read → decrypt → decompress → verify internally
- [ ] No separate `decrypt_chunk` NIF call — decryption happens inside the existing store read NIF (single NIF boundary crossing)
- [ ] Mixed key versions within a single file read handled correctly (each chunk may have a different key version during rotation)
- [ ] Authentication failure (tampered ciphertext) returns `{:error, :decryption_failed}` with chunk hash for debugging
- [ ] Unencrypted chunks (crypto == nil) skip decryption entirely (no key/nonce params passed to NIF)
- [ ] Remote chunk reads: encrypted chunks fetched from remote nodes arrive as ciphertext, decrypted locally by passing key/nonce to local BlobStore read
- [ ] ChunkCache integration: cache stores decrypted+decompressed data (plaintext), not ciphertext
- [ ] Telemetry events: `[:neonfs, :read, :decrypt]` with chunk_hash, volume_id, key_version
- [ ] Unit tests: write encrypted file, read back, verify contents match
- [ ] Unit tests: read with mixed key versions (simulate rotation in progress)
- [ ] Unit tests: tampered ciphertext returns decryption error

## Testing Strategy
- ExUnit tests: full round-trip (encrypted write → encrypted read → verify data matches)
- ExUnit tests: read from encrypted volume with chunks at different key versions
- ExUnit test: corrupt stored ciphertext, verify authentication failure detected
- ExUnit tests: unencrypted reads still work unchanged
- Verify ChunkCache stores plaintext (not ciphertext)
- All existing read path tests pass unchanged

## Dependencies
- task_0072 (Encrypted write path — needed to create encrypted test data)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (modify — look up key/nonce from ChunkMeta crypto, pass to BlobStore read call)
- `neonfs_core/test/neon_fs/core/read_operation_test.exs` (modify — add encrypted read tests)

## Reference
- spec/security.md — Read Path with Key Version Lookup
- spec/architecture.md — Read pipeline (reverse of write)
- spec/security.md — Key Versioning (reads during rotation)
- Existing pattern: `BlobStore.read_chunk_with_options/4` already passes verify/decompress options to the NIF

## Notes
The read path must handle the case where a chunk's key version no longer exists (key was deleted after rotation completed). This should return `{:error, :unknown_key_version}` rather than crashing. During key rotation, some chunks will be at the old version and some at the new — reads must transparently handle both by looking up the correct key for each chunk individually. The remote chunk fetch path already returns raw bytes; for encrypted volumes, these are ciphertext bytes that need decryption on the reading node. The key insight is that Elixir never touches ciphertext directly — it reads the nonce from ChunkMeta, fetches the unwrapped key from KeyManager, and passes both to the NIF along with the read request. Rust handles read → decrypt → decompress → verify in a single call.
