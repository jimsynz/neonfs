# Task 0067: Encryption Support in BlobStore NIFs

## Status
Complete

## Phase
6 - Security

## Description
Add AES-256-GCM authenticated encryption to the `neonfs_blob` Rust crate, integrated into the existing store read/write pipeline. Rather than exposing separate encrypt/decrypt NIFs (which would require copying chunk data across the NIF boundary multiple times), encryption is added as an optional step in the existing `WriteOptions` and `ReadOptions` — the same pattern used for compression. The Rust store handles the full pipeline internally: compress → encrypt → write on the write path, and read → decrypt → decompress → verify on the read path. A dedicated `store_reencrypt_chunk` NIF is also provided for key rotation (decrypt with old key → encrypt with new key without crossing the NIF boundary).

## Acceptance Criteria
- [x] New `encryption.rs` module in `neonfs_blob/src/` with internal `encrypt` and `decrypt` functions (not exposed as separate NIFs)
- [x] `encrypt(data, key, nonce)` — takes plaintext bytes, 256-bit key, and 96-bit nonce; returns ciphertext with appended 128-bit authentication tag
- [x] `decrypt(ciphertext_with_tag, key, nonce)` — takes ciphertext (with appended tag), 256-bit key, and 96-bit nonce; returns plaintext or error
- [x] `WriteOptions` extended with `encryption: Option<EncryptionParams>` where `EncryptionParams` holds key (32 bytes) and nonce (12 bytes)
- [x] `ReadOptions` extended with `encryption: Option<EncryptionParams>` (same struct, key + nonce for decryption)
- [x] Write pipeline order in `BlobStore::write_chunk_with_options`: compress → encrypt → write to disk
- [x] Read pipeline order in `BlobStore::read_chunk_with_options`: read from disk → decrypt → decompress → verify hash
- [x] `store_write_chunk_compressed` NIF extended with optional `key_binary` and `nonce_binary` params (empty binary = no encryption)
- [x] `store_read_chunk_with_options` NIF extended with optional `key_binary` and `nonce_binary` params (empty binary = no decryption)
- [x] New `store_reencrypt_chunk` NIF: reads chunk, decrypts with old key/nonce, re-encrypts with new key/nonce, writes back atomically — returns `{:ok, {stored_size}}` or `{:error, reason}`
- [x] `ChunkInfo` return from write NIF extended with encryption metadata (algorithm string, nonce echo)
- [x] Error cases handled: wrong key size (must be 32 bytes), wrong nonce size (must be 12 bytes), authentication failure on decrypt, empty data
- [x] Rust unit tests for encrypt/decrypt round-trip with known test vectors
- [x] Rust proptest: encrypt random data with random key, decrypt recovers original
- [x] Rust test: decrypt with wrong key returns authentication error
- [x] Rust test: tampered ciphertext returns authentication error
- [x] Rust test: write with encryption, read with decryption, verify round-trip through store
- [x] Rust test: write with compression + encryption, read with decryption + decompression, verify round-trip
- [x] Rust test: reencrypt_chunk produces valid ciphertext under new key
- [x] Elixir-side test calling extended NIFs verifies round-trip through NIF boundary
- [x] All existing tests still pass (no encryption params = existing behaviour)

## Testing Strategy
- Rust unit tests in `encryption.rs` with NIST test vectors for AES-256-GCM
- Rust proptest: random data sizes (0 to 1MB), random keys, verify round-trip
- Rust tests for error cases: wrong key, wrong nonce, tampered ciphertext, truncated ciphertext
- Rust integration tests through `BlobStore`: write with encryption options, read back with decryption options, verify data matches
- Rust tests for combined compression + encryption pipeline
- Rust tests for `reencrypt_chunk` (old key → new key, verify data integrity)
- Elixir ExUnit tests calling the extended NIFs through the NIF boundary
- Verify existing `mix test` and `cargo test` pass (no encryption params = backwards compatible)

## Dependencies
- None (first Phase 5 task, independent)

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/src/encryption.rs` (new — internal encrypt/decrypt functions using `ring`)
- `neonfs_core/native/neonfs_blob/src/store.rs` (modify — extend `WriteOptions`, `ReadOptions` with encryption fields; integrate encrypt/decrypt into read/write pipelines; add `reencrypt_chunk` method)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (modify — extend existing NIF signatures with optional key/nonce params; add `store_reencrypt_chunk` NIF; `mod encryption`)
- `neonfs_core/lib/neon_fs/core/blob/native.ex` (modify — update NIF declarations with optional key/nonce params; add `store_reencrypt_chunk/6`)
- `neonfs_core/test/neon_fs/core/blob/native_encryption_test.exs` (new — Elixir-side NIF round-trip tests)

## Reference
- spec/security.md — Data Encryption section (AES-256-GCM)
- spec/architecture.md — NIF boundaries, transformation pipeline
- spec/security.md — Key Hierarchy section
- Existing pattern: `WriteOptions`/`ReadOptions` in `store.rs`, `store_write_chunk_compressed` and `store_read_chunk_with_options` in `lib.rs`

## Notes
The `ring` crate is already in `Cargo.toml` for SHA-256 hashing, so AES-256-GCM comes at no additional dependency cost (`ring::aead::AES_256_GCM`). The key design decision here is that encryption is part of the store pipeline, not a separate NIF — this avoids copying chunk data (64KB–256KB) across the NIF boundary multiple times. An empty binary for key/nonce means "no encryption", preserving full backwards compatibility with existing callers. The `store_reencrypt_chunk` NIF is critical for key rotation performance: the entire decrypt-old → encrypt-new cycle happens in Rust without any data crossing the NIF boundary. The nonce must be unique per chunk encryption — the caller (Elixir write path) generates nonces and passes them in. These NIFs should run on normal schedulers since AES-256-GCM on typical chunk sizes completes in sub-millisecond time with AES-NI hardware acceleration.
