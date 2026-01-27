# Task 0003: Implement SHA-256 Hashing Module

## Status
Not Started

## Phase
1 - Foundation

## Description
Implement the SHA-256 hashing module in the neonfs_blob crate. This module computes content hashes for chunks, which form the basis of content-addressed storage. The hash is always computed on the original (uncompressed, unencrypted) data to enable deduplication.

## Acceptance Criteria
- [ ] Add `sha2` dependency to Cargo.toml
- [ ] Hash module at `neonfs_blob/src/hash.rs`
- [ ] `Hash` type wrapping `[u8; 32]` with Display, Debug, Clone, PartialEq, Eq, Hash traits
- [ ] `sha256(data: &[u8]) -> Hash` function
- [ ] `Hash::to_hex()` method returning lowercase hex string
- [ ] `Hash::from_hex()` method parsing hex string with error handling
- [ ] `Hash::from_bytes()` method from raw bytes
- [ ] `Hash::as_bytes()` method returning `&[u8; 32]`
- [ ] Proper error types for invalid hex input
- [ ] NIF function `compute_hash/1` exported to Elixir returning binary
- [ ] Rust unit tests for all functions
- [ ] Property test: `from_hex(hash.to_hex()) == hash`

## Cargo.toml Addition
```toml
[dependencies]
sha2 = "0.10"
hex = "0.4"  # For hex encoding/decoding
thiserror = "1.0"  # For error types
```

## Testing Strategy
- Rust unit tests:
  - Known SHA-256 test vectors (e.g., empty string -> "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855")
  - "hello world" -> known hash
  - Hex roundtrip test
  - Invalid hex string error handling (odd length, invalid chars)
- Elixir tests:
  - `compute_hash/1` returns correct hash for known inputs
  - Result matches Elixir's `:crypto.hash(:sha256, data)`
  - Handles empty binary
  - Handles large binary (1MB+)

## Dependencies
- task_0001_blob_crate_scaffolding
- task_0002_check_exs_rust_integration

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/Cargo.toml` (add dependencies)
- `neonfs_core/native/neonfs_blob/src/hash.rs` (new)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (add module, export NIF)
- `neonfs_core/lib/neon_fs/core/blob/native.ex` (add compute_hash/1)
- `neonfs_core/test/neon_fs/core/blob/native_test.exs`

## Reference
- spec/data-model.md - Chunk hash computed on original data
- spec/architecture.md - Content verification on read

## Notes
The hash is returned to Elixir as a 32-byte binary, not a hex string. Elixir code can convert to hex if needed for display. This keeps the NIF interface efficient.
