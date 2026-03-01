# Task 0147: Compression and Hashing Property Tests

## Status
Complete

## Phase
Gap Analysis — M-8 (3/4)

## Description
Add property-based tests for compression round-trips and content-addressed
hashing invariants. These verify that the Rust NIF boundary preserves data
integrity for all possible inputs.

The spec (`spec/testing.md`) identifies compression round-trips and hash
determinism as required property test targets. Also add Rust-side
`proptest` coverage for encryption round-trips in `neonfs_blob`.

## Acceptance Criteria
- [ ] Property: `decompress(compress(data, :lz4)) == data` for arbitrary binaries
- [ ] Property: `decompress(compress(data, :zstd)) == data` for arbitrary binaries
- [ ] Property: `hash(data) == hash(data)` (deterministic) for identical inputs
- [ ] Property: `hash(data_a) != hash(data_b)` when `data_a != data_b` (collision resistance — probabilistic)
- [ ] Property: `encrypt(decrypt(data, key), key) == data` for arbitrary data and keys
- [ ] Property: `encrypt(data, key_a) != encrypt(data, key_b)` when `key_a != key_b` (different keys produce different ciphertext)
- [ ] Property: chunk size after compression is ≤ original size + overhead bound (compression never explodes size beyond a bound)
- [ ] Elixir property tests use `ExUnitProperties` with `binary()` generator
- [ ] Rust property tests use `proptest` crate with `prop::collection::vec(any::<u8>(), 0..65536)`
- [ ] At least 100 iterations per property
- [ ] `mix format` passes
- [ ] `cargo clippy --all-targets -- -D warnings` passes

## Testing Strategy
- Elixir property tests in `neonfs_core/test/neon_fs/core/blob_store_property_test.exs`:
  - Generate arbitrary binaries (0 to 64 KB)
  - Compress with each algorithm, decompress, verify round-trip
  - Hash identical data twice, verify same result
  - Hash different data, verify different results
- Rust property tests in `neonfs_core/native/neonfs_blob/src/` (in relevant module test blocks):
  - `proptest!` for compression round-trips
  - `proptest!` for encryption round-trips (AES-256-GCM)
  - Add `proptest` as a dev-dependency to `Cargo.toml`

## Dependencies
- Task 0145 (stream_data dependency and generators)

## Files to Create/Modify
- `neonfs_core/test/neon_fs/core/blob_store_property_test.exs` (create — Elixir property tests)
- `neonfs_core/native/neonfs_blob/Cargo.toml` (modify — add proptest dev-dependency)
- `neonfs_core/native/neonfs_blob/src/compression.rs` (modify — add proptest tests)
- `neonfs_core/native/neonfs_blob/src/encryption.rs` (modify — add proptest tests)

## Reference
- `spec/testing.md` lines 290–336, 419–487
- `spec/gap-analysis.md` — M-8
- Existing: `neonfs_core/lib/neon_fs/core/blob_store.ex` (Elixir wrapper)
- Existing: `neonfs_core/native/neonfs_blob/src/compression.rs` (Rust implementation)
- Existing: `neonfs_core/native/neonfs_blob/src/encryption.rs` (Rust implementation)

## Notes
For the collision resistance property, we can't prove it for all inputs —
SHA-256 collisions exist in theory. Test that for 1000 randomly generated
distinct binaries, all hashes are distinct. This is a probabilistic test
that should never fail in practice.

For encryption, generate random 32-byte keys using `StreamData.binary(length: 32)`.
The AES-256-GCM nonce is generated internally, so each encryption of the
same plaintext with the same key produces different ciphertext (due to
random nonce). Test decryption round-trip, not ciphertext equality.

The `proptest` Rust tests run via `cargo test` independently of the Elixir
tests. Ensure they're included in the CI pipeline.
