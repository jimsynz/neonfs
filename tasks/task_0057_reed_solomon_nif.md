# Task 0057: Reed-Solomon NIF

## Status
Complete

## Phase
4 - Erasure Coding

## Description
Add Reed-Solomon erasure coding to the `neonfs_blob` Rust crate and expose encode/decode operations as NIFs. Uses the `solana-reed-solomon-erasure` crate (actively maintained Solana fork — the original `reed-solomon-erasure` crate is seeking new maintainers and should be avoided). Encoding computes parity shards from data shards; decoding reconstructs missing shards from any K-of-N available shards. Both operations are CPU-intensive and belong in Rust.

## Acceptance Criteria
- [x] `solana-reed-solomon-erasure` added to `neonfs_blob/Cargo.toml`
- [x] New `erasure.rs` module in `neonfs_blob/src/` with `encode` and `decode` functions
- [x] `encode(data_shards, parity_count)` — takes `Vec<Vec<u8>>` of equal-sized data shards plus a parity count, returns `Vec<Vec<u8>>` of parity shards
- [x] `decode(shards_with_indices, data_count, parity_count, shard_size)` — takes `Vec<(usize, Vec<u8>)>` of at least `data_count` available shards (any mix of data+parity), returns `Vec<Vec<u8>>` of all reconstructed data shards
- [x] NIF function `erasure_encode/2` declared in `native.ex` — accepts `(list_of_binaries, parity_count)`, returns `{:ok, list_of_parity_binaries}` or `{:error, reason}`
- [x] NIF function `erasure_decode/4` declared in `native.ex` — accepts `(list_of_{index,binary}_tuples, data_count, parity_count, shard_size)`, returns `{:ok, list_of_data_binaries}` or `{:error, reason}`
- [x] Error cases handled: mismatched shard sizes, insufficient shards for decode, zero shard count
- [x] Rust unit tests for encode (verify parity count matches config)
- [x] Rust proptest: encode random data, drop up to `parity_count` shards, decode recovers originals
- [x] Elixir-side test calling NIF with small data verifies round-trip
- [x] All existing tests still pass

## Testing Strategy
- Rust unit tests in `erasure.rs` for encode/decode with known data
- Rust proptest: random data sizes, random shard drops (up to parity count), verify recovery
- Rust tests for error cases: 0 data shards, 0 parity, unequal shard sizes, too few shards for decode
- Elixir ExUnit test calling `Native.erasure_encode/2` and `Native.erasure_decode/4` through the NIF boundary
- Verify existing `mix test` and `cargo test` pass

## Dependencies
- None (first Phase 4 task, independent)

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/Cargo.toml` (modify — add `solana-reed-solomon-erasure` dependency)
- `neonfs_core/native/neonfs_blob/src/erasure.rs` (new — encode/decode functions)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (modify — add NIF function registrations, `mod erasure`)
- `neonfs_core/lib/neon_fs/core/blob/native.ex` (modify — add `erasure_encode/2` and `erasure_decode/4` NIF declarations)
- `neonfs_core/test/neon_fs/core/blob/native_erasure_test.exs` (new — Elixir-side NIF round-trip tests)

## Reference
- spec/packages.md — Erasure Coding section (recommends `solana-reed-solomon-erasure`)
- spec/replication.md — Erasure coding write/read flows
- spec/architecture.md — NIF boundaries

## Notes
The `solana-reed-solomon-erasure` crate API operates on `Vec<Option<Vec<u8>>>` for reconstruction (where `None` marks missing shards). The NIF interface should accept `{index, binary}` tuples from Elixir and reconstruct the `Option` vector internally, which is a more natural interface for the Elixir side. Shard sizes for Reed-Solomon must be equal — the write path (task 0060) is responsible for padding chunks to equal size before calling encode. These NIFs should run on normal schedulers (not dirty) since Reed-Solomon on typical stripe sizes (10-14 × 256KB shards) completes in single-digit milliseconds.
