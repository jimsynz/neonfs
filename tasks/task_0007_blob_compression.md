# Task 0007: Implement Chunk Compression

## Status
Complete

## Phase
1 - Foundation

## Description
Add zstd compression support to the blob store. Compression is applied after hashing (hash is always of original data) and before writing to disk. Decompression happens after reading and before returning data. The compression state must be tracked so reads know whether to decompress.

## Acceptance Criteria
- [x] Add `zstd` dependency to Cargo.toml
- [x] `Compression` enum: `None`, `Zstd { level: i32 }`
- [x] `WriteOptions` struct with `compression: Option<Compression>` field
- [x] `write_chunk_with_options(hash, data, tier, options) -> Result<ChunkInfo>`
- [x] `ChunkInfo` struct: `{ hash, original_size, stored_size, compression }`
- [x] Compressed chunks stored with same path (no filename change)
- [x] `ReadOptions` extended with `decompress: bool` to indicate if decompression needed
- [x] Compression level configurable (zstd levels 1-19, default 3)
- [x] NIF functions accept compression options
- [x] Unit tests for compression roundtrip

## Important Design Note
The Rust layer does NOT track which chunks are compressed - it's stateless storage. The Elixir metadata layer tracks compression state per-chunk and passes the `compressed` flag on reads. This keeps Rust simple.

## Testing Strategy
- Rust unit tests:
  - Write compressed, read with decompress flag returns original data
  - Write uncompressed, read returns original data
  - Compressed stored_size < original_size for compressible data
  - Different compression levels produce different sizes
  - Incompressible data (random bytes) may have stored_size >= original_size
- Elixir tests:
  - Write with compression, read with decompression
  - ChunkInfo returned with correct sizes

## Dependencies
- task_0006_blob_verification

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/Cargo.toml` (add zstd)
- `neonfs_core/native/neonfs_blob/src/compression.rs` (new)
- `neonfs_core/native/neonfs_blob/src/store.rs` (integrate compression)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (update NIFs)
- `neonfs_core/lib/neon_fs/core/blob/native.ex` (update functions)
- `neonfs_core/test/neon_fs/core/blob/native_test.exs`

## Reference
- spec/architecture.md - Chunk File Format (transformation pipeline)
- spec/data-model.md - Volume compression configuration

## Notes
Hash is always computed on original data BEFORE compression. This is critical for deduplication - the same content always produces the same hash regardless of compression settings.
