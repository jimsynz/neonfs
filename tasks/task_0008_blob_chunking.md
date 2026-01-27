# Task 0008: Implement FastCDC Content-Defined Chunking

## Status
Not Started

## Phase
1 - Foundation

## Description
Implement content-defined chunking using the FastCDC algorithm. This splits data into variable-sized chunks based on content, enabling deduplication even when data shifts within files. For small data, fixed-size or single-chunk strategies are used.

## Acceptance Criteria
- [ ] Add `fastcdc` dependency to Cargo.toml
- [ ] Chunking module at `neonfs_blob/src/chunking.rs`
- [ ] `ChunkStrategy` enum: `Single`, `Fixed { size: usize }`, `FastCDC { min, avg, max }`
- [ ] `chunk_data(data, strategy) -> Vec<ChunkResult>` function
- [ ] `ChunkResult` struct: `{ data: Vec<u8>, hash: Hash, offset: usize, size: usize }`
- [ ] Strategy selection based on data size:
  - < 64KB: Single chunk
  - 64KB - 1MB: Fixed 256KB blocks
  - > 1MB: FastCDC with 256KB avg, 64KB min, 1MB max
- [ ] `auto_strategy(data_len) -> ChunkStrategy` helper
- [ ] NIF function `chunk_data/2` returning list of chunk info maps
- [ ] Unit tests for each strategy

## FastCDC Configuration
```rust
// Default CDC parameters
const CDC_MIN_SIZE: usize = 64 * 1024;      // 64 KB
const CDC_AVG_SIZE: usize = 256 * 1024;     // 256 KB
const CDC_MAX_SIZE: usize = 1024 * 1024;    // 1 MB
```

## Testing Strategy
- Rust unit tests:
  - Small data (< 64KB) produces single chunk
  - Medium data produces fixed-size chunks
  - Large data produces variable-size CDC chunks
  - All chunks concatenated equal original data
  - Shifting content: insert bytes at start, verify most chunk boundaries unchanged
  - Empty data produces empty chunk list
- Elixir tests:
  - chunk_data/2 returns list of maps with expected keys
  - Verify chunk hashes are correct

## Dependencies
- task_0003_blob_hash_module

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/Cargo.toml` (add fastcdc)
- `neonfs_core/native/neonfs_blob/src/chunking.rs` (new)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (add module, NIF)
- `neonfs_core/lib/neon_fs/core/blob/native.ex` (add chunk_data/2)
- `neonfs_core/test/neon_fs/core/blob/native_test.exs`

## Reference
- spec/data-model.md - Chunking Strategy table
- FastCDC paper: https://www.usenix.org/conference/atc16/technical-sessions/presentation/xia

## Notes
The chunking module doesn't write chunks to disk - it just splits data and computes hashes. The caller (Elixir) decides what to do with the chunks (store, replicate, etc.).
