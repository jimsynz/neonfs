# Task 0006: Implement Chunk Verification on Read

## Status
Not Started

## Phase
1 - Foundation

## Description
Add optional hash verification when reading chunks. The read operation can verify that the data matches the expected hash, detecting corruption. This is controlled by a `verify` flag passed to read operations.

## Acceptance Criteria
- [ ] `ReadOptions` struct with `verify: bool` field
- [ ] `read_chunk_with_options(hash, tier, options) -> Result<Vec<u8>>`
- [ ] When verify=true, compute SHA-256 of read data and compare to expected hash
- [ ] Return `CorruptChunk { expected, actual }` error if mismatch
- [ ] `read_chunk` convenience function defaults to verify=false (caller's choice)
- [ ] NIF `store_read_chunk/4` accepts options map with `verify` key
- [ ] Unit tests for verification behaviour

## Error Type
```rust
#[derive(Debug, thiserror::Error)]
pub enum Error {
    #[error("corrupt chunk: expected {expected}, got {actual}")]
    CorruptChunk { expected: Hash, actual: Hash },
    // ... other errors
}
```

## Testing Strategy
- Rust unit tests:
  - Read with verify=true on valid chunk succeeds
  - Read with verify=false on valid chunk succeeds
  - Manually corrupt a chunk file, read with verify=true fails with CorruptChunk
  - Read with verify=false on corrupt chunk returns corrupt data (no error)
- Elixir tests:
  - Read with verify option
  - Error tuple returned for corrupt chunk

## Dependencies
- task_0005_blob_store_read_write

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/src/store.rs` (add verification)
- `neonfs_core/native/neonfs_blob/src/error.rs` (add CorruptChunk variant)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (update NIF)
- `neonfs_core/lib/neon_fs/core/blob/native.ex` (update read function)
- `neonfs_core/test/neon_fs/core/blob/native_test.exs`

## Reference
- spec/architecture.md - Integrity Verification section
- spec/data-model.md - Volume verification configuration

## Notes
Verification is optional because it adds CPU overhead. The Elixir layer will decide whether to verify based on volume configuration (always, never, or sampling).
