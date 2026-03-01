# Task 0180: Chunk Parsing Fuzz Target

## Status
Blocked — depends on 0179 (fuzz scaffolding); needs CI design

## Phase
Gap Analysis — H-3 (2/5)

## Description
Create a fuzz target that exercises the chunk parsing and storage paths in
`neonfs_blob`. This target feeds arbitrary bytes into chunk deserialization,
hash verification, and path derivation functions to find crashes, panics,
or logic errors in parsing code.

## Acceptance Criteria
- [ ] `neonfs_core/native/neonfs_blob/fuzz/fuzz_targets/chunk_parsing.rs` created
- [ ] Fuzz target feeds data to `ChunkHeader::from_bytes()` (or equivalent deserialisation)
- [ ] Fuzz target feeds data to `chunk_path_from_hash()` (path derivation from arbitrary hash bytes)
- [ ] Fuzz target exercises `verify_chunk()` with arbitrary data and hash
- [ ] All parsing functions handle malformed input without panicking
- [ ] Seed corpus created at `fuzz/corpus/chunk_parsing/` with:
  - Valid chunk header bytes (extracted from a real stored chunk)
  - Valid SHA-256 hash (32 bytes)
  - Empty input
  - Maximum-length input (64 KB)
- [ ] 5-minute local fuzz run completes without finding crashes
- [ ] Placeholder target from task 0179 removed (or kept as example)
- [ ] `cargo fuzz list` shows `chunk_parsing` target

## Testing Strategy
- Run `cargo fuzz run chunk_parsing -- -max_total_time=300` (5 minutes)
- Verify no crashes or panics found
- If crashes are found, fix the underlying code and re-run

## Dependencies
- Task 0179 (neonfs_blob cargo-fuzz scaffolding)

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/fuzz/fuzz_targets/chunk_parsing.rs` (create — fuzz target)
- `neonfs_core/native/neonfs_blob/fuzz/corpus/chunk_parsing/` (create — seed corpus files)
- `neonfs_core/native/neonfs_blob/fuzz/fuzz_targets/placeholder.rs` (delete — no longer needed)

## Reference
- `spec/gap-analysis.md` — H-3
- `spec/testing.md` lines 430–450 (chunk parsing fuzz target)
- Existing: `neonfs_core/native/neonfs_blob/src/` (chunk storage, hashing, paths)

## Notes
The seed corpus should contain representative valid inputs to give the
fuzzer a starting point. Extract seed data by:
1. Writing a small Rust test that stores a chunk and captures the raw bytes
2. Saving those bytes as a file in the corpus directory

The fuzz target should be structured to exercise multiple code paths:

```rust
fuzz_target!(|data: &[u8]| {
    // Try parsing as chunk header
    let _ = ChunkHeader::from_bytes(data);

    // Try using as hash for path derivation
    if data.len() >= 32 {
        let _ = chunk_path_from_hash(&data[..32]);
    }

    // Try verification with data split into hash + content
    if data.len() > 32 {
        let (hash, content) = data.split_at(32);
        let _ = verify_chunk(hash, content);
    }
});
```

Any panics found by the fuzzer indicate missing bounds checks or unwrap
calls on fallible operations. Fix these in the library code, not in the
fuzz target.
