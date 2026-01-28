# Task 0004: Implement Blob Store Directory Layout

## Status
Complete

## Phase
1 - Foundation

## Description
Implement the directory sharding structure for blob storage. Chunks are stored in a content-addressed directory hierarchy using the first characters of the hash as directory prefixes. This prevents any single directory from containing too many entries.

## Acceptance Criteria
- [x] Path module at `neonfs_blob/src/path.rs`
- [x] `Tier` enum with `Hot`, `Warm`, `Cold` variants (derives Serialize/Deserialize for NIF)
- [x] `chunk_path(base_dir, hash, tier, prefix_depth) -> PathBuf` function
- [x] Configurable prefix depth (1=256 dirs, 2=65K dirs)
- [x] Path format: `{base_dir}/blobs/{tier}/{hash[0:2]}/{hash[2:4]}/{hash}`
- [x] Tier converts to lowercase string in path ("hot", "warm", "cold")
- [x] `ensure_parent_dirs(path)` creates parent directories atomically
- [x] Unit tests for path generation with different prefix depths and tiers

## Example Paths
```
hash: "abcd7c9e..."
prefix_depth: 2
tier: Hot
Result: /var/lib/neonfs/blobs/hot/ab/cd/abcd7c9e...

hash: "abcd7c9e..."
prefix_depth: 1
tier: Cold
Result: /var/lib/neonfs/blobs/cold/ab/abcd7c9e...
```

## Testing Strategy
- Rust unit tests:
  - Path generation for each tier (hot, warm, cold)
  - Path generation with prefix_depth 1 and 2
  - Verify path components are lowercase hex
  - Test ensure_parent_dirs with temp directories
  - Verify idempotence of ensure_parent_dirs

## Dependencies
- task_0003_blob_hash_module

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/src/path.rs` (new)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (add module)

## Reference
- spec/architecture.md - Directory Layout section
- spec/implementation.md - storage.hash_prefix_depth configuration

## Notes
This task only implements path calculation and directory creation. Actual chunk file I/O is in the next task. The Tier enum will be used extensively across the crate.
