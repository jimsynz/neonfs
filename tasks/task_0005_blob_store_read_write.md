# Task 0005: Implement Blob Store Read/Write Operations

## Status
Not Started

## Phase
1 - Foundation

## Description
Implement the core blob store operations: writing chunks to disk and reading them back. Writes must be atomic (write to temp file, then rename) to prevent partial chunks. Reads verify the file exists and return the raw bytes.

## Acceptance Criteria
- [ ] Store module at `neonfs_blob/src/store.rs`
- [ ] `BlobStore` struct holding base_dir and prefix_depth configuration
- [ ] `BlobStore::new(base_dir, config) -> Result<BlobStore>`
- [ ] `write_chunk(hash, data, tier) -> Result<()>` with atomic write via rename
- [ ] `read_chunk(hash, tier) -> Result<Vec<u8>>`
- [ ] `delete_chunk(hash, tier) -> Result<()>`
- [ ] `chunk_exists(hash, tier) -> bool`
- [ ] Proper error types: `ChunkNotFound`, `IoError`, `CorruptChunk`
- [ ] Atomic writes: write to `.tmp.{random}` then rename to final path
- [ ] NIF functions exported: `store_open/2`, `store_write_chunk/4`, `store_read_chunk/3`, `store_delete_chunk/3`
- [ ] BlobStore wrapped as Rustler Resource for NIF

## Atomic Write Pattern
```rust
fn write_chunk(&self, hash: &Hash, data: &[u8], tier: Tier) -> Result<()> {
    let final_path = self.chunk_path(hash, tier);
    let temp_path = format!("{}.tmp.{}", final_path.display(), random_id());

    ensure_parent_dirs(&final_path)?;

    let mut file = File::create(&temp_path)?;
    file.write_all(data)?;
    file.sync_all()?;  // Ensure durability

    fs::rename(&temp_path, &final_path)?;
    Ok(())
}
```

## Testing Strategy
- Rust unit tests (using tempdir):
  - Write then read returns same data
  - Read nonexistent chunk returns ChunkNotFound
  - Delete removes chunk from disk
  - chunk_exists returns correct boolean
  - Concurrent writes to same hash (idempotent due to content-addressing)
  - Write creates parent directories
- Elixir tests:
  - Open store, write chunk, read chunk, verify data matches
  - Read nonexistent returns error tuple
  - Resource cleanup when store goes out of scope

## Dependencies
- task_0004_blob_directory_layout

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/Cargo.toml` (add rand for temp file names)
- `neonfs_core/native/neonfs_blob/src/store.rs` (new)
- `neonfs_core/native/neonfs_blob/src/error.rs` (new - error types)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (add modules, NIF exports, Resource)
- `neonfs_core/lib/neon_fs/core/blob/native.ex` (add store functions)
- `neonfs_core/test/neon_fs/core/blob/native_test.exs`

## Reference
- spec/architecture.md - Atomic Writes section
- spec/architecture.md - On-Disk Blob Storage

## Notes
This task implements raw chunk storage without compression or verification. Those features are added in subsequent tasks. The BlobStore is wrapped as a Rustler Resource so Elixir can hold a reference to it.
