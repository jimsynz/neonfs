# Task 0009: Implement Chunk Tier Migration

## Status
Not Started

## Phase
1 - Foundation

## Description
Implement the ability to move chunks between storage tiers (hot, warm, cold). Migration reads the chunk from the source tier, writes to the destination tier, verifies the write, then deletes from the source. This is used for tiering policies (promoting hot data, demoting cold data).

## Acceptance Criteria
- [ ] `migrate_chunk(hash, from_tier, to_tier) -> Result<()>` function
- [ ] Migration is atomic: destination written and verified before source deleted
- [ ] Verification enabled during migration (detect corruption during copy)
- [ ] No-op if source and destination tier are the same
- [ ] Error if chunk doesn't exist in source tier
- [ ] NIF function `store_migrate_chunk/4`
- [ ] Unit tests for migration scenarios

## Migration Flow
```rust
fn migrate_chunk(&self, hash: &Hash, from: Tier, to: Tier) -> Result<()> {
    if from == to {
        return Ok(());  // No-op
    }

    // Read with verification
    let data = self.read_chunk_with_options(hash, from, ReadOptions { verify: true })?;

    // Write to destination
    self.write_chunk(hash, &data, to)?;

    // Verify destination (belt and suspenders)
    let _ = self.read_chunk_with_options(hash, to, ReadOptions { verify: true })?;

    // Delete source
    self.delete_chunk(hash, from)?;

    Ok(())
}
```

## Testing Strategy
- Rust unit tests:
  - Migrate hot -> cold, verify chunk in cold, not in hot
  - Migrate cold -> hot, verify chunk in hot, not in cold
  - Migrate to same tier is no-op
  - Migrate nonexistent chunk returns error
  - Verify data integrity after migration
- Elixir tests:
  - Call migrate via NIF, verify tier change

## Dependencies
- task_0006_blob_verification
- task_0005_blob_store_read_write

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/src/store.rs` (add migrate_chunk)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (add NIF)
- `neonfs_core/lib/neon_fs/core/blob/native.ex` (add migrate function)
- `neonfs_core/test/neon_fs/core/blob/native_test.exs`

## Reference
- spec/architecture.md - Tier Migration section
- spec/storage-tiering.md - Promotion and demotion logic

## Notes
Migration is initiated by the Elixir tiering manager based on access patterns and policies. The Rust layer just executes the move safely.
