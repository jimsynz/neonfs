# Task 0125: Truncate Removes Chunk References Beyond New Size

## Status
Complete

## Phase
Gap Analysis — QW-7

## Description
The FUSE `setattr` handler accepts size changes (truncate) but only updates the `size` field in `FileMeta`. The file's chunk list is not trimmed, so reading the file after truncation still returns the old data from chunks beyond the new size. The orphaned chunks also won't be collected by GC because they're still referenced.

Implement proper truncation: trim the chunk list to match the new size and update `FileIndex` accordingly.

## Acceptance Criteria
- [ ] `setattr` with a smaller `size` trims the file's `chunks` list to only include chunks covering bytes 0..new_size
- [ ] Partial chunks at the truncation boundary are handled: if the new size falls mid-chunk, the chunk remains referenced but `size` is set correctly so reads stop at the right offset
- [ ] `setattr` with `size: 0` clears the chunks list entirely
- [ ] `setattr` with a size equal to or larger than current size is a no-op on chunks (only updates the size field)
- [ ] Truncated files read back correctly — no stale data beyond the new size
- [ ] `modified_at` and `changed_at` are updated on truncation
- [ ] Orphaned chunks (no longer referenced by any file) become eligible for GC
- [ ] Erasure-coded files: truncation trims the `stripes` list and adjusts the last stripe's `byte_range`
- [ ] Existing tests pass
- [ ] `mix format` passes

## Testing Strategy
- Unit tests:
  - Write a 1 MB file (multiple chunks), truncate to 512 bytes, read back → exactly 512 bytes
  - Truncate to 0, read back → empty
  - Truncate to current size → no change to chunks list
  - Verify old chunks are no longer in the file's chunk list after truncation
- Integration test:
  - Write, truncate, read across FUSE handler → correct data

## Dependencies
- task_0120 (changed_at field) — truncation should update `changed_at`

## Files to Create/Modify
- `neonfs_fuse/lib/neon_fs/fuse/handler.ex` (modify — `build_setattr_updates` size handling)
- `neonfs_core/lib/neon_fs/core/file_index.ex` (modify if needed — ensure `update` handles chunk list trimming)
- `neonfs_fuse/test/neon_fs/fuse/handler_test.exs` (modify — add truncation tests)

## Reference
- `spec/architecture.md` — file operations
- `spec/gap-analysis.md` — QW-7
- Existing: `build_setattr_updates/1` in handler.ex lines 586–613

## Notes
The tricky part is determining which chunks to keep. Files store an ordered list of chunk hashes. Each chunk has a known size (from `ChunkIndex`). Walk the chunk list, accumulating byte offsets, and stop when the accumulated size reaches or exceeds the new file size.

For erasure-coded files with stripe references, the logic is similar but operates on `stripes` — trim to the stripe that contains the truncation point and adjust its `byte_range.end`.

Truncation to a larger size (extending) should NOT allocate zero-filled chunks — just update the size field. Reads beyond the current chunk data but within `size` should return zeros (sparse file semantics).
