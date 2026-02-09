# Task 0063: Stripe-Aware Garbage Collection

## Status
Complete

## Phase
4 - Erasure Coding

## Description
Extend the garbage collection logic to handle erasure-coded chunks and stripe metadata. Files on erasure-coded volumes reference stripes (not chunks directly), so the GC mark phase must resolve file → stripes → chunk hashes to build the referenced set. Orphaned stripe metadata (stripes not referenced by any file) must also be cleaned up. The existing `active_write_refs` mechanism on ChunkMeta protects in-flight writes without modification.

## Acceptance Criteria
- [ ] GC mark phase handles both file types: files with `chunks` (replicated) and files with `stripes` (erasure-coded)
- [ ] For erasure-coded files: walk `file_meta.stripes` → look up each stripe via StripeIndex → collect all chunk hashes from each stripe → add to referenced set
- [ ] Unreferenced committed chunks from erasure-coded stripes deleted (same as replicated chunks)
- [ ] `active_write_refs` on stripe chunks prevents deletion during in-flight writes (existing mechanism, verified working)
- [ ] Orphaned stripe cleanup: after chunk GC, scan StripeIndex for stripes not referenced by any file — delete orphaned stripe metadata
- [ ] GC handles mixed volumes: cluster with both replicated and erasure-coded volumes processed correctly in same GC pass
- [ ] Parity chunks included in GC — they are unreferenced when their stripe's file is deleted
- [ ] Telemetry events for stripe chunk collection and stripe metadata cleanup (count of stripes/chunks collected)
- [ ] Unit tests for GC with erasure-coded files
- [ ] Unit tests for orphaned stripe cleanup
- [ ] Unit tests for mixed-volume GC
- [ ] All existing GC tests still pass

## Testing Strategy
- Unit test: create erasure-coded file, delete it, run GC — verify all stripe chunks (data + parity) deleted
- Unit test: create erasure-coded file, keep it, run GC — verify no chunks deleted
- Unit test: two files sharing a stripe (if content-addressed chunks overlap) — verify chunks kept until both files deleted
- Unit test: orphaned stripe cleanup — create stripe metadata with no file reference, run GC, verify stripe deleted from StripeIndex
- Unit test: active_write_refs protection — create erasure-coded chunks with active write ref, run GC, verify not deleted
- Unit test: mixed volumes — replicated and erasure files coexist, GC handles both correctly
- Run full `mix test` in neonfs_core

## Dependencies
- task_0059 (StripeIndex for stripe metadata lookups)
- task_0060 (write path creates erasure-coded files/stripes to test against)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/garbage_collector.ex` (create or modify — add stripe-aware mark phase, orphaned stripe cleanup)
- `neonfs_core/lib/neon_fs/core/stripe_index.ex` (modify — add `list_all/0` or `list_unreferenced/1` for orphan detection)
- `neonfs_core/test/neon_fs/core/garbage_collector_test.exs` (create or modify — stripe GC tests)

## Reference
- spec/replication.md — Garbage Collection section (mark-and-sweep with active write protection)
- spec/replication.md — Reference-Counted Chunks, active_write_refs

## Notes
The spec states GC uses mark-and-sweep rather than reference counting for committed chunks, because distributed reference counting has failure modes. The same approach applies to erasure-coded stripes. The mark phase is the key change: instead of just walking `file.chunks`, we also walk `file.stripes → stripe.chunks`. Parity chunks are only referenced indirectly through stripes — they have no direct file reference — so they must be discovered via the stripe lookup path. The orphaned stripe cleanup is a second pass after chunk GC: any stripe in StripeIndex that isn't referenced by any file's `stripes` list can be deleted. This handles the case where a file is deleted but its stripe metadata lingers.
