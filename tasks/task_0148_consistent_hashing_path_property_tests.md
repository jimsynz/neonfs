# Task 0148: Consistent Hashing and Path Operation Property Tests

## Status
Complete

## Phase
Gap Analysis — M-8 (4/4)

## Description
Add property-based tests for the consistent hashing ring and FileIndex path
operations. The spec (`spec/testing.md`) identifies consistent hash
invariants (determinism, minimal disruption on node change) and path
handling as required property test targets.

## Acceptance Criteria
- [ ] Property: `MetadataRing.get_segment/1` is deterministic (same key always maps to same segment)
- [ ] Property: adding a node moves at most `1/n` of keys (minimal disruption)
- [ ] Property: removing a node moves at most `1/n` of keys (minimal disruption)
- [ ] Property: all segments have at least one assigned node (no orphan segments)
- [ ] Property: `FileIndex.resolve_path/1` is idempotent (`resolve(resolve(path)) == resolve(path)`)
- [ ] Property: parent of `"/a/b/c"` is always `"/a/b"` (path parent relationship holds)
- [ ] Property: root path `"/"` has no parent
- [ ] Property: path with trailing slash is equivalent to path without (normalisation)
- [ ] Property: directory listing for a parent contains all direct children (no orphans)
- [ ] All tests use `ExUnitProperties` with appropriate generators
- [ ] At least 100 iterations per property
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Property tests in `neonfs_core/test/neon_fs/core/metadata_ring_property_test.exs`:
  - Generate random keys, verify deterministic segment assignment
  - Generate random node sets, add/remove one node, count key movements
  - Generate random ring configs, verify no orphan segments
- Property tests in `neonfs_core/test/neon_fs/core/file_index_property_test.exs`:
  - Generate random valid paths, verify resolution properties
  - Generate parent-child path pairs, verify relationship
  - Generate directory trees, verify listing completeness

## Dependencies
- Task 0145 (stream_data dependency and generators — file_path/0 generator)

## Files to Create/Modify
- `neonfs_core/test/neon_fs/core/metadata_ring_property_test.exs` (create)
- `neonfs_core/test/neon_fs/core/file_index_property_test.exs` (create)

## Reference
- `spec/testing.md` lines 290–336
- `spec/gap-analysis.md` — M-8
- Existing: `neonfs_core/lib/neon_fs/core/metadata_ring.ex`
- Existing: `neonfs_core/lib/neon_fs/core/file_index.ex`
- Existing: `neonfs_core/test/neon_fs/core/metadata_ring_test.exs` (existing property tests to extend)

## Notes
The minimal disruption property for consistent hashing is the key invariant:
when a node is added or removed, the fraction of keys that change their
assignment should be approximately `1/n` where `n` is the number of nodes.
Allow a tolerance of 2x (at most `2/n` of keys move) to account for
virtual node distribution.

For path generators, construct paths by joining 1–5 random alphanumeric
segments with `/`. Avoid generating paths with special characters (`.`,
`..`, null bytes) — those are edge cases for unit tests, not property tests.
The property tests verify the core invariants hold across the valid input
space.

The existing `metadata_ring_test.exs` already has some property tests using
`ExUnitProperties`. The new tests can be in a separate file or merged into
the existing file — separate files are preferred to keep test organisation
clean.
