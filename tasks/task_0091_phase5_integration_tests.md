# Task 0091: Phase 5 Integration Tests and Full Verification

## Status
Not Started

## Phase
5 - Metadata Tiering

## Description
End-to-end integration tests for the tiered metadata architecture and a full verification pass ensuring all existing and new tests pass. This task validates that all Phase 5 components work together in a multi-node cluster via the PeerCluster test infrastructure. It also ensures no regressions in Phases 1-4 by running the complete check suite.

## Acceptance Criteria
- [ ] Integration test: multi-node quorum write/read consistency (3-node PeerCluster, write to one node, read from another, verify data matches)
- [ ] Integration test: node failure during quorum operation (kill one node, verify quorum of 2 still works for reads and writes)
- [ ] Integration test: directory listing via DirectoryEntry (create files in a directory, list_dir, verify all children returned)
- [ ] Integration test: cross-segment file creation atomicity (create file = FileMeta + DirectoryEntry, verify both written or neither)
- [ ] Integration test: crash recovery — simulate crash mid-create (intent exists but operation incomplete), restart node, verify intent resolved correctly
- [ ] Integration test: clock skew detection (manually offset a node's reported time, verify warning/quarantine behaviour)
- [ ] Integration test: read repair (write stale data to one replica directly, read via quorum, verify stale replica repaired in background)
- [ ] Integration test: concurrent writer detection (two nodes attempt to write same file simultaneously, verify one succeeds and one gets conflict error)
- [ ] Integration test: full write → read → delete cycle through FUSE handler on multi-node cluster
- [ ] Integration test: erasure-coded volume works with quorum-based metadata (write/read on erasure volume, verify stripes distributed correctly)
- [ ] Integration test: mixed volume types — replicated and erasure-coded volumes coexist, both work correctly
- [ ] `mix check --no-retry` passes from repository root (all subprojects: neonfs_client, neonfs_core, neonfs_fuse, neonfs_integration)
- [ ] All existing Phase 1-4 tests pass without modification (no regressions)
- [ ] Dialyzer passes with all new types (HLC, MetadataRing, DirectoryEntry, Intent, etc.)
- [ ] Credo passes on all new modules
- [ ] Rust `cargo test` passes in neonfs_blob (including new metadata NIF code)
- [ ] Rust `cargo clippy --all-targets -- -D warnings` passes
- [ ] tasks/README.md updated with Phase 5 metadata section and dependency graph

## Testing Strategy
- PeerCluster-based integration tests in neonfs_integration (follow existing Phase 2-4 test patterns)
- Write helper functions for common metadata test setup (create quorum-backed indexes, verify metadata on specific nodes)
- Test node failure by stopping a peer node process, then verifying operations still succeed with remaining quorum
- Test crash recovery by writing an incomplete intent to Ra, then restarting and verifying recovery
- Test read repair by writing directly to MetadataStore on one node (bypassing quorum), then reading via quorum and checking the stale replica gets repaired
- Full suite: `mix check --no-retry` from root

## Dependencies
- All Phase 5 tasks: task_0080, task_0081, task_0082, task_0083, task_0084, task_0085, task_0086, task_0087, task_0088, task_0089, task_0090

## Files to Create/Modify
- `neonfs_integration/test/integration/metadata_tiering_test.exs` (new — quorum, directory, intent tests)
- `neonfs_integration/test/integration/quorum_test.exs` (new — quorum consistency, node failure, read repair)
- `tasks/README.md` (modify — add Phase 5 metadata tiering section)

## Reference
- spec/metadata.md — full specification
- spec/testing.md — Testing strategy, integration test patterns
- Existing pattern: `neonfs_integration/test/integration/` (follow existing PeerCluster test patterns)
- CLAUDE.md — Phase Completion Requirements section

## Notes
This task is the Phase 5 gate — nothing should be declared complete until `mix check --no-retry` passes from the repository root. The quorum consistency test is the most important: it validates that data written on one node is readable from another via quorum. The crash recovery test is the second most important: it validates that the IntentLog correctly handles incomplete operations after a node restart. Be careful with test isolation — each test should create its own volume and directory hierarchy to avoid interference. The PeerCluster setup should create at least 3 core nodes to exercise full quorum behaviour (N=3, R=2, W=2).
