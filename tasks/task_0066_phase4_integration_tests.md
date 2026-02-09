# Task 0066: Phase 4 Integration Tests and Full Verification

## Status
Complete

## Phase
4 - Erasure Coding

## Description
End-to-end integration tests for erasure coding and a full verification pass ensuring all existing and new tests pass. This task validates that all Phase 4 components work together in a multi-node cluster via the PeerCluster test infrastructure. It also ensures no regressions in Phases 1-3 by running the complete check suite.

## Acceptance Criteria
- [ ] Integration test: create erasure-coded volume, write a file, read it back, verify contents match
- [ ] Integration test: write file to erasure volume, delete chunks from some nodes (up to parity count), read back — verify degraded read returns correct data
- [ ] Integration test: delete more chunks than parity count, verify `{:error, :insufficient_chunks}` returned
- [ ] Integration test: stripe repair — write file, delete chunks, trigger repair scan, verify stripe returns to `:healthy` state with chunks on new nodes
- [ ] Integration test: small file on erasure volume (single partial stripe) — write 50KB to a 10+4 volume, read back, verify correct data and no padding leaked
- [ ] Integration test: large file spanning multiple stripes — write file > 3× stripe capacity, read at various offsets spanning stripe boundaries, verify all data correct
- [ ] Integration test: GC on erasure-coded volume — create file, delete file, run GC, verify all stripe chunks and stripe metadata cleaned up
- [ ] Integration test: mixed cluster — create both a replicated volume and an erasure-coded volume, write/read to both, verify both work simultaneously without interference
- [ ] Integration test: FUSE handler read/write through erasure-coded volume via PeerCluster (if FUSE handler tests are feasible in CI)
- [ ] Integration test: volume creation via CLI handler with `"erasure:10:4"` durability
- [ ] `mix check --no-retry` passes from repository root (runs checks in all subprojects: neonfs_client, neonfs_core, neonfs_fuse, neonfs_integration)
- [ ] All existing Phase 1-3 tests pass without modification (no regressions)
- [ ] Dialyzer passes with new Stripe type and erasure durability config
- [ ] Credo passes on all new modules
- [ ] Rust `cargo test` passes in neonfs_blob (including new erasure tests)
- [ ] Rust `cargo clippy --all-targets -- -D warnings` passes
- [ ] tasks/README.md updated with Phase 4 task table and dependency graph

## Testing Strategy
- PeerCluster-based integration tests in neonfs_integration (follow existing Phase 2/3 test patterns)
- Write helper functions for common erasure test setup (create erasure volume, write file, verify chunks distributed)
- Test degraded reads by directly deleting chunk data from BlobStore on specific nodes
- Test repair by deleting chunks and calling StripeRepair.scan_stripes/0 + repair_stripe/1
- Test GC by creating/deleting files and calling GC directly
- Full suite: `mix check --no-retry` from root (compiles all, runs all tests, credo, dialyzer, format check)

## Dependencies
- All Phase 4 tasks: task_0057, task_0058, task_0059, task_0060, task_0061, task_0062, task_0063, task_0064, task_0065

## Files to Create/Modify
- `neonfs_integration/test/integration/erasure_coding_test.exs` (new — integration tests for erasure coding)
- `neonfs_integration/test/integration/erasure_repair_test.exs` (new — repair integration tests)
- `tasks/README.md` (modify — add Phase 4 section with task table and dependency graph)

## Reference
- spec/replication.md — Full erasure coding specification
- spec/testing.md — Testing strategy, integration test patterns
- Existing pattern: `neonfs_integration/test/integration/` (follow existing PeerCluster test patterns)
- CLAUDE.md — Phase Completion Requirements section

## Notes
This task is the Phase 4 gate — nothing should be declared complete until `mix check --no-retry` passes from the repository root. The integration tests should exercise the full stack: CLI creates volume → write path chunks and encodes stripes → read path decodes if needed → GC cleans up → repair restores health. The PeerCluster setup should create at least 3 core nodes to exercise stripe placement across multiple nodes. Be careful with test isolation — each test should create its own volume to avoid interference. The degraded read test is the most important: it validates the entire encode → lose chunks → decode round-trip that is the core value proposition of erasure coding.
