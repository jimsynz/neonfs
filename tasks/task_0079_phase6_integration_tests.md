# Task 0079: Phase 6 Integration Tests and Full Verification

## Status
Not Started

## Phase
6 - Security

## Description
End-to-end integration tests for all Phase 6 security features and a full verification pass ensuring no regressions. Tests run via PeerCluster in neonfs_integration, exercising encryption, ACLs, audit logging, and key rotation across a multi-node cluster. Also validates that all Phase 1-5 tests pass without modification.

## Acceptance Criteria
- [ ] Integration test: create encrypted volume (`server-side`), write file, read back, verify contents match
- [ ] Integration test: create encrypted volume, write file, read raw chunk from BlobStore, verify it is not plaintext
- [ ] Integration test: create unencrypted and encrypted volumes side by side, write/read both, verify no interference
- [ ] Integration test: key rotation — write file with key v1, rotate to v2, read back (mixed versions), verify correct data
- [ ] Integration test: key rotation — complete rotation, verify all chunks at new version
- [ ] Integration test: volume ACL — grant UID read permission, verify read succeeds and write is denied
- [ ] Integration test: volume ACL — grant UID write permission, verify both read and write succeed
- [ ] Integration test: volume ACL — volume owner UID has full access without explicit grant
- [ ] Integration test: volume ACL — GID-based permission, UID with matching GID can access
- [ ] Integration test: volume ACL — UID 0 (root) bypasses all checks
- [ ] Integration test: file ACL — set POSIX mode 0600, verify owner can read/write but other UIDs cannot
- [ ] Integration test: file ACL — extended ACL entry grants specific UID access beyond mode bits
- [ ] Integration test: directory default ACL — create directory with default ACL, create child file, verify child inherits ACL
- [ ] Integration test: audit log — perform security operations (ACL change, key rotation), query audit log, verify events recorded with correct UIDs
- [ ] Integration test: encrypted erasure-coded volume — write, degraded read with decryption, verify correct data
- [ ] Integration test: CLI volume creation with `--encryption server-side` via handler
- [ ] `mix check --no-retry` passes from repository root
- [ ] All Phase 1-5 tests pass without modification (no regressions)
- [ ] Dialyzer passes with all new types
- [ ] Credo passes on all new modules
- [ ] Rust `cargo test` and `cargo clippy` pass for neonfs_blob (encryption tests)
- [ ] Rust `cargo test` and `cargo clippy` pass for neonfs-cli (new command tests)
- [ ] tasks/README.md updated with Phase 6 task table and dependency graph
- [ ] spec/security.md updated to defer TLS distribution to a later phase

## Testing Strategy
- PeerCluster-based integration tests in neonfs_integration (follow existing patterns)
- Write helper functions for common security test setup (create encrypted volumes, set ACLs)
- Test encryption round-trip across nodes (write on node A, read on node B — both decrypt using shared master key)
- Test authorisation enforcement across nodes (FUSE handler on node A checks ACL via core_call to node B)
- Test key rotation across cluster (rotation runs on one node, reads on other nodes use correct key versions)
- Full suite: `mix check --no-retry` from root

## Dependencies
- All Phase 6 tasks: task_0067 through task_0078

## Files to Create/Modify
- `neonfs_integration/test/integration/encryption_test.exs` (new — encryption integration tests)
- `neonfs_integration/test/integration/acl_test.exs` (new — ACL integration tests)
- `neonfs_integration/test/integration/audit_test.exs` (new — audit log integration tests)
- `neonfs_integration/test/integration/key_rotation_test.exs` (new — key rotation integration tests)
- `tasks/README.md` (modify — add Phase 6 section)
- `spec/security.md` (modify — defer TLS distribution)

## Reference
- spec/security.md — Full security specification
- spec/testing.md — Integration test patterns
- CLAUDE.md — Phase Completion Requirements section
- Existing pattern: `neonfs_integration/test/integration/` (follow PeerCluster patterns)

## Notes
This is the Phase 6 gate — nothing should be declared complete until `mix check --no-retry` passes from the repository root. The encryption + erasure coding test is important: it validates that encryption and erasure coding pipelines compose correctly (encrypt → erasure encode on write, erasure decode → decrypt on read, with degraded mode working through both layers). The key rotation test should verify reads work throughout the rotation process. The ACL tests use numeric UIDs — no username resolution needed in tests.
