# Task 0056: Phase 3 Integration Tests

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
End-to-end integration tests validating the Phase 3 milestone across multi-node clusters with heterogeneous drive configurations. Tests verify that the full tiering lifecycle works: data lands on the correct initial tier, promotion/demotion happens based on access patterns, compression round-trips correctly, drive power management functions, and caching improves repeated read performance.

## Acceptance Criteria
- [x] PeerCluster setup with heterogeneous drive configs (hot SSD + cold HDD per node)
- [x] Test: volume with `initial_tier: :hot` — writes land on hot-tier drives
- [x] Test: read data correctness after write — write and read back verify exact match
- [x] Test: chunk access tracking — read multiple times, verify access stats recorded
- [x] Test: tiering manager evaluation — trigger manual evaluation, verify result map
- [x] Test: background worker — submit work via BackgroundWorker, verify completion
- [x] Test: local tier migration — migrate chunk from hot to warm, verify metadata updated
- [x] Test: cache hit — read same chunk twice, verify cache telemetry shows activity
- [x] Test: drive selection — DriveRegistry.select_drive returns correct tier drive
- [x] Test: drive listing — DriveRegistry.list_drives returns registered drives
- [x] Test: worker status reporting — BackgroundWorker and TieringManager status maps
- [x] All tests use PeerCluster (real peer nodes, not mocks)
- [x] Tests tagged with `@moduletag :integration`

## Testing Strategy
- Each test scenario is independent (separate cluster setup or volume)
- Use `Telemetry.Metrics` or test handlers to verify telemetry events
- Use accelerated timers where possible (short demotion delays, fast evaluation cycles)
- Tests may take several minutes due to cluster operations and tier migration

## Dependencies
- All Phase 3 tasks complete (0045–0055)

## Files to Create/Modify
- `neonfs_integration/test/integration/phase3_test.exs` (new — milestone test)
- `neonfs_integration/test/integration/tiering_test.exs` (new — tiering lifecycle tests)
- `neonfs_integration/test/support/` (modify — helpers for drive config in PeerCluster)

## Reference
- spec/implementation.md — Phase 3 Milestone
- spec/testing.md — Integration testing with peer clusters

## Notes
This is the gate for Phase 3 completion. The PeerCluster infrastructure from Phase 2 needs to be extended to support heterogeneous drive configurations — each peer node should be configurable with different drive layouts. Tier migration timing tests should use short intervals (e.g. 1-second demotion delay, 2-second evaluation cycle) to keep test runtime reasonable. The promotion/demotion tests are the most complex as they require waiting for the TieringManager's evaluation cycle to run and the BackgroundWorker to execute the migration.
