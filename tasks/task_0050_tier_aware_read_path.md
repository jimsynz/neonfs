# Task 0050: Tier-Aware Read Path

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
Replace the current `Enum.shuffle` in `ChunkFetcher.sort_locations_by_preference/1` with a score-based replica selection that considers drive tier, locality, and drive power state. Local SSDs are preferred over remote SSDs, which are preferred over active HDDs, with standby HDDs ranked lowest (as they require spin-up time).

## Acceptance Criteria
- [x] `sort_locations_by_preference/1` replaced with score-based sorting
- [x] Scoring hierarchy: local SSD (0) > remote SSD (10) > local HDD active (20) > remote HDD active (30) > HDD standby (50)
- [x] Tie-breaking within same score uses random selection (avoid hotspotting)
- [x] Drive state (active/standby) queried from DriveRegistry
- [x] Drive tier queried from DriveRegistry
- [x] Locality determined by comparing location node with `node()`
- [x] Locations with unknown drives gracefully handled (assigned worst score + warning log)
- [x] Score calculation is a pure function (testable without GenServer)
- [x] `ChunkFetcher` uses new scoring in fetch path
- [x] Existing read path tests pass
- [x] New unit tests for scoring function with various drive configurations
- [x] New tests for fallback behaviour when DriveRegistry unavailable

## Testing Strategy
- Unit tests for score calculation with mocked drive info
- Test all scoring tiers (local SSD, remote SSD, local HDD, remote HDD, standby)
- Test tie-breaking randomness
- Test graceful degradation when drive info unavailable
- Integration: read from cluster with mixed tier drives, verify preference ordering

## Dependencies
- task_0047 (needs DriveRegistry for drive state and tier info)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (modify — replace shuffle with scoring)
- `neonfs_core/test/neon_fs/core/chunk_fetcher_test.exs` (modify — scoring tests)

## Reference
- spec/architecture.md — Read path optimisation
- spec/implementation.md — Phase 3: Tier-aware reads

## Notes
The scoring values are deliberately coarse (gaps of 10) to allow future fine-tuning without reshuffling the hierarchy. The key insight is that spinning up a cold HDD can take 5-15 seconds, making standby drives dramatically worse than active ones. The scoring function should be extracted as a pure function for testability, taking a list of `%{node: node, drive_id: id, tier: tier, state: state}` and returning a sorted list.
