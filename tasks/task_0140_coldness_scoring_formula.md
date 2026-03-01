# Task 0140: Coldness Scoring Formula for Eviction

## Status
Complete

## Phase
Gap Analysis — M-5

## Description
Replace the simplistic `stats.daily` sort in `TieringManager.evict_from_tier/3`
with a proper coldness scoring formula that blends access recency, access
frequency, and chunk size.

The spec (`spec/storage-tiering.md`) defines the eviction score as:

```
coldness = -hours_since(last_access) + (access_count_24h * 10)
```

Lower scores indicate colder chunks (evicted first). The current
implementation sorts by `stats.daily` ascending only, ignoring recency
entirely — a chunk accessed once 6 days ago ranks the same as one accessed
once 1 minute ago.

## Acceptance Criteria
- [ ] `NeonFS.Core.TieringManager.ColdnessScore` module created with `score/2` function
- [ ] `score/2` accepts chunk stats map and optional config keyword list
- [ ] Formula: `coldness = -(hours_since_last_access) + (daily_count * frequency_weight)`
- [ ] `hours_since_last_access` calculated from `stats.last_accessed` relative to current time
- [ ] Chunks with `nil` last_accessed treated as maximally cold (lowest possible score)
- [ ] `frequency_weight` defaults to 10, configurable via opts
- [ ] `evict_from_tier/3` in TieringManager uses `ColdnessScore.score/2` for sorting
- [ ] Chunks sorted ascending by score (coldest first) for eviction
- [ ] Unit test: chunk accessed recently scores higher than chunk accessed long ago
- [ ] Unit test: chunk with many daily accesses scores higher than chunk with few
- [ ] Unit test: nil last_accessed produces lowest score
- [ ] Unit test: eviction order matches expected coldness ranking
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/tiering_manager/coldness_score_test.exs`:
  - Test score calculation with various stats combinations
  - Test nil handling for last_accessed
  - Test configurable frequency_weight
  - Test ordering: build a list of chunks with known stats, verify sort order
- Extend existing tiering_manager_test.exs:
  - Verify eviction selects the correct chunks based on the new scoring

## Dependencies
- None (TieringManager and ChunkAccessTracker already exist)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/tiering_manager/coldness_score.ex` (create — scoring module)
- `neonfs_core/lib/neon_fs/core/tiering_manager.ex` (modify — use ColdnessScore in evict_from_tier)
- `neonfs_core/test/neon_fs/core/tiering_manager/coldness_score_test.exs` (create — tests)

## Reference
- `spec/storage-tiering.md` — eviction under pressure
- `spec/gap-analysis.md` — M-5
- Existing: `neonfs_core/lib/neon_fs/core/tiering_manager.ex` (`evict_from_tier/3` at ~line 268)
- Existing: `neonfs_core/lib/neon_fs/core/chunk_access_tracker.ex` (`get_stats/1` return format)

## Notes
The formula from the spec (`-hours_since + daily * 10`) is intentionally
simple. A chunk accessed 1 hour ago with 0 daily hits scores -1. A chunk
accessed 24 hours ago with 3 daily hits scores -24 + 30 = 6. The recently
accessed chunk is colder by this formula, which seems counterintuitive —
but the daily count weighs heavily, so frequently accessed chunks are
protected even if the most recent access was a while ago.

If the formula produces ties (common for chunks with identical stats),
use chunk hash as a stable tiebreaker to keep eviction deterministic.
