# Task 0049: Chunk Access Tracking

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
Implement ETS-based access frequency tracking for chunks with sliding time windows. Track access counts in 1-hour and 24-hour windows. Wire into the read path so every chunk read is recorded. A periodic timer shifts 1-hour counters into the 24-hour aggregate and resets the hourly window. This data feeds the TieringManager's promotion/demotion decisions.

## Acceptance Criteria
- [x] `ChunkAccessTracker` GenServer backed by ETS
- [x] `record_access/1` — increment access count for a chunk hash (called from read path)
- [x] `get_stats/1` — return `%{hourly: count, daily: count, last_accessed: DateTime}` for a chunk
- [x] `list_hot_chunks/2` — chunks exceeding a given access threshold in the hourly window
- [x] `list_cold_chunks/2` — chunks with zero accesses in the last N hours
- [x] Periodic decay: every hour, roll hourly counts into daily aggregate, reset hourly counters
- [x] Daily window decays: reduce daily counts by hourly contribution older than 24 hours
- [x] ETS table uses `{chunk_hash, hourly_count, daily_count, last_accessed_unix}` tuples
- [x] `record_access/1` is fast (direct ETS `:ets.update_counter/3`, no GenServer call)
- [x] GenServer handles periodic cleanup and decay via `Process.send_after/3`
- [x] Integrated into `ChunkFetcher` — calls `record_access/1` after successful read
- [x] Telemetry event for access recording (sampled, not every access)
- [x] All new code has unit tests

## Testing Strategy
- Unit tests for `record_access/1` and `get_stats/1`
- Unit tests for decay logic (advance time, verify hourly rolls into daily)
- Unit tests for `list_hot_chunks/2` and `list_cold_chunks/2` with known access patterns
- Test that ETS direct access is used (no GenServer bottleneck for writes)

## Dependencies
- task_0045 (needs volume tiering config for threshold values)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_access_tracker.ex` (new)
- `neonfs_core/lib/neon_fs/core/chunk_fetcher.ex` (modify — call `record_access/1` on read)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add to supervision tree)
- `neonfs_core/test/neon_fs/core/chunk_access_tracker_test.exs` (new)

## Reference
- spec/implementation.md — Phase 3: Access tracking for tiering
- spec/architecture.md — Tiering policies

## Notes
Performance is critical here — every chunk read hits this code path. Using direct ETS operations (`ets.update_counter/3`) avoids making the GenServer a bottleneck. The GenServer's role is limited to periodic maintenance (decay, cleanup). The decay approach is deliberately simple: a sliding window approximation rather than exact per-access timestamps, which would consume too much memory for large chunk counts.
