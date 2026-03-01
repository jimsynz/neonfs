# Task 0141: ChunkAccessTracker DETS Persistence

## Status
Complete

## Phase
Gap Analysis — M-6

## Description
`ChunkAccessTracker` uses an in-memory ETS table that is lost on restart.
This task adds DETS-based persistence so access statistics survive process
and node restarts.

Follow the same persistence pattern used by other GenServers in the
codebase: trap exits in `init/1`, dump ETS to DETS in `terminate/2`, and
restore from DETS on startup. Additionally, add periodic DETS sync (on the
existing decay timer) so stats survive crashes without relying solely on
`terminate/2`.

Also add table size management: cap the number of tracked chunks and evict
stale entries (those with zero activity across multiple decay cycles) to
prevent unbounded growth.

## Acceptance Criteria
- [ ] `Process.flag(:trap_exit, true)` added to `init/1`
- [ ] On init, if a DETS file exists at the configured path, load entries into ETS
- [ ] On `terminate/2`, dump ETS table contents to DETS
- [ ] Periodic DETS sync added to the existing hourly decay timer callback
- [ ] DETS file path configurable via opts, defaulting to `data_dir/chunk_access_tracker.dets`
- [ ] Maximum tracked chunks configurable (default: 1_000_000)
- [ ] When max is exceeded during `record_access/1`, oldest zero-activity entries are evicted
- [ ] Entries with zero hourly and zero daily counts for 3+ consecutive decay cycles are pruned
- [ ] Unit test: stats survive a process restart (stop, start, verify stats present)
- [ ] Unit test: DETS sync occurs on decay timer
- [ ] Unit test: table size is bounded — inserting beyond max triggers pruning
- [ ] Unit test: stale entries are pruned after consecutive zero-activity cycles
- [ ] Existing ChunkAccessTracker tests still pass
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/core/chunk_access_tracker_test.exs` (extend):
  - Start tracker, record some accesses, stop it, restart it, verify stats are restored
  - Trigger decay multiple times, verify DETS is updated
  - Insert entries up to max, verify pruning occurs
  - Use a temporary directory for DETS files in tests

## Dependencies
- None (ChunkAccessTracker already exists)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/chunk_access_tracker.ex` (modify — add persistence and pruning)
- `neonfs_core/test/neon_fs/core/chunk_access_tracker_test.exs` (modify — add persistence tests)

## Reference
- `spec/storage-tiering.md` — access statistics
- `spec/gap-analysis.md` — M-6
- Existing: `neonfs_core/lib/neon_fs/core/chunk_access_tracker.ex`
- Existing pattern: other GenServers with DETS persistence (see `GenServer Persistence Patterns` in CLAUDE.md)

## Notes
DETS has a 2 GB file size limit and is not designed for high write
throughput. Since we only sync periodically (hourly on the decay timer),
this is acceptable — the hot path (`record_access/1`) still writes to ETS
only.

The `staleness_count` field tracks how many consecutive decay cycles an
entry has had zero activity. This is stored as an additional element in the
ETS tuple: `{chunk_hash, hourly, daily, last_accessed_unix, staleness_count}`.
Entries reaching 3 cycles of inactivity are pruned, which handles the
long-tail of chunks that were accessed once and never again.
