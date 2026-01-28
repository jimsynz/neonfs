# Task 0040: Implement Metadata Persistence with DETS

## Status

Not Started

## Phase

1 (Foundation Addendum - must complete before Phase 2)

## Description

The Phase 1 milestone states "data persists across restarts", but currently all metadata (ChunkIndex, FileIndex, VolumeRegistry) is stored only in ETS tables and lost on restart. While chunk data is stored on disk, the indices that map files to chunks are ephemeral.

This task implements DETS-backed persistence for all three metadata tables with atomic write-then-move semantics to prevent corruption during shutdown.

## Acceptance Criteria

- [ ] New `NeonFS.Core.Persistence` module coordinates metadata persistence
- [ ] ChunkIndex persists to `/var/lib/neonfs/meta/chunk_index.dets`
- [ ] FileIndex persists to `/var/lib/neonfs/meta/file_index.dets`
- [ ] VolumeRegistry persists to `/var/lib/neonfs/meta/volume_registry.dets`
- [ ] On startup, DETS tables are loaded into ETS via `:dets.to_ets/2`
- [ ] Periodic snapshots occur every N seconds (configurable, default 30s)
- [ ] Graceful shutdown triggers immediate snapshot before termination
- [ ] Atomic write-then-move: writes go to `.tmp` file, then `File.rename!/2`
- [ ] `:dets.sync/1` called before rename to ensure data is flushed
- [ ] Recovery from crash uses last valid snapshot (no corruption)
- [ ] Tests verify data survives simulated restart
- [ ] Tests verify partial write during crash doesn't corrupt existing data

## Implementation Notes

### Recommended Architecture

```elixir
defmodule NeonFS.Core.Persistence do
  use GenServer

  @snapshot_interval_ms 30_000
  @meta_dir "/var/lib/neonfs/meta"

  # On init: restore all tables from DETS
  # Periodic timer: snapshot_all()
  # On terminate: snapshot_all()

  def atomic_snapshot(ets_table, dets_path) do
    temp_path = "#{dets_path}.tmp"
    {:ok, dets} = :dets.open_file(:temp, type: :set, file: temp_path)
    :ets.to_dets(ets_table, dets)
    :dets.sync(dets)
    :dets.close(dets)
    File.rename!(temp_path, dets_path)
  end
end
```

### Startup Order

1. Persistence GenServer starts first
2. Loads DETS → ETS for all tables
3. ChunkIndex, FileIndex, VolumeRegistry start and find populated ETS tables

### DETS Gotchas

- DETS has no transactions - snapshot is per-table, not atomic across tables
- Max ~2GB per DETS file (sufficient for Phase 1 single-node)
- `:dets.repair/1` can recover some corruption scenarios
- Auto-save option exists but doesn't provide atomic semantics we need

## Testing Strategy

1. **Unit tests**: Verify atomic_snapshot writes correctly
2. **Integration test**: Start system, write files, restart process, verify files exist
3. **Crash test**: Kill process mid-snapshot, verify old snapshot still valid
4. **Property test**: Random operations, snapshot, restore, verify state matches

## Dependencies

- Task 0015 (ChunkIndex) - Complete
- Task 0016 (FileIndex) - Complete
- Task 0017 (VolumeRegistry) - Complete

## Files to Create/Modify

- `neonfs_core/lib/neon_fs/core/persistence.ex` (new)
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (add Persistence to children)
- `neonfs_core/lib/neon_fs/core/application.ex` (ensure meta dir exists)
- `neonfs_core/test/neon_fs/core/persistence_test.exs` (new)
- `neonfs_fuse/test/integration/phase1_test.exs` (add restart test)

## Reference

- spec/implementation.md - Phase 1 milestone
- Erlang DETS documentation
