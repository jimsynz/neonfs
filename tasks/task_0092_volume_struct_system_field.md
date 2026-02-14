# Task 0092: Volume Struct `system` Field and VolumeRegistry Guards

## Status
Complete

## Phase
7 - System Volume

## Description
Add a `system` boolean field to the `Volume` struct and implement the corresponding guards in `VolumeRegistry`. The system volume (`_system`) is a special-purpose volume that cannot be deleted, renamed, or created by operators. The `VolumeRegistry` gains `create_system_volume/0` for use during cluster init, `adjust_system_volume_replication/1` for node join/leave, and guards that reject destructive operations on system volumes. The volume list is filtered to exclude system volumes by default.

The system volume uses a deterministic ID derived from the cluster name (not a random UUIDv7) so that every node independently computes the same ID.

## Acceptance Criteria
- [ ] `Volume` struct has `system: false` field (default `false`)
- [ ] `Volume.validate/1` rejects `system: true` on volumes with names other than `_system`
- [ ] `Volume.validate/1` rejects user-supplied `system: true` (only settable by `create_system_volume/0`)
- [ ] `VolumeRegistry.create/2` rejects volume names starting with `_` (reserved namespace)
- [ ] `VolumeRegistry.create_system_volume/0` creates a volume with the properties from the spec: name `_system`, owner `:system`, durability `{:replicate, 1, 1}`, write_ack `:quorum`, repair_priority `:critical`, initial_tier `:hot`, compression zstd level 3, encryption `:none`, `system: true`
- [ ] System volume ID is deterministic: `sha256("neonfs:system_volume:#{cluster_name}")` truncated to 32 hex chars
- [ ] `VolumeRegistry.adjust_system_volume_replication/1` updates the system volume's replication factor to the given cluster size
- [ ] `VolumeRegistry.delete/1` returns `{:error, :system_volume}` for system volumes (by ID or name)
- [ ] `VolumeRegistry.update/2` rejects changes to `system`, `name`, `owner`, `encryption`, and `durability.type` for system volumes
- [ ] `VolumeRegistry.list/1` excludes system volumes by default; `include_system: true` option includes them
- [ ] `VolumeRegistry.get_system_volume/0` convenience function returns the system volume
- [ ] MetadataStateMachine handles `system` field in volume maps (no version bump needed — it's just a new map key in existing volume data)
- [ ] Persistence module handles `system` field in volume snapshots
- [ ] Unit tests for all guard clauses (delete, rename, create reserved name)
- [ ] Unit tests for `create_system_volume/0` and `adjust_system_volume_replication/1`
- [ ] Unit tests for list filtering with and without `include_system`

## Testing Strategy
- ExUnit tests for `create_system_volume/0` (verify all fields match spec, deterministic ID)
- ExUnit tests for guard clauses: attempt delete by ID and by name, attempt rename, attempt create with `_system` name, attempt create with `_reserved` name
- ExUnit tests for `adjust_system_volume_replication/1` (increment, decrement, verify durability.factor changes)
- ExUnit tests for `list/1` filtering (create system volume + user volume, verify default excludes system, `include_system: true` includes it)
- ExUnit tests for `update/2` restrictions on system volumes (reject protected field changes, allow non-protected changes like `repair_priority`)
- Verify existing VolumeRegistry tests pass without modification

## Dependencies
- All Phase 6 tasks complete (current codebase is the starting point)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/core/volume.ex` (modify — add `system: false` field, update validation)
- `neonfs_core/lib/neon_fs/core/volume_registry.ex` (modify — add `create_system_volume/0`, `adjust_system_volume_replication/1`, `get_system_volume/0`, delete/update guards, list filtering, reserved name check)
- `neonfs_core/lib/neon_fs/core/persistence.ex` (modify — handle `system` field in snapshots)
- `neonfs_core/test/neon_fs/core/volume_registry_test.exs` (modify — add system volume tests)

## Reference
- spec/system-volume.md — Properties table, VolumeRegistry Integration section
- spec/data-model.md — Volume type definition
- Existing pattern: `VolumeRegistry.create/2` for normal volume creation flow
- Existing pattern: `Volume.validate/1` for field validation

## Notes
The `system` field does not require a MetadataStateMachine version bump. The MSM stores volumes as maps, and adding a new key (`system: true`) to a volume map is backward-compatible — existing volumes simply won't have the key, which defaults to `false`. This is the same approach used when `encryption` and `metadata_consistency` fields were added in earlier phases.

The reserved name check (`_` prefix) is broader than just `_system` — it reserves the underscore prefix for future internal volumes if needed.
