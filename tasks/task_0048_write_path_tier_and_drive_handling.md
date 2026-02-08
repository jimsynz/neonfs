# Task 0048: Write Path Tier and Drive Handling

## Status
<<<<<<< HEAD
Complete
=======
Not Started
>>>>>>> bc4d2af (docs: add Phase 3 task specifications (0045-0056))

## Phase
3 - Policies, Tiering, and Compression

## Description
Replace the hardcoded `"hot"` tier in the write path with `volume.tiering.initial_tier`. Use `DriveRegistry.select_drive/1` to pick an appropriate drive within the target tier. Update replication to place replicas on different drives (preferring different nodes, then different drives on the same node). Chunk location metadata must now include `drive_id`.

## Acceptance Criteria
<<<<<<< HEAD
- [x] `WriteOperation` uses `volume.tiering.initial_tier` for tier selection (not hardcoded `"hot"`)
- [x] `WriteOperation` calls `DriveRegistry.select_drive/1` to get a drive_id for the target tier
- [x] `WriteOperation` passes selected `drive_id` to `BlobStore.write_chunk/4`
- [x] Chunk location metadata includes `drive_id` alongside `node` and `tier`
- [x] Replication selects target drives preferring: different node > different drive on same node
- [x] Replication passes `drive_id` to remote `BlobStore.write_chunk/4` calls
- [x] Write fails cleanly with `{:error, :tier_unavailable}` if no drives exist for the requested tier
- [x] Write fails cleanly with `{:error, :no_drives_available}` if all drives in tier are full
- [x] Existing write path tests updated for new tier/drive behaviour
- [x] New tests for tier-based write routing
- [x] New tests for drive-aware replica placement
=======
- [ ] `WriteOperation` uses `volume.tiering.initial_tier` for tier selection (not hardcoded `"hot"`)
- [ ] `WriteOperation` calls `DriveRegistry.select_drive/1` to get a drive_id for the target tier
- [ ] `WriteOperation` passes selected `drive_id` to `BlobStore.write_chunk/4`
- [ ] Chunk location metadata includes `drive_id` alongside `node` and `tier`
- [ ] Replication selects target drives preferring: different node > different drive on same node
- [ ] Replication passes `drive_id` to remote `BlobStore.write_chunk/4` calls
- [ ] Write fails cleanly with `{:error, :tier_unavailable}` if no drives exist for the requested tier
- [ ] Write fails cleanly with `{:error, :no_drives_available}` if all drives in tier are full
- [ ] Existing write path tests updated for new tier/drive behaviour
- [ ] New tests for tier-based write routing
- [ ] New tests for drive-aware replica placement
>>>>>>> bc4d2af (docs: add Phase 3 task specifications (0045-0056))

## Testing Strategy
- Unit tests: mock DriveRegistry to return specific drives, verify correct drive_id passed through
- Unit tests: verify replica placement prefers different nodes, then different drives
- Unit tests: verify error handling for unavailable tiers
- Integration: write to a volume with `initial_tier: :warm`, verify chunk lands on a warm-tier drive

## Dependencies
- task_0045 (volume tiering config)
- task_0047 (drive registry for drive selection)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — tier from volume, drive from registry)
- `neonfs_core/lib/neon_fs/core/replication.ex` (modify — drive-aware replica placement)
- `neonfs_core/lib/neon_fs/core/metadata_state_machine.ex` (modify — chunk locations include drive_id)
- `neonfs_core/test/neon_fs/core/write_operation_test.exs` (modify)
- `neonfs_core/test/neon_fs/core/replication_test.exs` (modify)

## Reference
- spec/architecture.md — Write path
- spec/implementation.md — Phase 3: Tier-aware writes

## Notes
This is the convergence point for tasks 0045 and 0047. The current write path has `BlobStore.write_chunk(data, "hot", ...)` hardcoded at line ~182 of write_operation.ex. The location stored in chunk metadata is currently `%{node: node(), drive_id: "default"}` — this must become `%{node: node(), drive_id: actual_drive_id, tier: tier}`.
