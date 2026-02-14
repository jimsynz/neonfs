# Task 0094: Cluster Init Creates System Volume

## Status
Complete

## Phase
7 - System Volume

## Description
Update `NeonFS.Cluster.Init.init_cluster/1` to create the system volume as the first action after Ra bootstrap. The system volume is created with replication factor 1 (single node at init time) and populated with the cluster identity file (`/cluster/identity.json`). This establishes the system volume as available infrastructure before any other cluster operations (CA generation, user volume creation, etc.).

The cluster identity file records the cluster name, init timestamp, and format version — providing a durable record of when and how the cluster was initialised.

## Acceptance Criteria
- [ ] `Cluster.Init.init_cluster/1` calls `VolumeRegistry.create_system_volume/0` after Ra bootstrap succeeds
- [ ] System volume creation happens before master encryption key generation and before any other volume operations
- [ ] `SystemVolume.write("/cluster/identity.json", identity_json)` is called after system volume creation
- [ ] Identity JSON contains: `cluster_name` (string), `initialized_at` (ISO 8601 UTC timestamp), `format_version` (integer, initially 1)
- [ ] If system volume creation fails, `init_cluster/1` returns an error (cluster init is aborted)
- [ ] If identity file write fails, `init_cluster/1` returns an error
- [ ] `init_cluster/1` is idempotent: calling it when a system volume already exists does not create a duplicate
- [ ] Unit tests for the updated init flow
- [ ] Unit tests for identity file content (valid JSON, correct fields)

## Testing Strategy
- ExUnit test: call `init_cluster/1`, verify system volume exists via `VolumeRegistry.get_system_volume/0`
- ExUnit test: verify identity file is readable via `SystemVolume.read("/cluster/identity.json")`
- ExUnit test: decode identity JSON, verify `cluster_name` matches, `initialized_at` is valid ISO 8601, `format_version` is 1
- ExUnit test: verify system volume creation happens before other init steps (mock or observe ordering)
- ExUnit test: verify error propagation if system volume creation fails

## Dependencies
- task_0092 (VolumeRegistry `create_system_volume/0`)
- task_0093 (SystemVolume access API for writing identity file)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cluster/init.ex` (modify — add system volume creation and identity file write)
- `neonfs_core/test/neon_fs/cluster/init_test.exs` (modify — add system volume tests)

## Reference
- spec/system-volume.md — Lifecycle: Creation section
- spec/system-volume.md — Directory Layout (`/cluster/identity.json`)
- Existing pattern: `Cluster.Init.init_cluster/1` current flow (Ra bootstrap, master key generation)

## Notes
The CA generation (steps 3-4 in the spec's init_cluster pseudocode) is a Phase 8 concern and should not be implemented here. This task only adds system volume creation and the identity file. The init flow after this task will be:

1. Initialise Ra cluster
2. Create system volume (NEW)
3. Write cluster identity (NEW)
4. Generate master encryption key (existing)
5. Register first node (existing)

The identity file uses `Jason.encode!/1` for JSON serialisation (Jason is already a dependency).
