# Task 0047: Drive Registry and Tier Discovery

## Status
<<<<<<< HEAD
Complete
=======
Not Started
>>>>>>> bc4d2af (docs: add Phase 3 task specifications (0045-0056))

## Phase
3 - Policies, Tiering, and Compression

## Description
Create a `DriveRegistry` GenServer that tracks all drives across the cluster, and a `Topology` module for querying tier availability. Each node reads its drive configuration from the application environment and registers drives with capacity, tier assignment, and state. The registry provides drive selection for writes (least-used drive within a tier) and cluster-wide tier discovery.

## Acceptance Criteria
<<<<<<< HEAD
- [x] `Drive` struct with fields: `id`, `node`, `path`, `tier`, `capacity_bytes`, `used_bytes`, `state` (`:active`/`:standby`), `power_management` (boolean), `idle_timeout` (seconds)
- [x] `DriveRegistry` GenServer backed by ETS
- [x] `DriveRegistry.register_drive/1` — register a local drive
- [x] `DriveRegistry.list_drives/0` — all drives across cluster
- [x] `DriveRegistry.drives_for_tier/1` — drives assigned to a specific tier
- [x] `DriveRegistry.drives_for_node/1` — drives on a specific node
- [x] `DriveRegistry.select_drive/1` — pick least-used drive in a tier (by `used_bytes / capacity_bytes` ratio)
- [x] `DriveRegistry.update_usage/2` — update `used_bytes` for a drive
- [x] `DriveRegistry.update_state/2` — update drive state (active/standby)
- [x] `Topology.available_tiers/0` — tiers available on local node
- [x] `Topology.cluster_tiers/0` — tiers available across the cluster
- [x] `Topology.validate_tier_available/1` — returns `:ok` or `{:error, :tier_unavailable}`
- [x] Drives registered on startup from application config
- [x] Cross-node drive info gathered via periodic RPC or ServiceRegistry
- [x] Telemetry events for drive registration and selection
=======
- [ ] `Drive` struct with fields: `id`, `node`, `path`, `tier`, `capacity_bytes`, `used_bytes`, `state` (`:active`/`:standby`), `power_management` (boolean), `idle_timeout` (seconds)
- [ ] `DriveRegistry` GenServer backed by ETS
- [ ] `DriveRegistry.register_drive/1` — register a local drive
- [ ] `DriveRegistry.list_drives/0` — all drives across cluster
- [ ] `DriveRegistry.drives_for_tier/1` — drives assigned to a specific tier
- [ ] `DriveRegistry.drives_for_node/1` — drives on a specific node
- [ ] `DriveRegistry.select_drive/1` — pick least-used drive in a tier (by `used_bytes / capacity_bytes` ratio)
- [ ] `DriveRegistry.update_usage/2` — update `used_bytes` for a drive
- [ ] `DriveRegistry.update_state/2` — update drive state (active/standby)
- [ ] `Topology.available_tiers/0` — tiers available on local node
- [ ] `Topology.cluster_tiers/0` — tiers available across the cluster
- [ ] `Topology.validate_tier_available/1` — returns `:ok` or `{:error, :tier_unavailable}`
- [ ] Drives registered on startup from application config
- [ ] Cross-node drive info gathered via periodic RPC or ServiceRegistry
- [ ] Telemetry events for drive registration and selection
>>>>>>> bc4d2af (docs: add Phase 3 task specifications (0045-0056))

## Testing Strategy
- Unit tests for DriveRegistry with multiple drives in different tiers
- Unit tests for `select_drive/1` — verify least-used selection
- Unit tests for Topology queries
- Test with drives in multiple tiers on one node
- Test `validate_tier_available/1` with missing tiers

## Dependencies
- task_0046 (needs BlobStore to support multiple drives)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/drive.ex` (new — Drive struct)
- `neonfs_core/lib/neon_fs/core/drive_registry.ex` (new — GenServer + ETS)
- `neonfs_core/lib/neon_fs/core/topology.ex` (new — tier availability queries)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add DriveRegistry to supervision tree)
- `neonfs_core/test/neon_fs/core/drive_registry_test.exs` (new)
- `neonfs_core/test/neon_fs/core/topology_test.exs` (new)

## Reference
- spec/architecture.md — Storage topology
- spec/implementation.md — Phase 3: Tier discovery

## Notes
The DriveRegistry is local-first: each node knows its own drives authoritatively. Cluster-wide view is assembled by querying remote nodes' registries. This avoids putting drive state into Ra consensus (too volatile, too frequent updates). The `select_drive/1` function is the main entry point for the write path — it replaces the current implicit "write to default drive" behaviour.
