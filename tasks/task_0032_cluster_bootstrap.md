# Task 0032: Implement Cluster Bootstrap

## Status
Not Started

## Phase
2 - Clustering

## Description
Implement the cluster initialisation flow. The first node creates the cluster with `neonfs cluster init`, generating the cluster ID, master key, and bootstrapping the Ra cluster with a single member.

## Acceptance Criteria
- [ ] `NeonFS.Cluster.Init` module for initialisation
- [ ] `cluster_init/1` creates new cluster
- [ ] Generate unique cluster ID
- [ ] Generate cryptographic master key (for future encryption)
- [ ] Persist cluster state to disk
- [ ] Bootstrap single-node Ra cluster
- [ ] CLI command `neonfs cluster init --name <name>`
- [ ] Prevent double-init (error if already initialised)
- [ ] Success message with cluster ID

## Cluster State File
```json
// /var/lib/neonfs/meta/cluster.json
{
  "cluster_id": "clust_8x7k9m2p",
  "cluster_name": "home-lab",
  "created_at": "2024-01-15T10:30:00Z",
  "this_node": {
    "id": "node_3k8x9m2p",
    "name": "neonfs@localhost",
    "joined_at": "2024-01-15T10:30:00Z"
  },
  "known_peers": [],
  "ra_cluster_members": ["neonfs@localhost"]
}
```

## Testing Strategy
- Unit tests:
  - Init on fresh system succeeds
  - Double-init returns error
  - Cluster state file created
  - Ra cluster started
- Integration test:
  - CLI init command works

## Dependencies
- task_0031_ra_integration_setup
- task_0025_cli_commands_implementation

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cluster/init.ex` (new)
- `neonfs_core/lib/neon_fs/cluster/state.ex` (new)
- `neonfs_core/lib/neon_fs/cli/handler.ex` (add cluster_init)
- `neonfs-cli/src/commands/cluster.rs` (add init command)
- `neonfs_core/test/neon_fs/cluster/init_test.exs` (new)

## Reference
- spec/deployment.md - Cluster Initialisation section
- spec/deployment.md - Persistent Cluster State

## Notes
The master key is generated but not used until Phase 5 (Security). Store it securely.
