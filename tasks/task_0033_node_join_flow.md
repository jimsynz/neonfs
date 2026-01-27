# Task 0033: Implement Node Join Flow

## Status
Not Started

## Phase
2 - Clustering

## Description
Implement the flow for additional nodes to join an existing cluster. This uses invite tokens for authentication and adds the new node to the Ra consensus group.

## Acceptance Criteria
- [ ] `NeonFS.Cluster.Invite` module for token management
- [ ] `create_invite/1` generates time-limited invite token
- [ ] `validate_invite/1` verifies token is valid
- [ ] `NeonFS.Cluster.Join` module for join flow
- [ ] `join_cluster/2` connects to existing node and joins
- [ ] Add new node to Ra cluster membership
- [ ] Persist cluster state on joining node
- [ ] CLI: `neonfs cluster create-invite --expires 1h`
- [ ] CLI: `neonfs cluster join --token <token> --via <node>`
- [ ] Joined node syncs existing metadata

## Invite Token Format
```
nfs_inv_<random>_<expiry_timestamp>_<signature>
```

## Join Flow
1. New node connects to existing node via `--via` address
2. Sends invite token for validation
3. Existing node validates token, adds new node to Ra
4. New node receives cluster info (ID, peers, Ra members)
5. New node persists cluster state locally
6. New node joins Ra cluster
7. Ra syncs state to new node

## Testing Strategy
- Unit tests:
  - Create and validate invite tokens
  - Reject expired tokens
  - Reject invalid tokens
- Integration tests (multi-node):
  - Create 2-node cluster via join
  - Verify both nodes see each other
  - Verify Ra membership updated

## Dependencies
- task_0032_cluster_bootstrap

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cluster/invite.ex` (new)
- `neonfs_core/lib/neon_fs/cluster/join.ex` (new)
- `neonfs_core/lib/neon_fs/cli/handler.ex` (add invite/join)
- `neonfs-cli/src/commands/cluster.rs` (add commands)
- `neonfs_core/test/neon_fs/cluster/join_test.exs` (new)

## Reference
- spec/deployment.md - Join Flow with Persistence
- spec/security.md - Cluster authentication (invite tokens)

## Notes
The invite token provides basic security for cluster joins. Full certificate-based auth comes in Phase 5.
