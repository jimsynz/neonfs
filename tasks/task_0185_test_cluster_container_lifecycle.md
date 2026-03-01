# Task 0185: TestCluster Module — Container Lifecycle

## Status
Blocked — premature; deferring chaos testing to a later stage

## Phase
Gap Analysis — H-4 (2/7)

## Description
Create `NeonFS.Integration.TestCluster`, a higher-level module that manages
a cluster of NeonFS containers for chaos testing. This module handles
creating a Docker network, starting core and FUSE containers with the
correct configuration, and tearing everything down cleanly.

Unlike `PeerCluster` (which uses `:peer` for in-process nodes), `TestCluster`
runs real containerised NeonFS nodes with actual network isolation.

## Acceptance Criteria
- [ ] `NeonFS.Integration.TestCluster` module created
- [ ] `start/1` accepts cluster configuration: node count, image tags, cookie, network name
- [ ] `start/1` creates a Docker bridge network for inter-container communication
- [ ] `start/1` launches N core node containers with unique names and IP addresses
- [ ] Each container started with matching `RELEASE_COOKIE` and appropriate `RELEASE_NODE`
- [ ] First node runs `cluster init`, subsequent nodes run `cluster join`
- [ ] `start/1` returns `{:ok, cluster}` with cluster state (container IDs, names, network)
- [ ] `stop/1` stops all containers, removes them, and removes the network
- [ ] `stop/1` is idempotent (safe to call multiple times)
- [ ] `node_name/2` returns the Erlang node name for a given cluster node index
- [ ] `container_id/2` returns the container ID for a given cluster node index
- [ ] `wait_until_healthy/2` polls health endpoints until all nodes report healthy (with timeout)
- [ ] Container images are the locally-built images from `bake.hcl`
- [ ] Unit test: start a 3-node cluster, verify all containers running
- [ ] Unit test: stop cleans up all containers and network
- [ ] Tests tagged `@tag :chaos`
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_integration/test/neonfs/integration/test_cluster_test.exs`:
  - Start a 3-node cluster, verify containers are running via `Container.inspect/1`
  - Wait until healthy, verify all nodes see each other
  - Stop the cluster, verify cleanup
  - Tag `:chaos` so these only run when explicitly requested

## Dependencies
- Task 0184 (Container runtime API — used for all container operations)

## Files to Create/Modify
- `neonfs_integration/lib/neonfs/integration/test_cluster.ex` (create — cluster lifecycle)
- `neonfs_integration/test/neonfs/integration/test_cluster_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-4
- `spec/testing.md` lines 680–750 (cluster setup for chaos tests)
- Existing: `neonfs_integration/lib/neonfs/integration/peer_cluster.ex` (pattern reference)
- Existing: `bake.hcl` (container image build definitions)

## Notes
The Docker network provides DNS resolution between containers. Container
names like `neonfs-core-1`, `neonfs-core-2` etc. are resolvable within the
network.

Erlang distribution requires:
- Matching cookies across all nodes
- Port 4369 (epmd) accessible between containers
- Distribution ports (dynamically assigned) accessible

The container startup sequence is important:
1. Create network
2. Start first core node
3. Wait for it to be healthy
4. Run `cluster init` on first node (via exec or CLI)
5. Start remaining core nodes
6. Each runs `cluster join` pointing to the first node

Consider using `NEONFS_BOOTSTRAP_NODES` environment variable for
auto-discovery instead of manual join, if the architecture supports it.

Test timeout should be generous (2-3 minutes) since container startup
involves image loading, application boot, and Ra cluster formation.
