# Task 0111: Phase 9 Integration Tests

## Status
Complete (2026-02-18)

## Phase
9 - Data Transfer

## Description
Write integration tests that verify the data transfer plane works correctly across a multi-node cluster. These tests use the `PeerCluster` framework to spawn real peer nodes and exercise the full data plane lifecycle: endpoint advertisement, connection pool creation, chunk transfer over TLS, replication via data plane, remote reads via data plane, and cluster stability under sustained transfer.

The most critical validation is proving the core design goal: separating bulk data transfer from the control plane so that Ra consensus is not disrupted by sustained chunk traffic.

## Acceptance Criteria
- [x] New integration test file `neonfs_integration/test/integration/data_transfer_test.exs`
- [x] Test: nodes advertise data transfer endpoints in ServiceInfo metadata after cluster init
- [x] Test: PoolManager creates connection pools to discovered peer endpoints
- [x] Test: `put_chunk` over data plane — send a chunk from node1 to node2 via `data_call`, verify chunk exists on node2
- [x] Test: `get_chunk` over data plane — store a chunk on node2, retrieve it from node1 via `data_call`, verify content matches
- [x] Test: `has_chunk` over data plane — check chunk existence on remote node, verify tier and size returned
- [x] Test: replicated write uses data plane — write a file to a replicated volume, verify chunks replicated to remote nodes
- [x] Test: remote read uses data plane — write a file, read it back from a node that doesn't have the chunk locally
- [x] Test: erasure-coded write distributes stripe chunks via data plane
- [x] Test: data plane does not block Ra — perform sustained bulk transfer while verifying Ra leader is stable
- [x] Test: node failure — stop a node during transfer, verify remaining nodes continue operating (no pool crash cascade)
- [x] Test: new node joins and becomes reachable via data plane
- [x] All tests pass with `mix test` in the `neonfs_integration` directory
- [x] Tests use `@moduletag timeout: 300_000` for adequate multi-node test time

## Testing Strategy
Use `PeerCluster.start_cluster!/3` to create a 3-node cluster and `PeerCluster.rpc/6` to execute operations on specific nodes. Follow existing patterns from `cluster_ca_test.exs` and `failure_test.exs`.

### Test Scenarios

1. **Endpoint Advertisement**: Init cluster, verify all nodes have `data_endpoint` in their ServiceInfo metadata. Verify the endpoint is reachable (connect via `:ssl.connect/3` using local CA cert).

2. **Point-to-Point Transfer**: Use `Router.data_call/4` from node1 to put a chunk on node2. Verify chunk exists on node2 via local BlobStore check. Retrieve the chunk from node2 to node1 via `data_call`. Verify content matches original.

3. **Replicated Volume Write**: Create a volume with replication factor 2. Write a file from node1. Verify chunks exist on at least one other node — confirming replication used the data plane.

4. **Remote Read**: Write a file, then read it from a different node that doesn't have local copies. Verify the read succeeds and returns correct data.

5. **Cluster Stability Under Load**: Start a sustained bulk transfer (write 50+ chunks in rapid succession). During the transfer, verify Ra can still process metadata operations within normal latency bounds. Check that no Ra leader elections occurred during the transfer window.

6. **Node Failure**: Start transferring chunks to node3. Stop node3 mid-transfer. Verify node1 and node2 continue operating normally. Verify PoolManager removes the pool for node3. Verify subsequent writes/reads to remaining nodes succeed.

7. **Node Join**: Start a 2-node cluster. Join a third node. Verify the third node's data endpoint is discovered by existing nodes. Verify pools are created and chunks can be transferred to the new node.

## Dependencies
- task_0105 (Transport.ConnPool)
- task_0106 (Transport.Listener and Handler)
- task_0107 (Transport.PoolManager)
- task_0108 (Router.data_call/4)
- task_0109 (Write path migration)
- task_0110 (Read path migration)

## Files to Create/Modify
- `neonfs_integration/test/integration/data_transfer_test.exs` (new — 12 integration tests)
- `neonfs_client/lib/neon_fs/transport/listener.ex` (added `rebind/0` for post-cert TLS binding)
- `neonfs_core/lib/neon_fs/core/service_registry.ex` (added `refresh_self/0` for endpoint re-registration)
- `neonfs_core/lib/neon_fs/cluster/init.ex` (activate data plane after issuing first node cert)
- `neonfs_core/lib/neon_fs/cluster/join.ex` (activate data plane after storing TLS certs)
- `neonfs_core/lib/neon_fs/core/file_index.ex` (fix stripe deserialization — string keys to atom keys after quorum read)

## Reference
- spec/data-transfer.md — full specification
- Existing pattern: `neonfs_integration/test/integration/cluster_ca_test.exs` for multi-node test structure
- Existing pattern: `neonfs_integration/test/integration/failure_test.exs` for node failure testing
- Existing pattern: `neonfs_integration/test/support/cluster_case.ex` for `wait_until`, `assert_eventually`

## Notes
The "cluster stability under load" test is the most important validation — it proves the core design goal of separating data plane from control plane. If Ra heartbeats are still delayed during sustained transfer, the separation isn't working correctly. To test this:
1. Start a sustained bulk transfer (many chunks, large data)
2. Concurrently perform Ra-dependent operations (metadata writes, volume operations)
3. Assert that Ra operations complete within acceptable latency (e.g., < 5 seconds)
4. Check Ra leader status — no unnecessary elections should have occurred

The `PeerCluster` framework handles node setup and teardown. Ensure that TLS certificates are available on all peer nodes (Phase 8's cluster init + join flow handles this). Data transfer configuration (port, pool_size) should use defaults in the peer node application env.

For the "has_chunk" test, if `BlobStore.chunk_info/2` was added in task 0106 to support the Handler, verify it returns correct tier and size through the full data plane path.

These tests complement the unit tests from tasks 0105-0110. The integration tests verify that all components work together in a realistic multi-node environment, while unit tests verify individual functions in isolation.
