# Task 0104: Phase 8 Integration Tests

## Status
Complete

## Phase
8 - Cluster CA

## Description
Write integration tests that verify the cluster CA works correctly across a multi-node cluster. These tests use the `PeerCluster` framework to spawn real peer nodes and exercise the full CA lifecycle: CA creation at cluster init, certificate issuance during node join, cross-node certificate verification, certificate revocation, and auto-renewal.

## Acceptance Criteria
- [x] New integration test file `neonfs_integration/test/integration/cluster_ca_test.exs`
- [x] Test: cluster init generates CA and stores it in the system volume (verify CA cert readable from any node)
- [x] Test: first node has a valid certificate after init (verify locally stored cert is signed by cluster CA)
- [x] Test: joining nodes receive valid certificates (verify each node's cert is signed by the cluster CA)
- [x] Test: each node has a unique serial number
- [x] Test: certificate subject contains the correct node name
- [x] Test: CA cert is identical on all nodes (all nodes cache the same CA)
- [x] Test: any core node can sign a CSR (not just the init node) — sign from node2 after init on node1
- [x] Test: certificate revocation updates CRL visible to all nodes
- [x] Test: revoking a certificate is reflected in `ca list` output
- [x] Test: CA operations survive single node failure (stop one node, verify signing still works on remaining nodes)
- [x] Test: serial numbers increase monotonically across multiple joins
- [x] All tests pass with `mix test` in the `neonfs_integration` directory
- [x] Tests use `@moduletag timeout: 300_000` for adequate multi-node test time

## Testing Strategy
Use `PeerCluster.start_cluster!/3` to create a 3-node cluster and `PeerCluster.rpc/6` to execute operations on specific nodes. Follow existing patterns from `system_volume_test.exs` and `failure_test.exs`.

### Test Scenarios

1. **CA Lifecycle**: Init cluster on node1, verify CA cert exists in system volume on node1, verify ca_info returns correct subject and validity. Join node2 and node3, verify each received a cert, verify serial numbers are 2 and 3 (serial 1 was the CA itself or the first node).

2. **Cross-Node Cert Verification**: Read CA cert from node1's system volume. Read node2's local cert. Verify node2's cert is signed by the CA using `:public_key.pkix_path_validation/3`.

3. **Any Core Node Can Sign**: Generate a CSR on node1, send it to node2 for signing (call `CertificateAuthority.sign_node_csr/2` on node2), verify the returned cert is valid.

4. **Revocation**: Revoke node3's certificate from node1. Verify the CRL on node2 contains node3's serial. Verify `handle_ca_list/0` on any node shows node3 as revoked.

5. **Node Failure Resilience**: Stop node3. Verify that `CertificateAuthority.sign_node_csr/2` still works on node1 and node2. Verify CA info is still readable.

6. **Serial Monotonicity**: Join multiple nodes sequentially. Verify serial numbers are strictly increasing with no gaps.

## Dependencies
- task_0098 (NeonFS.Transport.TLS module)
- task_0099 (CertificateAuthority module + cluster init CA)
- task_0100 (Node join certificate issuance)
- task_0101 (Certificate auto-renewal)
- task_0102 (Certificate revocation)
- task_0103 (CLI CA commands)

## Files to Create/Modify
- `neonfs_integration/test/integration/cluster_ca_test.exs` (new — integration tests)

## Reference
- spec/cluster-ca.md — full specification
- Existing pattern: `neonfs_integration/test/integration/system_volume_test.exs` for multi-node test structure
- Existing pattern: `neonfs_integration/test/integration/failure_test.exs` for node failure testing
- Existing pattern: `neonfs_integration/test/support/cluster_case.ex` for `wait_until`, `assert_eventually`

## Notes
Testing certificate auto-renewal in integration tests is challenging because the default check interval is 24 hours. Options:
1. Configure a very short check interval (e.g., 5 seconds) and use a short-lived cert (1 minute validity) in the test
2. Directly call the renewal function rather than waiting for the timer
3. Skip renewal in integration tests and rely on unit tests (task 0101)

Option 2 is most practical — call the renewal logic directly via RPC on a node whose cert is about to expire (created with a very short validity for testing purposes).

The `PeerCluster` framework handles node setup and teardown. Ensure that the TLS directory exists on peer nodes (may need to configure `data_dir` in the peer node config).

These tests complement the unit tests from tasks 0098-0103. The integration tests verify that all components work together in a realistic multi-node environment, while unit tests verify individual functions in isolation.
