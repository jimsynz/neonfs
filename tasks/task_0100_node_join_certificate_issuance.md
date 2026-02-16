# Task 0100: Node Join Certificate Issuance

## Status
Complete

## Phase
8 - Cluster CA

## Description
Extend the node join flow to include TLS certificate issuance. When a node joins the cluster, it generates an ECDSA P-256 keypair locally, creates a CSR, and sends it to the accepting core node. The core node signs the CSR via `CertificateAuthority.sign_node_csr/2` and returns the signed certificate along with the CA certificate. The joining node stores all three files (node.crt, node.key, ca.crt) on its local filesystem.

This task modifies both sides of the join flow: the joining node (which may be core or non-core) and the accepting core node. The private key never leaves the joining node.

## Acceptance Criteria
- [x] Joining node generates ECDSA P-256 keypair via `Transport.TLS.generate_node_key/0`
- [x] Joining node creates CSR via `Transport.TLS.create_csr/2` with node name
- [x] CSR is included in the join request sent to the core node
- [x] Core node validates the CSR signature via `Transport.TLS.validate_csr/1`
- [x] Core node signs CSR via `CertificateAuthority.sign_node_csr/2` with the joining node's hostname
- [x] Core node returns signed node certificate + CA certificate in the join response
- [x] Joining node stores node.crt, node.key, and ca.crt locally via `Transport.TLS.write_local_tls/3`
- [x] If CSR validation fails, the join is rejected with `{:error, :invalid_csr}`
- [x] If certificate signing fails, the join is rejected with a descriptive error
- [x] Non-core node joins (`:fuse`, `:s3`, etc.) also receive certificates
- [x] Existing join flow (token validation, Ra membership, service registration, system volume replication) continues to work
- [x] The certificate issuance is part of the join transaction — if it fails, the join does not complete
- [x] Unit tests for the extended join flow (both sides)

## Testing Strategy
- ExUnit test: core-type join — joining node receives a valid certificate signed by the cluster CA
- ExUnit test: non-core-type join (`:fuse`) — joining node receives a valid certificate
- ExUnit test: verify the returned certificate's subject matches the joining node's name
- ExUnit test: verify the returned certificate's SAN contains the joining node's hostname
- ExUnit test: verify the serial number increments with each join
- ExUnit test: verify node.crt and ca.crt are written to the local filesystem after join
- ExUnit test: join fails gracefully if CSR is invalid (tampered)
- ExUnit test: existing join tests continue to pass (backwards compatibility)

## Dependencies
- task_0098 (NeonFS.Transport.TLS — crypto primitives)
- task_0099 (CertificateAuthority.sign_node_csr/2 — CA signing via system volume)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cluster/join.ex` (modify — add CSR generation on joining side, CSR signing on accepting side, cert return)
- `neonfs_core/test/neon_fs/cluster/join_test.exs` (modify — add certificate issuance tests)

## Reference
- spec/cluster-ca.md — Flows: Node Join section (sequence diagram and code)
- Existing pattern: `NeonFS.Cluster.Join.join_cluster/3` and `accept_join/3`

## Notes
The hostname for the SAN field needs to come from the joining node. This could be the Erlang node name, the system hostname, or a configured value. For Phase 8, using `node()` (the Erlang node name) as the hostname is sufficient — this matches the identity used by Erlang distribution and will be consistent with the data transfer plane's connection targets in Phase 9.

The join flow currently returns cluster state info (peers, cluster_id, etc.). The response is extended to include `node_cert` and `ca_cert` PEM binaries. The joining node extracts these and writes them locally.

Be careful not to transmit the node's private key — it is generated locally on the joining node and never sent over the network. Only the CSR (which contains the public key) crosses the wire.
