# Task 0099: CertificateAuthority Module and Cluster Init CA Generation

## Status
Complete

## Phase
8 - Cluster CA

## Description
Create the `NeonFS.Core.CertificateAuthority` module in `neonfs_core` that manages the cluster CA lifecycle via the system volume. This module is the bridge between the pure crypto functions in `NeonFS.Transport.TLS` (neonfs_client) and the system volume where CA state is persisted.

Extend `NeonFS.Cluster.Init.init_cluster/1` to generate the cluster CA immediately after system volume creation. The CA key, certificate, initial serial counter, and empty CRL are stored in the system volume. The first node's certificate is issued and stored on the local filesystem.

## Acceptance Criteria
- [x] New `NeonFS.Core.CertificateAuthority` module in `neonfs_core`
- [x] `CertificateAuthority.init_ca/1` — takes cluster name; generates CA via `Transport.TLS.generate_ca/1`; writes `/tls/ca.crt`, `/tls/ca.key`, `/tls/serial` (initialised to `1`), `/tls/crl.pem` (empty) to system volume; returns `{:ok, ca_cert, ca_key}`
- [x] `CertificateAuthority.sign_node_csr/2` — takes CSR and hostname; reads CA cert + key from system volume; allocates next serial; signs via `Transport.TLS.sign_csr/5`; returns `{:ok, node_cert, ca_cert}`
- [x] `CertificateAuthority.next_serial/0` — reads `/tls/serial` from system volume, increments atomically, writes back; returns the allocated serial number
- [x] `CertificateAuthority.ca_info/0` — reads CA cert from system volume, returns map with subject, algorithm, valid_from, valid_to, current serial, node count (from issued certs)
- [x] Serial number allocation is serialised (GenServer or single-caller guarantee) to prevent duplicate serials on concurrent joins
- [x] `Cluster.Init.init_cluster/1` calls `CertificateAuthority.init_ca/1` after system volume creation and identity file write
- [x] `Cluster.Init.init_cluster/1` generates a keypair + CSR for the first node, signs it via `CertificateAuthority.sign_node_csr/2`, stores node.crt + node.key + ca.crt locally via `Transport.TLS.write_local_tls/3`
- [x] If CA init fails, `init_cluster/1` returns an error (cluster init is aborted)
- [x] If first node cert issuance fails, `init_cluster/1` returns an error
- [x] The init flow order is: Ra bootstrap → system volume → identity file → CA init → first node cert → master encryption key → node registration
- [x] Type specs on all public CertificateAuthority functions
- [x] Unit tests for CertificateAuthority functions
- [x] Unit tests for updated init flow

## Testing Strategy
- ExUnit test: `init_ca/1` writes CA cert, key, serial, and CRL to system volume (verify via `SystemVolume.read`)
- ExUnit test: `init_ca/1` returns valid CA cert and key (verify cert is self-signed, subject matches cluster name)
- ExUnit test: `sign_node_csr/2` returns a cert signed by the CA (verify chain)
- ExUnit test: `sign_node_csr/2` increments the serial counter (call twice, verify different serials)
- ExUnit test: `sign_node_csr/2` rejects invalid CSR (bad signature)
- ExUnit test: `next_serial/0` returns sequential values (1, 2, 3, ...)
- ExUnit test: `ca_info/0` returns correct subject and validity dates
- ExUnit test: `init_cluster/1` creates system volume with CA materials (verify all four files exist in system volume)
- ExUnit test: `init_cluster/1` stores node cert locally (verify via `Transport.TLS.read_local_cert/0`)
- ExUnit test: `init_cluster/1` fails gracefully if system volume creation fails
- ExUnit test: verify init ordering (CA init happens after system volume, before master key)

## Dependencies
- task_0098 (NeonFS.Transport.TLS module — provides the crypto primitives)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/certificate_authority.ex` (new — CertificateAuthority module)
- `neonfs_core/lib/neon_fs/cluster/init.ex` (modify — add CA generation and first node cert)
- `neonfs_core/test/neon_fs/core/certificate_authority_test.exs` (new — unit tests)
- `neonfs_core/test/neon_fs/cluster/init_test.exs` (modify — add CA-related tests)

## Reference
- spec/cluster-ca.md — Flows: Cluster Init section
- spec/cluster-ca.md — Serial Number Allocation section
- spec/system-volume.md — Directory Layout (`/tls/` subtree)
- Existing pattern: `NeonFS.Cluster.Init.init_cluster/1` current flow

## Notes
The serial number allocation uses a read-modify-write on the system volume's `/tls/serial` file. For concurrent joins (unlikely but possible), this must be serialised. The simplest approach is to make `CertificateAuthority` a GenServer or to use a lock via the intent log. For Phase 8, a GenServer is sufficient — the system volume's quorum writes provide durability, and the GenServer provides serialisation on the node handling the join.

The init flow after this task:
1. Initialise Ra cluster
2. Create system volume (Phase 7)
3. Write cluster identity (Phase 7)
4. Generate cluster CA (NEW)
5. Issue first node certificate (NEW)
6. Generate master encryption key (existing)
7. Register first node (existing)

Note that `ca_info/0` is intentionally simple for now — it reads the CA cert and current serial. A richer implementation (listing all issued certs) requires tracking issued certificates, which is a stretch goal. The serial counter gives an approximate count.
