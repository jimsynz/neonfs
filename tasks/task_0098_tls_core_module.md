# Task 0098: x509 Dependency and NeonFS.Transport.TLS Module

## Status
Complete

## Phase
8 - Cluster CA

## Description
Add the `x509` Hex package as a dependency of `neonfs_client` and create the `NeonFS.Transport.TLS` module with pure cryptographic functions for CA and certificate operations. This module provides the foundation for all Phase 8 work — CA generation, CSR creation, certificate signing, CRL management, PEM encoding/decoding, and local filesystem TLS file management.

All functions in this module are pure (no SystemVolume dependency, no RPC). Higher-level orchestration that reads/writes the system volume is handled by `NeonFS.Core.CertificateAuthority` (task 0099). This separation keeps `neonfs_client` independent of `neonfs_core` at compile time while providing all the crypto primitives both packages need.

## Acceptance Criteria
- [ ] `x509` added to `neonfs_client/mix.exs` dependencies
- [ ] New `NeonFS.Transport.TLS` module in `neonfs_client`
- [ ] `generate_ca/1` — takes cluster name, returns `{ca_cert, ca_key}` (ECDSA P-256, self-signed, `:root_ca` template, subject `/O=NeonFS/CN=#{cluster_name} CA`)
- [ ] `generate_node_key/0` — returns ECDSA P-256 private key
- [ ] `create_csr/2` — takes private key and node name, returns CSR with subject `/O=NeonFS/CN=#{node_name}`
- [ ] `sign_csr/5` — takes CSR, hostname, ca_cert, ca_key, serial; returns signed node certificate with `:server` template, SAN for hostname, ext_key_usage `[:serverAuth, :clientAuth]`
- [ ] `validate_csr/1` — returns boolean (signature verification)
- [ ] `create_empty_crl/2` — takes ca_cert and ca_key, returns CRL PEM with no entries
- [ ] `add_crl_entry/4` — takes certificate_to_revoke, existing CRL entries, ca_cert, ca_key; returns updated CRL PEM
- [ ] `parse_crl_entries/1` — takes CRL PEM, returns list of revocation entries
- [ ] `certificate_info/1` — takes cert (PEM or decoded), returns map with subject, serial, not_before, not_after, issuer
- [ ] `days_until_expiry/1` — takes cert, returns integer days until `notAfter`
- [ ] PEM encode/decode helpers: `encode_cert/1`, `decode_cert!/1`, `encode_key/1`, `decode_key!/1`, `encode_csr/1`, `decode_csr!/1`
- [ ] `tls_dir/0` — returns the local TLS directory path (`<data_dir>/tls/`)
- [ ] `write_local_tls/3` — writes ca.crt, node.crt, node.key to the local TLS directory with appropriate file permissions (key file 0600, cert files 0644)
- [ ] `read_local_cert/0` — reads and decodes the local node.crt
- [ ] `read_local_ca_cert/0` — reads and decodes the local ca.crt (cached copy)
- [ ] Validity periods configurable via application env: `:ca_validity_days` (default 3650), `:node_validity_days` (default 365), `:renewal_threshold_days` (default 30)
- [ ] CA certificate has `pathLen: 0` (no intermediate CAs), key usage `digitalSignature, keyCertSign, cRLSign`
- [ ] Node certificate has `CA: false`, key usage `digitalSignature`, ext_key_usage `serverAuth, clientAuth`
- [ ] Type specs on all public functions
- [ ] Unit tests for all public functions

## Testing Strategy
- ExUnit test: `generate_ca/1` returns valid self-signed cert (verify with `X509.Certificate.valid?` and subject/issuer match)
- ExUnit test: `generate_node_key/0` returns an ECDSA P-256 key
- ExUnit test: `create_csr/2` returns a valid CSR (verify with `validate_csr/1`)
- ExUnit test: `sign_csr/5` produces a cert signed by the CA (verify chain with `:public_key.pkix_path_validation/3`)
- ExUnit test: signed cert has correct SAN, ext_key_usage, subject, serial
- ExUnit test: `validate_csr/1` returns false for tampered CSR
- ExUnit test: `create_empty_crl/2` returns valid PEM-encoded CRL
- ExUnit test: `add_crl_entry/4` adds entry, `parse_crl_entries/1` returns it
- ExUnit test: PEM roundtrip — encode then decode returns equivalent value for certs, keys, and CSRs
- ExUnit test: `certificate_info/1` extracts correct fields
- ExUnit test: `days_until_expiry/1` returns correct value for a cert with known expiry
- ExUnit test: `write_local_tls/3` and `read_local_cert/0` roundtrip (use `tmp_dir` ExUnit tag)
- ExUnit test: file permissions are correct after `write_local_tls/3` (key 0600, certs 0644)
- ExUnit test: validity period configuration from application env

## Dependencies
- All Phase 7 tasks complete (current codebase is the starting point)

## Files to Create/Modify
- `neonfs_client/mix.exs` (modify — add `{:x509, "~> 0.8"}` dependency)
- `neonfs_client/lib/neon_fs/transport/tls.ex` (new — TLS module)
- `neonfs_client/test/neon_fs/transport/tls_test.exs` (new — unit tests)

## Reference
- spec/cluster-ca.md — Key and Certificate Format, Validity Periods sections
- spec/cluster-ca.md — Dependencies section (x509 package)
- spec/packages.md — x509 entry
- `x509` hex package documentation (https://hexdocs.pm/x509/)

## Notes
The `x509` package is pure Elixir with zero dependencies — it wraps OTP's `:public_key` module. It provides a much more ergonomic API than using `:public_key` directly.

The module uses the `:server` template from x509 for node certificates, which sets appropriate defaults. We override `extensions` to add SAN and ext_key_usage explicitly.

Serial numbers are passed into `sign_csr/5` as a parameter rather than being allocated internally — serial allocation is the responsibility of `NeonFS.Core.CertificateAuthority` which manages the serial counter in the system volume.

The `tls_dir/0` function derives the path from the configured data directory (same as blob store). The default is `/var/lib/neonfs/tls/`. The directory is created on first write if it doesn't exist.
