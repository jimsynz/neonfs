# Task 0102: Certificate Revocation (CRL Management)

## Status
Complete

## Phase
8 - Cluster CA

## Description
Implement certificate revocation via CRL (Certificate Revocation List) management in `NeonFS.Core.CertificateAuthority`. When a node is decommissioned or its certificate needs to be revoked, its serial number is added to the CRL stored in the system volume. The updated CRL is then available to all nodes (system volume is replicated cluster-wide).

Integrate revocation with the node decommission flow so that decommissioned nodes automatically have their certificates revoked.

CRL checking in TLS connections is a Phase 8 stretch goal per the spec — the primary protection against decommissioned nodes is removing them from the Erlang distribution cluster. The CRL provides a second layer for the data transfer plane (Phase 9).

## Acceptance Criteria
- [x] `CertificateAuthority.revoke_certificate/2` — takes node certificate (or serial number) and reason (default `:unspecified`); adds entry to CRL in system volume; returns `:ok`
- [x] `CertificateAuthority.revoke_certificate/2` preserves existing CRL entries when adding a new one
- [x] Supported revocation reasons: `:unspecified`, `:key_compromise`, `:superseded`, `:cessation_of_operation`
- [x] `CertificateAuthority.get_crl/0` — reads and returns the current CRL from system volume
- [x] `CertificateAuthority.list_revoked/0` — returns list of revoked serial numbers with revocation dates and reasons
- [x] `CertificateAuthority.is_revoked?/1` — takes a serial number, returns boolean
- [N/A] Node decommission flow calls `CertificateAuthority.revoke_certificate/2` with `:cessation_of_operation` reason — no decommission flow exists yet; the function is ready to be called when one is implemented
- [x] Revoking an already-revoked certificate returns `:ok` (idempotent)
- [x] Revoking returns `{:error, ...}` if CRL doesn't exist in system volume (SystemVolume.read returns error)
- [x] Type specs on all public functions
- [x] Unit tests for revocation functions
- [N/A] Unit tests for decommission integration — no decommission flow exists yet

## Testing Strategy
- ExUnit test: `revoke_certificate/2` adds entry to CRL (verify via `list_revoked/0`)
- ExUnit test: `revoke_certificate/2` preserves existing entries (revoke two certs, verify both in CRL)
- ExUnit test: `is_revoked?/1` returns true for revoked serial, false for non-revoked
- ExUnit test: revocation with different reasons (verify reason stored correctly)
- ExUnit test: revoking same cert twice is idempotent
- ExUnit test: `get_crl/0` returns valid CRL PEM
- ExUnit test: decommission triggers revocation (verify cert serial appears in CRL)
- ExUnit test: error when CA not initialised

## Dependencies
- task_0098 (NeonFS.Transport.TLS — CRL creation and entry addition functions)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/certificate_authority.ex` (modify — add revoke_certificate/2, get_crl/0, list_revoked/0, is_revoked?/1)
- `neonfs_core/lib/neon_fs/cluster/join.ex` (modify — call revoke_certificate during decommission, if decommission logic exists here)
- `neonfs_core/test/neon_fs/core/certificate_authority_test.exs` (modify — add revocation tests)

## Reference
- spec/cluster-ca.md — Certificate Revocation section
- spec/cluster-ca.md — CRL checking stretch goal note
- spec/node-management.md — Node decommission flow
- Existing pattern: `NeonFS.Core.CertificateAuthority` from task 0099

## Notes
The CRL is stored as PEM in the system volume at `/tls/crl.pem`. The `x509` package provides `X509.CRL.new/3` for creating CRLs and supports revocation entries with reason codes.

CRL checking in TLS connections (`:ssl` option `{:crl_check, :peer}`) requires a custom CRL lookup function because the CRL is stored in the system volume, not served via HTTP. This is explicitly marked as a stretch goal in the spec and is not required for Phase 8. If implemented, it would be a `NeonFS.Transport.CRLCache` module that reads the CRL from the local filesystem cache (the system volume is replicated locally).

The node decommission flow may be in `Cluster.Join` or a separate module — check the existing codebase. The integration point is wherever node removal/decommission is handled.
