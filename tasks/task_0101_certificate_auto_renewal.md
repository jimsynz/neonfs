# Task 0101: Certificate Auto-Renewal (CertRenewal GenServer)

## Status
Complete

## Phase
8 - Cluster CA

## Description
Create the `NeonFS.Transport.CertRenewal` GenServer in `neonfs_client` that automatically renews node certificates before they expire. The process periodically checks the local node certificate's `notAfter` field and, when within the configured threshold (default 30 days before expiry), generates a new keypair and CSR, sends it to a core node for signing via `NeonFS.Client.Router`, and writes the new credentials to the local filesystem.

This module runs on all node types (core and non-core). On core nodes, the signing happens locally via `CertificateAuthority.sign_node_csr/2`. On non-core nodes, the signing is routed to a core node via RPC.

## Acceptance Criteria
- [x] New `NeonFS.Transport.CertRenewal` GenServer in `neonfs_client`
- [x] Checks certificate expiry on a configurable interval (default 24 hours)
- [x] When within `renewal_threshold_days` of expiry (default 30 days), initiates renewal
- [x] Renewal flow: generate new ECDSA P-256 keypair → create CSR → send to core node → receive new cert → write locally
- [x] Uses `NeonFS.Client.Router.call/4` to reach `NeonFS.Core.CertificateAuthority.sign_node_csr/2` on a core node
- [x] Writes new node.crt, node.key, and ca.crt via `Transport.TLS.write_local_tls/3`
- [x] Logs at `:info` level when renewal succeeds, including old and new expiry dates
- [x] Logs at `:warning` level when renewal fails, with reason
- [x] Retries on failure with exponential backoff (1h, 2h, 4h, max 24h)
- [x] Does not crash on renewal failure — continues checking on schedule
- [x] Handles the case where no local certificate exists yet (skips check, logs debug)
- [x] Configurable via application env: `:check_interval` (default `86_400_000` ms / 24h), `:renewal_threshold_days` (default 30)
- [x] GenServer starts as part of the supervision tree but gracefully handles missing certs during startup
- [x] Type specs on all public functions
- [x] Unit tests for renewal logic

## Testing Strategy
- ExUnit test: GenServer starts without crashing when no local cert exists
- ExUnit test: `days_until_expiry` correctly computed from a cert with known expiry
- ExUnit test: renewal triggered when cert expires within threshold (use a short-lived test cert)
- ExUnit test: renewal not triggered when cert has plenty of time remaining
- ExUnit test: renewal writes new cert to local filesystem (verify file contents changed)
- ExUnit test: failed renewal is logged and retried (mock the RPC call to fail)
- ExUnit test: exponential backoff increases delay on repeated failures
- ExUnit test: configurable interval and threshold respected

## Dependencies
- task_0098 (NeonFS.Transport.TLS — crypto primitives, local file I/O)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/transport/cert_renewal.ex` (new — CertRenewal GenServer)
- `neonfs_client/test/neon_fs/transport/cert_renewal_test.exs` (new — unit tests)

## Reference
- spec/cluster-ca.md — Certificate Renewal section (flow and code example)
- Existing pattern: `NeonFS.Client.Router.call/4` for RPC to core nodes
- Existing pattern: `NeonFS.Client.CostFunction` for GenServer with periodic checks

## Notes
The CertRenewal GenServer should be added to the `neonfs_client` or application-specific supervision tree. The exact placement depends on how the application is structured — it needs to start after the `Connection` and `Discovery` GenServers (so Router can reach core nodes), but it should not block application startup if no cert exists yet (first cert comes from the join flow, not from renewal).

The spec mentions "Reload TLS sockets (graceful — existing connections continue with old cert)" — this is a Phase 9 concern (data transfer TLS sockets don't exist yet). For Phase 8, renewal just writes the new files. Phase 9 will add the socket reload hook.

For testing, create short-lived certificates (e.g., 1-day validity) to exercise the renewal logic without waiting for real expiry. The `x509` package allows specifying arbitrary validity periods.
