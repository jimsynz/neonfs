# Task 0103: CLI CA Commands

## Status
Complete

## Phase
8 - Cluster CA

## Description
Add certificate authority management commands to both the Elixir CLI handler and the Rust CLI. Operators can view CA information, list issued certificates, revoke node certificates, and rotate the CA. This follows the existing CLI pattern: Rust handles argument parsing and output formatting, Elixir handler functions do the actual work via RPC.

## Acceptance Criteria

### Elixir CLI Handler
- [x] `NeonFS.CLI.Handler.handle_ca_info/0` — returns CA info map (subject, algorithm, valid_from, valid_to, serial_counter, issued_count)
- [x] `NeonFS.CLI.Handler.handle_ca_list/0` — returns list of node certificate info maps (node_name, serial, expires, status: valid/revoked)
- [x] `NeonFS.CLI.Handler.handle_ca_revoke/1` — takes node name, finds its certificate, calls `CertificateAuthority.revoke_certificate/2`, returns `:ok` or error
- [x] `handle_ca_info/0` returns `{:error, :ca_not_initialized}` if CA hasn't been created yet
- [x] `handle_ca_revoke/1` returns `{:error, :node_not_found}` if the node name doesn't match any issued certificate
- [x] Type specs on all new handler functions

### Rust CLI
- [x] `neonfs cluster ca info` — displays CA subject, algorithm, validity dates, serial counter
- [x] `neonfs cluster ca list` — displays table: NODE, SERIAL, EXPIRES, STATUS
- [x] `neonfs cluster ca revoke <node>` — revokes the named node's certificate, confirms before proceeding
- [x] All CA commands support `--json` output flag
- [x] Term conversion for CA info and certificate list response types
- [x] Clear error messages for common failures (CA not initialised, node not found)

### CA Rotation (basic structure)
- [x] `neonfs cluster ca rotate` — basic CLI structure exists (argument parsing, confirmation prompt)
- [x] Handler function `handle_ca_rotate/0` exists with a clear implementation outline
- [x] CA rotation is documented as a rare, operator-initiated operation

## Testing Strategy
- ExUnit test: `handle_ca_info/0` returns correct CA info after cluster init
- ExUnit test: `handle_ca_list/0` returns the initial node after cluster init
- ExUnit test: `handle_ca_list/0` returns multiple nodes after joins
- ExUnit test: `handle_ca_revoke/1` marks a node's cert as revoked (verify via `handle_ca_list/0` showing "revoked" status)
- ExUnit test: `handle_ca_revoke/1` returns error for unknown node
- ExUnit test: `handle_ca_info/0` returns error before CA init
- Rust unit tests: clap argument parsing for `cluster ca info`, `cluster ca list`, `cluster ca revoke <node>`, `cluster ca rotate`
- Rust unit tests: term conversion for CA info and certificate list types
- Rust unit tests: table and JSON output formatting

## Dependencies
- task_0098 (NeonFS.Transport.TLS — certificate info extraction)
- task_0099 (CertificateAuthority.ca_info/0 — CA info from system volume)
- task_0100 (Node join certificate issuance — so there are certs to list)
- task_0102 (Certificate revocation — revoke handler calls CertificateAuthority.revoke_certificate/2)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — add handle_ca_info/0, handle_ca_list/0, handle_ca_revoke/1, handle_ca_rotate/0)
- `neonfs_core/native/neonfs-cli/src/commands/cluster.rs` (modify — add `ca` subcommand with info/list/revoke/rotate)
- `neonfs_core/native/neonfs-cli/src/commands/mod.rs` (modify — register ca subcommand if needed)
- `neonfs_core/native/neonfs-cli/src/main.rs` (modify — dispatch ca subcommands)
- `neonfs_core/native/neonfs-cli/src/term/types.rs` (modify — add CaInfo, CertificateEntry types)
- `neonfs_core/native/neonfs-cli/src/term/convert.rs` (modify — FromTerm for new types)
- `neonfs_core/test/neon_fs/cli/handler_test.exs` (modify — add CA command tests)

## Reference
- spec/cluster-ca.md — CLI Commands section (output examples)
- spec/deployment.md — CLI Architecture
- Existing pattern: `neonfs_core/native/neonfs-cli/src/commands/` (follow existing command structure)
- Existing pattern: task_0078 (CLI security commands) for how CLI tasks are structured

## Notes
The `ca list` command needs to track which certificates have been issued. Options:
1. Scan the serial counter and try to identify certs from ServiceRegistry node names
2. Store issued cert metadata in the system volume (e.g., `/tls/issued/` directory)
3. Use the ServiceRegistry to correlate node names with existence

Option 1 is simplest for Phase 8. The ServiceRegistry knows about registered nodes, and each node's cert serial can be tracked. The exact approach should follow whatever is most natural given the existing ServiceRegistry data.

CA rotation (`cluster ca rotate`) is a rare, disruptive operation (reissues all node certs). The basic CLI structure should exist, but the full implementation is complex (dual-CA transition period, rolling reissuance). For Phase 8, having the command parse arguments, display a warning, and either implement the flow or return a clear "not yet implemented" message with a log of what would happen is acceptable. Full implementation can be deferred if scope is a concern.
