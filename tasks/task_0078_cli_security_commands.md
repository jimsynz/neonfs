# Task 0078: CLI Security Commands

## Status
Complete

## Phase
6 - Security

## Description
Add security-related commands to the Rust CLI: volume encryption options on creation, key rotation commands, ACL management (using numeric UIDs/GIDs), and audit log queries. This task handles the Rust-side argument parsing, term conversion, and output formatting. The corresponding Elixir handler functions are added in their respective feature tasks — this task wires up the Rust CLI to call them.

## Acceptance Criteria
- [ ] Volume encryption: `neonfs volume create <name> --encryption server-side` (extends existing volume create)
- [ ] Volume encryption: `neonfs volume create <name> --encryption none` (explicit no encryption, default)
- [ ] `volume info` displays encryption status, current key version, rotation state (if any)
- [ ] Key rotation: `neonfs volume rotate-key <volume>`, confirms before proceeding, shows new key version
- [ ] Key rotation: `neonfs volume rotation-status <volume>` shows from/to versions, progress (N/M chunks, percentage)
- [ ] ACL commands: `neonfs acl grant <volume> uid:<N> <permissions>`, `neonfs acl grant <volume> gid:<N> <permissions>`
- [ ] ACL commands: `neonfs acl revoke <volume> uid:<N>`, `neonfs acl revoke <volume> gid:<N>`
- [ ] ACL commands: `neonfs acl show <volume>` displays table: principal (uid:1000/gid:100), permissions
- [ ] Permission format: comma-separated `read,write,admin`
- [ ] File ACL: `neonfs acl set-file <volume> <path> --mode <octal>` sets POSIX mode bits
- [ ] File ACL: `neonfs acl set-file <volume> <path> --uid <N>` changes owner UID
- [ ] File ACL: `neonfs acl get-file <volume> <path>` displays mode, owner UID/GID, extended entries
- [ ] Audit: `neonfs audit list [--type <type>] [--uid <N>] [--since <duration>] [--limit <n>]`
- [ ] `audit list` displays table: timestamp, event_type, actor_uid, resource, outcome
- [ ] All new commands support `--json` output flag (existing pattern)
- [ ] Rust term conversion for new response types (EncryptionStatus, RotationStatus, AclInfo, AuditEntry)
- [ ] Clear error messages for common failures (volume not encrypted, permission denied, invalid UID)
- [ ] All existing CLI commands continue to work

## Testing Strategy
- Rust unit tests for clap argument parsing of all new subcommands
- Rust unit tests for term conversion of new response types
- Rust unit tests for output formatting (table and JSON modes)
- Integration tests (via Elixir calling handler functions that the CLI would invoke)
- Verify existing CLI tests pass unchanged

## Dependencies
- task_0071 (Key management — volume encryption CLI handler functions)
- task_0074 (Key rotation — rotation CLI handler functions)
- task_0075 (Volume ACLs — ACL CLI handler functions)
- task_0077 (Audit logging — audit CLI handler functions)

## Files to Create/Modify
- `neonfs_core/native/neonfs-cli/src/commands/acl.rs` (new — ACL subcommands)
- `neonfs_core/native/neonfs-cli/src/commands/audit.rs` (new — audit subcommands)
- `neonfs_core/native/neonfs-cli/src/commands/volume.rs` (modify — add --encryption flag, rotate-key, rotation-status)
- `neonfs_core/native/neonfs-cli/src/commands/mod.rs` (modify — register new subcommands)
- `neonfs_core/native/neonfs-cli/src/main.rs` (modify — add new command dispatch)
- `neonfs_core/native/neonfs-cli/src/term/types.rs` (modify — add EncryptionStatus, RotationStatus, AclInfo, AuditEntry)
- `neonfs_core/native/neonfs-cli/src/term/convert.rs` (modify — FromTerm for new types)

## Reference
- spec/security.md — CLI Commands section (key rotation)
- spec/deployment.md — CLI Architecture section
- Existing pattern: `neonfs_core/native/neonfs-cli/src/commands/` (follow existing command patterns)

## Notes
ACL principals use the format `uid:1000` or `gid:100` — numeric only, no username resolution. This is intentional: NeonFS works with UIDs/GIDs, name resolution is the deployer's concern. The `--encryption` flag on volume create defaults to `none` for backwards compatibility. For `rotation-status`, estimated completion time is calculated from the current progress rate. All output should be consistent with existing CLI formatting (table by default, JSON with `--json`). No user/group management commands — NeonFS does not manage identities.
