# Task 0070: MetadataStateMachine v6 (Encryption Keys, Volume ACLs)

## Status
Complete

## Phase
6 - Security

## Description
Bump MetadataStateMachine from v5 to v6, adding `encryption_keys` and `volume_acls` maps to the Ra-backed state. Encryption keys are per-volume, wrapped with the cluster master key. Volume ACLs store UID/GID-based permission entries. No user/group identity storage — NeonFS works with numeric UIDs/GIDs only. Follows the established migration pattern from v4→v5 (metadata tiering). These fields belong in Ra (Tier 1) because they are small, critical, and need strong consistency.

## Acceptance Criteria
- [x] MetadataStateMachine `version/0` returns 6
- [x] State includes `encryption_keys: %{volume_id => %{version => wrapped_key_entry}}`
- [x] State includes `volume_acls: %{volume_id => acl_data}`
- [x] Migration handler for `{:machine_version, 5, 6}` adds empty maps for new fields
- [x] Encryption key commands: `{:put_encryption_key, volume_id, version, wrapped_entry}`, `{:delete_encryption_key, volume_id, version}`, `{:set_current_key_version, volume_id, version}`
- [x] Volume ACL commands: `{:put_volume_acl, volume_id, acl_data}`, `{:update_volume_acl, volume_id, updates}`
- [x] Query functions: `get_encryption_keys/2`, `get_volume_acl/2`
- [x] Volume deletion cascades: deleting a volume also removes its encryption keys and ACL
- [x] Persistence module updated to snapshot new state fields
- [x] All existing v5 state and commands continue to work
- [x] Unit tests for each new command type
- [x] Unit tests for v5→v6 migration
- [x] Unit tests for cascade deletion

## Testing Strategy
- ExUnit tests for each Ra command (put, delete for keys and ACLs)
- ExUnit test for migration from v5 state (verify existing data preserved, new fields initialised)
- ExUnit tests for query functions
- ExUnit test for volume deletion cascade
- Verify all existing MetadataStateMachine tests pass without modification

## Dependencies
- task_0068 (encryption types — VolumeEncryption, wrapped key entry structs)
- task_0069 (ACL types — VolumeACL struct)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/metadata_state_machine.ex` (modify — bump version, add fields, commands, migration)
- `neonfs_core/lib/neon_fs/core/persistence.ex` (modify — snapshot new state fields)
- `neonfs_core/test/neon_fs/core/metadata_state_machine_test.exs` (modify — add v6 command tests, migration test)

## Reference
- spec/security.md — Key Hierarchy
- spec/metadata.md — Tier 1: Cluster State (Ra) — encryption keys and volume ACLs belong here
- Existing pattern: MetadataStateMachine v4→v5 migration (task_0082)
- CLAUDE.md — Ra state machine versioning pattern

## Notes
The encryption keys stored in Ra are always wrapped (encrypted) with the cluster master key — Ra never sees plaintext key material. The volume_acls map is separate from the volumes map to keep ACL changes independent from volume config changes. This version intentionally does not add user/group identity storage — NeonFS works with numeric UIDs/GIDs throughout, and identity resolution is the interface layer's responsibility. Unlike the v4→v5 transition (which was a clean cutover), v5→v6 uses a proper migration since v5 will be in production use by the time Phase 6 is implemented.
