# Task 0069: ACL Types (UID/GID-Based)

## Status
Complete

## Phase
6 - Security

## Description
Create access control types in neonfs_client: `VolumeACL` and `FileACL` structs using numeric POSIX UIDs/GIDs throughout. NeonFS does not manage user identities — it stores and enforces numeric UIDs/GIDs, exactly like NFS AUTH_SYS. Name resolution (UID → username) is the responsibility of each interface layer (FUSE clients, S3 gateway, CIFS/Samba), not the filesystem. This keeps the core simple and avoids the problem of inconsistent `/etc/passwd` across containerised nodes.

## Acceptance Criteria
- [ ] New `NeonFS.Core.VolumeACL` struct with fields: `volume_id` (string), `owner_uid` (non-neg integer), `owner_gid` (non-neg integer), `entries` (list of ACL entries)
- [ ] VolumeACL entry struct: `principal` (`{:uid, integer}` or `{:gid, integer}`), `permissions` (MapSet of `:read`, `:write`, `:admin`)
- [ ] `VolumeACL.has_permission?/3` checks if a UID has a specific permission (direct UID match, or via GID match)
- [ ] `VolumeACL.has_permission?/4` variant accepts a list of supplementary GIDs for group checks
- [ ] Permission inheritance: `:admin` implies `:write` implies `:read`
- [ ] Volume owner UID always has all permissions (implicit admin)
- [ ] New `NeonFS.Core.FileACL` struct with fields: `mode` (integer, POSIX mode bits), `uid` (owner UID), `gid` (owner GID), `acl_entries` (list of extended ACL entries)
- [ ] Extended ACL entry struct: `type` (`:user`, `:group`, `:mask`, `:other`), `id` (UID or GID, nil for mask/other), `permissions` (MapSet of `:r`, `:w`, `:x`)
- [ ] `FileACL.check_access/4` takes `(file_acl, requesting_uid, requesting_gid, requested_permission)` and returns `:ok` or `{:error, :forbidden}`
- [ ] `FileACL.check_access/5` variant accepts supplementary GIDs
- [ ] POSIX ACL semantics: mask entry limits effective permissions of named users and named groups (not owner or other)
- [ ] All types have `@type t()` typespecs
- [ ] Unit tests for VolumeACL permission checks (direct UID, via GID, owner bypass, permission inheritance)
- [ ] Unit tests for FileACL access checks (owner, group, other, extended entries, mask interaction)

## Testing Strategy
- ExUnit tests for VolumeACL creation and permission checking
- ExUnit tests for FileACL POSIX mode checking (owner rwx, group rwx, other rwx)
- ExUnit tests for extended ACL with mask interaction
- ExUnit tests for supplementary group handling
- Property tests for permission checking consistency (granted permissions are a superset chain: admin ⊃ write ⊃ read)

## Dependencies
- None (independent, defines types only)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/core/volume_acl.ex` (new — VolumeACL struct and permission helpers)
- `neonfs_client/lib/neon_fs/core/file_acl.ex` (new — FileACL struct and access check helpers)
- `neonfs_client/test/neon_fs/core/volume_acl_test.exs` (new)
- `neonfs_client/test/neon_fs/core/file_acl_test.exs` (new)

## Reference
- spec/security.md — Volume Access Control, File/Directory ACLs
- POSIX.1e ACL specification (for mask and effective permission semantics)

## Notes
NeonFS intentionally stores only numeric UIDs/GIDs — no usernames, no identity provider integration. This mirrors NFS AUTH_SYS and is the simplest correct approach for containerised deployments where each node has its own `/etc/passwd`. UID consistency across nodes is the deployer's responsibility (same as NFS, GlusterFS). The `check_access` function implements standard POSIX ACL evaluation order: (1) if requesting_uid == owner_uid, use owner permissions; (2) if extended ACL entries exist and a named user entry matches, use it masked by the mask entry; (3) if requesting_gid == owner_gid or any supplementary GID matches, use group permissions (masked if extended ACLs exist); (4) if a named group entry matches, use it masked; (5) use other permissions.
