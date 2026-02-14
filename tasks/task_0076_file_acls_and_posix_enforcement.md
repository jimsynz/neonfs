# Task 0076: File/Directory ACLs and POSIX Enforcement

## Status
Complete

## Phase
6 - Security

## Description
Implement file and directory-level access control with POSIX mode enforcement and extended ACL entries. The FUSE handler enforces POSIX mode bits (already stored in FileMeta as `mode`, `uid`, `gid`) using the `FileACL.check_access/4` function from task 0069. Extended ACL entries allow granting specific UIDs/GIDs access beyond what mode bits permit. ACL inheritance allows directories to define default ACLs that apply to newly created children. All checks use numeric UIDs/GIDs — no name resolution.

## Acceptance Criteria
- [x] `Authorise.check/3` extended to handle `{:file, volume_id, path}` resources by checking file-level ACLs
- [x] POSIX mode enforcement: `mode` bits checked against requesting UID/GID using `FileACL.check_access/4`
- [x] Extended ACL entries stored in FileMeta metadata (extend FileMeta with `acl_entries` field, default empty list)
- [x] `ACLManager.set_file_acl/3` sets extended ACL entries on a file or directory
- [x] `ACLManager.get_file_acl/2` retrieves file ACL (mode + extended entries)
- [x] Directory default ACLs: directories can have a `default_acl` that is applied to newly created children
- [x] `WriteOperation` applies directory default ACL when creating new files (inherits parent directory's default ACL as the new file's ACL)
- [x] FUSE handler integration: permission checks before allowing read, write, unlink, rename, mkdir, rmdir operations
- [x] FUSE handler: `setattr` respects ownership rules (only owner UID or root can chmod/chown)
- [x] Default when no explicit ACL set: volume owner UID = file owner, mode = 0644 for files, 0755 for directories
- [x] MetadataStateMachine handles file ACL storage (stored as part of file metadata, not separate Ra state)
- [x] CLI handler: `handle_set_file_acl/3`, `handle_get_file_acl/2` for managing extended ACLs
- [x] Unit tests for POSIX mode checking (owner, group, other)
- [x] Unit tests for extended ACL with mask interaction
- [x] Unit tests for directory ACL inheritance
- [x] Unit tests for FUSE handler permission enforcement

## Testing Strategy
- ExUnit tests for POSIX mode bit checking (rwx for owner, group, other)
- ExUnit tests for extended ACL entries (named UID, named GID, mask)
- ExUnit tests for mask interaction (mask limits named entries but not owner)
- ExUnit tests for directory default ACL inheritance on file creation
- ExUnit tests for FUSE handler permission enforcement
- Test that root (UID 0) bypasses file-level checks
- Test newly created files inherit directory default ACL

## Dependencies
- task_0075 (Volume ACLs — Authorise module, ACLManager base)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/authorise.ex` (modify — add file-level permission checking)
- `neonfs_core/lib/neon_fs/core/acl_manager.ex` (modify — add file ACL CRUD, directory defaults)
- `neonfs_client/lib/neon_fs/core/file_meta.ex` (modify — add `acl_entries` and `default_acl` fields)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — apply directory default ACL on file creation)
- `neonfs_fuse/lib/neon_fs/fuse/handler.ex` (modify — enforce permissions in FUSE callbacks)
- `neonfs_core/lib/neon_fs/core/cli/handler.ex` (modify — add file ACL handler functions)
- `neonfs_core/test/neon_fs/core/authorise_file_test.exs` (new)

## Reference
- spec/security.md — File/Directory ACLs (Optional) section
- POSIX.1e ACL specification (for mask and effective permission semantics)

## Notes
POSIX ACL semantics are subtle. Key rules: (1) If no extended ACL entries exist, standard POSIX mode bits apply. (2) When extended entries exist, the group class permission bits in mode become the mask. (3) The mask limits effective permissions of named users and named groups, but never limits the owner class or other class. The FUSE handler changes are in neonfs_fuse which has no dependency on neonfs_core — permission checks must go through `NeonFS.Client.core_call/3` to the core node's Authorise module. For performance, consider having the FUSE node cache file ACLs locally (in ETS with a TTL) to avoid a core_call on every operation — invalidation can use the existing PubSub event system.
