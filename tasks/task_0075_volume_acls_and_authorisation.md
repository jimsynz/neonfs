# Task 0075: Volume ACLs and Authorisation

## Status
Not Started

## Phase
6 - Security

## Description
Implement volume-level access control with an `Authorise` module that enforces UID/GID-based permissions on volume operations. Each volume has an ACL (stored in Ra via MSM v6) defining which UIDs and GIDs have read, write, or admin permissions. The volume owner UID has implicit full control. All volume operations (read, write, create, delete, mount) are checked against the ACL before proceeding. An `ACLManager` GenServer with ETS cache (following the ChunkIndex/StripeIndex pattern) provides fast permission lookups.

## Acceptance Criteria
- [ ] New `NeonFS.Core.Authorise` module with `check/3` function: `check(uid, action, resource)` returns `:ok` or `{:error, :forbidden}`
- [ ] `check/4` variant accepts supplementary GIDs: `check(uid, gids, action, resource)`
- [ ] Actions: `:read`, `:write`, `:admin`, `:mount`
- [ ] Volume owner UID always has all permissions (implicit admin)
- [ ] Permission inheritance: `:admin` implies `:write` implies `:read`
- [ ] GID matching: if a `{:gid, gid}` entry exists and the requesting UID's GID (or supplementary GIDs) matches, permission is granted
- [ ] New `NeonFS.Core.ACLManager` GenServer with public ETS table for cached ACLs
- [ ] `ACLManager.set_volume_acl/2` creates or replaces volume ACL via Ra
- [ ] `ACLManager.grant/3` adds permission entry: `grant(volume_id, principal, permissions)`
- [ ] `ACLManager.revoke/2` removes permission entry: `revoke(volume_id, principal)`
- [ ] `ACLManager.get_volume_acl/1` returns current ACL from ETS cache
- [ ] ACLManager restores from Ra on startup
- [ ] WriteOperation, ReadOperation, and MountManager call `Authorise.check/3` before proceeding
- [ ] Volume creation sets creator UID as owner in the ACL (UID from the CLI caller or default 0)
- [ ] Volume deletion requires `:admin` permission
- [ ] Authorisation failures return `{:error, :forbidden}` with the action and resource for debugging
- [ ] UID 0 (root) bypasses all permission checks
- [ ] Added to core supervision tree
- [ ] Persistence module updated to snapshot ACL ETS table
- [ ] Telemetry events: `[:neonfs, :authorise, :granted]` and `[:neonfs, :authorise, :denied]`
- [ ] Unit tests for permission checking (direct UID, via GID, owner bypass, root bypass, permission inheritance)
- [ ] Unit tests for ACL CRUD operations
- [ ] Unit tests for authorisation integration in write/read/mount paths

## Testing Strategy
- ExUnit tests for Authorise.check with various UID/GID/permission combinations
- ExUnit tests for permission inheritance (admin → write → read)
- ExUnit tests for owner bypass (owner UID always authorised)
- ExUnit tests for root bypass (UID 0 always authorised)
- ExUnit tests for ACLManager CRUD operations and ETS cache sync
- ExUnit tests verifying WriteOperation rejects unauthorised writes
- ExUnit tests verifying ReadOperation rejects unauthorised reads
- ExUnit tests verifying MountManager rejects unauthorised mounts

## Dependencies
- task_0069 (ACL types — VolumeACL struct)
- task_0070 (MetadataStateMachine v6 — volume ACL Ra commands)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/authorise.ex` (new — permission checking)
- `neonfs_core/lib/neon_fs/core/acl_manager.ex` (new — GenServer with ETS cache, Ra-backed CRUD)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add ACLManager to supervision tree)
- `neonfs_core/lib/neon_fs/core/persistence.ex` (modify — snapshot ACL table)
- `neonfs_core/lib/neon_fs/core/write_operation.ex` (modify — add authorisation check)
- `neonfs_core/lib/neon_fs/core/read_operation.ex` (modify — add authorisation check)
- `neonfs_core/lib/neon_fs/fuse/mount_manager.ex` (modify — add authorisation check)
- `neonfs_core/test/neon_fs/core/authorise_test.exs` (new)
- `neonfs_core/test/neon_fs/core/acl_manager_test.exs` (new)

## Reference
- spec/security.md — Volume Access Control section
- spec/security.md — Authorisation Check section
- Existing pattern: StripeIndex GenServer with ETS + Ra

## Notes
The authorisation check is on the hot path (every read/write), so it must be fast. ACL data is cached in ETS — permission checks are local lookups, no Ra round-trip. For FUSE operations, the requesting UID comes from the FUSE request context (the kernel provides it). For CLI operations, the requesting UID is the UID of the CLI process (obtained via the Erlang distribution connection context or passed explicitly). UID 0 bypass is standard POSIX behaviour — root can do anything. The supplementary GIDs for a FUSE request come from the `fuse_ctx` (if available) or can be resolved on the FUSE node — this is the interface layer's responsibility, not the core's.
