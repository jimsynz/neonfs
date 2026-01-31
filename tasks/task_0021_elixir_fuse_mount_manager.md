# Task 0021: Implement FUSE Mount Manager

## Status
Complete

## Phase
1 - Foundation

## Description
Implement a manager that coordinates FUSE mounts. This handles starting the FUSE server, associating it with a volume, managing the lifecycle, and providing a clean API for mounting/unmounting volumes.

## Acceptance Criteria
- [x] `NeonFS.FUSE.MountManager` GenServer
- [x] `mount/3` - (volume_name, mount_point, opts) -> {:ok, mount_id}
- [x] `unmount/1` - (mount_id) -> :ok
- [x] `list_mounts/0` - return all active mounts
- [x] `get_mount/1` - get mount info by id or path
- [x] Track active mounts in state
- [x] Start FUSE server via NIF on mount
- [x] Stop FUSE server on unmount
- [x] Handle FUSE server crashes (restart or report)
- [x] Verify mount point is valid directory
- [x] Prevent duplicate mounts to same point

## Mount Info
```elixir
%MountInfo{
  id: "mount_abc123",
  volume_name: "documents",
  mount_point: "/mnt/neonfs/documents",
  started_at: ~U[...],
  fuse_server: fuse_server_resource,
  handler_pid: pid
}
```

## Testing Strategy
- Unit tests:
  - Mount and unmount lifecycle
  - List mounts
  - Prevent duplicate mount points
  - Invalid mount point handling
- Integration tests (require FUSE):
  - Actually mount filesystem
  - Verify mount point accessible
  - Unmount and verify cleanup

## Dependencies
- task_0020_elixir_fuse_handler
- task_0012_fuse_mount_operations

## Files to Create/Modify
- `neonfs_fuse/lib/neon_fs/fuse/mount_manager.ex` (new)
- `neonfs_fuse/lib/neon_fs/fuse/mount_info.ex` (new)
- `neonfs_fuse/lib/neon_fs/fuse/application.ex` (add to supervision)
- `neonfs_fuse/test/neon_fs/fuse/mount_manager_test.exs` (new)

## Reference
- spec/deployment.md - mount CLI commands
- spec/architecture.md - Per-Volume Supervision

## Notes
Each mount gets its own handler process. The mount manager supervises these. This allows independent failure/recovery per mount.
