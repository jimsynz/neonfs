# Task 0021: Implement FUSE Mount Manager

## Status
Not Started

## Phase
1 - Foundation

## Description
Implement a manager that coordinates FUSE mounts. This handles starting the FUSE server, associating it with a volume, managing the lifecycle, and providing a clean API for mounting/unmounting volumes.

## Acceptance Criteria
- [ ] `NeonFS.FUSE.MountManager` GenServer
- [ ] `mount/3` - (volume_name, mount_point, opts) -> {:ok, mount_id}
- [ ] `unmount/1` - (mount_id) -> :ok
- [ ] `list_mounts/0` - return all active mounts
- [ ] `get_mount/1` - get mount info by id or path
- [ ] Track active mounts in state
- [ ] Start FUSE server via NIF on mount
- [ ] Stop FUSE server on unmount
- [ ] Handle FUSE server crashes (restart or report)
- [ ] Verify mount point is valid directory
- [ ] Prevent duplicate mounts to same point

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
