# Task 0027: Implement FUSE Supervision Tree

## Status
Complete

## Phase
1 - Foundation

## Description
Design and implement the supervision tree for neonfs_fuse. This manages mount handlers and ensures proper cleanup when mounts fail.

## Acceptance Criteria
- [x] `NeonFS.FUSE.Application` starts supervision tree
- [x] `NeonFS.FUSE.Supervisor` as top-level supervisor
- [x] MountManager as supervised child
- [x] DynamicSupervisor for mount handlers
- [x] Each mount gets its own supervised handler process
- [x] Mount handler crash triggers unmount cleanup
- [x] Application depends on neonfs_core
- [x] Graceful shutdown unmounts all filesystems

## Supervision Tree
```
NeonFS.FUSE.Supervisor (one_for_one)
├── NeonFS.FUSE.MountManager
└── NeonFS.FUSE.MountSupervisor (DynamicSupervisor)
    ├── {Handler for mount 1}
    ├── {Handler for mount 2}
    └── ...
```

## Testing Strategy
- Application starts without errors
- Mount creates handler under DynamicSupervisor
- Handler crash triggers cleanup
- Shutdown unmounts all mounts
- Verify neonfs_core dependency

## Dependencies
- task_0020_elixir_fuse_handler
- task_0021_elixir_fuse_mount_manager
- task_0026_elixir_supervision_tree

## Files to Create/Modify
- `neonfs_fuse/lib/neon_fs/fuse/supervisor.ex` (new)
- `neonfs_fuse/lib/neon_fs/fuse/mount_supervisor.ex` (new)
- `neonfs_fuse/lib/neon_fs/fuse/application.ex` (update)
- `neonfs_fuse/mix.exs` (add neonfs_core dependency)
- `neonfs_fuse/test/neon_fs/fuse/supervisor_test.exs` (new)

## Reference
- spec/architecture.md - Per-Volume Supervision

## Notes
The DynamicSupervisor allows adding/removing mounts at runtime without affecting other mounts. Each mount is isolated.
