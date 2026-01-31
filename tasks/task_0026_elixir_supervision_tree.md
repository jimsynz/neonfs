# Task 0026: Implement Core Supervision Tree

## Status
Complete

## Phase
1 - Foundation

## Description
Design and implement the core supervision tree for neonfs_core. This establishes the process hierarchy, restart strategies, and ensures proper startup order for all components.

## Acceptance Criteria
- [x] `NeonFS.Core.Application` starts supervision tree
- [x] `NeonFS.Core.Supervisor` as top-level supervisor
- [x] BlobStore started first (other components depend on it)
- [x] ChunkIndex started after BlobStore
- [x] FileIndex started after ChunkIndex
- [x] VolumeRegistry started after FileIndex
- [x] CLI.Handler registered for RPC access
- [x] Proper restart strategies (one_for_one vs one_for_all)
- [x] Startup order enforced via child spec ordering
- [x] Application starts successfully with `mix run`
- [x] Graceful shutdown on SIGTERM

## Supervision Tree
```
NeonFS.Core.Supervisor (one_for_one)
├── NeonFS.Core.BlobStore
├── NeonFS.Core.ChunkIndex
├── NeonFS.Core.FileIndex
├── NeonFS.Core.VolumeRegistry
└── NeonFS.CLI.Handler (optional, if running as daemon)
```

## Testing Strategy
- Application starts without errors
- Each component is running after start
- Crash one component, verify it restarts
- Verify startup order via logging
- Graceful shutdown completes within timeout

## Dependencies
- task_0014_elixir_blob_wrapper
- task_0015_elixir_chunk_metadata
- task_0016_elixir_file_metadata
- task_0017_elixir_volume_config

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (new)
- `neonfs_core/lib/neon_fs/core/application.ex` (update)
- `neonfs_core/test/neon_fs/core/supervisor_test.exs` (new)

## Reference
- spec/architecture.md - Per-Volume Supervision section
- spec/architecture.md - Cluster Supervisor diagram

## Notes
Volume-specific supervisors (per the spec) come later when volumes have their own processes (tiering manager, scrubber, etc.). For Phase 1, the tree is simpler.
