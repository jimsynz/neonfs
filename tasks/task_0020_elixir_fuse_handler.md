# Task 0020: Implement FUSE Operation Handler

## Status
Complete

## Phase
1 - Foundation

## Description
Implement the Elixir GenServer that receives FUSE operations from the Rust NIF and dispatches them to the appropriate handlers. This bridges the FUSE crate with the core storage operations.

## Acceptance Criteria
- [x] `NeonFS.FUSE.Handler` GenServer receiving FUSE operations
- [x] Handle `{:fuse_op, ref, operation}` messages
- [x] Dispatch to read/write operations based on operation type
- [x] Map inode numbers to file paths/metadata
- [x] Implement `lookup` - name resolution
- [x] Implement `getattr` - return file attributes
- [x] Implement `read` - delegate to ReadOperation
- [x] Implement `write` - delegate to WriteOperation
- [x] Implement `readdir` - list directory contents
- [x] Implement `create`, `mkdir`, `unlink`, `rmdir`
- [x] Reply via NIF with results
- [x] Inode allocation and tracking
- [x] Root inode (1) handling

## Inode Management
```elixir
defmodule NeonFS.FUSE.InodeTable do
  # Map inodes <-> file paths
  # Inode 1 is always root directory
  def allocate_inode(path)
  def get_path(inode)
  def get_inode(path)
  def release_inode(inode)
end
```

## Testing Strategy
- Unit tests:
  - Handle lookup operation
  - Handle read operation
  - Handle write operation
  - Handle directory operations
  - Inode allocation/lookup
  - Error responses for missing files

## Dependencies
- task_0011_fuse_channel_communication
- task_0018_elixir_write_path
- task_0019_elixir_read_path

## Files to Create/Modify
- `neonfs_fuse/lib/neon_fs/fuse/handler.ex` (new)
- `neonfs_fuse/lib/neon_fs/fuse/inode_table.ex` (new)
- `neonfs_fuse/lib/neon_fs/fuse/application.ex` (add to supervision)
- `neonfs_fuse/test/neon_fs/fuse/handler_test.exs` (new)

## Reference
- spec/architecture.md - FUSE operations flow
- spec/data-model.md - File metadata structure

## Notes
The handler needs access to neonfs_core modules. Ensure proper application dependency is configured. This is where POSIX semantics meet NeonFS abstractions.
