# Task 0013: Implement FUSE Write Operations

## Status
Complete

## Phase
1 - Foundation

## Description
Implement FUSE write operations: write, create, mkdir, unlink, rmdir, rename, setattr. These operations modify the filesystem and require careful coordination with the Elixir metadata layer.

## Acceptance Criteria
- [x] Implement `write` operation (inode, offset, data -> bytes written)
- [x] Implement `create` operation (parent, name, mode -> inode, handle)
- [x] Implement `mkdir` operation (parent, name, mode -> inode)
- [x] Implement `unlink` operation (parent, name -> success)
- [x] Implement `rmdir` operation (parent, name -> success)
- [x] Implement `rename` operation (old_parent, old_name, new_parent, new_name)
- [x] Implement `setattr` operation (inode, attributes to set)
- [x] Implement `open` and `release` for file handle management
- [x] All operations forward to Elixir for handling
- [x] Proper error handling and propagation

## File Handle Management
```rust
// Track open file handles
struct OpenFile {
    ino: u64,
    flags: u32,
    // Write buffer for streaming writes (optional optimisation)
}

fn open(&mut self, _req: &Request, ino: u64, flags: i32, reply: ReplyOpen) {
    let op = FuseOperation::Open { ino, flags };
    match self.call_elixir(op) {
        Ok(FuseReply::Open { fh }) => reply.opened(fh, 0),
        Ok(FuseReply::Error(errno)) => reply.error(errno),
        Err(_) => reply.error(libc::EIO),
    }
}
```

## Testing Strategy
- Integration tests (require FUSE mount):
  - Create file, verify exists
  - Write data, read back, verify content
  - Create directory, verify exists
  - Delete file, verify gone
  - Rename file, verify old gone, new exists
  - Note: Full tests require Elixir metadata implementation

## Dependencies
- task_0012_fuse_mount_operations

## Files to Create/Modify
- `neonfs_fuse/native/neonfs_fuse/src/filesystem.rs` (add write operations)
- `neonfs_fuse/native/neonfs_fuse/src/operation.rs` (add operation variants)
- `neonfs_fuse/native/neonfs_fuse/src/lib.rs` (if needed)

## Reference
- spec/architecture.md - FUSE data path
- spec/specification.md - Write Path (Simplified)

## Notes
The actual data storage and metadata management happens in Elixir. FUSE just translates POSIX calls to Elixir messages. Write buffering/batching could be added later for performance.
