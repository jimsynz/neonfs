# Task 0012: Implement FUSE Mount and Basic Operations

## Status
Complete

## Phase
1 - Foundation

## Description
Implement the actual FUSE filesystem mounting using the fuser library. This task implements the fuser::Filesystem trait with basic operations that forward to Elixir for handling. The FUSE thread blocks on kernel requests and forwards them via the channel.

## Acceptance Criteria
- [x] `NeonFilesystem` struct implementing `fuser::Filesystem` trait
- [x] `mount/3` NIF: mount point path, callback PID, options -> server resource
- [x] `unmount/1` NIF: gracefully unmount and cleanup
- [x] Implement `lookup` operation (name -> inode)
- [x] Implement `getattr` operation (inode -> attributes)
- [x] Implement `read` operation (inode, offset, size -> data)
- [x] Implement `readdir` operation (inode -> directory entries)
- [x] All operations forward to Elixir and await reply
- [x] Timeout handling for Elixir responses (don't hang FUSE forever)
- [x] FUSE errors mapped from Elixir error atoms (`:enoent` -> ENOENT)

## FUSE Filesystem Trait (Partial)
```rust
impl Filesystem for NeonFilesystem {
    fn lookup(&mut self, _req: &Request, parent: u64, name: &OsStr, reply: ReplyEntry) {
        let op = FuseOperation::Lookup { parent, name: name.to_string_lossy().into() };
        match self.call_elixir(op) {
            Ok(FuseReply::Entry(attrs, ttl)) => reply.entry(&ttl, &attrs, 0),
            Ok(FuseReply::Error(errno)) => reply.error(errno),
            Err(_) => reply.error(libc::EIO),
        }
    }
    // ... other operations
}
```

## Testing Strategy
- Rust unit tests:
  - Error code mapping
  - Operation serialisation
- Integration tests (require FUSE):
  - Mount filesystem in temp directory
  - Verify mount point exists
  - Unmount and verify cleanup
  - Note: Full read/write tests require Elixir integration

## Dependencies
- task_0011_fuse_channel_communication

## Files to Create/Modify
- `neonfs_fuse/native/neonfs_fuse/src/filesystem.rs` (new)
- `neonfs_fuse/native/neonfs_fuse/src/mount.rs` (new)
- `neonfs_fuse/native/neonfs_fuse/src/error.rs` (new/update)
- `neonfs_fuse/native/neonfs_fuse/src/lib.rs` (add modules, NIFs)
- `neonfs_fuse/lib/neon_fs/fuse/native.ex` (add mount/unmount)
- `neonfs_fuse/test/neon_fs/fuse/native_test.exs`

## Reference
- spec/architecture.md - FUSE Driver section
- fuser documentation: https://docs.rs/fuser

## Notes
Write operations (write, create, mkdir, etc.) are deferred to the next task. This task focuses on read-only operations that are simpler to test. FUSE tests may need to run as root or with appropriate capabilities.
