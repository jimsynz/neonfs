# Task 0011: Implement FUSE-Elixir Channel Communication

## Status
Complete

## Phase
1 - Foundation

## Description
Implement the communication channel between the FUSE Rust thread and Elixir processes. FUSE operations run in a dedicated Rust thread and communicate with Elixir via channels. This task sets up the message passing infrastructure without implementing actual FUSE operations.

## Acceptance Criteria
- [x] `FuseServer` struct managing the FUSE session and channels
- [x] `FuseOperation` enum representing operations (Read, Write, Lookup, etc.)
- [x] `FuseReply` enum for responses back to FUSE
- [x] `Rustler` Resource wrapping FuseServer for Elixir to hold
- [x] `start_fuse_server/2` NIF that spawns FUSE thread and returns resource
- [x] `stop_fuse_server/1` NIF that signals shutdown
- [x] Channel from FUSE thread to Elixir (operation requests)
- [x] Channel from Elixir to FUSE thread (operation replies)
- [x] Operations sent to Elixir process via `send` with ref for reply matching
- [x] `reply_fuse_operation/3` NIF to send reply back to FUSE thread
- [x] Graceful shutdown when Elixir process terminates

## Message Format
```elixir
# FUSE -> Elixir
{:fuse_op, ref, {:read, %{ino: 1, offset: 0, size: 4096}}}
{:fuse_op, ref, {:lookup, %{parent: 1, name: "file.txt"}}}

# Elixir -> FUSE (via reply NIF)
NeonFS.FUSE.Native.reply(server, ref, {:ok, data})
NeonFS.FUSE.Native.reply(server, ref, {:error, :enoent})
```

## Testing Strategy
- Rust unit tests:
  - Channel creation and message passing
  - Shutdown signal handling
- Elixir tests:
  - Start server, receive mock operation, send reply
  - Stop server gracefully
  - Server cleanup when Elixir process dies

## Dependencies
- task_0010_fuse_crate_scaffolding

## Files to Create/Modify
- `neonfs_fuse/native/neonfs_fuse/src/channel.rs` (new)
- `neonfs_fuse/native/neonfs_fuse/src/operation.rs` (new)
- `neonfs_fuse/native/neonfs_fuse/src/server.rs` (new)
- `neonfs_fuse/native/neonfs_fuse/src/lib.rs` (add modules, NIFs)
- `neonfs_fuse/lib/neon_fs/fuse/native.ex` (add functions)
- `neonfs_fuse/test/neon_fs/fuse/native_test.exs`

## Reference
- spec/architecture.md - neonfs_fuse NIF section
- spec/architecture.md - FUSE operations flow diagram

## Notes
This task doesn't mount an actual filesystem - it just sets up the communication infrastructure. The actual FUSE mounting and operation handling come in subsequent tasks. Using tokio channels for the Rust side.
