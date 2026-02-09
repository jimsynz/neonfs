# Task 0051: Drive Power Management

## Status
Complete

## Phase
3 - Policies, Tiering, and Compression

## Description
Implement per-drive `DriveState` GenServer with a state machine for power management (active → idle → spinning_down → standby → spinning_up → active). Use a pluggable command behaviour for spin-down/spin-up operations so the logic is testable without physical HDDs. Concurrent readers waiting for a spin-up share the same spin-up operation rather than issuing duplicate commands.

## Acceptance Criteria
- [x] `DriveCommand` behaviour with callbacks: `spin_down/1` (path → `:ok`/error), `spin_up/1` (path → `:ok`/error), `check_state/1` (path → `:active`/`:standby`)
- [x] `DriveCommand.Default` implementation using `hdparm` / `sdparm` system commands
- [x] `DriveCommand.Test` implementation that tracks calls and returns configurable results
- [x] `DriveState` GenServer per drive with states: `:active`, `:idle`, `:spinning_down`, `:standby`, `:spinning_up`
- [x] Idle timeout: after configurable period of no I/O, transition active → idle → spinning_down → standby
- [x] Spin-up on read: when a read targets a standby drive, spin up first, then serve
- [x] Concurrent spin-up coalescing: multiple readers block on the same spin-up, not duplicate commands
- [x] `DriveState.ensure_active/1` — returns `:ok` when drive is active, blocks during spin-up, initiates spin-up from standby
- [x] `DriveState.record_io/1` — resets idle timer (called by BlobStore on every read/write)
- [x] `DriveState.get_state/1` — returns current power state
- [x] DriveRegistry updated with power state changes
- [x] Drives without `power_management: true` in config skip all power management (always `:active`)
- [x] Telemetry events for state transitions
- [x] All state transitions have unit tests

## Testing Strategy
- Unit tests using `DriveCommand.Test` implementation
- Test full state machine cycle: active → idle → standby → spin-up → active
- Test concurrent spin-up coalescing (multiple callers, one spin-up)
- Test idle timeout triggers correctly
- Test that SSDs (power_management: false) stay permanently active
- Test error handling: spin-up failure, spin-down failure

## Dependencies
- task_0047 (needs DriveRegistry for state reporting)
- task_0050 (integrates with read scoring — standby drives ranked lower)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/drive_state.ex` (new — per-drive GenServer)
- `neonfs_core/lib/neon_fs/core/drive_command.ex` (new — behaviour + default + test impls)
- `neonfs_core/lib/neon_fs/core/drive_registry.ex` (modify — power state updates)
- `neonfs_core/lib/neon_fs/core/blob_store.ex` (modify — call `record_io/1` on operations)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — start DriveState per drive)
- `neonfs_core/test/neon_fs/core/drive_state_test.exs` (new)
- `neonfs_core/test/neon_fs/core/drive_command_test.exs` (new)

## Reference
- spec/architecture.md — Drive power management
- spec/implementation.md — Phase 3: Spin-down/spin-avoidance

## Notes
The concurrent spin-up coalescing is the trickiest part. Pattern: the first caller starts the spin-up and stores a reference in state. Subsequent callers receive the same reference and `receive` on it. When spin-up completes, all waiters are notified. This avoids issuing multiple `hdparm -Y` commands to the same drive. The idle timeout should be generous (default 30 minutes) to avoid excessive spin-up/spin-down cycling which reduces drive lifespan.
