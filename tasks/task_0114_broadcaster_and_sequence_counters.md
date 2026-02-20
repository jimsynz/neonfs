# Task 0114: Broadcaster and Sequence Counters

## Status
Complete

## Phase
10 - Event Notification

## Description
Implement `NeonFS.Events.Broadcaster` in the `neonfs_core` package. This module is responsible for wrapping events in `Envelope` structs (with source node, sequence number, and HLC timestamp) and broadcasting them to all `:pg` group members for the relevant volume. It also manages per-volume monotonically increasing sequence counters using `:atomics` for lock-free atomic increments and `:persistent_term` for fast counter lookups.

The Broadcaster is a stateless module (not a GenServer) — it provides a `broadcast/2` function called by index modules after successful metadata writes. The sequence counter state is stored externally in `:atomics` refs keyed by volume ID in `:persistent_term`.

## Acceptance Criteria
- [ ] New `NeonFS.Events.Broadcaster` module in `neonfs_core`
- [ ] `broadcast(volume_id, event)` — wraps event in Envelope, sends to all `:pg` members:
  - [ ] Gets or creates per-volume `:atomics` counter
  - [ ] Atomically increments sequence via `:atomics.add_get/3`
  - [ ] Gets HLC timestamp via `NeonFS.Core.HLC.now/1` (threading HLC state)
  - [ ] Constructs `%Envelope{event: event, source_node: node(), sequence: sequence, hlc_timestamp: hlc_timestamp}`
  - [ ] Sends `{:neonfs_event, envelope}` to all members of `:pg` group `{:volume, volume_id}`
  - [ ] For volume events (VolumeCreated/Updated/Deleted), also sends to `:pg` group `{:volumes}`
- [ ] Per-volume sequence counters:
  - [ ] `get_or_create_counter/1` — looks up `:atomics` ref in `:persistent_term` under `{__MODULE__, :counter, volume_id}`, creates if not found
  - [ ] `next_sequence/1` — atomically increments and returns the new value
  - [ ] Counters start at 0, first event gets sequence 1
- [ ] HLC state management:
  - [ ] Broadcaster maintains an HLC instance (via `:persistent_term` or process dictionary) for timestamp generation
  - [ ] HLC state is updated on each `broadcast/2` call to ensure monotonicity
- [ ] `broadcast/2` is fire-and-forget — uses `send/2`, never blocks on delivery
- [ ] `broadcast/2` returns `:ok` regardless of whether any subscribers exist
- [ ] Type specs on all public functions
- [ ] Unit tests for Broadcaster

## Testing Strategy
- ExUnit test: `broadcast/2` sends event to `:pg` group members (set up a test process in the `:pg` group, verify it receives `{:neonfs_event, %Envelope{}}`)
- ExUnit test: sequence numbers are monotonically increasing for the same volume
- ExUnit test: sequence counters are independent per volume (volume A and B have separate sequences)
- ExUnit test: envelope contains correct `source_node`, `sequence`, and `hlc_timestamp`
- ExUnit test: volume events are sent to both `{:volume, volume_id}` and `{:volumes}` groups
- ExUnit test: non-volume events are sent only to `{:volume, volume_id}` group
- ExUnit test: broadcasting with no subscribers does not error
- ExUnit test: `get_or_create_counter/1` is idempotent (calling twice returns the same counter)

## Dependencies
- task_0112 (Event structs and Envelope)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/events/broadcaster.ex` (new — Broadcaster module)
- `neonfs_core/test/neon_fs/events/broadcaster_test.exs` (new — unit tests)

## Reference
- spec/pubsub.md — Broadcasting section
- spec/pubsub.md — Broadcast Semantics section
- spec/pubsub.md — Event Envelope section (sequence counters)

## Notes
The Broadcaster is deliberately stateless (a module with functions, not a GenServer). This avoids serialising all event broadcasts through a single process bottleneck. Each index module calls `broadcast/2` from its own process, and the `:atomics` ref provides lock-free concurrent sequence numbering.

HLC state is trickier — `NeonFS.Core.HLC.now/1` returns `{timestamp, new_state}` and requires threading state. Options:
1. Use a per-process HLC (stored in process dictionary via `Process.get/put`) — each calling process gets its own HLC state. Simple, but timestamps from different processes on the same node may have slight differences.
2. Use a single HLC GenServer — provides globally ordered timestamps on the node but adds a bottleneck.
3. Use `:persistent_term` with a CAS-like pattern — complex but lock-free.

Option 1 (process dictionary) is recommended: each index GenServer already has its own process, and HLC timestamps only need to be comparable (not globally ordered on the same node). The per-source-node sequence counter provides the total ordering that subscribers need.

The `:pg` scope `:neonfs_events` must be running before `broadcast/2` is called. Since the Broadcaster is called by index modules which start after the `:pg` scope in the supervision tree, this ordering is guaranteed.

Note: `:atomics.add_get/3` performs an atomic add and returns the new value in a single operation. This is the recommended approach for sequence counters in the BEAM — no locks, no GenServer serialisation.
