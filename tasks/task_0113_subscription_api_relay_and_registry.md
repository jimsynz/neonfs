# Task 0113: Subscription API, Relay, and Registry

## Status
Complete

## Phase
10 - Event Notification

## Description
Implement the subscription side of the event notification system: the `NeonFS.Events` subscribe/unsubscribe API, the `NeonFS.Events.Relay` GenServer for `:pg` membership management and local `Registry` dispatch, and the `NeonFS.Client.EventHandler` behaviour. Also update the supervision trees in both `neonfs_core` and `neonfs_fuse` to start the `:pg` scope (`:neonfs_events`), the `NeonFS.Events.Registry` (OTP `Registry` with `keys: :duplicate`), and the `NeonFS.Events.Relay` process.

The two-layer dispatch architecture uses `:pg` for cross-node messaging (one relay process per volume per node in the `:pg` group) and `Registry` for node-local fan-out (each subscribing process registers in the local Registry). This means if a node has 5 FUSE mounts on the same volume, only one message crosses the network.

## Acceptance Criteria
- [ ] `NeonFS.Events` module extended with public functions:
  - [ ] `subscribe(volume_id)` — registers calling process in local Registry under `{:volume, volume_id}`, tells Relay to join `:pg` group if not already joined
  - [ ] `unsubscribe(volume_id)` — unregisters from Registry, tells Relay to leave `:pg` group if no more local subscribers
  - [ ] `subscribe_volumes()` — registers calling process in local Registry under `{:volumes}` (no `:pg` join needed — Relay always joins the volumes group)
  - [ ] `unsubscribe_volumes()` — unregisters from Registry under `{:volumes}`
- [ ] `NeonFS.Events.Relay` GenServer in `neonfs_client`:
  - [ ] Joins `:pg` group `{:volumes}` on init (volume lifecycle events are always relayed)
  - [ ] Tracks per-volume reference counts in state (`volume_refs :: %{binary() => non_neg_integer()}`)
  - [ ] `ensure_volume_group/1` — increments ref count, joins `:pg` group `{:volume, volume_id}` on first subscriber
  - [ ] `maybe_leave_volume_group/1` — decrements ref count, leaves `:pg` group when count reaches zero
  - [ ] Handles `{:neonfs_event, %Envelope{}}` messages by dispatching via `Registry.dispatch/3` to the appropriate group key
  - [ ] Routes volume events (VolumeCreated/Updated/Deleted) to `{:volumes}` group
  - [ ] Routes all other events to `{:volume, volume_id}` group
- [ ] `NeonFS.Client.EventHandler` behaviour in `neonfs_client`:
  - [ ] `@callback handle_event(envelope :: NeonFS.Events.Envelope.t()) :: :ok`
- [ ] Supervision tree updates:
  - [ ] `NeonFS.Core.Supervisor` — add `{:pg, :neonfs_events}` and `{Registry, keys: :duplicate, name: NeonFS.Events.Registry}` and `NeonFS.Events.Relay` before the index modules
  - [ ] `NeonFS.FUSE.Supervisor` — add `{:pg, :neonfs_events}` and `{Registry, keys: :duplicate, name: NeonFS.Events.Registry}` and `NeonFS.Events.Relay` after client connectivity but before FUSE-specific modules
- [ ] Automatic cleanup: when a subscribing process exits, `Registry` automatically unregisters it (OTP behaviour). Relay reference counts are adjusted via a monitor-based cleanup mechanism
- [ ] Type specs on all public functions
- [ ] Unit tests for Relay and subscription API

## Testing Strategy
- ExUnit test: `subscribe/1` registers the calling process in the local Registry
- ExUnit test: `unsubscribe/1` removes the registration
- ExUnit test: `subscribe_volumes/0` and `unsubscribe_volumes/0` work correctly
- ExUnit test: Relay joins `:pg` group on first subscriber for a volume
- ExUnit test: Relay leaves `:pg` group when last subscriber unsubscribes
- ExUnit test: Relay dispatches received `{:neonfs_event, envelope}` to local subscribers via Registry
- ExUnit test: volume events are dispatched to `{:volumes}` subscribers
- ExUnit test: file events are dispatched to `{:volume, volume_id}` subscribers
- ExUnit test: subscriber process exit triggers automatic unregistration
- ExUnit test: multiple subscribers for the same volume — Relay joins `:pg` once, all receive events

## Dependencies
- task_0112 (Event structs and Envelope)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/events.ex` (modify — add subscribe/unsubscribe functions)
- `neonfs_client/lib/neon_fs/events/relay.ex` (new — Relay GenServer)
- `neonfs_client/lib/neon_fs/client/event_handler.ex` (new — behaviour)
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (modify — add `:pg` scope, Registry, Relay to children)
- `neonfs_fuse/lib/neon_fs/fuse/supervisor.ex` (modify — add `:pg` scope, Registry, Relay to children)
- `neonfs_client/test/neon_fs/events/relay_test.exs` (new — unit tests)
- `neonfs_client/test/neon_fs/events/subscription_test.exs` (new — subscribe/unsubscribe tests)

## Reference
- spec/pubsub.md — Subscription Model section
- spec/pubsub.md — Two-Layer Dispatch: `:pg` + `Registry` section
- spec/pubsub.md — Relay Process section
- spec/pubsub.md — Module Placement table

## Notes
The `:pg` scope `:neonfs_events` is started in both `neonfs_core` and `neonfs_client` application supervision trees (via the consuming application's supervisor — core and fuse). OTP's `:pg` handles the case where multiple nodes start the same scope — they automatically merge group membership when nodes connect.

The Relay process must handle the case where `:pg` is not yet started (e.g., during supervision tree startup ordering). Since `:pg` and `Registry` are started before the Relay in the supervision tree, this should be handled by start ordering, but defensive coding (catching exits from `:pg.join/3`) is worthwhile.

For process exit cleanup: `Registry` handles unregistration automatically when the registering process dies. However, the Relay's reference counts need separate cleanup. The Relay should monitor subscribing processes (via `Process.monitor/1` during `ensure_volume_group/1`) and decrement counts in `handle_info({:DOWN, ...})`. Alternatively, the Relay can periodically reconcile counts against Registry entries.

The `EventHandler` behaviour is optional — subscribers can handle `{:neonfs_event, envelope}` messages directly in their `handle_info` without implementing the behaviour. The behaviour is provided for documentation and Dialyzer.
