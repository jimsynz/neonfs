# Task 0116: Partition Recovery

## Status
Complete

## Phase
10 - Event Notification

## Description
Implement `NeonFS.Client.PartitionRecovery` GenServer in `neonfs_client`. This module monitors core node connections via `:net_kernel.monitor_nodes/2` and triggers full cache invalidation on interface nodes when a core node reconnects after being unreachable. This handles the case where events were lost during a network partition — since `:pg` broadcasts use `send/2` which silently drops messages to unreachable nodes.

The recovery strategy is aggressive but correct: invalidate all caches on reconnect, then let them rebuild from fresh RPC calls. This temporarily increases RPC load but guarantees correctness. A debounce mechanism prevents rapid invalidation during connection flapping.

## Acceptance Criteria
- [x] New `NeonFS.Client.PartitionRecovery` GenServer in `neonfs_client`
- [x] Calls `:net_kernel.monitor_nodes(true)` in `init/1` to receive `{:nodedown, node}` and `{:nodeup, node}` messages
- [x] Tracks known core nodes in state (`known_core_nodes :: MapSet.t(node())`)
- [x] On `{:nodedown, node}` — if the node is a core node, removes it from `known_core_nodes`
- [x] On `{:nodeup, node}` — if the node is a core node that was previously unknown/down:
  - [x] Schedules a debounced invalidation via `Process.send_after(self(), {:do_invalidate, node}, @debounce_ms)`
  - [x] Adds node to `pending_invalidations` set
  - [x] Adds node to `known_core_nodes`
- [x] `{:do_invalidate, node}` handler:
  - [x] If node is still in `pending_invalidations`, performs cache invalidation
  - [x] If node went down again before debounce expired, skips (will re-trigger on next nodeup)
  - [x] Removes node from `pending_invalidations`
- [x] Cache invalidation mechanism:
  - [x] Dispatches `:neonfs_invalidate_all` message to all processes registered in `NeonFS.Events.Registry`
  - [x] Uses `Registry.select/2` to collect all unique PIDs and iterates all Registry entries
  - [x] Also dispatches to each volume group and the `{:volumes}` group individually, or iterates all Registry entries
- [x] Debounce period configurable via application env `:neonfs_client, :partition_recovery_debounce_ms` (default 5_000)
- [x] Core node detection: uses a configurable predicate or checks if the node has `NeonFS.Core.ServiceRegistry` registered (via `Node.list/1` + `:erpc`)
- [x] Added to supervision trees:
  - [x] `NeonFS.FUSE.Supervisor` — add after `NeonFS.Events.Relay`
  - [x] `NeonFS.Core.Supervisor` — add after `NeonFS.Events.Relay` (core nodes also subscribe to events for their own local caches)
- [x] Type specs on all public functions
- [x] Unit tests for PartitionRecovery

## Testing Strategy
- ExUnit test: `init/1` starts monitoring nodes
- ExUnit test: `{:nodedown, core_node}` removes the node from known set
- ExUnit test: `{:nodeup, core_node}` schedules debounced invalidation
- ExUnit test: `{:do_invalidate, node}` sends invalidation messages via Registry
- ExUnit test: rapid nodedown + nodeup within debounce period — only one invalidation
- ExUnit test: nodeup + nodedown before debounce expires — no invalidation (node went away again)
- ExUnit test: non-core nodes are ignored (no invalidation triggered)
- ExUnit test: debounce period is configurable

## Dependencies
- task_0113 (Subscription API, Relay, and Registry — needs Registry for invalidation dispatch)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/client/partition_recovery.ex` (new — GenServer)
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (modify — add PartitionRecovery to children)
- `neonfs_fuse/lib/neon_fs/fuse/supervisor.ex` (modify — add PartitionRecovery to children)
- `neonfs_client/test/neon_fs/client/partition_recovery_test.exs` (new — unit tests)

## Reference
- spec/pubsub.md — Partition Recovery section
- spec/pubsub.md — Core Node Recovery section
- spec/node-management.md — Partition behaviour

## Notes
For unit testing, use `:net_kernel` mock or simulate nodeup/nodedown messages by directly sending them to the GenServer process. The GenServer handles standard `{:nodedown, node}` and `{:nodeup, node}` messages from `:net_kernel.monitor_nodes/1`.

The core node detection predicate should be configurable for testing. In production, the simplest approach is to check against a list of known core nodes from `NeonFS.Client.Discovery` or `NeonFS.Client.Connection`. A callback function in the init opts (`core_node?: fn node -> ... end`) makes testing straightforward.

The invalidation dispatch uses a broadcast approach: send `:neonfs_invalidate_all` to all local event subscribers. Each subscriber handles this message in its `handle_info` by clearing its cache. This is simpler than trying to identify which specific caches are affected by which core node — a full invalidation is correct and the cost (cache miss → RPC) is acceptable for what should be a rare event.

Gap detection (per-message sequence checking) is separate from partition recovery. Gap detection catches individual dropped messages while still connected. Partition recovery catches wholesale event loss during disconnection. They are complementary — see task 0117 for gap detection in the FUSE MetadataCache.
