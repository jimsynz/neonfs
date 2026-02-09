# Event Notification and PubSub

NeonFS interface nodes (FUSE, S3, CIFS, etc.) currently make synchronous RPC calls to core nodes for every metadata operation. This document describes a cluster-wide event notification system that enables interface nodes to maintain local metadata caches with push-based invalidation, reducing RPC round-trips during normal operation while preserving correctness during failures.

## Problem

Without event notification, interface nodes face a choice between:

1. **No local cache** (current approach): Every `getattr`, `lookup`, `readdir`, etc. requires an RPC to a core node. Correct, but adds latency to every operation.
2. **TTL-based cache**: Cache metadata locally and expire after N seconds. Introduces a staleness window where writes on one node aren't visible from another.
3. **Poll-based cache**: Periodically query core for changes. Either polls too often (wasting resources) or too rarely (stale data).

The event notification system enables a fourth option: **cache with push invalidation**. Interface nodes cache metadata locally and receive immediate notification when it changes, giving them both performance and correctness during normal cluster operation.

## Design Principles

1. **Correctness over performance**: When in doubt, invalidate. A cache miss causes an RPC; a stale cache causes wrong behaviour.
2. **Events are hints, not data**: Events signal that something changed, not what changed. The subscriber decides whether to invalidate, re-fetch, or ignore.
3. **At-most-once delivery**: Events may be lost during network partitions. The recovery mechanism (full cache invalidation on reconnect) handles this.
4. **No new dependencies**: Uses OTP's built-in `:pg` module, which is already available and works over Erlang distribution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Core Nodes (Ra Cluster)                      │
│                                                                  │
│  FileIndex ──┐                                                   │
│  VolumeReg ──┤── broadcast to :pg groups after Ra writes         │
│  ChunkIndex ─┘                                                   │
│                                                                  │
│              :pg scope = :neonfs_events                           │
│              (membership auto-synced across cluster)              │
└──────────────────────────────┬────────────────────────────────────┘
                               │ Erlang distribution
          ┌────────────────────┼────────────────────┐
          │                    │                    │
   ┌──────┴──────┐     ┌──────┴──────┐     ┌──────┴──────┐
   │ FUSE node   │     │ S3 gateway  │     │ CIFS node   │
   │             │     │             │     │             │
   │ Subscriber  │     │ Subscriber  │     │ Subscriber  │
   │ joins :pg   │     │ joins :pg   │     │ joins :pg   │
   │ groups for  │     │ groups for  │     │ groups for  │
   │ mounted     │     │ accessed    │     │ shared      │
   │ volumes     │     │ buckets     │     │ volumes     │
   └─────────────┘     └─────────────┘     └─────────────┘
```

### Transport: OTP `:pg` (Process Groups)

`:pg` provides cluster-wide process group membership over Erlang distribution:

- Processes join named groups to express interest in events
- Group membership is automatically synchronised when nodes connect
- When a process terminates, it is automatically removed from all groups
- No external dependencies — `:pg` ships with OTP

Each NeonFS node starts a `:pg` scope as part of its supervision tree:

```elixir
# In both neonfs_core and neonfs_client application supervision trees
children = [
  {:pg, :neonfs_events},
  # ... other children
]
```

## Event Model

### Event Types

Events represent metadata state changes. They are intentionally minimal — just enough information for a subscriber to decide what to invalidate.

```elixir
@type event :: file_event() | directory_event() | volume_event()

@type file_event ::
  {:file_created, volume_id :: binary(), file_id :: binary(), path :: String.t()}
  | {:file_updated, volume_id :: binary(), file_id :: binary(), path :: String.t()}
  | {:file_deleted, volume_id :: binary(), file_id :: binary(), path :: String.t()}

@type directory_event ::
  {:dir_created, volume_id :: binary(), path :: String.t()}
  | {:dir_deleted, volume_id :: binary(), path :: String.t()}

@type volume_event ::
  {:volume_created, volume_id :: binary()}
  | {:volume_updated, volume_id :: binary()}
  | {:volume_deleted, volume_id :: binary()}
```

Events are wrapped in a standard envelope:

```elixir
@type event_envelope :: %{
  event: event(),
  source_node: node(),
  ra_version: non_neg_integer(),
  timestamp: integer()     # System.monotonic_time(:millisecond)
}
```

The `ra_version` field is the MetadataStateMachine version counter at the time of the event. Subscribers can use this to detect gaps (missed events) and trigger a full cache rebuild if needed.

### What is NOT an Event

The following state changes do **not** generate events:

- **Chunk operations** (`put_chunk`, `commit_chunk`, `delete_chunk`, write refs): These are internal to the storage engine. Interface nodes don't cache chunk metadata.
- **Service registry changes** (`register_service`, `deregister_service`): Already handled by `NeonFS.Client.Discovery` with its own polling and `nodeup`/`nodedown` mechanisms.
- **Metric updates** (`update_service_metrics`): Observability data, not user-facing state.

### Subscription Groups

Subscribers join `:pg` groups based on the scope of events they want to receive:

| Group Key | Events Received | Typical Subscriber |
|-----------|----------------|-------------------|
| `{:volume, volume_id}` | All file and directory events within the volume, plus volume config changes | FUSE mount process, S3 bucket handler |
| `{:volumes}` | Volume create/update/delete events only | Volume listing caches, mount managers |

Subscription is at the volume level. File-level and directory-level subscriptions are intentionally omitted to keep group count bounded — a FUSE mount subscribes to its volume and filters locally.

```elixir
# FUSE mount process subscribes when mounting a volume
def handle_mount(volume_id) do
  :pg.join(:neonfs_events, {:volume, volume_id}, self())
  :pg.join(:neonfs_events, {:volumes}, self())
  # ...
end

# Unsubscribe on unmount (also happens automatically if process exits)
def handle_unmount(volume_id) do
  :pg.leave(:neonfs_events, {:volume, volume_id}, self())
  :pg.leave(:neonfs_events, {:volumes}, self())
  # ...
end
```

## Broadcasting

### Where Events Originate

Events are broadcast by the wrapper modules on core nodes (FileIndex, VolumeRegistry) after a successful Ra write. This keeps the MetadataStateMachine pure (no side effects in `apply/3`) and places broadcasting at the level where we know the write has been committed and the local ETS cache has been updated.

```elixir
# In FileIndex, after a successful Ra write + ETS update
defp broadcast_event(volume_id, event) do
  envelope = %{
    event: event,
    source_node: node(),
    ra_version: get_current_ra_version(),
    timestamp: System.monotonic_time(:millisecond)
  }

  # Broadcast to volume subscribers
  for pid <- :pg.get_members(:neonfs_events, {:volume, volume_id}) do
    send(pid, {:neonfs_event, envelope})
  end

  # For volume-level events, also broadcast to the volumes group
  case event do
    {:volume_created, _} -> broadcast_to_volumes_group(envelope)
    {:volume_updated, _} -> broadcast_to_volumes_group(envelope)
    {:volume_deleted, _} -> broadcast_to_volumes_group(envelope)
    _ -> :ok
  end
end

defp broadcast_to_volumes_group(envelope) do
  for pid <- :pg.get_members(:neonfs_events, {:volumes}) do
    send(pid, {:neonfs_event, envelope})
  end
end
```

### Broadcast Semantics

- **Fire and forget**: `send/2` is non-blocking. If a subscriber's mailbox is full or the node is unreachable, the message is silently dropped.
- **No ordering guarantees across volumes**: Events for different volumes may arrive out of order. Events within a single volume are ordered by `ra_version`.
- **Single broadcaster per write**: Only the core node that initiated the Ra write broadcasts the event. Other core nodes see the Ra state change via log replication and update their local ETS, but do not re-broadcast.

## Subscribing

### Client-Side Event Handler

Interface nodes implement a behaviour for handling events. The subscriber process is typically the same process that owns the local metadata cache.

```elixir
defmodule NeonFS.Client.EventHandler do
  @callback handle_event(event_envelope :: map()) :: :ok
end
```

A reference implementation for FUSE:

```elixir
defmodule NeonFS.FUSE.MetadataCache do
  use GenServer
  @behaviour NeonFS.Client.EventHandler

  # Subscribe to the mounted volume's events
  def init(%{volume_id: volume_id}) do
    :pg.join(:neonfs_events, {:volume, volume_id}, self())
    :pg.join(:neonfs_events, {:volumes}, self())

    {:ok, %{volume_id: volume_id, cache: %{}, ra_version: 0}}
  end

  # Handle incoming events
  def handle_info({:neonfs_event, envelope}, state) do
    handle_event(envelope)
    {:noreply, state}
  end

  @impl NeonFS.Client.EventHandler
  def handle_event(%{event: {:file_updated, _vol, file_id, path}}) do
    # Invalidate cached metadata for this file
    invalidate_file(file_id, path)
  end

  def handle_event(%{event: {:file_created, _vol, _file_id, path}}) do
    # Invalidate parent directory listing cache
    invalidate_directory(Path.dirname(path))
  end

  def handle_event(%{event: {:file_deleted, _vol, file_id, path}}) do
    invalidate_file(file_id, path)
    invalidate_directory(Path.dirname(path))
  end

  def handle_event(%{event: {:dir_created, _vol, path}}) do
    invalidate_directory(Path.dirname(path))
  end

  def handle_event(%{event: {:dir_deleted, _vol, path}}) do
    invalidate_directory(path)
    invalidate_directory(Path.dirname(path))
  end

  def handle_event(_), do: :ok
end
```

## Partition Recovery

### Problem

During a network partition, events are lost — `:pg` broadcasts use `send/2` which silently drops messages to unreachable nodes. When the partition heals, a subscriber's cache may be arbitrarily stale.

### Strategy: Invalidate Everything on Reconnect

When a node detects it has been partitioned and subsequently reconnects, it **blows away all ephemeral caches**. This temporarily increases RPC load as caches are rebuilt from scratch, but guarantees correctness.

```elixir
defmodule NeonFS.Client.PartitionRecovery do
  use GenServer

  def init(_) do
    :net_kernel.monitor_nodes(true)
    {:ok, %{known_core_nodes: MapSet.new()}}
  end

  # A core node went down (potential partition)
  def handle_info({:nodedown, node}, state) do
    if core_node?(node) do
      {:noreply, %{state | known_core_nodes: MapSet.delete(state.known_core_nodes, node)}}
    else
      {:noreply, state}
    end
  end

  # A core node came back (partition healed or node restarted)
  def handle_info({:nodeup, node}, state) do
    if core_node?(node) and not MapSet.member?(state.known_core_nodes, node) do
      # We lost contact with a core node and regained it.
      # Assume we missed events — invalidate all caches.
      invalidate_all_caches()

      {:noreply, %{state | known_core_nodes: MapSet.put(state.known_core_nodes, node)}}
    else
      {:noreply, state}
    end
  end

  defp invalidate_all_caches do
    # Clear all cached metadata on this node.
    # Each cache module exposes a clear/0 function.
    NeonFS.FUSE.MetadataCache.clear()

    require Logger
    Logger.warning("Partition recovery: all metadata caches invalidated")
  end
end
```

### Core Node Recovery

When a core node reconnects after a partition:

1. **Ra handles state catch-up**: The Ra follower replays the leader's log to reach the current state. This is automatic.
2. **ETS caches are rebuilt from Ra**: After Ra catch-up completes, the core node's wrapper modules (FileIndex, ChunkIndex, VolumeRegistry) re-populate their ETS tables from the current Ra state.
3. **`:pg` group membership re-syncs**: OTP's `:pg` automatically merges group membership when nodes reconnect. No manual intervention needed.

```elixir
# In FileIndex, ChunkIndex, VolumeRegistry — called after Ra catch-up
def rebuild_from_ra do
  # Query current Ra state and repopulate ETS
  case :ra.local_query(server_id(), fn state -> state end) do
    {:ok, {_, state}, _} ->
      rebuild_ets_from_state(state)
      :ok

    {:error, reason} ->
      Logger.error("Failed to rebuild cache from Ra: #{inspect(reason)}")
      {:error, reason}
  end
end
```

### Handling Rapid Reconnection

To avoid thrashing (repeated invalidation during flapping connections), the recovery process debounces invalidation:

```elixir
@debounce_ms 5_000

def handle_info({:nodeup, node}, state) do
  if core_node?(node) and should_invalidate?(node, state) do
    # Debounce: wait before invalidating in case the connection flaps
    Process.send_after(self(), {:do_invalidate, node}, @debounce_ms)
    {:noreply, %{state | pending_invalidations: MapSet.put(state.pending_invalidations, node)}}
  else
    {:noreply, state}
  end
end

def handle_info({:do_invalidate, node}, state) do
  if MapSet.member?(state.pending_invalidations, node) do
    invalidate_all_caches()
    {:noreply, %{state | pending_invalidations: MapSet.delete(state.pending_invalidations, node)}}
  else
    # Node went down again before debounce expired — skip, will re-trigger on next nodeup
    {:noreply, state}
  end
end
```

## Gap Detection

While full invalidation on reconnect handles the common case, subscribers can optionally detect missed events during normal operation using the `ra_version` field:

```elixir
def handle_info({:neonfs_event, %{ra_version: version} = envelope}, state) do
  if version > state.ra_version + 1 do
    # Gap detected — we missed events. Invalidate everything.
    Logger.warning("Event gap detected: expected version #{state.ra_version + 1}, got #{version}")
    invalidate_all_caches()
    %{state | ra_version: version}
  else
    handle_event(envelope)
    %{state | ra_version: version}
  end
end
```

This provides defence in depth: partition recovery handles the macro case (full disconnection), and gap detection catches the micro case (individual dropped messages while still connected).

## Module Placement

| Module | Package | Role |
|--------|---------|------|
| Broadcasting (in FileIndex, VolumeRegistry) | `neonfs_core` | Emit events after Ra writes |
| `NeonFS.Client.EventHandler` | `neonfs_client` | Behaviour for event subscribers |
| `NeonFS.Client.PartitionRecovery` | `neonfs_client` | Monitor nodes, invalidate on reconnect |
| `NeonFS.FUSE.MetadataCache` | `neonfs_fuse` | FUSE-specific cache with event handling |

The `:pg` scope (`:neonfs_events`) is started in both `neonfs_core` and `neonfs_client` supervision trees. OTP's `:pg` handles the case where multiple nodes start the same scope — they automatically merge.

## What This Spec Does NOT Cover

- **Metadata cache implementation details**: The cache data structure, eviction policy, and lookup API for each interface node are implementation concerns, not specified here.
- **Chunk data caching**: This spec covers metadata event notification only. Caching of actual chunk data (blob content) is a separate concern with different invalidation semantics (content-addressed chunks are immutable, so they never need invalidation).
- **Cross-volume transactions**: Events are per-volume. There is no mechanism for atomic operations spanning multiple volumes, and therefore no cross-volume event ordering.
- **Persistent event log**: Events are ephemeral. There is no replay capability beyond what Ra's log provides. If a subscriber needs the current state, it queries the core node directly.

## Related Documents

- [Discovery](discovery.md) — Service discovery and node connectivity
- [Node Management](node-management.md) — Partition behaviour and recovery
- [Architecture](architecture.md) — System structure and supervision trees
- [Replication](replication.md) — Write flows that trigger events
