# Event Notification

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
4. **No new dependencies**: Uses OTP's built-in `:pg` and `Registry` modules.

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Core Nodes                                    │
│                                                                   │
│  FileIndex ──┐                                                    │
│  VolumeReg ──┤── broadcast to :pg relay processes                 │
│  ChunkIndex ─┘   after successful metadata writes                 │
│                                                                   │
│              :pg scope = :neonfs_events                            │
│              (membership auto-synced across cluster)               │
└──────────────────────────────┬────────────────────────────────────┘
                               │ Erlang distribution
                               │ (one message per node per event)
          ┌────────────────────┼────────────────────┐
          │                    │                    │
   ┌──────┴──────┐     ┌──────┴──────┐     ┌──────┴──────┐
   │ FUSE node   │     │ S3 gateway  │     │ CIFS node   │
   │             │     │             │     │             │
   │ Relay proc  │     │ Relay proc  │     │ Relay proc  │
   │ receives    │     │ receives    │     │ receives    │
   │ :pg message │     │ :pg message │     │ :pg message │
   │      │      │     │      │      │     │      │      │
   │  Registry   │     │  Registry   │     │  Registry   │
   │  fans out   │     │  fans out   │     │  fans out   │
   │  locally    │     │  locally    │     │  locally    │
   └─────────────┘     └─────────────┘     └─────────────┘
```

### Two-Layer Dispatch: `:pg` + `Registry`

The notification system uses a two-layer architecture to minimise cross-node traffic:

**Cross-node layer (`:pg`)**: A single relay process per volume per node joins the `:pg` group for that volume. When a core node broadcasts an event, it sends one message per member of the `:pg` group — which is one message per interested node, not one per subscriber.

**Node-local layer (`Registry`)**: The relay process dispatches received events to all local subscribers via `Registry.dispatch/3`. This fans out within the node using ETS lookups with no network traffic.

This means if a node has 5 FUSE mounts on the same volume, only one message crosses the network. The relay process then dispatches to all 5 locally.

```elixir
# In both neonfs_core and neonfs_client application supervision trees
children = [
  {:pg, :neonfs_events},
  {Registry, keys: :duplicate, name: NeonFS.Events.Registry},
  # ... other children
]
```

## Event Model

### Event Types

Events are defined as structs under the `NeonFS.Events` namespace. Each struct enforces exactly the fields relevant to that event type, with `@enforce_keys` preventing accidental omission. Every event struct includes a `volume_id` field for uniform routing.

#### File Content Events

Emitted when file data changes.

```elixir
defmodule NeonFS.Events.FileCreated do
  @enforce_keys [:volume_id, :file_id, :path]
  defstruct [:volume_id, :file_id, :path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    file_id: binary(),
    path: String.t()
  }
end

defmodule NeonFS.Events.FileContentUpdated do
  @enforce_keys [:volume_id, :file_id, :path]
  defstruct [:volume_id, :file_id, :path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    file_id: binary(),
    path: String.t()
  }
end

defmodule NeonFS.Events.FileTruncated do
  @enforce_keys [:volume_id, :file_id, :path]
  defstruct [:volume_id, :file_id, :path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    file_id: binary(),
    path: String.t()
  }
end

defmodule NeonFS.Events.FileDeleted do
  @enforce_keys [:volume_id, :file_id, :path]
  defstruct [:volume_id, :file_id, :path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    file_id: binary(),
    path: String.t()
  }
end
```

#### File Attribute Events

Emitted when file metadata changes without content modification.

```elixir
defmodule NeonFS.Events.FileAttrsChanged do
  @moduledoc """
  Covers chmod, chown, utimens, and extended attribute (xattr) modifications.
  Subscribers invalidate cached getattr results for the file. These are not
  split into separate event types because the subscriber response is the same:
  invalidate the cached stat for this file.
  """
  @enforce_keys [:volume_id, :file_id, :path]
  defstruct [:volume_id, :file_id, :path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    file_id: binary(),
    path: String.t()
  }
end

defmodule NeonFS.Events.FileRenamed do
  @moduledoc """
  Carries both paths so subscribers can invalidate the old path's cache entry
  and the parent directory listings for both old and new locations.
  """
  @enforce_keys [:volume_id, :file_id, :old_path, :new_path]
  defstruct [:volume_id, :file_id, :old_path, :new_path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    file_id: binary(),
    old_path: String.t(),
    new_path: String.t()
  }
end
```

#### ACL Events

Emitted when access control changes. These are distinct from attribute events because ACL changes can affect permission checks for many files, not just the target.

```elixir
defmodule NeonFS.Events.VolumeAclChanged do
  @moduledoc """
  Volume access control list was modified (user/group added, removed, or
  permissions changed). Subscribers should re-evaluate cached permission
  decisions for the volume.
  """
  @enforce_keys [:volume_id]
  defstruct [:volume_id]

  @type t :: %__MODULE__{volume_id: binary()}
end

defmodule NeonFS.Events.FileAclChanged do
  @moduledoc """
  POSIX ACL change on a specific file or directory.
  """
  @enforce_keys [:volume_id, :path]
  defstruct [:volume_id, :path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    path: String.t()
  }
end
```

#### Directory Events

```elixir
defmodule NeonFS.Events.DirCreated do
  @enforce_keys [:volume_id, :path]
  defstruct [:volume_id, :path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    path: String.t()
  }
end

defmodule NeonFS.Events.DirDeleted do
  @enforce_keys [:volume_id, :path]
  defstruct [:volume_id, :path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    path: String.t()
  }
end

defmodule NeonFS.Events.DirRenamed do
  @enforce_keys [:volume_id, :old_path, :new_path]
  defstruct [:volume_id, :old_path, :new_path]

  @type t :: %__MODULE__{
    volume_id: binary(),
    old_path: String.t(),
    new_path: String.t()
  }
end
```

#### Volume Events

```elixir
defmodule NeonFS.Events.VolumeCreated do
  @enforce_keys [:volume_id]
  defstruct [:volume_id]

  @type t :: %__MODULE__{volume_id: binary()}
end

defmodule NeonFS.Events.VolumeUpdated do
  @enforce_keys [:volume_id]
  defstruct [:volume_id]

  @type t :: %__MODULE__{volume_id: binary()}
end

defmodule NeonFS.Events.VolumeDeleted do
  @enforce_keys [:volume_id]
  defstruct [:volume_id]

  @type t :: %__MODULE__{volume_id: binary()}
end
```

#### Combined Type

```elixir
@type event ::
  NeonFS.Events.FileCreated.t()
  | NeonFS.Events.FileContentUpdated.t()
  | NeonFS.Events.FileTruncated.t()
  | NeonFS.Events.FileDeleted.t()
  | NeonFS.Events.FileAttrsChanged.t()
  | NeonFS.Events.FileRenamed.t()
  | NeonFS.Events.VolumeAclChanged.t()
  | NeonFS.Events.FileAclChanged.t()
  | NeonFS.Events.DirCreated.t()
  | NeonFS.Events.DirDeleted.t()
  | NeonFS.Events.DirRenamed.t()
  | NeonFS.Events.VolumeCreated.t()
  | NeonFS.Events.VolumeUpdated.t()
  | NeonFS.Events.VolumeDeleted.t()
```

### Event Envelope

Events are wrapped in a standard envelope struct:

```elixir
defmodule NeonFS.Events.Envelope do
  @enforce_keys [:event, :source_node, :sequence, :hlc_timestamp]
  defstruct [:event, :source_node, :sequence, :hlc_timestamp]

  @type t :: %__MODULE__{
    event: NeonFS.Events.event(),
    source_node: node(),
    sequence: non_neg_integer(),
    hlc_timestamp: NeonFS.HLC.t()
  }
end
```

- **`sequence`**: A per-volume monotonically increasing counter maintained by the broadcasting module. Used for gap detection — if a subscriber receives sequence 5 then 7, it knows it missed sequence 6.
- **`hlc_timestamp`**: The Hybrid Logical Clock timestamp at the time of the event. Comparable across nodes (unlike `System.monotonic_time`). Used for ordering events from different sources and for debugging.

The sequence counter is independent of any backing store. The broadcasting module on the core node that initiates a write increments its volume-local counter before broadcasting. This is cheaper and more reliable than extracting a version from Ra or the quorum store.

### What is NOT an Event

The following state changes do **not** generate events:

- **Chunk operations** (`put_chunk`, `commit_chunk`, `delete_chunk`, write refs): Internal to the storage engine. Interface nodes don't cache chunk metadata.
- **Service registry changes** (`register_service`, `deregister_service`): Already handled by `NeonFS.Client.Discovery` with its own polling and `nodeup`/`nodedown` mechanisms.
- **Metric updates** (`update_service_metrics`): Observability data, not user-facing state.

## Subscription Model

### Groups and Registration

Subscription is at the volume level. File-level and directory-level subscriptions are intentionally omitted to keep group count bounded — a subscriber that cares about one file still subscribes to the whole volume and filters locally.

| Layer | Mechanism | Key | Who Joins |
|-------|-----------|-----|-----------|
| Cross-node | `:pg` group | `{:volume, volume_id}` | One relay process per volume per node |
| Cross-node | `:pg` group | `{:volumes}` | One relay process per node |
| Node-local | `Registry` | `{:volume, volume_id}` | Each subscribing process (FUSE mount, S3 handler, etc.) |
| Node-local | `Registry` | `{:volumes}` | Processes interested in volume lifecycle |

### Relay Process

Each node runs a `NeonFS.Events.Relay` GenServer that manages `:pg` membership on behalf of local subscribers. When the first local process subscribes to a volume, the relay joins the `:pg` group. When the last local process unsubscribes, it leaves.

```elixir
defmodule NeonFS.Events.Relay do
  use GenServer

  alias NeonFS.Events.{Envelope, VolumeCreated, VolumeUpdated, VolumeDeleted}

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(_opts) do
    # Always join the volumes group — volume lifecycle events are cheap
    :pg.join(:neonfs_events, {:volumes}, self())
    {:ok, %{volume_refs: %{}}}
  end

  # Called by NeonFS.Events.subscribe/1
  def ensure_volume_group(volume_id) do
    GenServer.call(__MODULE__, {:ensure_volume_group, volume_id})
  end

  # Called by NeonFS.Events.unsubscribe/1
  def maybe_leave_volume_group(volume_id) do
    GenServer.call(__MODULE__, {:maybe_leave_volume_group, volume_id})
  end

  def handle_call({:ensure_volume_group, volume_id}, _from, state) do
    refs = Map.get(state.volume_refs, volume_id, 0)

    if refs == 0 do
      :pg.join(:neonfs_events, {:volume, volume_id}, self())
    end

    {:reply, :ok, put_in(state.volume_refs[volume_id], refs + 1)}
  end

  def handle_call({:maybe_leave_volume_group, volume_id}, _from, state) do
    refs = Map.get(state.volume_refs, volume_id, 0) - 1

    state =
      if refs <= 0 do
        :pg.leave(:neonfs_events, {:volume, volume_id}, self())
        %{state | volume_refs: Map.delete(state.volume_refs, volume_id)}
      else
        put_in(state.volume_refs[volume_id], refs)
      end

    {:reply, :ok, state}
  end

  # Receive cross-node event, fan out locally via Registry
  def handle_info({:neonfs_event, %Envelope{event: event} = envelope}, state) do
    group = event_group(event)

    Registry.dispatch(NeonFS.Events.Registry, group, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {:neonfs_event, envelope})
      end
    end)

    {:noreply, state}
  end

  defp event_group(%VolumeCreated{}), do: {:volumes}
  defp event_group(%VolumeUpdated{}), do: {:volumes}
  defp event_group(%VolumeDeleted{}), do: {:volumes}
  defp event_group(event), do: {:volume, event.volume_id}
end
```

### Subscribing and Unsubscribing

```elixir
defmodule NeonFS.Events do
  @doc """
  Subscribe the calling process to events for a volume.
  """
  @spec subscribe(binary()) :: :ok
  def subscribe(volume_id) do
    Registry.register(NeonFS.Events.Registry, {:volume, volume_id}, [])
    NeonFS.Events.Relay.ensure_volume_group(volume_id)
    :ok
  end

  @doc """
  Subscribe the calling process to volume lifecycle events.
  """
  @spec subscribe_volumes() :: :ok
  def subscribe_volumes do
    Registry.register(NeonFS.Events.Registry, {:volumes}, [])
    :ok
  end

  @doc """
  Unsubscribe the calling process from a volume's events.
  """
  @spec unsubscribe(binary()) :: :ok
  def unsubscribe(volume_id) do
    Registry.unregister(NeonFS.Events.Registry, {:volume, volume_id})
    NeonFS.Events.Relay.maybe_leave_volume_group(volume_id)
    :ok
  end

  @doc """
  Unsubscribe the calling process from volume lifecycle events.
  """
  @spec unsubscribe_volumes() :: :ok
  def unsubscribe_volumes do
    Registry.unregister(NeonFS.Events.Registry, {:volumes})
    :ok
  end
end
```

Usage from a FUSE mount:

```elixir
# On mount
NeonFS.Events.subscribe(volume_id)
NeonFS.Events.subscribe_volumes()

# On unmount (also happens automatically if process exits, via Registry monitors)
NeonFS.Events.unsubscribe(volume_id)
NeonFS.Events.unsubscribe_volumes()
```

## Broadcasting

### Where Events Originate

Events are broadcast by the index wrapper modules on core nodes (FileIndex, VolumeRegistry) after a successful metadata write. This keeps the storage layer pure (no side effects in write paths) and places broadcasting at the level where we know the write has been committed and any local caches have been updated.

After Phase 5, most metadata writes go through the QuorumCoordinator (FileIndex, ChunkIndex, StripeIndex). Volume operations remain Ra-backed. The broadcasting code is the same regardless of backing store — it runs in the wrapper module after a successful write, not inside the storage engine.

```elixir
defmodule NeonFS.Events.Broadcaster do
  alias NeonFS.Events.{Envelope, VolumeCreated, VolumeUpdated, VolumeDeleted}

  @volume_events [VolumeCreated, VolumeUpdated, VolumeDeleted]

  @doc """
  Broadcast an event to all nodes interested in the given volume.
  Called by index modules after successful metadata writes.
  """
  @spec broadcast(binary(), NeonFS.Events.event()) :: :ok
  def broadcast(volume_id, event) do
    sequence = next_sequence(volume_id)

    envelope = %Envelope{
      event: event,
      source_node: node(),
      sequence: sequence,
      hlc_timestamp: NeonFS.HLC.now()
    }

    # Send to all :pg members for this volume (one relay per remote node)
    for pid <- :pg.get_members(:neonfs_events, {:volume, volume_id}) do
      send(pid, {:neonfs_event, envelope})
    end

    # Volume-level events also go to the volumes group
    if event.__struct__ in @volume_events do
      for pid <- :pg.get_members(:neonfs_events, {:volumes}) do
        send(pid, {:neonfs_event, envelope})
      end
    end

    :ok
  end

  # Per-volume monotonic sequence counter.
  # Stored in :persistent_term for fast reads, :atomics for atomic increment.
  defp next_sequence(volume_id) do
    counter = get_or_create_counter(volume_id)
    :atomics.add_get(counter, 1, 1)
  end
end
```

Usage from an index module:

```elixir
# In FileIndex, after a successful write
alias NeonFS.Events.{Broadcaster, FileCreated, FileAttrsChanged}

def create_file(volume_id, file_id, path, meta) do
  with :ok <- QuorumCoordinator.write(...) do
    Broadcaster.broadcast(volume_id, %FileCreated{
      volume_id: volume_id,
      file_id: file_id,
      path: path
    })

    :ok
  end
end

def chmod(volume_id, file_id, path, mode) do
  with :ok <- QuorumCoordinator.write(...) do
    Broadcaster.broadcast(volume_id, %FileAttrsChanged{
      volume_id: volume_id,
      file_id: file_id,
      path: path
    })

    :ok
  end
end
```

### Broadcast Semantics

- **Fire and forget**: `send/2` is non-blocking. If a relay's mailbox is full or the node is unreachable, the message is silently dropped.
- **No ordering guarantees across volumes**: Events for different volumes may arrive out of order.
- **Within-volume ordering**: Events from a single core node for a single volume are ordered by sequence number. Events from different core nodes for the same volume may interleave (each node has its own sequence counter).
- **Single broadcaster per write**: Only the core node that initiates the metadata write broadcasts the event. Other core nodes see the state change through normal replication but do not re-broadcast.

## Client-Side Event Handler

Interface nodes implement a behaviour for handling events:

```elixir
defmodule NeonFS.Client.EventHandler do
  @doc """
  Called when an event is received for a subscribed volume.
  The process should invalidate any cached state affected by the event.
  """
  @callback handle_event(envelope :: NeonFS.Events.Envelope.t()) :: :ok
end
```

A reference implementation for FUSE:

```elixir
defmodule NeonFS.FUSE.MetadataCache do
  use GenServer
  @behaviour NeonFS.Client.EventHandler

  alias NeonFS.Events.{
    Envelope,
    FileCreated,
    FileContentUpdated,
    FileTruncated,
    FileDeleted,
    FileAttrsChanged,
    FileRenamed,
    VolumeAclChanged,
    FileAclChanged,
    DirCreated,
    DirDeleted,
    DirRenamed,
    VolumeUpdated
  }

  def init(%{volume_id: volume_id}) do
    NeonFS.Events.subscribe(volume_id)
    NeonFS.Events.subscribe_volumes()
    {:ok, %{volume_id: volume_id, cache: %{}, last_sequences: %{}}}
  end

  # Dispatch incoming events
  def handle_info({:neonfs_event, %Envelope{} = envelope}, state) do
    state = check_sequence_gap(envelope, state)
    handle_event(envelope)
    {:noreply, put_in(state.last_sequences[envelope.source_node], envelope.sequence)}
  end

  # File content events — invalidate file and parent directory
  @impl NeonFS.Client.EventHandler
  def handle_event(%Envelope{event: %FileCreated{path: path}}) do
    invalidate_directory(Path.dirname(path))
  end

  def handle_event(%Envelope{event: %FileContentUpdated{file_id: file_id, path: path}}) do
    invalidate_file(file_id, path)
  end

  def handle_event(%Envelope{event: %FileTruncated{file_id: file_id, path: path}}) do
    invalidate_file(file_id, path)
  end

  def handle_event(%Envelope{event: %FileDeleted{file_id: file_id, path: path}}) do
    invalidate_file(file_id, path)
    invalidate_directory(Path.dirname(path))
  end

  # Attribute events — invalidate stat cache
  def handle_event(%Envelope{event: %FileAttrsChanged{file_id: file_id, path: path}}) do
    invalidate_file(file_id, path)
  end

  def handle_event(%Envelope{event: %FileRenamed{file_id: file_id, old_path: old_path, new_path: new_path}}) do
    invalidate_file(file_id, old_path)
    invalidate_directory(Path.dirname(old_path))
    invalidate_directory(Path.dirname(new_path))
  end

  # ACL events — invalidate permission cache
  def handle_event(%Envelope{event: %VolumeAclChanged{}}) do
    invalidate_all_permissions()
  end

  def handle_event(%Envelope{event: %FileAclChanged{path: path}}) do
    invalidate_permissions(path)
  end

  # Directory events
  def handle_event(%Envelope{event: %DirCreated{path: path}}) do
    invalidate_directory(Path.dirname(path))
  end

  def handle_event(%Envelope{event: %DirDeleted{path: path}}) do
    invalidate_directory(path)
    invalidate_directory(Path.dirname(path))
  end

  def handle_event(%Envelope{event: %DirRenamed{old_path: old_path, new_path: new_path}}) do
    invalidate_directory(old_path)
    invalidate_directory(Path.dirname(old_path))
    invalidate_directory(Path.dirname(new_path))
  end

  # Volume events
  def handle_event(%Envelope{event: %VolumeUpdated{}}) do
    invalidate_volume_config()
  end

  def handle_event(_), do: :ok

  # Gap detection — track last sequence per source node
  defp check_sequence_gap(%Envelope{source_node: source, sequence: seq}, state) do
    last = Map.get(state.last_sequences, source, 0)

    if seq > last + 1 do
      Logger.warning("Event gap from #{source}: expected #{last + 1}, got #{seq}")
      invalidate_all_caches()
    end

    state
  end
end
```

## Partition Recovery

### Problem

During a network partition, events are lost — `:pg` broadcasts use `send/2` which silently drops messages to unreachable nodes. When the partition heals, a subscriber's cache may be arbitrarily stale.

### Strategy: Invalidate Everything on Reconnect

When a node detects it has been partitioned and subsequently reconnects, it **invalidates all ephemeral caches**. This temporarily increases RPC load as caches are rebuilt from scratch, but guarantees correctness.

```elixir
defmodule NeonFS.Client.PartitionRecovery do
  use GenServer

  def init(_) do
    :net_kernel.monitor_nodes(true)
    {:ok, %{known_core_nodes: MapSet.new(), pending_invalidations: MapSet.new()}}
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
      # Debounce: wait before invalidating in case the connection flaps
      Process.send_after(self(), {:do_invalidate, node}, @debounce_ms)

      {:noreply, %{state |
        known_core_nodes: MapSet.put(state.known_core_nodes, node),
        pending_invalidations: MapSet.put(state.pending_invalidations, node)
      }}
    else
      {:noreply, state}
    end
  end

  @debounce_ms 5_000

  def handle_info({:do_invalidate, node}, state) do
    if MapSet.member?(state.pending_invalidations, node) do
      invalidate_all_caches()

      {:noreply, %{state |
        pending_invalidations: MapSet.delete(state.pending_invalidations, node)
      }}
    else
      # Node went down again before debounce expired — skip, will re-trigger on next nodeup
      {:noreply, state}
    end
  end

  defp invalidate_all_caches do
    # Notify all local event subscribers to clear their caches.
    # Broadcast a synthetic invalidation event via the local Registry.
    Registry.dispatch(NeonFS.Events.Registry, {:invalidate_all}, fn entries ->
      for {pid, _value} <- entries do
        send(pid, :neonfs_invalidate_all)
      end
    end)

    require Logger
    Logger.warning("Partition recovery: all metadata caches invalidated")
  end
end
```

### Core Node Recovery

When a core node reconnects after a partition:

1. **Ra handles state catch-up**: For Tier 1 metadata (volumes, encryption keys), the Ra follower replays the leader's log. This is automatic.
2. **Quorum store converges**: For Tier 2/3 metadata (chunks, files, stripes), read repair and anti-entropy bring replicas back into sync. This is background and continuous.
3. **Local caches are rebuilt**: After catch-up completes, wrapper modules (VolumeRegistry from Ra; FileIndex, ChunkIndex, StripeIndex from quorum store) re-populate their ETS tables.
4. **`:pg` group membership re-syncs**: OTP's `:pg` automatically merges group membership when nodes reconnect. No manual intervention needed.

## Gap Detection

While full invalidation on reconnect handles the common case (network partition), gap detection catches the micro case: individual dropped messages while still connected (e.g., due to mailbox overflow or transient network issues).

Sequence numbers are per-source-node. If a volume is written to by different core nodes, a subscriber sees interleaved sequences (e.g., 1 from node A, 1 from node B, 2 from node A). The subscriber tracks `{source_node, sequence}` pairs independently:

```elixir
defp check_sequence_gap(%Envelope{source_node: source, sequence: seq}, state) do
  last = Map.get(state.last_sequences, source, 0)

  if seq > last + 1 do
    Logger.warning("Event gap from #{source}: expected #{last + 1}, got #{seq}")
    invalidate_all_caches()
  end

  put_in(state.last_sequences[source], seq)
end
```

This provides defence in depth: partition recovery handles full disconnections, and gap detection catches individual dropped messages.

## Module Placement

| Module | Package | Role |
|--------|---------|------|
| `NeonFS.Events.Broadcaster` | `neonfs_core` | Emit events after metadata writes |
| `NeonFS.Events` | `neonfs_client` | Subscribe/unsubscribe API |
| `NeonFS.Events.Relay` | `neonfs_client` | `:pg` membership, local `Registry` dispatch |
| `NeonFS.Events.Registry` | `neonfs_client` | Node-local subscriber registry (OTP `Registry`) |
| `NeonFS.Events.Envelope` | `neonfs_client` | Event envelope struct |
| `NeonFS.Events.*` (event structs) | `neonfs_client` | Event type definitions |
| `NeonFS.Client.EventHandler` | `neonfs_client` | Behaviour for event subscribers |
| `NeonFS.Client.PartitionRecovery` | `neonfs_client` | Monitor nodes, invalidate on reconnect |
| `NeonFS.FUSE.MetadataCache` | `neonfs_fuse` | FUSE-specific cache with event handling |

The `:pg` scope (`:neonfs_events`) is started in both `neonfs_core` and `neonfs_client` supervision trees. OTP's `:pg` handles the case where multiple nodes start the same scope — they automatically merge.

The event structs and `Envelope` live in `neonfs_client` so both core (broadcasting) and interface (subscribing) nodes have access to the same types. Dialyzer can verify that broadcasters construct valid events and subscribers handle all variants.

## What This Spec Does NOT Cover

- **Metadata cache implementation details**: The cache data structure, eviction policy, and lookup API for each interface node are implementation concerns, not specified here.
- **Chunk data caching**: This spec covers metadata event notification only. Content-addressed chunks are immutable and never need invalidation.
- **Cross-volume transactions**: Events are per-volume. There is no mechanism for atomic operations spanning multiple volumes, and therefore no cross-volume event ordering.
- **Persistent event log**: Events are ephemeral. There is no replay capability. If a subscriber needs the current state, it queries the core node directly.

## Related Documents

- [Discovery](discovery.md) — Service discovery and node connectivity
- [Node Management](node-management.md) — Partition behaviour and recovery
- [Metadata](metadata.md) — Metadata tiering, HLC timestamps, quorum model
- [Security](security.md) — Volume and file ACLs
- [Architecture](architecture.md) — System structure and supervision trees
- [Replication](replication.md) — Write flows that trigger events
