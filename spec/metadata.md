# Metadata Storage

This document describes how NeonFS stores and manages metadata across the cluster, including the tiered architecture, leaderless quorum model, and consistency mechanisms.

## The Scale Problem

A large filesystem might have millions of files, each with POSIX metadata (permissions, timestamps, extended ACLs). Keeping all of this in Ra (Raft) memory is impractical:

- 1 million files × 500 bytes metadata each = 500 MB minimum
- Plus chunk mappings, access stats, etc.
- Ra replicates everything to all nodes

## Tiered Metadata Architecture

Split metadata into tiers based on access patterns:

### Tier 1: Cluster State (Ra)

Small, critical, strongly consistent:
- Node membership and health
- Volume definitions
- Encryption keys
- User and group definitions
- Segment → replica set assignments
- Active write sessions

```elixir
# Always in Ra - small, critical
%ClusterState{
  nodes: [...],
  volumes: [...],
  users: [...],
  segments: [...],  # Which nodes replicate which hash ranges
  active_writes: [...]
}
```

### Tier 2: Chunk Metadata (Distributed Index)

Moderate size, frequently accessed:
- Chunk → location mappings
- Replication status
- Stripe definitions

Sharded across nodes using consistent hashing with quorum-based replication (see Leaderless Quorum Model below).

### Tier 3: Filesystem Metadata (Leaderless Quorum)

Large, requires both consistency and availability:
- Directory structures (parent → children mappings)
- File metadata (file_id → chunks, POSIX attrs)

Separated into two concerns with different sharding keys:
- **Directory entries**: Sharded by `hash(parent_path)`
- **File metadata**: Sharded by `hash(file_id)`

This separation ensures directory listings remain efficient (single segment query) while file metadata is evenly distributed regardless of path structure.

### Tier 4: Access Statistics (Ephemeral)

High-volume, loss-tolerant:
- Per-chunk access counts
- Tiering decisions
- Performance metrics

Keep in ETS, aggregate periodically, don't replicate.

### How the Consistency Models Interact

NeonFS uses multiple consistency mechanisms that are **layered**, not competing:

| Layer | Mechanism | Scope | Purpose |
|-------|-----------|-------|---------|
| Control plane | Ra (Raft) | Cluster-wide | Who replicates what (membership, segment assignments) |
| Data plane | Leaderless quorum | Per-segment | Actual metadata operations (files, directories, chunks) |
| Conflict resolution | HLC | Per-record | Ordering concurrent writes within quorum |

These don't interfere because they operate on different data:

- **Ra leader election** doesn't affect in-flight quorum writes. Quorum writes target replica sets determined by segment assignments, not the Ra leader. Segment assignments are stable unless explicitly changed via Ra.
- **HLC timestamps** operate within the quorum system for per-record conflict resolution. They don't interact with Ra's Raft log.
- **Quorum operations** (R+W>N) provide consistency for metadata reads/writes. Ra provides the configuration that determines which nodes form each replica set.

This is similar to how Cassandra separates gossip (membership) from quorum (data operations), or how CockroachDB separates Raft groups by key range.

## Leaderless Quorum Model

Instead of single-node ownership, metadata segments use leaderless quorum replication. Each segment has a replica set, and operations require agreement from a quorum.

### Core Principle

```
Write → Any replica → Quorum of replicas → Ack
Read  → Quorum of replicas → Return most recent
```

No single node "owns" the data. Any replica can serve requests, and quorum ensures consistency.

### Quorum Configuration

The classic formula **R + W > N** guarantees strong consistency.

| Setting | R | W | Behaviour |
|---------|---|---|-----------|
| Strong consistency | 2 | 2 | Always consistent, tolerates 1 failure |
| Fast reads | 1 | 3 | Read from any, write to all |
| Fast writes | 3 | 1 | Write to any, read from all |
| Eventual | 1 | 1 | Fast but may read stale |

Per-volume configuration:

```elixir
%Volume{
  name: "documents",
  metadata_consistency: %{
    replicas: 3,           # N - auto-scales with cluster size by default
    read_quorum: 2,        # R
    write_quorum: 2,       # W
    # R + W = 4 > 3 = N ✓ Strong consistency
  }
}

%Volume{
  name: "scratch",
  metadata_consistency: %{
    replicas: 3,
    read_quorum: 1,        # Fast reads, may be stale
    write_quorum: 2,
    # R + W = 3 = N, eventual consistency possible
  }
}
```

### Write Flow

```elixir
def quorum_write(segment_id, key, value) do
  replicas = get_replicas(segment_id)  # From Ra
  write_quorum = get_write_quorum(segment_id)

  # Assign timestamp for conflict resolution
  timestamped_value = %{value | hlc_timestamp: HLC.now()}

  # Send to all replicas in parallel
  responses = replicas
    |> Enum.map(&async_write(&1, key, timestamped_value))
    |> await_quorum(write_quorum)

  case responses do
    {:ok, acks} when length(acks) >= write_quorum -> :ok
    {:error, reason} -> {:error, reason}
  end
end
```

### Read Flow with Read Repair

```elixir
def quorum_read(segment_id, key) do
  replicas = get_replicas(segment_id)
  read_quorum = get_read_quorum(segment_id)

  responses = replicas
    |> Enum.map(&async_read(&1, key))
    |> await_quorum(read_quorum)

  {latest, stale_replicas} = find_latest_and_stale(responses)

  # Background repair via I/O scheduler - don't block the read
  # Scheduler handles backpressure, coalescing, and priority
  if stale_replicas != [] do
    IOScheduler.submit(:read_repair,
      key: key,
      stale: stale_replicas,
      latest: latest
    )
  end

  {:ok, latest.value}
end
```

See [Architecture - I/O Scheduler](architecture.md#io-scheduler) for how read repairs are prioritised and coalesced.

## Conflict Resolution: Hybrid Logical Clocks

Metadata records use Hybrid Logical Clocks (HLC) for ordering and conflict resolution at the quorum level:

```elixir
%FileMeta{
  id: "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  hlc_timestamp: {wall_time, logical_counter, node_id},
  chunks: [...],
  # ... other fields
}
```

HLC combines wall clock time with a logical counter, ensuring:
- Timestamps are monotonically increasing
- Causally related events are correctly ordered
- Clock skew within bounds doesn't cause consistency violations

Higher timestamp wins when replicas disagree during quorum reads. This is simpler than version vectors and sufficient for resolving replica divergence.

### Clock Skew Bounds

HLC correctness depends on bounded clock skew between nodes. Without bounds, a node with a clock far in the future would "win" all LWW conflicts, and its high timestamps would propagate to other nodes.

**Maximum Skew Enforcement**

Timestamps more than `max_clock_skew_ms` ahead of local time are rejected:

```elixir
defmodule NeonFS.HLC do
  @max_clock_skew_ms 1_000  # 1 second default

  @doc """
  Receive a timestamp from another node. Returns error if the
  timestamp is too far in the future, indicating clock skew.
  """
  def receive_timestamp(remote_hlc, local_state) do
    {remote_wall, _counter, _node} = remote_hlc
    local_wall = System.system_time(:millisecond)

    if remote_wall > local_wall + @max_clock_skew_ms do
      {:error, :clock_skew_detected, remote_wall - local_wall}
    else
      {:ok, advance(local_state, remote_hlc)}
    end
  end

  @doc """
  Generate a new timestamp. Bounds the wall time component to prevent
  runaway timestamps from propagating indefinitely.
  """
  def now(state) do
    wall = System.system_time(:millisecond)

    # Prevent runaway timestamps: if our last timestamp is far ahead
    # of current wall time (e.g., after clock correction), bound it
    max_wall = wall + @max_clock_skew_ms
    effective_wall = min(max(wall, state.last_wall + 1), max_wall)

    if effective_wall > state.last_wall do
      {effective_wall, 0, state.node_id}
    else
      {state.last_wall, state.last_counter + 1, state.node_id}
    end
  end
end
```

**Cluster-Wide Clock Monitoring**

Nodes periodically check clock alignment with peers. Excessive skew triggers warnings, alerts, or quarantine:

```elixir
defmodule NeonFS.ClockMonitor do
  @warning_threshold_ms 200
  @critical_threshold_ms 500
  @quarantine_threshold_ms 1_000

  def check_cluster_clocks do
    for node <- ClusterState.active_nodes() do
      case check_node_clock(node) do
        {:ok, _node, skew} ->
          :telemetry.execute([:neonfs, :clock, :skew], %{skew_ms: skew}, %{node: node})

        {:warning, node, skew} ->
          Logger.warning("Clock skew detected: #{node} is #{skew}ms off")
          :telemetry.execute([:neonfs, :clock, :skew], %{skew_ms: skew}, %{node: node})

        {:critical, node, skew} ->
          Logger.error("Critical clock skew: #{node} is #{skew}ms off")
          Alerts.send(:clock_skew_critical, node: node, skew_ms: skew)

        {:quarantine, node, skew} ->
          Logger.error("Quarantining #{node}: clock skew #{skew}ms exceeds threshold")
          ClusterState.quarantine_node(node, :clock_skew)
      end
    end
  end

  defp check_node_clock(node) do
    t1 = System.system_time(:millisecond)
    {:ok, remote_time} = RPC.call(node, System, :system_time, [:millisecond])
    t2 = System.system_time(:millisecond)

    # Account for round-trip time
    estimated_local = t1 + div(t2 - t1, 2)
    skew = abs(remote_time - estimated_local)

    cond do
      skew > @quarantine_threshold_ms -> {:quarantine, node, skew}
      skew > @critical_threshold_ms -> {:critical, node, skew}
      skew > @warning_threshold_ms -> {:warning, node, skew}
      true -> {:ok, node, skew}
    end
  end
end
```

**Quarantine Behaviour**

A quarantined node:
- Cannot perform quorum writes (requests rejected by other replicas)
- Can still serve reads (stale data is better than no data)
- Is excluded from new replica set assignments
- Remains quarantined until clock skew is corrected and verified

**Edge Cases**

| Scenario | Behaviour |
|----------|-----------|
| Node clock 100ms ahead | Normal operation, within bounds |
| Node clock 800ms ahead | Warning logged, operation proceeds |
| Node clock 1.5s ahead | Writes rejected, node quarantined |
| Node clock corrected backward | HLC logical counter ensures monotonicity locally |
| HLC timestamp from before correction | Bounded by `max_clock_skew_ms` above wall time |
| All nodes' clocks jump forward | System continues, old timestamps lose LWW conflicts |
| Leap second | Handled by OS; HLC monotonicity prevents issues |

**Operational Requirements**

Nodes should run NTP or similar time synchronisation. For clusters spanning multiple sites, consider PTP (Precision Time Protocol) or GPS-disciplined clocks.

```yaml
# Recommended NTP configuration for NeonFS nodes
# /etc/chrony/chrony.conf
server time.cloudflare.com iburst
server time.google.com iburst
makestep 0.1 3      # Allow large correction on startup
maxdrift 100        # Maximum drift rate in ppm
```

### HLC vs Intent Log

HLC and the intent log serve different purposes:

| Mechanism | Purpose | Scope |
|-----------|---------|-------|
| Intent log | Prevent concurrent writes to the same file | Before operation starts |
| HLC | Order and merge divergent replicas | During quorum operations |

For **file writes**, the intent log ensures only one writer can modify a file at a time. Concurrent writers are rejected before any data is written, so HLC conflict resolution never sees conflicting file contents.

HLC/LWW is still used for:
- **Read repair**: When replicas disagree, the highest HLC timestamp wins
- **Anti-entropy**: Merkle tree sync uses HLC to determine which version is newer
- **Metadata-only updates**: Operations like `chmod` or `chown` that don't go through the intent log

## Directory and File Metadata Separation

To enable efficient directory listings while evenly distributing file metadata:

### Directory Entries (sharded by `hash(parent_path)`)

```elixir
%DirectoryEntry{
  parent_path: "/documents",
  volume_id: "vol_123",
  children: %{
    "report.pdf" => %{type: :file, id: "f47ac10b-58cc-4372-..."},
    "drafts" => %{type: :dir, id: "9c7d8e6f-1234-5678-..."}
  },
  mode: 0o755,
  uid: 1000,
  gid: 1000,
  hlc_timestamp: {...}
}
```

### File Metadata (sharded by `hash(file_id)`)

```elixir
%FileMeta{
  id: "f47ac10b-58cc-4372-a567-0e02b2c3d479",
  volume_id: "vol_123",
  chunks: ["sha256:abc...", "sha256:def..."],
  size: 15_728_640,
  mode: 0o644,
  uid: 1000,
  gid: 1000,
  created_at: ~U[...],
  modified_at: ~U[...],
  hlc_timestamp: {...}
}
```

### Operation Examples

*Create `/documents/report.pdf`:*
1. `hash("/documents/")` → Segment A (directory entry)
2. Generate `file_id = UUID`
3. Quorum write to Segment A: add child entry
4. `hash(file_id)` → Segment B (file metadata)
5. Quorum write to Segment B: create file metadata

*List `/documents/`:*
1. `hash("/documents/")` → Segment A
2. Quorum read from Segment A → children list
3. Single segment query, no scatter-gather

*Rename within directory (`report.pdf` → `final.pdf`):*
1. Single quorum write to Segment A (update children map)
2. File metadata unchanged (same file_id)

*Move across directories (`/documents/report.pdf` → `/archive/report.pdf`):*
1. Quorum write to `hash("/documents/")`: remove child
2. Quorum write to `hash("/archive/")`: add child
3. File metadata unchanged

## Intent Log: Transaction Safety and Write Coordination

The intent log is a coordination mechanism backed by Ra that solves two related problems:

1. **Cross-segment atomicity**: Operations spanning multiple segments need crash recovery
2. **Concurrent writer detection**: Multiple writers to the same file must be detected and rejected

Both problems are solved by the same mechanism: registering exclusive intents in Ra before performing operations.

### Problem 1: Cross-Segment Operations

**File creation** requires writes to two segments:
1. Create FileMeta in segment B (`hash(file_id)`)
2. Add directory entry in segment A (`hash(parent_path)`)

**Cross-directory move** requires writes to two segments:
1. Remove child from source directory segment
2. Add child to destination directory segment

If the process crashes between steps, or if one quorum write succeeds but another fails:
- **Dangling reference**: Directory entry points to non-existent FileMeta
- **Orphaned file**: FileMeta exists but isn't reachable from any directory
- **Partial move**: File removed from source but not added to destination

### Problem 2: Concurrent Writers

Without coordination, two concurrent writers to the same file would use HLC last-writer-wins semantics, silently discarding one writer's changes. NeonFS instead detects concurrent writes and rejects them, forcing writers to coordinate explicitly.

### Solution: Intent Log + Reactor Orchestration

NeonFS uses a two-layer approach:

1. **Intent log in Ra**: Provides crash-safe record of in-flight operations with exclusive access
2. **Reactor orchestration**: Handles step execution, dependencies, and in-process rollback

This combination provides:
- **Crash recovery**: Intent log survives process crashes
- **Concurrent writer detection**: Exclusive intents prevent conflicting operations
- **Automatic rollback**: Reactor undoes completed steps when later steps fail
- **Observability**: Step-level tracing and error reporting

### Intent Log Structure

Before starting an operation, register an intent record in Ra:

```elixir
%Intent{
  id: "intent_7x8k2m9p...",
  operation: :write_file,
  conflict_key: {:file, "f47ac10b-..."},  # For exclusive access
  params: %{
    volume_id: "vol_123",
    parent_path: "/documents",
    name: "report.pdf",
    file_id: "f47ac10b-..."
  },
  state: :pending,          # :pending | :completed | :failed | :rolled_back
  started_at: ~U[...],
  expires_at: ~U[...],      # TTL for stale intent cleanup
  completed_at: nil,
  error: nil
}
```

The `conflict_key` field enables exclusive access: only one intent with a given conflict_key can be active at a time.

### Exclusive Intent Acquisition

The key operation is `IntentLog.try_acquire/1`, an atomic check-and-set in Ra:

```elixir
defmodule NeonFS.IntentLog do
  @doc """
  Attempt to acquire an exclusive intent. Fails if another intent
  with the same conflict_key is already active.
  """
  def try_acquire(%Intent{} = intent) do
    command = {:try_acquire_intent, intent}

    case Ra.process_command(@ra_cluster, command) do
      {:ok, :acquired, _} -> {:ok, intent.id}
      {:ok, :conflict, existing} -> {:error, :conflict, existing}
    end
  end
end

# In the Ra state machine:
def apply(_meta, {:try_acquire_intent, intent}, state) do
  case Map.get(state.active_intents_by_conflict_key, intent.conflict_key) do
    nil ->
      # No conflict, acquire the intent
      new_state = state
        |> put_in([:intents, intent.id], intent)
        |> put_in([:active_intents_by_conflict_key, intent.conflict_key], intent.id)
      {:ok, :acquired, new_state}

    existing_id ->
      existing = get_in(state, [:intents, existing_id])

      if intent_expired?(existing) do
        # Expired intent from crashed writer, we can take over
        new_state = state
          |> put_in([:intents, existing_id, :state], :expired)
          |> put_in([:intents, intent.id], intent)
          |> put_in([:active_intents_by_conflict_key, intent.conflict_key], intent.id)
        {:ok, :acquired, new_state}
      else
        # Active intent exists, conflict
        {:ok, :conflict, existing}
      end
  end
end
```

### Concurrent Writer Detection

When two writers attempt to modify the same file:

```
Writer A                           Ra (Intent Log)                    Writer B
   │                                    │                                 │
   ├─ try_acquire(file_123) ───────────►│                                 │
   │                                    │◄─────── try_acquire(file_123) ──┤
   │◄─────────── :acquired ─────────────┤                                 │
   │                                    ├─── {:error, :conflict} ────────►│
   │                                    │                                 │
   ├─ write chunk data                  │                      {:error, :concurrent_modification}
   ├─ update metadata                   │                                 │
   ├─ complete(intent_id) ─────────────►│                                 │
   │                                    │ (conflict_key released)         │
   │◄─────────── :ok ───────────────────┤                                 │
```

Writer B receives a clear error and can retry after Writer A completes.

### Conflict Keys by Operation

| Operation | Conflict Key | Effect |
|-----------|--------------|--------|
| Write file | `{:file, file_id}` | One writer per file |
| Create file | `{:create, volume_id, parent_path, name}` | Prevents duplicate creation |
| Move file | `{:file, file_id}` | Prevents concurrent moves |
| Delete file | `{:file, file_id}` | Prevents write during delete |
| Rename in directory | `{:dir, volume_id, parent_path}` | Serialises renames |
| Tier migration | `{:chunk_migration, chunk_hash}` | One migration per chunk |
| Volume key rotation | `{:volume_key_rotation, volume_id}` | One rotation per volume |

### Intent TTL and Lease Extension

Intents have a TTL to handle crashed writers. For long-running operations, the lease can be extended:

```elixir
defmodule NeonFS.IntentLog do
  @default_ttl_seconds 300  # 5 minutes

  def extend(intent_id, additional_seconds \\ @default_ttl_seconds) do
    command = {:extend_intent, intent_id, additional_seconds}
    Ra.process_command(@ra_cluster, command)
  end
end

# For large file writes, extend the lease periodically
defp write_with_lease_extension(intent_id, chunks) do
  # Spawn a process to keep the lease alive
  lease_keeper = spawn_link(fn ->
    keep_lease_alive(intent_id, interval: 60_000)
  end)

  try do
    Enum.each(chunks, &write_chunk/1)
  after
    Process.exit(lease_keeper, :normal)
  end
end

defp keep_lease_alive(intent_id, opts) do
  interval = Keyword.fetch!(opts, :interval)

  receive do
  after
    interval ->
      case IntentLog.extend(intent_id) do
        :ok -> keep_lease_alive(intent_id, opts)
        {:error, :not_found} -> :ok  # Intent completed or expired
      end
  end
end
```

### Intent Cleanup and Archival

Completed and expired intents are moved from Ra to an audit log on the system volume, then removed from Ra state. This keeps Ra lean while preserving history for debugging.

**Cleanup triggers:**

| Intent State | Trigger | Action |
|--------------|---------|--------|
| `:completed` | Immediately after completion | Archive and remove |
| `:failed` | Immediately after failure | Archive and remove |
| `:rolled_back` | Immediately after rollback | Archive and remove |
| `:pending` / `:in_progress` | TTL expired, lease not extended | Handle expiry, then archive |

**Expired intent handling:**

When an intent's TTL expires without extension, it indicates a crashed or stalled operation. Any node can handle cleanup:

```elixir
defmodule NeonFS.IntentLog.Cleanup do
  @doc """
  Periodic process that handles expired intents.
  Leader election via Ra ensures only one node acts at a time.
  """

  def handle_expired_intents do
    for intent <- IntentLog.get_expired() do
      case intent.operation do
        op when op in [:rename_directory, :migrate_chunk, :rotate_volume_key] ->
          # These can be recovered by any node - check state and complete/rollback
          recover_or_fail(intent)

        :write_file ->
          handle_expired_write(intent)
      end
    end
  end

  defp handle_expired_write(intent) do
    case intent.params.write_ack do
      :local ->
        # Chunks only on original node - can't clean up remotely
        # Release conflict key so new writes can proceed
        # Chunks orphaned until node returns or is decommissioned
        archive(intent, :node_unavailable)

      ack when ack in [:quorum, :all] ->
        # Chunks were replicated - clean up refs, GC handles physical cleanup
        abort_write_chunks(intent.params.chunk_hashes, intent.id)
        archive(intent, :write_timeout)
    end
  end

  defp recover_or_fail(intent) do
    # Delegate to operation-specific recovery (see storage-tiering.md, etc.)
    case OperationRecovery.attempt(intent) do
      {:ok, :completed} -> archive(intent, :recovered_complete)
      {:ok, :rolled_back} -> archive(intent, :recovered_rollback)
      {:error, reason} -> archive(intent, {:recovery_failed, reason})
    end
  end
end
```

**Audit log on system volume:**

```elixir
defmodule NeonFS.IntentLog.Archive do
  @system_volume "_system"
  @audit_path "/audit/intents"

  def archive(intent, outcome) do
    entry = %{
      intent_id: intent.id,
      operation: intent.operation,
      conflict_key: intent.conflict_key,
      node: intent.node,
      started_at: intent.started_at,
      ended_at: DateTime.utc_now(),
      outcome: outcome,
      params: intent.params,
      error: intent.error
    }

    # Append to daily log file (JSONL format)
    date = Date.utc_today() |> Date.to_iso8601()
    path = "#{@audit_path}/#{date}.jsonl"

    NeonFS.Volume.append(@system_volume, path, Jason.encode!(entry) <> "\n")

    # Remove from Ra state
    IntentLog.delete(intent.id)
  end
end
```

**Log rotation:**

```yaml
system_volume:
  audit:
    intent_log:
      retention_days: 90      # Keep 90 days of history
      rotation: daily         # One file per day
```

Old audit logs are deleted by a background process after `retention_days`.

### Reactor-Based Orchestration

Multi-segment operations are implemented as Reactor workflows with explicit undo handlers:

```elixir
defmodule NeonFS.Operations.CreateFile do
  use Reactor

  input :volume_id
  input :parent_path
  input :name
  input :file_id
  input :chunks

  step :create_file_metadata, NeonFS.Steps.CreateFileMeta do
    argument :file_id, input(:file_id)
    argument :volume_id, input(:volume_id)
    argument :chunks, input(:chunks)
    # run: quorum write to file metadata segment
    # undo: delete the FileMeta record
  end

  step :add_directory_entry, NeonFS.Steps.AddDirectoryEntry do
    argument :parent_path, input(:parent_path)
    argument :name, input(:name)
    argument :file_id, input(:file_id)
    argument :volume_id, input(:volume_id)
    # run: quorum write to directory segment
    # undo: remove the directory entry
  end

  return :add_directory_entry
end
```

Each step implements the `Reactor.Step` behaviour:

```elixir
defmodule NeonFS.Steps.CreateFileMeta do
  use Reactor.Step

  @impl true
  def run(%{file_id: file_id, volume_id: volume_id, chunks: chunks}, _, _) do
    case Metadata.create_file_meta(file_id, volume_id, chunks) do
      {:ok, file_meta} -> {:ok, file_meta}
      {:error, reason} -> {:error, reason}
    end
  end

  @impl true
  def undo(file_meta, _, _, _) do
    Metadata.delete_file_meta(file_meta.id)
    :ok
  end
end
```

If `add_directory_entry` fails, Reactor automatically calls `undo` on `create_file_metadata`, removing the orphaned FileMeta.

### Execution Flow

```elixir
def create_file(volume_id, parent_path, name, chunks) do
  file_id = UUID.uuid4()
  now = DateTime.utc_now()

  intent = %Intent{
    id: UUID.uuid4(),
    operation: :create_file,
    conflict_key: {:create, volume_id, parent_path, name},
    params: %{volume_id: volume_id, parent_path: parent_path, name: name, file_id: file_id},
    state: :pending,
    started_at: now,
    expires_at: DateTime.add(now, 300, :second)
  }

  # 1. Try to acquire exclusive intent (fails if concurrent create)
  case IntentLog.try_acquire(intent) do
    {:error, :conflict, _existing} ->
      {:error, :concurrent_modification}

    {:ok, intent_id} ->
      # 2. Execute with Reactor (handles in-process failures and rollback)
      result = Reactor.run(NeonFS.Operations.CreateFile, %{
        volume_id: volume_id,
        parent_path: parent_path,
        name: name,
        file_id: file_id,
        chunks: chunks
      })

      # 3. Update intent based on result
      case result do
        {:ok, _} ->
          IntentLog.complete(intent_id)
          {:ok, file_id}

        {:error, reason} ->
          # Reactor already rolled back completed steps
          IntentLog.fail(intent_id, reason)
          {:error, reason}
      end
  end
end

def write_file(file_id, new_chunks) do
  now = DateTime.utc_now()

  intent = %Intent{
    id: UUID.uuid4(),
    operation: :write_file,
    conflict_key: {:file, file_id},
    params: %{file_id: file_id},
    state: :pending,
    started_at: now,
    expires_at: DateTime.add(now, 300, :second)
  }

  case IntentLog.try_acquire(intent) do
    {:error, :conflict, _existing} ->
      {:error, :concurrent_modification}

    {:ok, intent_id} ->
      try do
        result = do_write_file(file_id, new_chunks, intent_id)
        IntentLog.complete(intent_id)
        result
      catch
        kind, reason ->
          IntentLog.fail(intent_id, {kind, reason})
          :erlang.raise(kind, reason, __STACKTRACE__)
      end
  end
end
```

### Crash Recovery

On node startup, the recovery process scans for incomplete intents and resolves them:

```elixir
defmodule NeonFS.IntentLog.Recovery do
  def recover_incomplete_intents do
    IntentLog.list_pending()
    |> Enum.each(&recover_intent/1)
  end

  defp recover_intent(%Intent{operation: :create_file} = intent) do
    %{file_id: file_id, parent_path: parent_path, name: name} = intent.params

    file_exists? = Metadata.file_meta_exists?(file_id)
    entry_exists? = Metadata.directory_has_child?(parent_path, name)

    case {file_exists?, entry_exists?} do
      {false, false} ->
        # Nothing happened, clean up intent
        IntentLog.mark_rolled_back(intent.id, :no_changes_made)

      {true, false} ->
        # Partial state: file created but not in directory
        # Roll back by deleting the orphaned FileMeta
        Metadata.delete_file_meta(file_id)
        IntentLog.mark_rolled_back(intent.id, :rolled_back_orphaned_file)

      {true, true} ->
        # Fully completed, just mark intent done
        IntentLog.complete(intent.id)

      {false, true} ->
        # Inconsistent: directory entry without file (shouldn't happen)
        # Remove the dangling reference
        Metadata.remove_directory_child(parent_path, name)
        IntentLog.mark_rolled_back(intent.id, :removed_dangling_reference)
    end
  end

  defp recover_intent(%Intent{operation: :move_file} = intent) do
    %{source_path: source, dest_path: dest, name: name, file_id: file_id} = intent.params

    in_source? = Metadata.directory_has_child?(source, name)
    in_dest? = Metadata.directory_has_child?(dest, name)

    case {in_source?, in_dest?} do
      {true, false} ->
        # Move never started or was rolled back
        IntentLog.mark_rolled_back(intent.id, :no_changes_made)

      {false, true} ->
        # Move completed
        IntentLog.complete(intent.id)

      {true, true} ->
        # Partial: exists in both (added to dest but not removed from source)
        # Complete the move by removing from source
        Metadata.remove_directory_child(source, name)
        IntentLog.complete(intent.id)

      {false, false} ->
        # File disappeared entirely - this is a serious error
        Logger.error("File #{file_id} lost during move operation")
        IntentLog.fail(intent.id, :file_lost)
    end
  end
end
```

### Recovery Policy

The recovery process follows a **roll-forward when safe, roll-back otherwise** policy:

| Operation | Partial State | Recovery Action |
|-----------|---------------|-----------------|
| Create file | FileMeta exists, no directory entry | Roll back (delete FileMeta) |
| Create file | Directory entry exists, no FileMeta | Roll back (remove entry) |
| Move file | In both directories | Roll forward (remove from source) |
| Move file | In neither directory | Error (data loss) |
| Delete file | Directory entry removed, FileMeta exists | Roll forward (delete FileMeta) |
| Tier migration | Chunk in both tiers | Roll forward (delete from source) |
| Tier migration | Chunk in destination only | Roll forward (clean up metadata) |
| Tier migration | Chunk in neither tier | Error (data loss) |

### Intent Log Housekeeping

Completed and failed intents are retained briefly for debugging, then purged:

```yaml
metadata:
  intent_log:
    retention_completed: 1h    # Keep completed intents for 1 hour
    retention_failed: 24h      # Keep failed intents for 24 hours
    cleanup_interval: 15m      # Run cleanup every 15 minutes
```

### Limitations

- **Performance overhead**: One additional Ra write per multi-segment operation
- **Not a general transaction system**: Only covers predefined operation types
- **Recovery is heuristic**: Some failure modes require operator intervention

The intent log approach is simpler and more robust than distributed transactions (2PC) for the specific operations NeonFS needs to support.

### Limitation: Flat Directory Hot Spots

Sharding by `hash(parent_path)` means all children of a directory reside in the same segment. This enables efficient single-segment directory listings but creates a potential hot spot for directories with very large numbers of children (e.g., upload buckets, log directories, mail spools).

**Guidance for current design:**

- Directories with tens of thousands of entries will work but may show increased latency
- For large collections, prefer hierarchical organisation (e.g., date-based subdirectories) over flat structures
- Monitor segment load distribution via telemetry

**Future optimisation:**

If flat directory performance becomes a bottleneck, implement segmented directory sharding: directories exceeding a threshold would shard children by `hash(parent_path + child_name_prefix)`, spreading load across multiple segments while preserving locality for prefix-based listings. This is deferred until real-world usage indicates the need.

## Consistent Hashing with Virtual Nodes

Segments are assigned to nodes using consistent hashing:

```elixir
defmodule NeonFS.MetadataRing do
  @virtual_nodes_per_physical 64  # Configurable

  def locate(key) do
    hash = :crypto.hash(:sha256, key)
    ring_position = hash_to_position(hash)
    segment = find_segment(ring_position)
    get_replicas(segment)  # Returns replica set, not single owner
  end

  def rebalance_on_join(new_node) do
    # Only ~1/N of segments affected
    # Virtual nodes spread load evenly
    # Ra updates segment → replica mappings
  end
end
```

Benefits:
- Even distribution regardless of path structure
- Node join/leave only moves ~1/N of data
- No hot spots from popular paths

## Anti-Entropy (Background Consistency)

Quorum operations handle consistency for active data. For rarely-accessed data, periodic anti-entropy ensures replicas converge:

```elixir
defmodule NeonFS.AntiEntropy do
  # Merkle tree comparison between replicas
  # Runs during low-activity periods

  def sync_segment(segment_id) do
    replicas = get_replicas(segment_id)

    trees = replicas
      |> Enum.map(&get_merkle_tree(&1, segment_id))

    differences = compare_trees(trees)

    for {key, nodes_needing_update} <- differences do
      latest = fetch_latest(key, replicas)
      repair_replicas(nodes_needing_update, key, latest)
    end
  end
end
```

## Metadata Configuration

```yaml
metadata:
  # Quorum defaults (overridable per-volume)
  default_replicas: 3           # Auto-scales: min(3, cluster_size)
  default_read_quorum: 2
  default_write_quorum: 2

  # Conflict resolution
  conflict_strategy: lww_hlc    # Last-writer-wins with HLC

  # HLC clock skew handling
  hlc:
    max_clock_skew_ms: 1000     # Reject timestamps further ahead than this

  # Clock monitoring
  clock_monitoring:
    enabled: true
    check_interval: 30s
    warning_threshold_ms: 200   # Log warning
    critical_threshold_ms: 500  # Alert operators
    quarantine_threshold_ms: 1000  # Prevent writes from node

  # Consistent hashing
  virtual_nodes_per_physical: 64
  hash_algorithm: sha256

  # Background consistency
  anti_entropy:
    enabled: true
    interval: 6h

  # Read repair
  read_repair: async            # sync | async | disabled

  # Local storage engine
  storage_engine: sqlite        # sqlite | rocksdb
```

## Caching

Each node caches recently accessed metadata regardless of segment assignment:

```elixir
%MetadataCache{
  # LRU cache of file metadata
  files: %LRU{max_size: 100_000},

  # Directory entries
  directories: %LRU{max_size: 50_000},

  # Chunk locations (heavily cached)
  chunks: %LRU{max_size: 1_000_000},

  # Negative cache (path doesn't exist)
  negative: %LRU{max_size: 10_000, ttl: seconds(30)}
}
```

Cache invalidation via pub/sub when quorum writes complete. Stale cache entries are also detected and repaired during quorum reads.
