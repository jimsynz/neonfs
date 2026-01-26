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

Without a single leader, concurrent writes can conflict. We use Hybrid Logical Clocks (HLC) with last-writer-wins semantics:

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
- Clock skew doesn't cause consistency violations

Higher timestamp wins on conflict. This is simpler than version vectors and sufficient for filesystem metadata operations.

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
