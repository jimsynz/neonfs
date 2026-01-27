# Architecture

This document describes the system architecture of NeonFS, including the Elixir control plane, Rust data plane, and their integration via Rustler NIFs.

## Component Overview

```
┌───────────────────────────────────────────────────────────────┐
│                WireGuard Mesh (Tailscale/Headscale)       │
│                                                           │
│  ┌─────────────────────────────────────────────────────┐  │
│  │                   Elixir Control Plane              │  │
│  │                                                     │  │
│  │  ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │  │
│  │  │ Ra Cluster  │  │   Policy    │  │    API      │  │  │
│  │  │ (Metadata)  │  │   Engine    │  │  Surfaces   │  │  │
│  │  └─────────────┘  └─────────────┘  └─────────────┘  │  │
│  │         │                │                │         │  │
│  │         └────────────────┼────────────────┘         │  │
│  │                          │                          │  │
│  │            ┌─────────────┴─────────────┐            │  │
│  │            │                           │            │  │
│  │      Rustler NIF                 Rustler NIF        │  │
│  │            │                           │            │  │
│  └────────────┼───────────────────────────┼────────────┘  │
│               │                           │               │
│  ┌────────────┼────────────┐  ┌───────────┼───────────┐  │
│  │            │            │  │           │           │  │
│  │   neonfs_fuse (crate)   │  │  neonfs_blob (crate)  │  │
│  │                         │  │                       │  │
│  │  ┌───────────────────┐  │  │  ┌─────────────────┐  │  │
│  │  │    FUSE Driver    │  │  │  │  Chunk Engine   │  │  │
│  │  │  (fuser library)  │  │  │  │  (FastCDC, RS)  │  │  │
│  │  └───────────────────┘  │  │  ├─────────────────┤  │  │
│  │                         │  │  │   Blob Store    │  │  │
│  │  All data flows via     │  │  │  (local I/O)    │  │  │
│  │  Elixir - no direct     │  │  ├─────────────────┤  │  │
│  │  blob store access      │  │  │   Compression   │  │  │
│  │                         │  │  │   (zstd)        │  │  │
│  │                         │  │  └─────────────────┘  │  │
│  └─────────────────────────┘  └───────────────────────┘  │
│                                                           │
└───────────────────────────────────────────────────────────┘
```

## Elixir Control Plane

The control plane manages all coordination, policy decisions, and external APIs.

### Per-Volume Supervision

Each volume runs in its own supervision tree, providing isolation and independent resource management:

```
                    ┌─────────────────────────────────┐
                    │       Cluster Supervisor        │
                    │  (Ra, node membership, users)   │
                    └─────────────────────────────────┘
                                    │
          ┌─────────────────────────┼─────────────────────────┐
          ▼                         ▼                         ▼
┌─────────────────────┐   ┌─────────────────────┐   ┌─────────────────────┐
│  Volume: documents  │   │   Volume: media     │   │  Volume: scratch    │
├─────────────────────┤   ├─────────────────────┤   ├─────────────────────┤
│ • MetadataServer    │   │ • MetadataServer    │   │ • MetadataServer    │
│ • PolicyEngine      │   │ • PolicyEngine      │   │ • PolicyEngine      │
│ • TieringManager    │   │ • TieringManager    │   │ • TieringManager    │
│ • ReplicationMgr    │   │ • ReplicationMgr    │   │ • ReplicationMgr    │
│ • Scrubber          │   │ • Scrubber          │   │ • Scrubber          │
│ • ChunkReaper       │   │ • ChunkReaper       │   │ • ChunkReaper       │
└─────────────────────┘   └─────────────────────┘   └─────────────────────┘
          │                         │                         │
          └─────────────────────────┼─────────────────────────┘
                                    ▼
                    ┌─────────────────────────────────┐
                    │     Rust Blob Store (shared)    │
                    └─────────────────────────────────┘
```

This architecture isolates volumes at the **coordination layer** (Elixir), not the **I/O layer** (Rust/disk). Each volume has independent supervision, backpressure, and rate limits, but all volumes contend for shared disk I/O and blob store memory.

**What's isolated (per-volume):**

- **Supervision**: Volume supervisor crashes and restarts independently without affecting other volumes
- **Backpressure**: Each volume manages its own request queue and rate limits
- **Scheduling**: Documents scrubs daily, media monthly, scratch never
- **Concurrency**: BEAM scheduler distributes volume work across cores

**What's shared (contention possible):**

- **Disk I/O**: Volumes on the same drives compete for bandwidth
- **Blob store memory**: Memory pressure affects all volumes
- **Physical storage**: Capacity is shared unless volumes are pinned to specific drives

Rust NIFs use async message passing via channels and Rustler resources to avoid blocking the BEAM scheduler. Long-running I/O operations yield back to Elixir promptly, preserving scheduler fairness across volumes.

For true I/O isolation, place volumes on separate physical drives.

### Backpressure and Graceful Degradation

A key reason for routing all filesystem operations through Elixir (rather than direct Rust-to-disk paths) is natural backpressure. Each file operation runs in its own process, streaming data between client and storage:

```
Client ←→ Operation Process ←→ Chunk Fetcher ←→ Blob Store
              │                      │
         (can slow down)      (can slow down)
```

Under load, this architecture degrades gracefully:

- **Process-per-operation**: BEAM scheduler distributes CPU fairly across operations
- **Streaming**: Large operations don't buffer entire files in memory; data flows through in chunks
- **Mailbox backpressure**: If a process can't keep up, its mailbox grows, naturally slowing senders
- **Memory limits**: Per-process heap limits prevent any single operation from exhausting memory

**Explicit limits where needed:**

While BEAM provides natural backpressure, some explicit limits are still appropriate:

| Limit | Purpose |
|-------|---------|
| Max concurrent operations per volume | Prevent one volume from starving others |
| Max in-flight replication bytes | Prevent replication from saturating network |
| Max uncommitted write age | Force cleanup of stalled writes |
| Client connection limits | Prevent connection exhaustion |

These are configured per-volume and enforced at the Elixir layer:

```elixir
%Volume{
  name: "documents",
  limits: %{
    max_concurrent_ops: 100,
    max_uncommitted_bytes: gb(10),
    max_replication_bandwidth: mbps(500)
  }
}
```

When limits are reached, new operations receive `{:error, :overloaded}` and clients see appropriate errors (HTTP 503, FUSE EIO with retry hint).

```elixir
defmodule NeonFS.VolumeSupervisor do
  use Supervisor

  def start_link(volume) do
    Supervisor.start_link(__MODULE__, volume, name: via(volume.id))
  end

  def init(volume) do
    children = [
      {NeonFS.Volume.MetadataServer, volume},
      {NeonFS.Volume.PolicyEngine, volume},
      {NeonFS.Volume.TieringManager, volume},
      {NeonFS.Volume.ReplicationManager, volume},
      {NeonFS.Volume.Scrubber, volume},
      {NeonFS.Volume.ChunkReaper, volume}
    ]

    Supervisor.init(children, strategy: :one_for_one)
  end
end
```

All volume supervisors share the same Rust blob store via the Rustler NIF — Rust doesn't know or care which volume is making requests.

### Distributed Volume Coordination

Each node runs a VolumeSupervisor for each volume it participates in. Volume processes on different nodes coordinate via standard BEAM distribution:

```elixir
# Node 1's documents MetadataServer talks to Node 2's
GenServer.call({NeonFS.Volume.MetadataServer, :node2}, {:lookup, path})

# Or use pg (process groups) for broadcast
:pg.get_members(:neonfs, {:volume, "documents", :metadata})
|> Enum.each(&send(&1, {:invalidate_cache, path}))
```

This means a volume with `replication: 3` across 5 nodes has 5 independent VolumeSupervisor trees, all cooperating to manage that volume's data.

### Control Plane Components

**Ra Cluster (Cluster-Wide State)**
- Raft-based consensus for cluster-wide state
- Stores: node membership, volume definitions, user/group definitions, encryption keys
- Per-volume metadata is managed by each volume's own processes (see [Metadata Storage](metadata.md))

**Policy Engine (Per-Volume)**
- Each volume has its own policy engine process
- Evaluates placement decisions based on volume configuration
- Manages tiering promotions/demotions for that volume
- Coordinates repair operations
- Handles replication scheduling

**API Surfaces**
- S3-compatible HTTP API
- CIFS/SMB via Samba VFS integration
- gRPC for CSI (Kubernetes integration)
- Docker/Podman volume plugin

## Rust Data Plane

The data plane is split into two separate Rustler crates, each with a single NIF boundary to Elixir.

### neonfs_fuse crate

The FUSE driver handles userspace filesystem operations:
- Runs in dedicated Rust thread, communicates with Elixir via channels
- Translates POSIX operations (read, write, readdir, etc.) to chunk requests
- All data access flows through Elixir — no direct blob store access
- Elixir handles both local and remote chunk resolution identically

This separation means the FUSE driver doesn't need to know about storage tiers, compression, or encryption — it just asks Elixir for bytes and receives bytes.

### neonfs_blob crate

The blob store handles local storage and chunk processing:

*Chunk Engine*
- Content-defined chunking (FastCDC) for deduplication
- SHA-256 content addressing (hash computed on original data)
- Reed-Solomon erasure coding
- Encryption (AES-256-GCM)
- Compression (zstd) — applied after hashing, before storage

*Blob Store*
- Local chunk storage on each node
- Tiered storage management (SSD, HDD)
- Content verification on read
- Atomic writes via rename

**Why separate crates?**

- **Clear boundaries**: FUSE is about kernel interface; blob store is about persistent storage
- **Independent testing**: Each crate has focused unit tests
- **Single code path**: All chunk access (local and remote) flows through Elixir, simplifying reasoning

## Performance Consideration: FUSE Data Path

The current design routes all FUSE operations through Elixir for simplicity and consistency. This adds latency (~100-500μs per operation) compared to direct Rust-to-disk access.

**Acceptable for**: Large sequential I/O, media streaming, infrequent access patterns

**May need optimisation for**: Database workloads (SQLite, LevelDB), build systems, IDE indexing

**Future optimisation**: When profiling indicates this is a bottleneck, implement a direct Rust fast path for local chunks that are:
- Already in page cache or blob store
- Not encrypted (no decryption needed)
- Not compressed (no decompression needed)

Elixir would still handle:
- Remote chunk fetches (requires cluster coordination)
- Encrypted/compressed chunks (Elixir manages keys and transformation state)
- Metadata operations (quorum reads/writes)
- Cache population on miss

**Trigger metrics to watch**:
- p99 read latency > 1ms for local cached chunks
- FUSE operation throughput < 10K ops/sec on idle system
- Significant CPU time in NIF marshalling (visible in profiler)

This optimisation is deferred to prioritise correctness in the initial implementation.

## On-Disk Blob Storage

Each node stores chunks in a content-addressed directory structure.

### Directory Layout

```
/var/lib/neonfs/
├── blobs/
│   ├── hot/                          # SSD tier
│   │   ├── ab/cd/
│   │   │   ├── abcd7c...             # Chunk file (hash as filename)
│   │   │   └── abcd2e...
│   │   └── ef/01/
│   │       └── ef01a0...
│   ├── warm/                         # Secondary SSD tier
│   │   └── ...
│   └── cold/                         # HDD tier
│       └── ...
├── meta/
│   ├── node.json                     # Node identity, cluster membership
│   └── drives.json                   # Drive configuration
└── wal/                              # Write-ahead log for crash recovery
    └── pending_writes.log
```

Linux's page cache handles caching of recently accessed chunks automatically. Application-level caching is only used for transformed data (see [Storage Tiering - Caching](storage-tiering.md#caching-strategy)).

### Chunk File Format

Chunks are stored with the content hash as the filename. Directory sharding prevents any single directory from containing too many entries. The hash is always computed on the original (uncompressed, unencrypted) data to preserve deduplication.

```
Path: /var/lib/neonfs/blobs/{tier}/{hash[0:2]}/{hash[2:4]}/{hash}
Content: chunk bytes after transformation pipeline:
         original → compress (if enabled) → encrypt (if enabled) → store
```

The transformation pipeline on write:
1. Compute SHA-256 hash of original data
2. Compress with zstd (if volume compression enabled)
3. Encrypt with AES-256-GCM (if volume encryption enabled)
4. Write to disk

The read pipeline reverses this:
1. Read from disk
2. Decrypt (if encrypted)
3. Decompress (if compressed)
4. Verify hash matches (if verification enabled)
5. Return original data

Two levels of prefix directories (4 hex chars = 65,536 possible directories) handles very large storage pools. For smaller deployments, a single level (256 directories) may suffice — this is configurable:

```yaml
storage:
  hash_prefix_depth: 2   # 1 = 256 dirs, 2 = 65K dirs, 3 = 16M dirs
```

No sidecar metadata files — all chunk metadata lives in the Elixir metadata layer.

### Integrity Verification

Verification behaviour is configured per-volume:

```elixir
%Volume{
  name: "documents",
  verification: %{
    on_read: :always,        # :always | :never | :sampling
    sampling_rate: 0.01,     # If sampling, verify 1% of reads
    scrub_interval: days(30) # Background verification cycle
  }
}

%Volume{
  name: "scratch",
  verification: %{
    on_read: :never,         # Trust the filesystem for temp data
    scrub_interval: days(90) # Infrequent but still required
  }
}
```

**Scrubbing is mandatory**: While `on_read` verification can be disabled for performance, background scrubbing cannot be completely disabled. Volumes with `on_read: :never` must still have a `scrub_interval` set (though it can be infrequent, e.g., 90 days). This ensures eventual detection of corruption even when per-read verification is skipped.

On read, Rust verifies content matches hash if requested:

```rust
fn read_chunk(hash: &Hash, verify: bool) -> Result<Vec<u8>> {
    let path = chunk_path(hash);
    let data = fs::read(&path)?;

    if verify {
        let actual_hash = sha256(&data);
        if actual_hash != *hash {
            return Err(Error::CorruptChunk { expected: hash, actual: actual_hash });
        }
    }

    Ok(data)
}
```

Elixir passes the `verify` flag based on volume config.

### Scrubbing

Background process reads and verifies all chunks:

```elixir
def scrub_schedule do
  # Verify all chunks once per month
  # Spread across time to avoid I/O spikes
  total_chunks = Metadata.chunk_count()
  chunks_per_hour = total_chunks / (30 * 24)

  # Scrub in batches
  stream_all_local_chunks()
  |> Stream.chunk_every(chunks_per_hour)
  |> Stream.each(&scrub_batch/1)
end
```

Corrupted chunks trigger repair (fetch from another replica).

### Tier Migration

Moving a chunk between tiers:

```rust
fn migrate_chunk(hash: &Hash, from_tier: Tier, to_tier: Tier) -> Result<()> {
    let data = read_chunk(hash, true)?;  // Verify on read

    let to_path = chunk_path_for_tier(hash, to_tier);
    fs::write(&to_path, &data)?;
    fs::sync_all(&to_path)?;             // Ensure durability

    let from_path = chunk_path_for_tier(hash, from_tier);
    fs::remove(&from_path)?;

    Ok(())
}
```

### Atomic Writes

New chunks written atomically via rename:

```rust
fn write_chunk(hash: &Hash, data: &[u8], tier: Tier) -> Result<()> {
    let final_path = chunk_path_for_tier(hash, tier);
    let temp_path = format!("{}.tmp.{}", final_path, random_id());

    // Write to temp file
    let mut file = File::create(&temp_path)?;
    file.write_all(data)?;
    file.sync_all()?;

    // Atomic rename
    fs::rename(&temp_path, &final_path)?;

    Ok(())
}
```

### Filesystem Recommendations

- **XFS or ext4**: Good general choices, handle many small files well
- **Enable checksums**: If using btrfs/ZFS, enable checksums for additional integrity
- **Disable atime**: Reduces unnecessary writes (`noatime` mount option)
- **Appropriate block size**: 4K typically fine, larger if chunks are always large

## Rustler NIF Integration

Each Rust crate exposes its own NIF module, with resource objects maintaining handles to Rust-side state.

### neonfs_fuse NIF

```elixir
# Start FUSE mount
{:ok, fuse} = NeonFS.Fuse.Native.mount("/mnt/cluster", callback_pid, opts)

# fuse is an opaque resource containing:
# - FUSE session (Rust thread)
# - Channel for sending operations to callback_pid
```

FUSE operations flow through channels to Elixir:

```
FUSE kernel → fuser event loop (Rust thread)
                    ↓
              mpsc::Sender (held in resource)
                    ↓
              Elixir process receives {:fuse_op, op, args, reply_ref}
                    ↓
              Elixir resolves metadata, fetches chunks (local or remote)
                    ↓
              NeonFS.Fuse.Native.reply(fuse, reply_ref, data)
```

The FUSE crate never touches the blob store directly — all data comes from Elixir.

### neonfs_blob NIF

```elixir
# Initialise blob store
{:ok, store} = NeonFS.Blob.Native.open(data_dir, opts)

# Chunk operations
{:ok, hash} = NeonFS.Blob.Native.write_chunk(store, data, tier, opts)
{:ok, data} = NeonFS.Blob.Native.read_chunk(store, hash, opts)
:ok = NeonFS.Blob.Native.migrate_chunk(store, hash, from_tier, to_tier)
:ok = NeonFS.Blob.Native.delete_chunk(store, hash)

# Chunking
chunks = NeonFS.Blob.Native.chunk_stream(store, data, strategy)
```

Options control per-call behaviour:
```elixir
# Read with decompression and verification
NeonFS.Blob.Native.read_chunk(store, hash,
  compressed: true,   # Decompress after read
  verify: true        # Verify hash matches
)

# Write with compression
NeonFS.Blob.Native.write_chunk(store, data, :hot,
  compress: :zstd,    # Compress before write
  level: 3            # Compression level
)
```

## I/O Scheduler

All I/O operations between the Elixir coordinator and the Rust storage engine flow through a demand-driven scheduler. This provides unified backpressure, priority management, and per-drive load balancing.

### Why a Scheduler?

Without centralised scheduling, background operations (repair, scrubbing, read repair) would compete uncontrolled with user-facing I/O:

```
# Bad: ad-hoc spawning with no backpressure
spawn(fn -> repair_chunk(chunk) end)  # Could spawn thousands
spawn(fn -> scrub_chunk(chunk) end)   # Competing with repairs
# Meanwhile, user reads are starved
```

The I/O scheduler ensures user-facing operations take priority while background work proceeds at a sustainable pace.

### Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     Elixir Coordinator                          │
│                                                                 │
│  ┌──────────┐  ┌──────────┐  ┌──────────┐  ┌──────────┐        │
│  │  User    │  │  Read    │  │  Repair  │  │  Scrub   │        │
│  │  Reads   │  │  Repair  │  │  Manager │  │  Worker  │        │
│  └────┬─────┘  └────┬─────┘  └────┬─────┘  └────┬─────┘        │
│       │             │             │             │               │
│       └─────────────┴──────┬──────┴─────────────┘               │
│                            ▼                                    │
│              ┌─────────────────────────────┐                    │
│              │       I/O Scheduler         │                    │
│              │  (GenStage / Broadway)      │                    │
│              │                             │                    │
│              │  • Priority queues          │                    │
│              │  • Per-drive demand         │                    │
│              │  • Work coalescing          │                    │
│              └─────────────┬───────────────┘                    │
│                            │                                    │
│       ┌────────────────────┼────────────────────┐               │
│       ▼                    ▼                    ▼               │
│  ┌─────────┐          ┌─────────┐          ┌─────────┐          │
│  │ /nvme0  │          │ /ssd0   │          │ /hdd0   │          │
│  │ worker  │          │ worker  │          │ worker  │          │
│  └────┬────┘          └────┬────┘          └────┬────┘          │
└───────┼────────────────────┼────────────────────┼───────────────┘
        │                    │                    │
        ▼                    ▼                    ▼
   Rust NIF             Rust NIF             Rust NIF
```

### Priority Classes

Operations are assigned priority classes that determine scheduling order:

| Priority | Class | Examples |
|----------|-------|----------|
| 1 (highest) | User read | FUSE read, S3 GET |
| 2 | User write | FUSE write, S3 PUT |
| 3 | Replication | Synchronous replica writes |
| 4 | Read repair | Background consistency repair |
| 5 | Repair/resilver | Chunk re-replication after node failure |
| 6 (lowest) | Scrubbing | Background integrity verification |

Higher priority work preempts lower priority. Within a priority class, work is processed fairly (round-robin or FIFO).

### Per-Drive Demand Management

Each physical drive has its own worker with configurable concurrency:

```elixir
%DriveWorker{
  path: "/mnt/nvme0",
  tier: :hot,

  # Concurrency limits by priority class
  max_concurrent: %{
    user_read: 32,      # NVMe can handle many parallel reads
    user_write: 16,
    replication: 8,
    background: 4       # Repair, scrub, etc.
  },

  # Current utilisation (updated dynamically)
  active: %{user_read: 12, user_write: 3, ...},
  queue_depth: 47
}
```

When a drive is saturated (queue_depth exceeds threshold), the scheduler:
1. Stops accepting new background work for that drive
2. Continues accepting user I/O (may queue)
3. Signals upstream producers to reduce demand

### Drive-Type Scheduling Strategies

Different storage media require different I/O patterns for optimal performance:

**SSD (NVMe, SATA SSD):**

```elixir
%DriveSchedulingStrategy{
  type: :ssd,

  # SSDs handle random I/O well - maximise parallelism
  io_pattern: :parallel,
  max_concurrent_ops: 32,

  # Can freely interleave reads and writes
  interleave_reads_writes: true,

  # No seek penalty, process in submission order
  ordering: :fifo
}
```

**HDD (spinning disk):**

```elixir
%DriveSchedulingStrategy{
  type: :hdd,

  # Minimise seeking - lower parallelism, batch by operation type
  io_pattern: :batched,
  max_concurrent_ops: 4,

  # Interleaving reads/writes causes constant seeking
  interleave_reads_writes: false,

  # Batch operations: process all pending reads, then all pending writes
  # Within each batch, sort by on-disk location if known
  ordering: :elevator,

  # Batch size before switching operation type
  batch_size: 16,
  batch_timeout_ms: 50   # Don't starve one type waiting for full batch
}
```

The batched strategy for HDDs:
1. Collect pending reads until batch is full or timeout
2. Sort by chunk hash prefix (approximates disk locality)
3. Execute read batch
4. Collect pending writes, sort, execute
5. Repeat

This dramatically reduces seek time compared to interleaved random I/O.

**Configuration per drive:**

```yaml
storage:
  drives:
    - path: /mnt/nvme0
      tier: hot
      type: ssd              # Uses SSD strategy

    - path: /mnt/hdd0
      tier: cold
      type: hdd              # Uses HDD strategy
      scheduling:
        batch_size: 32       # Override default
        max_concurrent_ops: 2
```

### Work Coalescing

The scheduler coalesces duplicate work requests:

```elixir
# Multiple read repairs for the same chunk become one
IOScheduler.submit(:read_repair, chunk_hash: "abc123", ...)
IOScheduler.submit(:read_repair, chunk_hash: "abc123", ...)  # Coalesced

# Multiple scrub requests for the same chunk
IOScheduler.submit(:scrub, chunk_hash: "abc123", ...)  # Only runs once
```

This prevents thundering herd problems when many concurrent reads discover the same stale replica.

### Dynamic Priority Adjustment

The scheduler can temporarily boost or reduce priorities based on system state:

```elixir
# Storage pressure: boost GC and repair priority
if storage_pressure > 0.9 do
  IOScheduler.boost_priority(:repair, by: 2)
  IOScheduler.boost_priority(:gc, by: 2)
end

# Partition recovery: boost repair after partition heals
on_partition_healed do
  IOScheduler.boost_priority(:read_repair, by: 1, duration: minutes(30))
end
```

### Configuration

```yaml
io_scheduler:
  # Global settings
  implementation: gen_stage    # gen_stage | broadway | simple_pool

  # Per-priority concurrency (default, overridable per-drive)
  default_concurrency:
    user_read: 16
    user_write: 8
    replication: 4
    background: 2

  # Backpressure thresholds
  queue_high_watermark: 100    # Start shedding background work
  queue_low_watermark: 20      # Resume background work

  # Coalescing
  coalesce_window_ms: 50       # Group duplicate requests within window
```

### Impact on Other Components

With the I/O scheduler in place, other components submit work rather than spawning directly:

**Read repair** (metadata.md):
```elixir
# Instead of: spawn(fn -> repair_replicas(...) end)
IOScheduler.submit(:read_repair, key: key, stale: stale_replicas, latest: latest)
```

**Scrubbing** (architecture.md):
```elixir
# Instead of direct iteration
IOScheduler.submit_batch(:scrub, chunks, priority: :lowest)
```

**Chunk reaper** (replication.md):
```elixir
# Cleanup uses background priority
IOScheduler.submit(:delete, chunk_hash: hash, priority: :background)
```

This unified approach ensures all I/O is coordinated, preventing any single operation type from starving others.

## Package Structure

NeonFS is organised as a collection of separate Elixir applications, each deployable as an independent service or container. Services discover each other and coordinate using Erlang distribution over the WireGuard mesh.

### Design Rationale

Different deployment scenarios benefit from different service topologies:

| Scenario | Topology |
|----------|----------|
| Home NAS (single node) | All services on one machine |
| Edge + central storage | FUSE services at edge, core services centralised |
| Kubernetes cluster | CSI driver pods, separate S3 gateway pods |
| Multi-site | Core services per site, cross-site replication |

By packaging each API surface as a separate application, operators can:
- Deploy services independently with their own resource limits
- Scale API surfaces horizontally (multiple S3 gateways)
- Isolate failures (FUSE crash doesn't affect S3 service)
- Run services on appropriate hardware (FUSE near clients, core near storage)

### Service Architecture

```
┌─────────────────────────────────────────────────────────────────────────┐
│                    WireGuard Mesh (Tailscale/Headscale)                 │
│                                                                         │
│  ┌─────────────────┐  ┌─────────────────┐  ┌─────────────────┐          │
│  │  Storage Node   │  │  Storage Node   │  │  Gateway Node   │          │
│  │                 │  │                 │  │                 │          │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │  ┌───────────┐  │          │
│  │  │neonfs_core│  │  │  │neonfs_core│  │  │  │ neonfs_s3 │  │          │
│  │  └───────────┘  │  │  └───────────┘  │  │  └───────────┘  │          │
│  │  ┌───────────┐  │  │  ┌───────────┐  │  │                 │          │
│  │  │neonfs_fuse│  │  │  │neonfs_fuse│  │  │                 │          │
│  │  └───────────┘  │  │  └───────────┘  │  │                 │          │
│  └────────┬────────┘  └────────┬────────┘  └────────┬────────┘          │
│           │                    │                    │                   │
│           └────────────────────┴────────────────────┘                   │
│                        Erlang Distribution                              │
└─────────────────────────────────────────────────────────────────────────┘
```

Services connect to each other via Erlang distribution. A `neonfs_s3` gateway on one node calls `neonfs_core` on storage nodes to read/write data. The distribution layer handles service discovery, load balancing, and failover.

### Elixir Applications

| Application | Namespace | Purpose | Rust NIFs |
|-------------|-----------|---------|-----------|
| `neonfs_core` | `NeonFS.Core` | Storage engine, metadata, coordination | `neonfs_blob` |
| `neonfs_fuse` | `NeonFS.FUSE` | FUSE filesystem mounting | `neonfs_fuse` |
| `neonfs_s3` | `NeonFS.S3` | S3-compatible HTTP API | — |
| `neonfs_docker` | `NeonFS.Docker` | Docker/Podman volume plugin | — |
| `neonfs_csi` | `NeonFS.CSI` | Kubernetes CSI driver | — |
| `neonfs_cifs` | `NeonFS.CIFS` | CIFS/SMB via Samba VFS | — |

Each application is a complete OTP application with its own supervision tree, configuration, and release.

**neonfs_core** (`:neonfs_core`)

The storage engine service. Nodes running `neonfs_core` participate in:
- Ra consensus cluster for metadata
- Blob storage (via embedded `neonfs_blob` Rust NIF)
- Replication and repair
- I/O scheduling

Core nodes form the storage cluster. Other services connect to core nodes to access data.

**neonfs_fuse** (`:neonfs_fuse`)

FUSE filesystem service. Provides local filesystem access by:
- Mounting FUSE filesystems (via embedded `neonfs_fuse` Rust NIF)
- Translating POSIX operations to NeonFS calls
- Connecting to `neonfs_core` nodes for data access

Can run on the same nodes as `neonfs_core` (co-located) or on separate client machines.

**neonfs_s3** (`:neonfs_s3`)

S3-compatible HTTP gateway:
- Bucket and object operations
- S3 signature v4 authentication
- Connects to `neonfs_core` nodes for storage

Stateless — can be horizontally scaled behind a load balancer.

**neonfs_docker** (`:neonfs_docker`)

Container volume plugin:
- Implements Docker/Podman plugin protocol
- Coordinates with `neonfs_fuse` for mounts
- Manages volume lifecycle

**neonfs_csi** (`:neonfs_csi`)

Kubernetes CSI driver:
- Controller service (volume provisioning)
- Node service (volume mounting via `neonfs_fuse`)
- gRPC interface

**neonfs_cifs** (`:neonfs_cifs`)

Windows/macOS file sharing:
- Samba VFS module integration
- SMB protocol translation
- Connects to `neonfs_core` for data

### Embedded Rust NIFs

Rust crates are embedded within the Elixir applications that need them using Rustler:

```
neonfs_core/
├── lib/
│   └── neon_fs/core/
│       └── blob/native.ex      # NIF module
└── native/
    └── neonfs_blob/            # Rust crate
        ├── Cargo.toml
        └── src/
            └── lib.rs

neonfs_fuse/
├── lib/
│   └── neon_fs/fuse/
│       └── native.ex           # NIF module
└── native/
    └── neonfs_fuse/            # Rust crate
        ├── Cargo.toml
        └── src/
            └── lib.rs
```

The Rust crates are not separately deployable — they're compiled into the Elixir releases that contain them.

### Service Discovery and Clustering

Services find each other using Erlang distribution:

```elixir
# neonfs_s3 connecting to core nodes
defmodule NeonFS.S3.CoreClient do
  def read_chunk(hash) do
    core_node = NeonFS.S3.NodeRegistry.select_core_node()
    :rpc.call(core_node, NeonFS.Core.BlobStore, :read, [hash])
  end
end
```

Configuration specifies which core nodes to connect to:

```yaml
# neonfs_s3 config
core_nodes:
  - neonfs_core@storage1.tail1234.ts.net
  - neonfs_core@storage2.tail1234.ts.net
  - neonfs_core@storage3.tail1234.ts.net
```

Services can also discover core nodes dynamically via the Ra cluster membership.

### Deployment Examples

**Single-node (home NAS):**

```yaml
# All services in one release
services:
  - neonfs_core
  - neonfs_fuse
  - neonfs_cifs
```

**Separated (edge + central):**

```yaml
# Storage nodes
storage1:
  services: [neonfs_core]
storage2:
  services: [neonfs_core]

# Edge/client nodes
desktop1:
  services: [neonfs_fuse]
  core_nodes: [storage1, storage2]

laptop1:
  services: [neonfs_fuse]
  core_nodes: [storage1, storage2]
```

**Kubernetes:**

```yaml
# Core StatefulSet
neonfs-core:
  replicas: 3
  services: [neonfs_core]

# S3 gateway Deployment
neonfs-s3:
  replicas: 2
  services: [neonfs_s3]
  core_nodes: [neonfs-core-0, neonfs-core-1, neonfs-core-2]

# CSI driver DaemonSet
neonfs-csi-node:
  services: [neonfs_csi, neonfs_fuse]
  core_nodes: [neonfs-core-0, neonfs-core-1, neonfs-core-2]
```

### Module Namespaces

Each application uses a distinct top-level namespace:

| Application | Namespace | File path |
|-------------|-----------|-----------|
| `neonfs_core` | `NeonFS.Core.*` | `lib/neon_fs/core/*.ex` |
| `neonfs_fuse` | `NeonFS.FUSE.*` | `lib/neon_fs/fuse/*.ex` |
| `neonfs_s3` | `NeonFS.S3.*` | `lib/neon_fs/s3/*.ex` |
| `neonfs_docker` | `NeonFS.Docker.*` | `lib/neon_fs/docker/*.ex` |
| `neonfs_csi` | `NeonFS.CSI.*` | `lib/neon_fs/csi/*.ex` |
| `neonfs_cifs` | `NeonFS.CIFS.*` | `lib/neon_fs/cifs/*.ex` |
