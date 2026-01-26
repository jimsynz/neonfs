# NeonFS: A BEAM-Orchestrated Distributed Filesystem

## Overview

NeonFS is a distributed, content-addressed filesystem that combines the coordination strengths of Elixir/BEAM with the performance characteristics of Rust for low-level storage operations. It provides network-transparent storage with configurable durability, tiered storage, and multiple access methods including FUSE, S3-compatible API, and CIFS.

### Design Principles

1. **Separation of concerns**: Elixir handles coordination, policy, and APIs; Rust handles I/O, chunking, and cryptography
2. **Content-addressed storage**: All data stored as immutable, hash-identified chunks enabling deduplication and integrity verification
3. **Policy-driven replication**: Per-volume configuration of durability, performance, and cost tradeoffs
4. **Operational pragmatism**: Human-in-the-loop for ambiguous decisions; automation for routine operations

---

## Architecture

### Component Overview

```
┌───────────────────────────────────────────────────────────┐
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

### Elixir Control Plane

The control plane manages all coordination, policy decisions, and external APIs.

**Per-Volume Supervision**

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

This architecture provides:

- **Process isolation**: Volume supervisor crashes and restarts independently without affecting other volumes
- **Independent backpressure**: Each volume manages its own request queue
- **Per-volume resource limits**: Memory caps, operation rate limits at the Elixir layer
- **Independent scheduling**: Documents scrubs daily, media monthly, scratch never
- **Natural concurrency**: BEAM scheduler distributes volume work across cores

**Shared resources (not isolated):**

All volumes share the underlying Rust blob store and physical storage. This means:
- Disk I/O contention between volumes on same drives
- NIF scheduler impact from long-running Rust operations
- Memory pressure in the blob store affects all volumes

The isolation is at the **Elixir coordination layer**, not the **I/O layer**. A volume doing heavy sequential reads will impact latency for other volumes sharing the same drives. For true I/O isolation, place volumes on separate physical drives.

**Backpressure and Graceful Degradation**

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

**Distributed Volume Coordination**

Each node runs a VolumeSupervisor for each volume it participates in. Volume processes on different nodes coordinate via standard BEAM distribution:

```elixir
# Node 1's documents MetadataServer talks to Node 2's
GenServer.call({NeonFS.Volume.MetadataServer, :node2}, {:lookup, path})

# Or use pg (process groups) for broadcast
:pg.get_members(:neonfs, {:volume, "documents", :metadata})
|> Enum.each(&send(&1, {:invalidate_cache, path}))
```

This means a volume with `replication: 3` across 5 nodes has 5 independent VolumeSupervisor trees, all cooperating to manage that volume's data.

**Ra Cluster (Cluster-Wide State)**
- Raft-based consensus for cluster-wide state
- Stores: node membership, volume definitions, user/group definitions, encryption keys
- Per-volume metadata is managed by each volume's own processes (see Metadata Storage)

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

### Rust Data Plane

The data plane is split into two separate Rustler crates, each with a single NIF boundary to Elixir.

**neonfs_fuse crate**

The FUSE driver handles userspace filesystem operations:
- Runs in dedicated Rust thread, communicates with Elixir via channels
- Translates POSIX operations (read, write, readdir, etc.) to chunk requests
- All data access flows through Elixir — no direct blob store access
- Elixir handles both local and remote chunk resolution identically

This separation means the FUSE driver doesn't need to know about storage tiers, compression, or encryption — it just asks Elixir for bytes and receives bytes.

**neonfs_blob crate**

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

### Performance Consideration: FUSE Data Path

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

This optimisation is deferred to prioritise correctness in the initial implementation

### On-Disk Blob Storage

Each node stores chunks in a content-addressed directory structure.

**Directory Layout**

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

Linux's page cache handles caching of recently accessed chunks automatically. Application-level caching is only used for transformed data (see Caching section).

**Chunk File Format**

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

**Integrity Verification**

Verification behavior is configured per-volume:

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
    scrub_interval: nil      # No background scrubbing
  }
}
```

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

**Scrubbing**

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

**Tier Migration**

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

**Atomic Writes**

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

**Filesystem Recommendations**

- **XFS or ext4**: Good general choices, handle many small files well
- **Enable checksums**: If using btrfs/ZFS, enable checksums for additional integrity
- **Disable atime**: Reduces unnecessary writes (`noatime` mount option)
- **Appropriate block size**: 4K typically fine, larger if chunks are always large

### Rustler NIF Integration

Each Rust crate exposes its own NIF module, with resource objects maintaining handles to Rust-side state.

**neonfs_fuse NIF**

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

**neonfs_blob NIF**

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

---

## Data Model

### Chunks (Rust Layer)

The fundamental storage unit. Rust only knows about local chunks — it stores bytes and retrieves bytes by hash.

```rust
// What Rust knows about a chunk (local only)
struct LocalChunk {
    hash: [u8; 32],       // SHA-256 of ORIGINAL data (before compression/encryption)
    original_size: u32,   // Size of original data
    stored_size: u32,     // Size on disk (after compression)
    tier: Tier,           // Where it's stored locally
    compression: Option<Compression>,  // None, Zstd { level }
    created_at: u64,      // Unix timestamp
}
```

Rust tracks compression state because it needs to decompress on read. But Rust doesn't know or care about:
- Replica locations (other nodes)
- Which files reference this chunk
- Whether this chunk is part of a stripe
- Replication status
- Encryption (handled at Elixir layer, Rust stores ciphertext)

All of that is Elixir's concern.

### Chunk Metadata (Elixir Layer)

Elixir tracks everything about chunks across the cluster:

```elixir
%ChunkMeta{
  hash: <<sha256 bytes>>,       # Hash of ORIGINAL data
  original_size: 262_144,       # Size before compression
  stored_size: 180_000,         # Size on disk (after compression)

  # Transformation state
  compression: :zstd | :none,   # Compression algorithm used
  encrypted: false,             # Whether chunk is encrypted

  # Cluster-wide location tracking
  locations: [
    %{node: :node1, tier: :hot},
    %{node: :node2, tier: :cold},
    %{node: :node3, tier: :warm}
  ],

  # Durability state
  target_replicas: 3,
  state: :satisfied,  # :satisfied | :under_replicated | :over_replicated

  # For erasure-coded chunks
  stripe_id: nil,     # or stripe reference
  stripe_index: nil,  # position in stripe (0-9 data, 10-13 parity)

  # Lifecycle
  created_at: ~U[...],
  last_verified: ~U[...],
  write_id: nil       # non-nil if uncommitted
}
```

**Chunking Strategy**

| File Size | Strategy |
|-----------|----------|
| < 64 KB | Store as single chunk |
| 64 KB - 1 MB | Fixed-size 256 KB blocks |
| > 1 MB | Content-defined chunking (FastCDC), 256 KB - 1 MB variable |

Content-defined chunking uses rolling hash to find chunk boundaries, enabling deduplication even when content shifts within files.

### Stripes (Elixir Layer)

Groups of chunks encoded together for space-efficient durability. Stripes are purely an Elixir metadata concept — Rust just stores chunks.

```elixir
%Stripe{
  id: stripe_id,
  config: %{data_chunks: 10, parity_chunks: 4},
  
  # Chunk hashes in order (data first, then parity)
  chunks: [
    # Data chunks (indices 0-9)
    "sha256:abc...", "sha256:def...", ...
    # Parity chunks (indices 10-13)  
    "sha256:xyz...", ...
  ],
  
  # Derived from chunk metadata
  state: :healthy | :degraded | :critical
}
```

When Elixir needs to read from a degraded stripe:
1. Elixir determines which chunks are available
2. Elixir requests available chunks from Rust (possibly across nodes)
3. Elixir performs Reed-Solomon decode to reconstruct missing data
4. Elixir returns data to client

Rust never knows it's participating in erasure coding — it just fetches chunks by hash.

### Files (Elixir Layer)

Logical files are purely an Elixir metadata concept. Rust has no concept of files — only chunks.

```elixir
%File{
  id: file_id,
  volume_id: volume_id,
  path: "/documents/report.pdf",
  
  # Content: ordered list of chunk hashes
  chunks: ["sha256:abc...", "sha256:def...", ...],
  size: 15_728_640,
  
  # POSIX metadata
  mode: 0o644,
  uid: 1000,
  gid: 1000,
  
  # Timestamps
  created_at: ~U[...],
  modified_at: ~U[...],
  accessed_at: ~U[...],
  
  # Copy-on-write versioning
  version: 3,
  previous_version_id: prev_file_id
}
```

When a client reads a file:
1. FUSE request arrives at Rust (neonfs_fuse)
2. Rust sends request to Elixir: "read path X, offset Y, length Z"
3. Elixir looks up file metadata, determines which chunks are needed
4. Elixir fetches chunks via neonfs_blob NIF (local) or RPC (remote)
5. Elixir assembles data and returns to FUSE via reply channel
6. Rust returns data to client

All data flows through Elixir — this gives a single code path for local and remote access.

### Volumes

Logical containers with their own durability, tiering, and access configuration.

```
Volume {
  id: VolumeId
  name: String

  # Access control
  owner: UserId
  # (ACL entries stored separately, see Identity and Access Control)

  # Durability
  durability: {
    type: :replicate | :erasure

    # For replication
    factor: int
    min_copies: int

    # For erasure coding
    data_chunks: int
    parity_chunks: int
  }
  write_ack: :local | :quorum | :all
  repair_priority: :critical | :high | :normal | :low

  # Tiering
  tiering: {
    initial_tier: :hot | :warm | :cold
    promotion_threshold: int
    demotion_delay: Duration
  }

  # Compression
  compression: {
    algorithm: :zstd | :lz4 | :none
    level: int                       # Algorithm-specific level (zstd: 1-19)
    min_size: int                    # Don't compress chunks smaller than this
  }

  # Encryption
  encryption: {
    mode: :none | :server_side | :envelope
    key_id: String (if server_side)
  }

  # Caching (for transformed data only - raw chunks use Linux page cache)
  caching: {
    transformed_chunks: bool         # Cache decrypted/decompressed chunks
    reconstructed_stripes: bool      # Cache erasure-decoded data
    remote_chunks: bool              # Cache chunks fetched from other nodes
    max_memory: Size                 # Memory limit for app-level cache
  }

  # Integrity verification
  verification: {
    on_read: :always | :never | :sampling
    sampling_rate: float (if sampling)
    scrub_interval: Duration | nil
  }

  # Statistics
  logical_size: u64       # Size after dedup
  physical_size: u64      # Actual storage used
  chunk_count: u64
}
```

### Volume Configuration

Each volume has its own durability, tiering, and access settings configured directly.

**Example Volumes**

```elixir
# High-value documents - encrypted, compressed, heavily cached
%Volume{
  name: "documents",
  owner: "alice",

  durability: %{
    type: :replicate,
    factor: 3,
    min_copies: 2
  },
  write_ack: :quorum,

  tiering: %{
    initial_tier: :warm,
    promotion_threshold: 1,    # Accesses before promoting to hotter tier
    demotion_delay: days(7)    # Time without access before demoting
  },

  compression: %{
    algorithm: :zstd,
    level: 3,                  # Good balance of speed and ratio
    min_size: 4096             # Don't compress tiny chunks
  },

  encryption: %{
    mode: :server_side,
    key_id: "vol-documents-key"
  },

  # Cache decrypted+decompressed chunks to avoid repeated transformation
  caching: %{
    transformed_chunks: true,
    reconstructed_stripes: false,  # Not erasure-coded
    remote_chunks: true,
    max_memory: mb(512)
  },

  verification: %{
    on_read: :always,
    scrub_interval: days(30)
  },

  repair_priority: :critical
}

# Media archive - erasure coded, no compression (already compressed media)
%Volume{
  name: "media",
  owner: "alice",

  durability: %{
    type: :erasure,
    data_chunks: 10,
    parity_chunks: 4
  },
  write_ack: :local,

  tiering: %{
    initial_tier: :cold,
    promotion_threshold: 3,
    demotion_delay: days(30)
  },

  compression: %{
    algorithm: :none           # JPEG/MP4 don't compress well
  },

  encryption: %{mode: :none},

  # Cache reconstructed stripes (expensive to rebuild from parity)
  caching: %{
    transformed_chunks: false, # No encryption/compression
    reconstructed_stripes: true,
    remote_chunks: true,
    max_memory: gb(2)
  },

  verification: %{
    on_read: :sampling,
    sampling_rate: 0.01,
    scrub_interval: days(90)
  },

  repair_priority: :low
}

# Scratch space - minimal everything, rely on Linux page cache
%Volume{
  name: "scratch",
  owner: "alice",

  durability: %{
    type: :replicate,
    factor: 1,
    min_copies: 1
  },
  write_ack: :local,

  tiering: %{initial_tier: :hot},
  compression: %{algorithm: :none},
  encryption: %{mode: :none},

  # No app-level caching needed - unencrypted, uncompressed, local only
  # Linux page cache handles everything
  caching: %{
    transformed_chunks: false,
    reconstructed_stripes: false,
    remote_chunks: false,
    max_memory: 0
  },

  verification: %{on_read: :never, scrub_interval: nil},
  repair_priority: :low
}
```

---

## Metadata Storage

### The Scale Problem

A large filesystem might have millions of files, each with POSIX metadata (permissions, timestamps, extended ACLs). Keeping all of this in Ra (Raft) memory is impractical:

- 1 million files × 500 bytes metadata each = 500 MB minimum
- Plus chunk mappings, access stats, etc.
- Ra replicates everything to all nodes

### Tiered Metadata Architecture

Split metadata into tiers based on access patterns:

**Tier 1: Cluster State (Ra)**

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

**Tier 2: Chunk Metadata (Distributed Index)**

Moderate size, frequently accessed:
- Chunk → location mappings
- Replication status
- Stripe definitions

Sharded across nodes using consistent hashing with quorum-based replication (see Leaderless Quorum Model below).

**Tier 3: Filesystem Metadata (Leaderless Quorum)**

Large, requires both consistency and availability:
- Directory structures (parent → children mappings)
- File metadata (file_id → chunks, POSIX attrs)

Separated into two concerns with different sharding keys:
- **Directory entries**: Sharded by `hash(parent_path)`
- **File metadata**: Sharded by `hash(file_id)`

This separation ensures directory listings remain efficient (single segment query) while file metadata is evenly distributed regardless of path structure.

**Tier 4: Access Statistics (Ephemeral)**

High-volume, loss-tolerant:
- Per-chunk access counts
- Tiering decisions
- Performance metrics

Keep in ETS, aggregate periodically, don't replicate.

### Leaderless Quorum Model

Instead of single-node ownership, metadata segments use leaderless quorum replication. Each segment has a replica set, and operations require agreement from a quorum.

**Core Principle**

```
Write → Any replica → Quorum of replicas → Ack
Read  → Quorum of replicas → Return most recent
```

No single node "owns" the data. Any replica can serve requests, and quorum ensures consistency.

**Quorum Configuration**

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

**Write Flow**

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

**Read Flow with Read Repair**

```elixir
def quorum_read(segment_id, key) do
  replicas = get_replicas(segment_id)
  read_quorum = get_read_quorum(segment_id)

  responses = replicas
    |> Enum.map(&async_read(&1, key))
    |> await_quorum(read_quorum)

  {latest, stale_replicas} = find_latest_and_stale(responses)

  # Background repair - don't block the read
  if stale_replicas != [] do
    spawn(fn -> repair_replicas(stale_replicas, key, latest) end)
  end

  {:ok, latest.value}
end
```

**Conflict Resolution: Hybrid Logical Clocks**

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

### Directory and File Metadata Separation

To enable efficient directory listings while evenly distributing file metadata:

**Directory Entries** (sharded by `hash(parent_path)`)

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

**File Metadata** (sharded by `hash(file_id)`)

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

**Operation Examples**

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

### Consistent Hashing with Virtual Nodes

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

### Anti-Entropy (Background Consistency)

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

### Metadata Configuration

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

### Caching

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

---

## Node Management

### Node States

```
:online          # Normal operation
:draining        # Accepting reads, rejecting writes, migrating roles
:unreachable     # Cannot contact, unknown cause
:suspect         # Unreachable for extended period
:maintenance     # Operator-initiated downtime, expected return
:decommissioning # Migrating all data off, then remove
:dead            # Confirmed gone, repair aggressively
```

### Network Partition Behaviour

NeonFS relies on Ra (Raft) for cluster-wide consensus and quorum operations for metadata. Both provide well-defined partition behaviour.

**During a partition:**

| Component | Majority Partition | Minority Partition |
|-----------|-------------------|-------------------|
| Ra consensus | Continues operating | Cannot commit; reads may work |
| Metadata quorum ops | Work if quorum reachable | Fail (can't reach quorum) |
| Local chunk reads | Work (local data available) | Work (local data available) |
| Writes | Work if quorum reachable | Fail |

**Practical implications:**

- Minority partition becomes effectively read-only for data on local node
- No split-brain writes: quorum requirement prevents conflicting writes
- Operations in flight when partition occurs may timeout and fail
- HLC timestamps ensure write ordering is preserved across partition heal

**After partition heals:**

- Ra automatically reconciles (Raft guarantees)
- Metadata quorum reads will perform read-repair if stale
- Anti-entropy process catches any missed repairs
- No manual intervention required for data consistency

**Limitations (accepted):**

This design assumes:
- Partitions are rare (trusted network, typically LAN or WireGuard mesh)
- Partitions are transient (minutes to hours, not permanent)
- Minority side being read-only is acceptable during partition

For deployments requiring availability during partitions (e.g., geo-distributed with unreliable links), a more sophisticated approach would be needed—this is out of scope for the target environment.

**Monitoring:**

Partition events should trigger alerts:
- Node unreachable from multiple peers simultaneously
- Ra leader election (may indicate partition or leader failure)
- Quorum operation failure rate spike

### Clock Synchronisation

NeonFS uses timestamps for conflict resolution (HLC), lease management, and operational metrics. Correct operation requires reasonably synchronised clocks across nodes.

**Requirements:**

- All nodes must run NTP (or equivalent: chrony, systemd-timesyncd)
- Maximum acceptable clock skew: 1 second
- HLC tolerates small skew but cannot correct large drift

**Why clocks matter:**

| Feature | Clock Dependency | Effect of Skew |
|---------|------------------|----------------|
| HLC timestamps | Wall-clock component | Large skew → HLC degrades to logical clock only |
| Lease expiry | Absolute time comparison | Skew → early/late expiry, potential conflicts |
| Invite token TTL | Expiry checking | Skew → tokens expire early/late |
| Access statistics | Recency calculations | Skew → incorrect tiering decisions |
| Audit logs | Event ordering | Skew → misleading timelines |

**Clock skew detection:**

Nodes compare clocks during regular heartbeats and gossip:

```elixir
defmodule NeonFS.ClockMonitor do
  @max_skew_ms 1_000

  def check_peer_clock(peer_node) do
    t1 = System.system_time(:millisecond)
    {:ok, peer_time} = :rpc.call(peer_node, System, :system_time, [:millisecond])
    t2 = System.system_time(:millisecond)

    # Account for round-trip time
    local_estimate = div(t1 + t2, 2)
    skew = abs(peer_time - local_estimate)

    if skew > @max_skew_ms do
      Logger.warning("Clock skew detected",
        peer: peer_node,
        skew_ms: skew,
        threshold_ms: @max_skew_ms
      )
      :telemetry.execute([:neonfs, :clock, :skew_warning], %{skew_ms: skew}, %{peer: peer_node})
    end

    {:ok, skew}
  end
end
```

**Behaviour under skew:**

- **Skew < 1s**: Normal operation, HLC handles it
- **Skew 1-10s**: Warning logged, operations continue but lease timing may be affected
- **Skew > 10s**: Error logged, node should be investigated; may cause operational issues

**Operational guidance:**

```yaml
# /etc/chrony/chrony.conf or equivalent
server time.cloudflare.com iburst
server time.google.com iburst
makestep 0.1 3
```

Ensure NTP is running and synchronised before joining a cluster. The `neonfs cluster join` command should verify local clock is within tolerance.

### State Transitions

```
                    ┌─────────────┐
        ┌──────────▶│   online    │◀──────────┐
        │           └─────────────┘           │
        │                 │                   │
        │                 │ (network issue)   │
        │                 ▼                   │
        │           ┌─────────────┐           │
        │           │ unreachable │           │
        │           └─────────────┘           │
        │                 │                   │
   (reconnect)            │ (timeout)    (operator)
        │                 ▼                   │
        │           ┌─────────────┐           │
        │           │   suspect   │───────────┤
        │           └─────────────┘           │
        │                 │                   │
        │       (operator │ (extended         │
        │        input)   │  timeout)         │
        │                 ▼                   │
        │           ┌─────────────┐     ┌─────────────┐
        │           │    dead     │     │ maintenance │
        │           └─────────────┘     └─────────────┘
        │                                     │
        └─────────────────────────────────────┘
```

### Escalation Ladder

| Duration Unreachable | Action |
|---------------------|--------|
| < 5 minutes | Wait, likely transient |
| 5-30 minutes | Prepare repair plan, don't execute |
| 30 min - 2 hours | Escalate to operator for decision |
| > 2 hours (no response) | Begin repair if capacity available |

### Repair Prioritization

When repair is triggered, chunks are processed in priority order:

1. **Risk tier**: At-risk (0 healthy copies) > Degraded (below minimum) > Below target > Satisfied
2. **Volume priority**: Per-policy priority (critical > high > normal > low)
3. **Access recency**: Hot data before cold
4. **Repair cost**: Cheaper repairs first within tier (tiebreaker)

```elixir
def repair_priority(chunk) do
  {
    risk_tier(chunk),           # 0 = at-risk, 3 = satisfied
    volume_priority(chunk),     # Per policy
    -access_recency(chunk),     # More recent = higher priority
    repair_cost(chunk)          # Cheaper first
  }
end
```

### Node Cost Function

Each node has a cost function used for placement decisions, read routing, and multi-site replication.

```elixir
%NodeCost{
  node: :node1,
  
  # Network characteristics (measured or configured)
  latency_ms: %{
    node2: 1,        # Same rack
    node3: 2,        # Same datacenter
    node4: 50,       # Remote site
    node5: 120       # Different continent
  },
  bandwidth_mbps: %{
    node2: 10_000,
    node3: 10_000,
    node4: 1_000,
    node5: 100
  },
  
  # Current load
  io_pressure: 0.3,           # 0-1, current I/O utilization
  storage_pressure: 0.7,      # 0-1, how full
  cpu_pressure: 0.2,
  
  # Administrative preferences
  site: "us-west",
  rack: "rack-1",
  maintenance_window: ~T[02:00:00]..~T[06:00:00]
}
```

**Placement cost calculation:**

```elixir
def placement_cost(source_node, target_node, chunk_size) do
  costs = get_node_costs(source_node)
  target = get_node_costs(target_node)
  
  # Transfer cost (latency + bandwidth)
  transfer_time = costs.latency_ms[target_node] + 
                  (chunk_size / costs.bandwidth_mbps[target_node] * 8)
  
  # Load cost
  load_penalty = target.io_pressure * 100 + target.storage_pressure * 50
  
  # Site diversity bonus (negative cost = good)
  site_bonus = if target.site != costs.site, do: -200, else: 0
  rack_bonus = if target.rack != costs.rack, do: -50, else: 0
  
  transfer_time + load_penalty + site_bonus + rack_bonus
end
```

**Read routing:**

```elixir
def select_read_source(chunk_hash, requesting_node) do
  locations = get_chunk_locations(chunk_hash)
  
  locations
  |> Enum.map(fn loc -> 
    {loc, read_cost(requesting_node, loc.node, loc.tier)}
  end)
  |> Enum.min_by(fn {_loc, cost} -> cost end)
  |> elem(0)
end

def read_cost(from, to, tier) do
  base = if from == to, do: 0, else: get_latency(from, to)
  tier_cost = tier_latency(tier)  # SSD fast, HDD slow/maybe spinning
  base + tier_cost
end
```

**Write placement with site awareness:**

For multi-site deployments, ensure replicas span sites:

```elixir
def select_write_targets(chunk_hash, volume) do
  candidates = available_nodes()
  required = volume.durability.factor
  
  # Group by site
  by_site = Enum.group_by(candidates, & &1.site)
  
  # Select from different sites where possible
  selected = by_site
    |> Enum.flat_map(fn {_site, nodes} ->
      # Prefer lowest-cost node per site
      nodes |> Enum.sort_by(&placement_cost(Node.self(), &1, chunk_size)) |> Enum.take(1)
    end)
    |> Enum.take(required)
  
  # If not enough sites, fill from best remaining
  if length(selected) < required do
    remaining = candidates -- selected
    additional = remaining
      |> Enum.sort_by(&placement_cost(Node.self(), &1, chunk_size))
      |> Enum.take(required - length(selected))
    selected ++ additional
  else
    selected
  end
end
```

**Async replication to remote sites:**

For writes, prioritize local/fast nodes, replicate to remote sites asynchronously:

```elixir
def write_with_async_remote(chunk, data, volume) do
  # Immediate: write to local + nearby nodes (sync)
  local_targets = select_local_targets(chunk, volume)
  :ok = write_sync(local_targets, chunk, data, volume.write_ack)
  
  # Deferred: replicate to remote sites (async)
  remote_targets = select_remote_targets(chunk, volume)
  spawn_replication(remote_targets, chunk, data)
  
  :ok
end
```

---

## Storage Tiering

### Tier Definitions

| Tier | Typical Media | Use Case |
|------|--------------|----------|
| Hot | NVMe SSD | Active working set |
| Warm | SATA SSD | Recent/moderate access |
| Cold | HDD | Archive, infrequent access |

Linux's page cache provides automatic caching of recently accessed file data, which covers most caching needs for raw (unencrypted, uncompressed) chunks. Application-level caching is only used for transformed data — see the Caching section below.

### Drive State Tracking

Each node can have multiple drives, including multiple drives at the same tier.

```elixir
%Drive{
  id: drive_id,
  node: :node1,
  path: "/mnt/nvme0",     # Mount point
  tier: :hot,
  
  state: :active | :spinning_up | :standby,
  last_active: ~U[...],
  
  capacity: 500_000_000_000,  # 500 GB
  used: 350_000_000_000,      # 350 GB
  reserved: 25_000_000_000,   # 25 GB reserved for incoming replication
  
  # Performance characteristics (measured or configured)
  read_latency_p50_us: 100,
  write_latency_p50_us: 150,
  iops_limit: 100_000
}
```

**Example: Node with multiple hot drives**

```yaml
storage:
  drives:
    - path: /mnt/nvme0
      tier: hot
    - path: /mnt/nvme1
      tier: hot
    - path: /mnt/ssd0
      tier: warm
    - path: /mnt/hdd0
      tier: cold
    - path: /mnt/hdd1
      tier: cold
```

When writing to a tier with multiple drives, distribute based on:
- Available space
- Current I/O load
- Drive health

### Read Path Optimization

When locating a chunk for read, prefer sources that minimize latency and avoid spinning up idle drives:

```elixir
def locate_chunk(hash) do
  replicas = Metadata.get_replicas(hash)

  replicas
  |> Enum.sort_by(fn location ->
    {
      media_score(location.tier, location.state),  # SSD always beats HDD
      locality_score(location)                     # Local beats remote (tiebreaker)
    }
  end)
  |> List.first()
end

defp media_score(:hot, _), do: 0          # NVMe SSD - always fast
defp media_score(:warm, _), do: 1         # SATA SSD - always fast
defp media_score(:cold, :active), do: 2   # HDD already spinning
defp media_score(:cold, :spinning_up), do: 3
defp media_score(:cold, :standby), do: 10 # Avoid spin-up if possible
```

Priority order:
1. Local SSD (hot tier)
2. Local SSD (warm tier)
3. Remote SSD
4. Local HDD (already spinning)
5. Remote HDD (already spinning)
6. Local HDD (requires spin up)
7. Remote HDD (requires spin up)

### Drive Power Management

Power management is configured per-drive based on drive characteristics and user preference.

```yaml
storage:
  drives:
    - path: /mnt/nvme0
      tier: hot
      power_management: always_on    # SSDs - no spin-down concept

    - path: /mnt/hdd0
      tier: cold
      power_management: spin_down    # Desktop drive - spin down when idle
      idle_timeout: 30m              # Spin down after 30 minutes idle

    - path: /mnt/hdd1
      tier: cold
      power_management: always_on    # NAS/NVR drive - designed for 24/7 operation
```

**Power management modes:**

| Mode | Behaviour | Use For |
|------|-----------|---------|
| `always_on` | Never spin down | SSDs, NAS drives (WD Red, Seagate IronWolf), NVR drives |
| `spin_down` | Spin down after idle timeout | Desktop drives, drives where noise/power matters |

**Spin-down considerations:**

Drives configured for `spin_down` will enter standby after `idle_timeout` with no pending I/O. Before spinning down, check:
- Pending requests in queue
- Scheduled scrub or repair operations

**Drive selection preference:**

When multiple replicas exist, prefer reading from drives that are already active to avoid unnecessary spin-ups:

```elixir
defp drive_preference(drive) do
  case {drive.power_management, drive.state} do
    {:always_on, _} -> 0           # Always prefer always-on drives
    {:spin_down, :active} -> 1     # Already spinning, use it
    {:spin_down, :standby} -> 10   # Avoid spin-up if possible
  end
end
```

If the only replica is on a spun-down drive, it will be spun up—availability always wins over power savings.

### Tier Capacity and Contention

Tiers have limited capacity. When a tier is full, chunks must be evicted to make room for hotter data.

**Promotion/Demotion Logic**

Each chunk tracks access frequency:

```elixir
%ChunkAccessStats{
  hash: "sha256:abc...",
  access_count_1h: 15,      # Accesses in last hour
  access_count_24h: 47,     # Accesses in last 24 hours
  last_access: ~U[...],
  current_tier: :warm,
  preferred_tier: :hot      # Based on access pattern
}
```

**Eviction under pressure:**

When hot tier reaches capacity threshold (e.g., 90%):

```elixir
def evict_from_tier(tier, bytes_needed) do
  candidates = chunks_in_tier(tier)
    |> Enum.sort_by(&coldness_score/1)  # Least accessed first
    |> Stream.take_while(fn _ -> bytes_freed < bytes_needed end)
  
  for chunk <- candidates do
    demote_chunk(chunk, next_colder_tier(tier))
  end
end

defp coldness_score(chunk) do
  # Lower score = colder = better eviction candidate
  recency = hours_since(chunk.last_access)
  frequency = chunk.access_count_24h
  
  -recency + (frequency * 10)
end
```

**Promotion under contention:**

When a cold chunk is accessed frequently but hot tier is full:
1. Calculate if this chunk is "hotter" than coldest hot-tier chunk
2. If yes, demote coldest hot-tier chunk, promote this one
3. If no, leave it in place (it will be served from cold tier)

```elixir
def maybe_promote(chunk) do
  if should_promote?(chunk) do
    target_tier = preferred_tier(chunk)
    
    if tier_has_space?(target_tier) do
      promote_chunk(chunk, target_tier)
    else
      coldest = coldest_chunk_in_tier(target_tier)
      if hotter_than?(chunk, coldest) do
        demote_chunk(coldest, next_colder_tier(target_tier))
        promote_chunk(chunk, target_tier)
      end
    end
  end
end
```

### Caching Strategy

NeonFS uses a two-tier caching approach:

**Linux Page Cache (automatic)**

For raw chunk data (unencrypted, uncompressed), Linux's page cache provides automatic, efficient caching:
- Recently read chunks stay in memory
- Kernel manages eviction based on memory pressure
- No application overhead
- Works transparently for all file reads

This handles the common case well. An unencrypted, uncompressed volume with local access needs no application-level caching.

**Application-Level Cache (for transformed data)**

Application-level caching is only beneficial when there's expensive transformation work to save:

| Scenario | Why cache? |
|----------|------------|
| Encrypted chunks | Avoid repeated AES decryption |
| Compressed chunks | Avoid repeated zstd decompression |
| Erasure-coded reads | Avoid reconstructing from parity (expensive) |
| Remote chunks | Avoid network round-trips |

For volumes with no encryption, no compression, and local-only access, the application cache provides no benefit over the page cache.

**Cache Implementation**

```elixir
defmodule NeonFS.ChunkCache do
  # LRU cache keyed by {hash, transform_state}
  # transform_state = :raw | :decrypted | :decompressed | :reconstructed

  def get(hash, volume) do
    cache_key = {hash, transform_key(volume)}

    case :ets.lookup(@cache_table, cache_key) do
      [{_, data, _timestamp}] ->
        touch(cache_key)
        {:hit, data}
      [] ->
        :miss
    end
  end

  def put(hash, data, volume) do
    if should_cache?(volume) do
      cache_key = {hash, transform_key(volume)}
      :ets.insert(@cache_table, {cache_key, data, now()})
      maybe_evict()
    end
  end

  defp should_cache?(volume) do
    volume.caching.transformed_chunks or
    volume.caching.reconstructed_stripes or
    volume.caching.remote_chunks
  end

  defp transform_key(volume) do
    cond do
      volume.encryption.mode != :none -> :decrypted
      volume.compression.algorithm != :none -> :decompressed
      true -> :raw
    end
  end
end
```

**Cache Eviction**

Per-volume memory limits are enforced with LRU eviction:

```elixir
defp maybe_evict do
  current_size = :ets.info(@cache_table, :memory) * :erlang.system_info(:wordsize)

  if current_size > @max_cache_size do
    # Evict oldest entries until under limit
    entries = :ets.tab2list(@cache_table)
    |> Enum.sort_by(fn {_key, _data, timestamp} -> timestamp end)
    |> Enum.take(div(length(entries), 4))  # Evict oldest 25%

    Enum.each(entries, fn {key, _, _} -> :ets.delete(@cache_table, key) end)
  end
end
```

**When NOT to use application caching**

For scratch volumes or any volume where:
- `encryption.mode == :none`
- `compression.algorithm == :none`
- Data is primarily local (not erasure-coded, not multi-site)

Set all caching options to false and let Linux handle it.

---

## Replication and Durability

### Write Flow (Replicated Volume)

Writes are streamed — chunks are created and replicated as bytes arrive, with metadata committed only on completion.

```
1. Client opens write stream, receives write_id
2. As bytes arrive:
   a. Chunk engine splits stream into chunks
   b. Each chunk written immediately to local storage
   c. Replication initiated in parallel (pipelined)
   d. Chunks tagged with write_id, state: :uncommitted
3. When client signals completion:
   a. Verify all chunks received and replicated per policy
   b. Atomically: create/update file metadata, mark chunks :committed
   c. Ack to client
4. If client disconnects or aborts:
   a. Mark write_id as :abandoned
   b. Chunks remain uncommitted, eligible for cleanup
```

This pipelining means a large file upload can have chunks replicating to other nodes while still receiving bytes from the client.

### Write Flow (Erasure-Coded Volume)

```
1. Client opens write stream, receives write_id
2. As bytes arrive:
   a. Chunk engine accumulates data chunks
   b. When stripe complete (e.g., 10 data chunks):
      - Compute parity chunks (e.g., 4 parity)
      - Distribute all 14 chunks to different nodes
      - Tag with write_id, state: :uncommitted
3. On completion:
   a. Handle any partial final stripe (pad or use smaller config)
   b. Verify all stripes replicated
   c. Commit metadata
4. On abort: mark abandoned, cleanup later
```

### Uncommitted Chunks and Orphan Cleanup

Chunks from incomplete writes must not leak storage.

```
UncommittedChunk {
  hash: SHA256
  write_id: WriteId
  created_at: DateTime
  ttl: Duration              # Default: 24 hours
  state: :uncommitted | :abandoned
}
```

**Lifecycle:**

```
                    ┌─────────────┐
  write arrives ──▶ │ uncommitted │
                    └─────────────┘
                          │
           ┌──────────────┼──────────────┐
           │              │              │
           ▼              ▼              ▼
    ┌───────────┐  ┌───────────┐  ┌───────────┐
    │ committed │  │ abandoned │  │  expired  │
    │ (success) │  │  (abort)  │  │  (ttl)    │
    └───────────┘  └───────────┘  └───────────┘
           │              │              │
           │              └──────┬───────┘
           │                     │
           ▼                     ▼
      permanent             GC eligible
```

**Message-driven cleanup:**

Rather than periodic sweeps, the system uses message-driven cleanup:

```elixir
defmodule ChunkReaper do
  use GenServer
  
  # Called when a write is explicitly abandoned
  def handle_cast({:write_abandoned, write_id}, state) do
    schedule_cleanup(write_id, delay: :soon)
    {:noreply, state}
  end
  
  # Called when TTL expires (via Process.send_after)
  def handle_info({:ttl_expired, write_id}, state) do
    schedule_cleanup(write_id, delay: :soon)
    {:noreply, state}
  end
  
  # Actual cleanup considers current system state
  def handle_info({:do_cleanup, write_id}, state) do
    if good_time_to_cleanup?() do
      chunks = get_uncommitted_chunks(write_id)
      Enum.each(chunks, &delete_chunk/1)
    else
      # System busy, try again later
      schedule_cleanup(write_id, delay: :later)
    end
    {:noreply, state}
  end
  
  defp good_time_to_cleanup? do
    # Don't compete with active I/O
    io_pressure = Metrics.current_io_pressure()
    storage_pressure = Metrics.storage_pressure()
    
    cond do
      storage_pressure > 0.9 -> true   # Need space now
      io_pressure < 0.3 -> true        # System is idle
      true -> false                     # Wait for better time
    end
  end
end
```

This approach:
- Responds immediately to explicit aborts
- Handles TTL expiry via scheduled messages
- Defers actual deletion to low-contention periods
- Prioritizes cleanup when storage is tight

### Quorum Configurations

| Policy | Behaviour | Durability | Latency |
|--------|----------|------------|---------|
| `local` | Ack after local write | Lowest | Lowest |
| `quorum` | Ack after W of N confirm (e.g., 2 of 3) | Medium | Medium |
| `all` | Ack after all replicas confirm | Highest | Highest |

### Write Hole Mitigation

The write hole (client ack'd but replication incomplete when primary fails) is addressed by:

1. **Quorum writes**: Don't ack until W of N replicas confirm
2. **Pending write tracking**: Background process monitors incomplete replications
3. **Deadline enforcement**: Writes must complete replication within timeout or alert

### Known Risk: Local Write Acknowledgement

For volumes configured with `write_ack: :local`, there is an acknowledged data loss window:

```
1. Client writes chunk
2. Local node stores chunk, acks to client ✓
3. Background replication begins...
4. Local node fails before replication completes
5. Chunk is lost (client believed write was durable)
```

**This is by design**: Users choosing `:local` explicitly trade durability for write latency. This policy is intended for:
- Scratch/temp data that can be regenerated
- Build artifacts and caches
- Data where speed matters more than durability

**Volume creation should warn users**:
```
$ neonfs volume create scratch --write-ack local

WARNING: write_ack=local means data may be lost if this node fails
before background replication completes. Only use for data that
can be regenerated. Continue? [y/N]
```

**Possible future mitigations** (not currently implemented):
1. **Local WAL**: Write to fast local WAL before ack, replicate from WAL asynchronously. Survives process crash but not disk failure.
2. **Delayed ack**: Ack after min(replication_complete, N milliseconds). Bounds the risk window.
3. **Synchronous local redundancy**: Write to two local drives before ack. Survives single drive failure.

For now, the risk is documented and users must explicitly opt in.

```elixir
%PendingWrite{
  chunk: "sha256:abc123",
  targets: [:node1, :node2, :node3],
  confirmed: [:node1],
  created_at: ~U[...],
  deadline: ~U[...]  # Must complete by this time
}
```

### Erasure Coding Configuration

For storage-efficient durability, use erasure coding rather than low replication factors.

Recommended configurations:

| Config | Overhead | Fault Tolerance | Use Case |
|--------|----------|-----------------|----------|
| 4+2 | 1.5x | 2 failures | Small clusters |
| 10+4 | 1.4x | 4 failures | Large media archives |
| 8+3 | 1.375x | 3 failures | Balanced |

**Read path:**

- Best case: all data chunks available, no decoding needed
- Degraded: some data chunks missing, fetch any K of N (data + parity), decode

**Repair:** More expensive than replication (must read K chunks to rebuild 1) but storage-efficient.

### Partial Stripe Handling

Erasure coding requires a minimum amount of data to form a stripe (e.g., 10 data chunks × 256KB = 2.5MB for a 10+4 config). Files smaller than this, or the tail end of larger files, need special handling.

**Strategy: Hybrid Replication Fallback**

| Scenario | Strategy |
|----------|----------|
| File < min_stripe_size | Replicate using fallback replica count |
| Full stripes in large file | Erasure code normally |
| Partial final stripe | Replicate the remainder |

This avoids:
- Massive overhead from padding small files (50KB → 2.5MB)
- Complexity of packing unrelated files into shared stripes
- Under-protected partial stripes waiting for more data

**Configuration:**

```elixir
%Volume{
  name: "media",
  durability: %{
    type: :erasure,
    data_chunks: 10,
    parity_chunks: 4,

    # Handling data that doesn't fill a stripe
    small_file_strategy: :replicate,    # :replicate | :pad
    small_file_replicas: 3,             # Replica count when using :replicate
    min_stripe_threshold: 2_500_000     # Bytes; below this, use small_file_strategy
  }
}
```

**Example: 5MB file on 10+4 volume (256KB chunks)**

```
File: 5MB total
Stripe capacity: 10 × 256KB = 2.56MB

Stripe 1: chunks 0-9 (2.56MB) → erasure coded (10 data + 4 parity)
Remainder: ~2.44MB (chunks 10-18) → replicated (3 copies each)
```

The remainder doesn't fill a stripe, so it falls back to replication. If the file grows later (append), the replicated chunks can be promoted into a full stripe.

**Alternative: Padding (not recommended)**

Setting `small_file_strategy: :pad` will pad partial stripes with zero-filled chunks. This wastes storage but keeps all data under the same durability model. Only use if consistency of durability model matters more than storage efficiency.

### Garbage Collection

GC uses a simple metadata-walk approach rather than distributed reference counting. This trades some efficiency for correctness and simplicity.

**Approach: Mark and Sweep**

```
1. Mark phase: Walk all committed file metadata, collect referenced chunk hashes
2. Sweep phase: Any chunk not in the referenced set is garbage
3. Delete phase: Remove garbage chunks (with grace period)
```

**Why not reference counting?**

Distributed reference counting (including weighted variants) has failure modes where lost messages can prevent chunks from ever being collected, or worse, cause premature deletion. The coordination required to handle these failures correctly negates the performance benefit.

GC is a background operation—it can afford to be slow and thorough.

**Implementation:**

```elixir
defmodule NeonFS.GarbageCollector do
  def collect(volume) do
    # Phase 1: Build set of all referenced chunks
    referenced = volume
      |> stream_all_files()
      |> Stream.flat_map(& &1.chunks)
      |> MapSet.new()

    # Phase 2: Find unreferenced chunks
    garbage = volume
      |> stream_all_chunks()
      |> Stream.reject(&MapSet.member?(referenced, &1.hash))
      |> Enum.to_list()

    # Phase 3: Delete with grace period
    for chunk <- garbage do
      if chunk.created_at < hours_ago(24) do
        schedule_deletion(chunk, grace_period: hours(1))
      end
    end
  end
end
```

**Safety measures:**

- **Grace period**: Chunks aren't deleted immediately; scheduled for deletion after 1 hour. Allows cancellation if a race condition is detected.
- **Age threshold**: Only chunks older than 24 hours are considered for GC. Recent chunks may be part of in-progress writes.
- **Uncommitted chunk exclusion**: Chunks with `write_id` set (uncommitted) are never collected by GC—handled separately by the ChunkReaper.

**Scheduling:**

- GC runs during low-activity periods (configurable)
- Can be triggered manually or by storage pressure
- Per-volume scheduling: critical volumes checked more frequently

```yaml
gc:
  schedule: "0 3 * * *"      # Daily at 3 AM
  storage_pressure_threshold: 0.85  # Also run if >85% full
  grace_period: 1h
  age_threshold: 24h
```

---

## Security Model

### Network Security (Deployment Recommendation)

NeonFS itself doesn't mandate a specific network security model — this is a deployment concern. However, we strongly recommend running nodes on a private network, such as:

- **WireGuard mesh** via Tailscale or self-hosted Headscale
- **Private VLAN** if all nodes are in the same datacenter
- **VPN** for cross-site deployments

BEAM distribution is trusting by default. Once a node connects, it has full RPC access. Protect the network perimeter accordingly.

```
┌─────────────────────────────────────────────────┐
│  Recommended: Private network (WireGuard/VLAN)  │
│                                                 │
│  ┌─────────┐  ┌─────────┐  ┌─────────┐          │
│  │ node1   │──│ node2   │──│ node3   │          │
│  └─────────┘  └─────────┘  └─────────┘          │
│                                                 │
│  BEAM distribution + data plane traffic         │
└─────────────────────────────────────────────────┘
         │
    Your responsibility to secure
```

### Cluster Authentication

Nodes authenticate to join the cluster via a single-use invite token system.

**Node Join Flow:**

```
1. Operator creates invite token (short-lived, single-use)
   $ neonfs cluster create-invite --expires 1h
   Token: bfs_inv_7x8k2m9p...

2. New node presents token
   $ neonfs cluster join --token bfs_inv_7x8k2m9p...

3. Cluster validates token:
   - Check token exists and not expired
   - Check token not already used
   - Mark token as used in Ra (before returning success)

4. Cluster registers new node
   Returns: cluster state, peer list

5. New node joins BEAM cluster and Ra consensus
```

**Token Properties:**

```elixir
%InviteToken{
  id: "bfs_inv_7x8k2m9p...",
  created_at: ~U[...],
  expires_at: ~U[...],
  created_by: :node1,
  used: false,          # Set to true on successful join
  used_by: nil,         # Node that used this token
  used_at: nil          # When it was used
}
```

**Security properties:**
- **Single-use**: Token is marked used in Ra before join completes; replay attempts fail
- **Short-lived**: Default 1 hour expiry limits exposure window
- **Auditable**: Token usage recorded with timestamp and joining node

This prevents random nodes from joining but doesn't require complex PKI. For production deployments requiring stronger authentication, implement TLS on BEAM distribution with mutual certificate verification — but this is optional and deployment-specific.

### Key Hierarchy

Keys are used for encryption only (not network auth):

```
Cluster Master Key
    │
    └─► Per-Volume Keys (for server-side encryption)
            │
            └─► Wrapped with master key, stored in metadata

User Public Keys (for envelope encryption)
    │
    └─► Stored in metadata, used to wrap chunk DEKs
```

**Master Key Storage:**

The cluster master key should be protected. Options:

- Derived from passphrase at cluster startup
- Stored in external secret manager (Vault, AWS KMS, etc.)
- Stored encrypted on disk, unlocked at boot

For a home lab, passphrase-at-boot is reasonable. For production, integrate with a proper secret manager.

### Data Encryption

Three encryption modes, with different tradeoffs for multi-user scenarios and deduplication effectiveness.

**Mode 1: No encryption (`mode: :none`)**

- Chunks stored in plaintext
- Deduplication works fully across configured scope
- Suitable for non-sensitive data

**Mode 2: Server-side encryption (`mode: :server_side`)**

- Volume has a symmetric key (AES-256)
- Chunks encrypted before storage, decrypted on read
- All users with volume access share the key
- Server can read all data

```
Chunk on disk:
  ciphertext: AES-256-GCM(data, volume_key)
  nonce: unique per chunk
```

**Mode 3: Envelope encryption with user keys (`mode: :envelope`)**

PGP-style approach for true multi-user access control:

```
Chunk on disk:
  ciphertext: AES-256-GCM(data, chunk_dek)
  wrapped_keys: [
    {user: "alice", wrapped_dek: RSA_encrypt(chunk_dek, alice_pubkey)},
    {user: "bob", wrapped_dek: RSA_encrypt(chunk_dek, bob_pubkey)},
  ]
  nonce: unique per chunk
```

- Each chunk encrypted with a random DEK (data encryption key)
- DEK wrapped to each authorised user's public key
- Adding a user: wrap DEK to their key, no re-encryption of data
- Removing a user: remove their wrapped key (they can't decrypt new chunks; for old chunks, must re-encrypt if you want true revocation)

**Envelope Encryption: Key Management Responsibilities**

Envelope encryption is a power-user feature requiring external key management. NeonFS provides the cryptographic infrastructure but does not manage private keys.

*What NeonFS provides:*
- Public key registration and storage
- DEK generation and wrapping to registered public keys
- Wrapped DEK storage and retrieval
- Re-wrapping DEKs when users are added to a volume

*What users must provide:*
- Private key generation (RSA-4096 or Ed25519 recommended)
- Private key storage (GPG keyring, hardware token, Vault, etc.)
- Client-side decryption (private key never sent to server)
- Key backup and recovery strategy

*Access patterns:*
```
Read flow (envelope encryption):
1. Client requests file
2. Server returns ciphertext + wrapped DEK for requesting user
3. Client unwraps DEK using their private key (client-side)
4. Client decrypts ciphertext (client-side)
```

*Implications:*
- **Lost private key = lost access**: NeonFS cannot recover data if user loses their key
- **Headless access**: Service accounts need accessible private keys (HSM, Vault agent, mounted keyfile)
- **Key rotation**: User generates new keypair, admin re-wraps DEKs to new public key, old key can be revoked
- **No server-side search**: Server cannot inspect plaintext for indexing or search

*Future integration points* (not in initial implementation):
- PKCS#11 / HSM integration
- Vault Transit secrets engine
- Agent-based key injection

### Deduplication and Encryption Interaction

Content hashing is always performed on **original (plaintext) data**, enabling duplicate detection regardless of encryption. However, actual storage savings depend on whether the ciphertext can be shared.

**Key distinction**: Duplicate *detection* vs storage *savings*

| Scenario | Detection | Storage Savings | Why |
|----------|-----------|-----------------|-----|
| No encryption | ✓ | ✓ | Same bytes stored |
| Same volume, server-side encryption | ✓ | ✓ | Same key → same ciphertext |
| Cross-volume, different encryption keys | ✓ | ✗ | Different keys → different ciphertext |
| Same volume, envelope encryption | ✓ | ✓ | Same DEK reused → same ciphertext |

**Example: Cross-volume with different keys**

```
Volume A (key: K1):
  file1.txt = "hello world"
  → hash = sha256("hello world") = abc123
  → ciphertext = AES(K1, "hello world") = XXXXX

Volume B (key: K2):
  file2.txt = "hello world"
  → hash = sha256("hello world") = abc123 (match!)
  → ciphertext = AES(K2, "hello world") = YYYYY (different!)
```

The hashes match, so deduplication *detects* the duplicate. But the ciphertexts differ, so both must be stored. The system tracks them as logically deduplicated (shared hash) but physically separate (different stored bytes).

**Implications by encryption mode:**

| Mode | Dedup Scope | Notes |
|------|-------------|-------|
| `:none` | Full (per config) | Cross-volume dedup works if enabled |
| `:server_side` | Within same-key boundary | Typically per-volume; cross-volume only if volumes share key |
| `:envelope` | Within same-DEK boundary | DEK reuse enables dedup; fresh DEK per chunk disables it |

**Envelope encryption DEK strategy:**

For envelope encryption, the DEK selection strategy affects deduplication:

```elixir
%Volume{
  encryption: %{
    mode: :envelope,
    dek_strategy: :content_derived  # :random | :content_derived
  }
}
```

- `:random` — Fresh DEK per chunk. Maximum security, no ciphertext dedup.
- `:content_derived` — DEK derived from content hash + volume secret. Same plaintext → same DEK → same ciphertext. Enables dedup but reveals duplicate existence to storage layer.

Default is `:random` for maximum security. Use `:content_derived` only when dedup savings outweigh the metadata leakage concern.

**Multi-user implications:**

| Scenario | Server-side | Envelope |
|----------|-------------|----------|
| Shared volume, all users equal | Simple | Overkill |
| Need to revoke access immediately | Re-key volume | Re-encrypt affected chunks |
| Distrust server operators | No protection | Full E2E |
| Deduplication | Full | Per-user (same DEK) or limited |

**Recommendation:** Start with server-side encryption. Add envelope mode later for high-security use cases.

**Key Storage:**

- Server-side: volume keys stored in metadata, wrapped with cluster master key
- Envelope: user public keys stored in metadata; private keys managed by users (or a separate key service)

### Identity and Access Control

Access control requires storing users, groups, and permissions in the metadata layer.

**Users**

```
User {
  id: UserId
  name: String
  
  # Authentication
  password_hash: Argon2 hash (for API auth)
  public_key: RSA/Ed25519 (for envelope encryption)
  api_keys: [ApiKey]
  
  # Metadata
  created_at: DateTime
  disabled: bool
}
```

**Groups**

```
Group {
  id: GroupId
  name: String
  members: [UserId]
  created_at: DateTime
}
```

**Volume Access Control**

Each volume has an access control list:

```
VolumeACL {
  volume_id: VolumeId
  owner: UserId              # Full control
  
  entries: [
    %{principal: {:user, UserId} | {:group, GroupId},
      permissions: [:read | :write | :admin]}
  ]
}
```

**File/Directory ACLs (Optional)**

For finer-grained control, support POSIX-style ACLs on files and directories:

```
FileACL {
  path: String
  volume_id: VolumeId
  
  # POSIX mode bits (for basic compatibility)
  mode: u16
  uid: UserId
  gid: GroupId
  
  # Extended ACL entries (optional)
  acl_entries: [
    %{type: :user | :group | :mask | :other,
      id: UserId | GroupId | nil,
      permissions: :r | :w | :x | :rw | :rx | :rwx}
  ]
}
```

**CIFS/SMB ACL Mapping**

When accessed via CIFS, map to Windows-style ACLs:

- NeonFS users ↔ Windows SIDs (via Samba's idmap)
- NeonFS groups ↔ Windows groups
- Permission mapping: read/write/admin → appropriate Windows ACEs

This is handled at the Samba VFS layer, translating between Windows semantics and NeonFS's internal model.

**Authorization Check**

```elixir
def authorize(user, action, resource) do
  cond do
    # Volume-level check
    is_volume_owner?(user, resource.volume) -> :ok
    has_volume_permission?(user, resource.volume, action) -> :ok
    
    # File-level check (if ACLs enabled)
    has_file_permission?(user, resource.path, action) -> :ok
    
    true -> {:error, :forbidden}
  end
end

defp has_volume_permission?(user, volume, action) do
  acl = get_volume_acl(volume)
  
  user_permissions = acl.entries
    |> Enum.filter(fn entry ->
      match_principal?(entry.principal, user)
    end)
    |> Enum.flat_map(& &1.permissions)
    |> MapSet.new()
  
  required = permission_for_action(action)
  MapSet.member?(user_permissions, required)
end
```

### Threat Model Summary

**Primary threats addressed:**
- Unauthorised volume/file access (via access control)
- Data at rest on stolen drives (via encryption)
- Corrupted data (via content-addressing and verification)

**Deployment concerns (your responsibility):**
- Network security between nodes
- Physical security of nodes
- Secure storage of master key / passphrase

**Accepted risks — Cluster Node Trust:**

BEAM distribution is inherently trusting. Once a node joins the cluster, it has full RPC access to all other nodes. This is a fundamental property of the Erlang runtime, not something NeonFS can meaningfully restrict without abandoning BEAM's benefits.

A compromised cluster node can:
- Read any chunk from any other node
- Modify cluster metadata (volumes, users, ACLs)
- Impersonate users for internal operations
- Delete or corrupt data across the cluster

**This means:**
- All cluster nodes must be equally trusted
- A single compromised node = full cluster compromise
- Network perimeter security is critical (WireGuard/VPN strongly recommended)
- Physical access to any node should be treated as access to all data

**Why we accept this:**
- Fighting BEAM's trust model would sacrifice its operational benefits (hot code reload, distributed debugging, cluster management)
- The target deployment is trusted environments (home lab, small team infrastructure)
- Network-level isolation provides practical security for these use cases
- Multi-tenant or zero-trust deployments are explicitly out of scope

**Additional accepted risks:**
- Admin with master key access can read all server-side encrypted data
- Envelope encryption provides user-level isolation, but requires users to manage their own keys

---

## Cluster Upgrades

### Version Compatibility

Upgrading a running cluster requires careful coordination. Nodes running different versions must interoperate during rolling upgrades.

**Metadata versioning:**

All persistent metadata structures include a schema version:

```elixir
%FileMeta{
  schema_version: 2,
  id: "...",
  # ... fields
}

%ChunkMeta{
  schema_version: 1,
  hash: "...",
  # ... fields
}
```

**Migration approach:**

Similar to database migrations, metadata schema changes are versioned and applied incrementally:

```elixir
defmodule NeonFS.Migrations do
  # Migrations are numbered and applied in order
  @migrations [
    {1, NeonFS.Migrations.V1_InitialSchema},
    {2, NeonFS.Migrations.V2_AddHLCTimestamps},
    {3, NeonFS.Migrations.V3_SplitDirectoryEntries}
  ]

  def migrate(from_version, to_version) do
    @migrations
    |> Enum.filter(fn {v, _} -> v > from_version and v <= to_version end)
    |> Enum.each(fn {_v, module} -> module.up() end)
  end
end
```

**Rolling upgrade protocol:**

```
1. New version deployed to one node, joins cluster
2. New node advertises its version capabilities
3. Cluster operates in "compatibility mode" (lowest common version)
4. Once all nodes upgraded, leader triggers migration
5. Migration runs as background operation
6. Cluster exits compatibility mode
```

**Design requirements (to be detailed during implementation):**

- Backwards-compatible wire protocol changes (additive only, or versioned)
- Metadata readers must handle older schema versions
- Migrations must be resumable (crash-safe)
- Rollback path for failed upgrades
- Version skew limits (e.g., max 1 major version difference between nodes)

This area requires detailed design before Phase 2 (clustering) to ensure upgrades are possible from the start.

---

## Disaster Recovery

### Recovery Scenarios

**Single node failure:**

1. Cluster detects node unreachable
2. Escalation ladder determines timing
3. When repair triggered, chunks re-replicated from surviving copies
4. Prioritized by risk tier and volume configuration

**Multiple node failure (quorum intact):**

1. Same as single node, parallelized
2. May trigger capacity warnings if insufficient space
3. Repair rate-limited to avoid overwhelming remaining nodes

**Catastrophic failure (single survivor):**

1. Surviving node continues serving local data (degraded mode)
2. New nodes join via invite token
3. Data re-replicated from survivor
4. Once quorum restored, normal operation resumes

**Total loss (restore from backup):**

1. Initialize new cluster
2. Restore metadata from backup
3. Restore chunks from external backup (S3, tape, etc.)
4. Verify integrity via scrubbing

### Backup Strategy

NeonFS replication is not backup — it protects against node failure, not against accidental deletion, corruption, or ransomware.

**Recommended backup approach:**

- Regular snapshots of metadata (Ra state)
- Periodic export of chunks to external storage (S3-compatible)
- Test restores periodically

```bash
# Export volume to external S3
$ neonfs backup create documents \
    --destination s3://backup-bucket/neonfs/ \
    --schedule daily

# Restore
$ neonfs backup restore documents \
    --source s3://backup-bucket/neonfs/documents-2025-01-15/
```

---

## API Surfaces

### S3-Compatible API

Standard S3 operations mapped to NeonFS:

| S3 Operation | NeonFS Mapping |
|--------------|----------------|
| CreateBucket | Create volume |
| PutObject | Write file |
| GetObject | Read file |
| DeleteObject | Delete file (CoW, GC later) |
| ListObjects | Directory listing |
| HeadObject | File metadata |

Authentication via S3 signature v4 mapped to NeonFS users.

### FUSE Mount

Local filesystem access via FUSE driver:

```bash
$ neonfs mount /mnt/cluster --volume documents
```

Supports standard POSIX operations. Limitations:
- No hard links (content-addressed model)
- No extended attributes (future consideration)
- Append-only semantics for concurrent writers

### Docker/Podman Volume Plugin

HTTP-based plugin protocol:

```
POST /VolumeDriver.Create   → Create subvolume
POST /VolumeDriver.Mount    → FUSE mount at container path
POST /VolumeDriver.Unmount  → Unmount
POST /VolumeDriver.Remove   → Delete subvolume (optional)
```

Usage:

```bash
$ docker volume create -d neonfs -o volume=mydata myvolume
$ docker run -v myvolume:/data myimage
```

### CSI Driver (Kubernetes)

gRPC interface implementing Container Storage Interface:

**Controller Service:**
- CreateVolume / DeleteVolume
- ControllerPublishVolume / ControllerUnpublishVolume

**Node Service:**
- NodeStageVolume / NodeUnstageVolume
- NodePublishVolume / NodeUnpublishVolume

### CIFS/SMB (via Samba)

Samba VFS module translates SMB operations to NeonFS API calls. Enables Windows client access and macOS Finder integration.

---

## Deployment and CLI

NeonFS is deployed as a self-contained daemon with a separate Rust-based CLI for fast interaction.

### Architecture Overview

```
┌─────────────────┐         ┌──────────────────────────────────┐
│   Rust CLI      │         │     Elixir Daemon (BEAM)         │
│   (neonfs)      │         │     (neonfs@localhost)           │
│                 │         │                                  │
│  ┌───────────┐  │  EPMD   │  ┌────────────────────────────┐  │
│  │ erl_rpc   │──┼─ port ──┼─▶│  NeonFS.CLI.Handler        │  │
│  │ client    │  │  4369   │  │  (RPC interface module)    │  │
│  └───────────┘  │         │  └────────────────────────────┘  │
│        │        │         │              │                   │
│        │        │  dist   │              ▼                   │
│        └────────┼─ port ──┼─▶ NeonFS.Volume.Registry        │
│                 │  ~9xxx  │   NeonFS.Cluster                 │
│                 │         │   NeonFS.Fuse                    │
└─────────────────┘         └──────────────────────────────────┘
```

**Design rationale:**

- **Rust CLI for fast startup**: BEAM startup is ~1-3 seconds; Rust CLI connects in 10-70ms
- **Erlang distribution**: Native to the Elixir ecosystem, no custom protocol needed
- **Local-only by default**: CLI runs on same machine as daemon, bound to localhost

### Server Daemon

The daemon is a standard Elixir release running as a BEAM node:

- **Node name**: `neonfs@localhost` (short name, localhost binding)
- **EPMD registration**: Daemon registers with Erlang Port Mapper Daemon on port 4369
- **Distribution port**: Dynamically assigned by EPMD (typically 9xxx range)

The daemon writes runtime information to well-known locations:

```
/run/neonfs/
├── node_name            # Current node name (e.g., "neonfs@localhost")
├── daemon.pid           # PID file for process management
└── daemon_port          # Optional: cached distribution port
```

### CLI Architecture

The CLI is a separate Rust binary (`neonfs`) using the `erl_dist` and `erl_rpc` crates to communicate with the daemon via Erlang distribution protocol.

**CLI crate structure:**

```
neonfs-cli/
├── Cargo.toml
└── src/
    ├── main.rs           # Entry point, clap argument parsing
    ├── daemon.rs         # DaemonConnection using erl_rpc
    ├── commands/
    │   ├── mod.rs
    │   ├── volume.rs     # volume list, create, delete, info
    │   ├── cluster.rs    # cluster init, join, status
    │   ├── node.rs       # node status, maintenance
    │   └── mount.rs      # mount, unmount, list mounts
    ├── term/
    │   ├── mod.rs
    │   ├── convert.rs    # FromTerm trait implementations
    │   └── types.rs      # VolumeInfo, ClusterInfo, etc.
    ├── output/
    │   ├── mod.rs
    │   ├── table.rs      # Table formatting
    │   └── json.rs       # JSON output mode (--json flag)
    └── error.rs          # CliError enum with user-friendly messages
```

**Connection flow:**

1. Read cookie from `/var/lib/neonfs/.erlang.cookie`
2. Connect to EPMD on `localhost:4369`
3. Query for `neonfs` node to get distribution port
4. Connect to daemon on distribution port
5. Perform Erlang distribution handshake with cookie
6. Call RPC functions on `NeonFS.CLI.Handler` module

**Typical latency:**

| Phase | Time |
|-------|------|
| Read cookie | < 1ms |
| EPMD lookup | 1-5ms |
| TCP connect | < 1ms |
| Distribution handshake | 5-10ms |
| RPC call | 1-50ms |
| **Total** | **10-70ms** |

### Communication Protocol

The CLI communicates with the daemon using Erlang's distribution protocol via the `erl_rpc` Rust crate. This enables calling Elixir functions directly.

**Daemon-side RPC interface:**

```elixir
defmodule NeonFS.CLI.Handler do
  @moduledoc """
  RPC handler for CLI commands. All functions return tagged tuples
  for easy pattern matching on the Rust side.
  """

  @spec list_volumes() :: {:ok, [map()]} | {:error, term()}
  def list_volumes do
    case NeonFS.Volume.Registry.list() do
      volumes when is_list(volumes) ->
        {:ok, Enum.map(volumes, &volume_to_map/1)}
      error ->
        {:error, error}
    end
  end

  @spec cluster_status() :: {:ok, map()}
  def cluster_status do
    {:ok, %{
      name: cluster_name(),
      nodes: node_statuses(),
      volumes: volume_summaries(),
      healthy: cluster_healthy?()
    }}
  end

  @spec mount(String.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def mount(volume_name, mount_point, opts) do
    NeonFS.Fuse.mount(volume_name, mount_point, opts)
  end

  # ... additional operations
end
```

**Response format:**

All functions return:
- `{:ok, data}` for success (data is a map or list of maps)
- `{:error, reason}` for failure (reason is an atom or string)
- `:ok` for void operations

This makes Rust-side pattern matching straightforward after term conversion.

### Cookie Management

Authentication uses Erlang's standard cookie mechanism.

**Cookie location:** `/var/lib/neonfs/.erlang.cookie`
**Permissions:** `0600`, owned by `neonfs` user

**Flow:**

1. During `neonfs cluster init`, daemon generates a cryptographically secure random cookie
2. Cookie is written to `/var/lib/neonfs/.erlang.cookie` with mode `0600`
3. CLI reads cookie from this file before connecting
4. CLI must run as the same user as daemon (or root) to read the cookie

This mirrors how `rabbitmqctl`, `riak-admin`, and other Erlang-based tools handle authentication.

### Error Handling

The CLI provides clear, actionable error messages:

| Scenario | Message |
|----------|---------|
| EPMD not running | "Cannot connect to NeonFS daemon. Is the service running?\n  Try: systemctl status neonfs" |
| Daemon not registered | "NeonFS daemon not found. The service may be starting.\n  Try again in a few seconds" |
| Cookie permission denied | "Permission denied reading cookie.\n  Run as neonfs user or use sudo" |
| Cookie not found | "NeonFS not initialised.\n  Run: neonfs cluster init" |
| Connection timeout | "Connection timed out. The daemon may be overloaded" |

### Directory Layout

```
/etc/neonfs/
└── daemon.conf          # Daemon configuration (node name, listen addresses)

/var/lib/neonfs/
├── .erlang.cookie       # Cookie file (mode 0600)
├── data/                # Chunk storage
│   ├── hot/
│   ├── warm/
│   └── cold/
├── meta/
│   └── node.json        # Node identity
└── wal/                 # Write-ahead log

/run/neonfs/
├── node_name            # Runtime node name (for CLI discovery)
└── daemon.pid           # PID file
```

### systemd Integration

```ini
# /etc/systemd/system/neonfs.service
[Unit]
Description=NeonFS Distributed Filesystem Daemon
After=network.target
Wants=epmd.service

[Service]
Type=notify
User=neonfs
Group=neonfs
ExecStart=/usr/bin/neonfs-daemon start
ExecStop=/usr/bin/neonfs-daemon stop
Restart=on-failure
RestartSec=5

Environment=RELEASE_NODE=neonfs@localhost
Environment=RELEASE_COOKIE_PATH=/var/lib/neonfs/.erlang.cookie
Environment=NEONFS_DATA_DIR=/var/lib/neonfs

RuntimeDirectory=neonfs
StateDirectory=neonfs

[Install]
WantedBy=multi-user.target
```

**Service management:**

```bash
# Start/stop
sudo systemctl start neonfs
sudo systemctl stop neonfs

# Check status
sudo systemctl status neonfs
journalctl -u neonfs

# Enable at boot
sudo systemctl enable neonfs
```

---

## Operations

### Cluster Initialization

```bash
$ neonfs cluster init --name my-cluster

Generating cluster identity...
Generating master encryption key...

Store this master key securely (needed for recovery):
  Master key: bfs_mk_7x8k2m9p...
  
  Options:
  - Save to password manager
  - Store in external secret manager (--vault-url)
  - Derive from passphrase at startup (--passphrase-mode)

Cluster 'my-cluster' initialized.

Add nodes with:
  $ neonfs cluster create-invite
```

### Adding Nodes

```bash
# On existing node
$ neonfs cluster create-invite --expires 1h
Invite token: bfs_inv_7x8k2m...

# On new node
$ neonfs cluster join --token bfs_inv_7x8k2m...
Connecting to cluster...
Joining BEAM cluster...
Joining Ra consensus...
Syncing metadata...
Node successfully joined cluster 'my-cluster'

# Verify
$ neonfs cluster status
Cluster: my-cluster
Nodes:
  node1 (this node)  online   leader
  node2              online   follower
  node3              online   follower
```

### Creating Volumes

```bash
$ neonfs volume create documents --policy documents-policy
Volume 'documents' created

$ neonfs volume create media --policy media-archive-policy
Volume 'media' created
```

### Monitoring

See **Telemetry and Observability** section for detailed metrics. Key dashboards to build:

- **Cluster health**: Node states, Ra leader, consensus latency
- **Storage capacity**: Per-node, per-tier usage and projections
- **Replication status**: Under-replicated chunks, repair progress
- **Performance**: Read/write latencies, throughput, cache hit rates
- **Drive health**: Spin states, SMART data, error rates

### Decision Points

Events requiring operator input (CLI, web UI, or agent):

```
┌────────────────────────────────────────────────────────────────┐
│ DECISION REQUIRED: Node Unreachable                            │
├────────────────────────────────────────────────────────────────┤
│ Node: node3                                                    │
│ Unreachable since: 2h 15m ago                                  │
│                                                                │
│ Affected chunks: 145,000                                       │
│ Current durability:                                            │
│   - Fully satisfied: 94%                                       │
│   - Degraded: 5.9%                                             │
│   - At risk: 0.1%                                              │
│                                                                │
│ Repair estimate:                                               │
│   - Data to move: 2.3 TB                                       │
│   - Time: ~8 hours                                             │
│   - Storage required: 2.3 TB (available: 4.1 TB)               │
│                                                                │
│ Options:                                                       │
│   [1] Wait - Continue monitoring                               │
│   [2] Partial repair - Fix at-risk chunks only (~50 GB)        │
│   [3] Full repair - Assume dead, complete resilver             │
│   [4] Maintenance - I'm working on it, remind in __ hours      │
└────────────────────────────────────────────────────────────────┘
```

### Audit Logging

Security-relevant events are logged for operational visibility and debugging. These are operational logs, not tamper-proof forensic records.

```elixir
%AuditEvent{
  timestamp: ~U[2025-01-15T10:30:00Z],
  event_type: :node_joined,
  node: :node5,
  details: %{invite_token_hash: "abc123..."}
}
```

Events logged:
- Node join/leave
- Volume create/delete
- Access control changes
- Repair operations
- Administrative actions

**Limitations:**

Audit logs are stored within the cluster. Given the BEAM trust model (any node has full cluster access), a compromised node can modify or delete audit entries. These logs are useful for:
- Operational debugging
- Understanding cluster history
- Detecting accidental misconfigurations

They are **not** suitable for:
- Forensic evidence after a breach
- Compliance requiring tamper-proof audit trails
- Detecting malicious insider activity

**For tamper-resistant audit trails**: Ship logs to an external system (syslog, SIEM, append-only S3 bucket) that cluster nodes cannot modify. This is a deployment concern, not something NeonFS provides.

### Telemetry and Observability

Both Elixir and Rust components emit telemetry for monitoring and debugging.

**Metrics (Prometheus/StatsD)**

Elixir control plane:
```
# Cluster state
neonfs_nodes_total{state="online|degraded|unreachable"}
neonfs_ra_term
neonfs_ra_commit_index

# Metadata operations
neonfs_metadata_ops_total{op="read|write|delete"}
neonfs_metadata_op_duration_seconds{op, quantile}

# Replication
neonfs_replication_queue_depth
neonfs_replication_lag_seconds
neonfs_chunks_under_replicated
```

Rust data plane:
```
# Storage I/O
neonfs_blob_read_bytes_total{node, tier}
neonfs_blob_write_bytes_total{node, tier}
neonfs_blob_read_duration_seconds{tier, quantile}
neonfs_blob_write_duration_seconds{tier, quantile}

# Chunking
neonfs_chunking_duration_seconds{quantile}
neonfs_chunks_created_total

# FUSE operations
neonfs_fuse_ops_total{op="read|write|lookup|readdir|..."}
neonfs_fuse_op_duration_seconds{op, quantile}
neonfs_fuse_errors_total{op, error_type}

# Drive state
neonfs_drive_state{node, path, state="active|standby|spinning_up"}
neonfs_drive_spinups_total{node, path}
```

**Distributed Tracing**

Traces span both Elixir and Rust:

```
[Client write request]
  └─ [Elixir: receive request]
       ├─ [Rust: chunk data] ─── chunk_count, total_bytes
       │    └─ [Rust: hash chunks]
       ├─ [Elixir: check dedup]
       ├─ [Rust: write local blob] ─── tier, duration
       ├─ [Elixir: initiate replication]
       │    ├─ [RPC to node2]
       │    │    └─ [Rust: write blob on node2]
       │    └─ [RPC to node3]
       │         └─ [Rust: write blob on node3]
       └─ [Elixir: commit metadata]
```

Use OpenTelemetry for cross-language trace context propagation.

**Structured Logging**

Both components emit JSON-structured logs:

```json
{
  "timestamp": "2025-01-15T10:30:00.123Z",
  "level": "info",
  "component": "rust.blob_store",
  "node": "node1",
  "event": "chunk_written",
  "chunk_hash": "sha256:abc123...",
  "tier": "hot",
  "size_bytes": 262144,
  "duration_ms": 12,
  "trace_id": "abc123",
  "span_id": "def456"
}
```

**Debug Endpoints**

Each node exposes debug endpoints (authenticated):

```
GET /debug/ra_state       # Ra cluster state
GET /debug/pending_writes # In-flight writes
GET /debug/replication    # Replication queue
GET /debug/drives         # Drive states and health
GET /debug/cache          # Cache hit rates, contents
```

---

## Implementation Phases

### Phase 1: Foundation

**Goal:** Basic single-node operation with local storage and CLI

Deliverables:
- neonfs_blob crate: content-addressed chunk storage
- neonfs_blob crate: FastCDC chunking, SHA-256 hashing
- neonfs_fuse crate: FUSE filesystem interface
- neonfs-cli crate: CLI tool using erl_dist/erl_rpc
- NeonFS.CLI.Handler module: RPC interface for CLI
- Basic Elixir supervision tree
- Rustler NIF integration for both crates
- All data flows through Elixir (single code path)
- systemd unit file and basic packaging

**Milestone:** Mount a directory, read/write files, data persists across restarts, CLI can query status

### Phase 2: Clustering

**Goal:** Multi-node cluster with metadata consensus

Deliverables:
- Ra integration for metadata storage
- WireGuard mesh setup (Headscale integration)
- Node discovery and join flow
- Basic replication (configurable factor)
- Quorum writes

**Milestone:** 3-node cluster, data replicated, survives single node failure

### Phase 3: Policies, Tiering, and Compression

**Goal:** Volume policies, tiered storage management, compression

Deliverables:
- Volume policy system
- Hot/warm/cold tier definitions
- Spin-down and spin-avoidance logic
- Compression (zstd) with hash-then-compress semantics
- Purpose-specific caching for transformed data
- Promotion/demotion background processes

**Milestone:** Multiple volumes with different policies, compression working, intelligent drive management

### Phase 4: Erasure Coding

**Goal:** Space-efficient durability for large files

Deliverables:
- Reed-Solomon encoding (Rust library integration)
- Stripe management in metadata
- Degraded read path (reconstruct from parity)
- Stripe repair process

**Milestone:** Volumes can use erasure coding, ~1.4x overhead with 4-fault tolerance

### Phase 5: Security

**Goal:** Production-ready security model

Deliverables:
- Cluster CA and certificate management
- Node join ceremony with invite tokens
- Volume encryption (server-side)
- Access control enforcement
- Audit logging

**Milestone:** Secure multi-user deployment

### Phase 6: APIs and Integration

**Goal:** External access methods

Deliverables:
- S3-compatible HTTP API
- Docker volume plugin
- CIFS via Samba VFS module
- CSI driver for Kubernetes

**Milestone:** Access from containers, S3 clients, Windows/macOS mounts

### Phase 7: Operations

**Goal:** Production operations support

Deliverables:
- Prometheus metrics exporter
- Decision escalation system (CLI + webhook)
- Capacity planning tools
- Backup/restore procedures
- Comprehensive documentation

**Milestone:** Operable production system with monitoring and alerting

---

## Technical Dependencies

### Elixir

| Dependency | Purpose |
|------------|---------|
| Ra | Raft consensus for metadata |
| Rustler | NIF integration with Rust |
| Plug + Bandit | HTTP server for S3 API |
| GRPC | CSI driver protocol |
| Phoenix (optional) | Web UI for operations |

### Rust

**neonfs_fuse crate**

| Dependency | Purpose |
|------------|---------|
| fuser | FUSE filesystem implementation |
| tokio | Async runtime, channels to Elixir |
| rustler | NIF bindings |

**neonfs_blob crate**

| Dependency | Purpose |
|------------|---------|
| fastcdc | Content-defined chunking |
| reed-solomon-erasure | Erasure coding |
| ring | Cryptography (AES-GCM, SHA-256) |
| zstd | Compression (primary algorithm) |
| lz4 | Compression (fast alternative) |
| tokio | Async runtime |
| rustler | NIF bindings |

**neonfs-cli crate**

| Dependency | Purpose |
|------------|---------|
| erl_rpc | RPC client for Erlang distribution protocol |
| erl_dist | Erlang distribution protocol implementation |
| clap | CLI argument parsing |
| tokio | Async runtime |
| thiserror | Error handling |

### External Services (Optional)

| Service | Purpose |
|---------|---------|
| Samba | CIFS/SMB protocol support |
| HashiCorp Vault | External secret management (optional) |
| Prometheus | Metrics collection |
| Jaeger/Zipkin | Distributed tracing |

---

## Configuration Reference

### Node Configuration

```yaml
# /etc/neonfs/node.yaml

node:
  name: node1
  data_dir: /var/lib/neonfs

  # Listen addresses
  api_listen: 0.0.0.0:8080        # HTTP API (S3, admin)
  cluster_listen: 0.0.0.0:4369    # BEAM distribution

cluster:
  name: my-cluster
  # Populated during join

storage:
  hash_prefix_depth: 2            # Directory sharding depth

  drives:
    - path: /mnt/nvme0
      tier: hot
      power_management: always_on

    - path: /mnt/hdd0
      tier: cold
      power_management: spin_down
      idle_timeout: 30m

# Application-level cache for transformed data only
# (Linux page cache handles raw chunks automatically)
cache:
  max_memory: 2GB                 # Total memory for all volume caches

telemetry:
  metrics:
    enabled: true
    endpoint: prometheus  # or statsd
  tracing:
    enabled: true
    endpoint: http://jaeger:14268/api/traces
```

### Volume Creation

Volumes are created via CLI or API with inline configuration:

```bash
$ neonfs volume create documents \
    --owner alice \
    --durability replicate:3 \
    --min-copies 2 \
    --write-ack quorum \
    --initial-tier warm \
    --compression zstd:3 \
    --encryption server-side \
    --cache-transformed \
    --repair-priority critical

$ neonfs volume create media \
    --owner alice \
    --durability erasure:10:4 \
    --write-ack local \
    --initial-tier cold \
    --compression none \
    --encryption none \
    --cache-reconstructed \
    --repair-priority low
```

Or via configuration file:

```yaml
# volumes.yaml
volumes:
  - name: documents
    owner: alice
    durability:
      type: replicate
      factor: 3
      min_copies: 2
    write_ack: quorum
    tiering:
      initial_tier: warm
      promotion_threshold: 1
      demotion_delay: 7d
    compression:
      algorithm: zstd
      level: 3
      min_size: 4096
    encryption:
      mode: server_side
    caching:
      transformed_chunks: true
      reconstructed_stripes: false
      remote_chunks: true
      max_memory: 512MB
    repair_priority: critical

  - name: media
    owner: alice
    durability:
      type: erasure
      data_chunks: 10
      parity_chunks: 4
    write_ack: local
    tiering:
      initial_tier: cold
      promotion_threshold: 3
      demotion_delay: 30d
    compression:
      algorithm: none
    encryption:
      mode: none
    caching:
      transformed_chunks: false
      reconstructed_stripes: true
      remote_chunks: true
      max_memory: 2GB
    repair_priority: low
```

---

## Open Questions

1. **Metadata storage backend:** SQLite vs RocksDB vs custom B-tree for per-node filesystem metadata. What are the performance characteristics for our access patterns?

2. **Chunk index sharding:** How to handle chunk index rebalancing when nodes join/leave? Consistent hashing with virtual nodes?

3. **Quotas:** Per-user and per-volume quota enforcement. Enforce at write time (blocks) or asynchronously (eventual)?

4. **Snapshots:** Volume-level snapshots as metadata-only operation (CoW makes this cheap). API design and retention policies?

5. **Dedup scope:** Cross-volume dedup (more savings, more complexity in GC) vs per-volume only (simpler)?

6. **Small file optimization:** Pack many small files into single chunks to reduce metadata overhead? Or just accept the overhead?

7. ~~**Partial stripe writes:**~~ Resolved: Use hybrid approach—replicate small files and partial final stripes, erasure code full stripes. See "Partial Stripe Handling" section.

8. **POSIX compliance level:** Full POSIX semantics are expensive. Which features do we actually need? (Hard links probably not, what about flock?)

9. **Concurrent writers:** What happens when two clients write to the same file simultaneously? Last-writer-wins? Conflict detection?

---

## Glossary

| Term | Definition |
|------|------------|
| Chunk | Content-addressed block of data, identified by SHA-256 hash of original (uncompressed) data |
| Stripe | Group of chunks encoded together with erasure coding |
| Volume | Logical storage container with its own policy |
| Tier | Storage class (hot, warm, cold) based on media type |
| Ra | Raft consensus library for Erlang/Elixir |
| Rustler | Library for writing Erlang NIFs in Rust |
| FUSE | Filesystem in Userspace |
| CDC | Content-Defined Chunking |
| DEK | Data Encryption Key (encrypts chunk data) |
| KEK | Key Encryption Key (wraps DEKs) |
| CoW | Copy-on-Write |
| CSI | Container Storage Interface |
| zstd | Zstandard compression algorithm (primary compression option) |
| neonfs_fuse | Rust crate providing FUSE filesystem interface |
| neonfs_blob | Rust crate providing chunk storage, compression, and cryptography |
| neonfs-cli | Rust crate providing the CLI tool for daemon interaction |
| EPMD | Erlang Port Mapper Daemon, provides node discovery for Erlang distribution |
| erl_dist | Rust crate implementing the Erlang distribution protocol |
| erl_rpc | Rust crate providing RPC client for Erlang nodes (built on erl_dist) |
