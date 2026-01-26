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
