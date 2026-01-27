# Data Model

This document describes the core data structures in NeonFS: chunks, stripes, files, and volumes.

## Chunks (Rust Layer)

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

## Chunk Metadata (Elixir Layer)

Elixir tracks everything about chunks across the cluster:

```elixir
%ChunkMeta{
  hash: <<sha256 bytes>>,       # Hash of ORIGINAL data
  original_size: 262_144,       # Size before compression
  stored_size: 180_000,         # Size on disk (after compression)

  # Transformation state
  compression: :zstd | :none,   # Compression algorithm used
  encrypted: false,             # Whether chunk is encrypted

  # Cluster-wide location tracking (see storage-tiering.md#chunk-location-tracking)
  # Each location identifies a specific drive, enabling drive-level redundancy
  locations: [
    %{node: :node1, drive_id: "nvme0", tier: :hot},
    %{node: :node1, drive_id: "nvme1", tier: :hot},  # Same node, different drive
    %{node: :node2, drive_id: "ssd0", tier: :warm}
  ],

  # Durability configuration
  target_replicas: 3,

  # Note: replication_state is derived at query time from locations + current
  # node states, not stored. See node-management.md#replication-state.
  # This ensures immediate awareness when nodes become unreachable.

  # For erasure-coded chunks
  stripe_id: nil,     # or stripe reference
  stripe_index: nil,  # position in stripe (0-9 data, 10-13 parity)

  # Commit state (see replication.md for details)
  commit_state: :uncommitted | :committed,

  # Active write references: which in-flight writes are using this chunk
  # - For uncommitted chunks: writes that created or are sharing this chunk
  # - For committed chunks: writes that are reusing this existing chunk
  # When a write commits or aborts, it removes itself from this set
  # GC only deletes chunks when this set is empty (regardless of commit_state)
  active_write_refs: MapSet.t(write_id),

  # Lifecycle
  created_at: ~U[...],
  last_verified: ~U[...]
}
```

### Chunk Commit State and Active Write Refs

Chunks go through a lifecycle tied to write operations, with `active_write_refs` tracking all in-flight writes that depend on the chunk:

1. **Created as uncommitted**: When a write stores a new chunk, it's created with `commit_state: :uncommitted` and the write_id added to `active_write_refs`
2. **Shared between writes**: If another concurrent write produces the same content (same hash), it adds its write_id to `active_write_refs` rather than creating a duplicate
3. **Reusing committed chunks**: When a write reuses an already-committed chunk, it adds its write_id to `active_write_refs` (protecting the chunk from GC during the write)
4. **Committed**: When any write that references an uncommitted chunk commits successfully, the chunk transitions to `commit_state: :committed`. The committing write removes itself from `active_write_refs`, but other active writes remain.
5. **Write completion**: When any write commits or aborts, it removes itself from `active_write_refs` for all its chunks
6. **Deletion**: GC only deletes chunks when `active_write_refs` is empty. For uncommitted chunks, this means all writes aborted. For committed chunks, this means no file references it AND no active writes are using it.

This design ensures:
- No duplicate storage for identical concurrent uploads
- No orphaned chunks from failed writes
- No race conditions between GC and active writes (even for committed chunks)
- No grace periods needed anywhere in the system

### Chunking Strategy

| File Size | Strategy |
|-----------|----------|
| < 64 KB | Store as single chunk |
| 64 KB - 1 MB | Fixed-size 256 KB blocks |
| > 1 MB | Content-defined chunking (FastCDC), 256 KB - 1 MB variable |

Content-defined chunking uses rolling hash to find chunk boundaries, enabling deduplication even when content shifts within files.

## Stripes (Elixir Layer)

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

## Files (Elixir Layer)

Logical files are purely an Elixir metadata concept. Rust has no concept of files — only chunks.

```elixir
%File{
  id: file_id,
  volume_id: volume_id,
  path: "/documents/report.pdf",

  # Content reference depends on volume durability type
  # For replicated volumes: ordered list of chunk hashes
  chunks: ["sha256:abc...", "sha256:def...", ...],

  # For erasure-coded volumes: stripe references with byte ranges (chunks is nil)
  # See "File References for Erasure-Coded Volumes" below
  stripes: nil,

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

### File References for Erasure-Coded Volumes

Files on erasure-coded volumes reference stripes instead of individual chunks. Each stripe reference includes the byte range it covers within the file:

```elixir
%File{
  id: file_id,
  volume_id: volume_id,
  path: "/media/video.mp4",

  # Replicated: nil for erasure-coded volumes
  chunks: nil,

  # Erasure-coded: stripe references with byte ranges
  stripes: [
    %{stripe_id: "s1", byte_range: {0, 2_621_440}},           # Full stripe (2.5MB)
    %{stripe_id: "s2", byte_range: {2_621_440, 5_242_880}},   # Full stripe
    %{stripe_id: "s3", byte_range: {5_242_880, 7_864_320}},   # Full stripe
    %{stripe_id: "s4", byte_range: {7_864_320, 10_485_760}},  # Partial stripe
  ],

  size: 10_485_760,  # Must equal sum of stripe byte ranges
  # ...
}
```

The byte ranges must be contiguous and sum to `size`. The read path uses these ranges to determine which stripes to fetch for a given file offset.

When a client reads a file:
1. FUSE request arrives at Rust (neonfs_fuse)
2. Rust sends request to Elixir: "read path X, offset Y, length Z"
3. Elixir looks up file metadata, determines which chunks are needed
4. Elixir fetches chunks via neonfs_blob NIF (local) or RPC (remote)
5. Elixir assembles data and returns to FUSE via reply channel
6. Rust returns data to client

All data flows through Elixir — this gives a single code path for local and remote access.

## Volumes

Logical containers with their own durability, tiering, and access configuration.

```
Volume {
  id: VolumeId
  name: String

  # Access control
  owner: UserId
  # (ACL entries stored separately, see security.md)

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

## Volume Configuration Examples

### High-Value Documents

Encrypted, compressed, heavily cached:

```elixir
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
```

### Media Archive

Erasure coded, no compression (already compressed media):

```elixir
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
```

### Scratch Space

Minimal everything, rely on Linux page cache:

```elixir
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

  verification: %{on_read: :never, scrub_interval: days(90)},
  repair_priority: :low
}
```
