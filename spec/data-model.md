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
