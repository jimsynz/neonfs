# External Packages and Dependencies

This document catalogues external packages, crates, and OTP modules that provide pre-tested functionality useful for NeonFS. The goal is to identify opportunities to leverage existing, well-maintained solutions rather than building from scratch.

*Last updated: 2026-02-02*

## Overview

NeonFS uses three technology stacks:
- **Elixir/Erlang** (control plane): Coordination, metadata, APIs
- **Rust** (data plane): Storage I/O, chunking, cryptography via NIFs
- **OTP** (built-in): Distribution, cryptography, storage primitives

---

## Elixir Hex Packages

### Currently in Use

| Package | Version | Purpose |
|---------|---------|---------|
| `ra` | ~> 2.13 | Raft consensus for cluster metadata |
| `rustler` | ~> 0.37 | NIF integration with Rust crates |
| `telemetry` | ~> 1.2 | Event instrumentation |
| `uuid_v7` | ~> 0.6 | Time-ordered unique identifiers |

### Recommended Additions

#### Phase 1: Foundation

**briefly** - Temporary Files
- **Hex**: https://hex.pm/packages/briefly
- **Purpose**: Temporary file management with automatic cleanup
- **Why**: Staging areas during write path for compression, encryption, chunk assembly. Robust cleanup on process exit.

**stream_data** - Property Testing
- **Hex**: https://hex.pm/packages/stream_data
- **Purpose**: Data generation and property-based testing
- **Why**: Critical for testing metadata operations, chunk handling, replication logic. Runs 100 iterations by default with automatic shrinking.

**mimic** - Mocking
- **Hex**: https://hex.pm/packages/mimic
- **Purpose**: Mocking library with minimal ceremony
- **Why**: Mock any module without requiring explicit behaviour definitions. Less boilerplate than Mox while still supporting concurrent tests.

#### Phase 2: Clustering

**libcluster** - Cluster Formation
- **Hex**: https://hex.pm/packages/libcluster
- **Purpose**: Automatic cluster formation and healing
- **Why**: Pluggable discovery strategies (DNS, Kubernetes, EPMD). DNS strategy works well with WireGuard/Tailscale deployments.
- **Strategies**: DNS polling for WireGuard mesh, Kubernetes for container deployments

**broadway** - Data Pipelines
- **Hex**: https://hex.pm/packages/broadway
- **Purpose**: Concurrent, multi-stage data processing with backpressure
- **Why**: Replication queue processing, chunk migration pipelines, garbage collection batches. Built on GenStage with automatic acknowledgement.
- **Alternative**: `gen_stage` for custom producer/consumer patterns

**reactor** - Saga Orchestration
- **Hex**: https://hex.pm/packages/reactor
- **Purpose**: Dynamic, concurrent saga orchestration with compensation
- **Why**: Multi-stage operations (replication, erasure coding stripes, GC workflows). Three-tier error handling: retry → compensation → undo. Perfect for cross-segment operations in `metadata.md`.

#### Phase 3: Policies and Compression

**lru_cache** - LRU Cache
- **Hex**: https://github.com/arago/lru_cache
- **Purpose**: ETS-based fixed-size LRU cache
- **Why**: Application-level chunk cache for transformed/reconstructed data. Linux page cache handles raw chunks.
- **Alternative**: `nebulex` for distributed caching across nodes

**yaml_elixir** - YAML Parsing
- **Hex**: https://hex.pm/packages/yaml_elixir
- **Purpose**: Parse YAML configuration files
- **Why**: Cluster and volume configuration files use YAML format (per `implementation.md` examples).
- **Alternative**: `toml` for TOML format if preferred

#### Phase 5: Metadata Tiering

**hlclock** - Hybrid Logical Clocks
- **Hex**: https://hex.pm/packages/hlclock
- **Purpose**: Timestamp-based conflict resolution for leaderless quorum metadata operations
- **Why**: Combines physical NTP timestamps with logical counters for causality detection. Essential for the HLC conflict resolution described in `metadata.md`.
- **Features**: Globally-unique monotonic timestamps, bounded clock drift handling (300s default)
- **Alternative**: Custom `NeonFS.Core.HLC` module (task 0080) — evaluate whether hlclock provides sufficient control or if a custom implementation is needed

**libring** - Consistent Hashing
- **Hex**: https://hex.pm/packages/libring
- **Purpose**: Consistent hash ring for metadata segment assignment
- **Why**: Fast lookup using Erlang's `:gb_tree`. Critical for distributed segment assignment as described in `metadata.md`.
- **Alternative**: Custom `NeonFS.Core.MetadataRing` module (task 0081) — evaluate whether libring's API fits our needs

#### Phase 6: Security

**jose** - JSON Object Signing and Encryption
- **Hex**: https://hex.pm/packages/jose
- **Purpose**: Standards-based signing and encryption
- **Why**: Foundation for encryption modes (envelope, server-side), cluster CA, certificate management, API authentication.

**joken** - JWT Library
- **Hex**: https://hex.pm/packages/joken
- **Purpose**: JWT token handling (uses JOSE)
- **Why**: API authentication, invite token signing

**argon2_elixir** - Password Hashing
- **Hex**: https://hex.pm/packages/argon2_elixir
- **Purpose**: Argon2 password hashing (PHC winner)
- **Why**: User password hashing for access control

#### Phase 7: APIs and Integration

**bandit** - HTTP Server
- **Hex**: https://hex.pm/packages/bandit
- **Purpose**: Pure Elixir HTTP server for Plug applications
- **Why**: S3-compatible API server, health check endpoints, Prometheus metrics. Up to 4x faster than Cowboy for HTTP/1.x. Default for Phoenix since 1.7.11.

**plug** - HTTP Middleware
- **Hex**: https://hex.pm/packages/plug
- **Purpose**: Adapter interface and middleware toolkit
- **Why**: Essential for building HTTP endpoints. Required by Bandit.

**grpc** - gRPC Implementation
- **Hex**: https://hex.pm/packages/grpc
- **Purpose**: Full Elixir gRPC with HTTP transcoding
- **Why**: CSI driver protocol for Kubernetes integration. Depends on `protobuf` for serialisation.

**msgpax** - MessagePack
- **Hex**: https://hex.pm/packages/msgpax
- **Purpose**: High-performance binary serialisation
- **Why**: Efficient serialisation for chunk metadata, replication queue items. Middle ground between JSON and Protocol Buffers.
- **Status**: Optional optimisation

#### Phase 8: Operations

**telemetry_metrics_prometheus** - Prometheus Metrics
- **Hex**: https://hex.pm/packages/telemetry_metrics_prometheus
- **Purpose**: Prometheus-compatible metrics exporter
- **Why**: Exposes `/metrics` endpoint for Prometheus scraping. Builds on existing telemetry foundation.

**prom_ex** - Prometheus with Dashboards
- **Hex**: https://hex.pm/packages/prom_ex
- **Purpose**: Higher-level Prometheus metrics with pre-built Grafana dashboards
- **Why**: Faster dashboard setup (<5 minutes). Alternative/complement to TelemetryMetricsPrometheus.
- **Status**: Optional polish

### Reference Packages (Not Dependencies)

**ex_aws_s3** - AWS S3 Client
- **Hex**: https://hex.pm/packages/ex_aws_s3
- **Purpose**: Reference implementation for S3-compatible API
- **Why**: Useful for understanding S3 API surface, testing NeonFS S3 gateway. Not a runtime dependency.

### Summary: Hex Package Additions by Phase

```
Phase 1: briefly, stream_data, mimic
Phase 2: libcluster, broadway, reactor
Phase 3: lru_cache, yaml_elixir
Phase 5: hlclock (or custom HLC), libring (or custom ring)
Phase 6: jose, joken, argon2_elixir
Phase 7: bandit, plug, grpc, msgpax (optional)
Phase 8: telemetry_metrics_prometheus, prom_ex (optional)
```

---

## Rust Crates

### neonfs_blob Crate (Storage Engine)

| Crate | Purpose | Status |
|-------|---------|--------|
| `fastcdc` | Content-defined chunking | Recommended |
| `sha2` | SHA-256 content addressing | Recommended |
| `aes-gcm` | AES-256-GCM encryption | Recommended |
| `zstd` | Zstandard compression | Recommended |
| `solana-reed-solomon-erasure` | Erasure coding | Phase 4 |
| `tokio` | Async runtime | Required |
| `rustler` | NIF bindings | Required |

#### Content-Defined Chunking

**fastcdc**
- **Crates.io**: https://crates.io/crates/fastcdc
- **Purpose**: FastCDC content-defined chunking algorithm
- **Why**: Deterministic chunking produces identical boundaries for same input, enabling deduplication. Includes streaming and async variants via `AsyncStreamCDC`.
- **Features**: v2020 implementation with 64-bit hashing, zero-allocation methods, configurable chunk sizes
- **Alternative**: `cdchunking` uses ZPAQ-style rolling hash

#### Cryptography

**sha2** (RustCrypto)
- **Crates.io**: https://crates.io/crates/sha2
- **Purpose**: SHA-256 hashing for content addressing
- **Why**: Part of actively-maintained RustCrypto family. Constant-time guarantees.

**aes-gcm** (RustCrypto)
- **Crates.io**: https://crates.io/crates/aes-gcm
- **Purpose**: AES-GCM authenticated encryption
- **Why**: Hardware acceleration (AES-NI, CLMUL) on x86/x86_64. Audited by NCC Group.
- **Note**: Avoid deprecated `sodiumoxide`; use RustCrypto instead.

**ring**
- **Crates.io**: https://crates.io/crates/ring
- **Purpose**: Alternative crypto library ("safe, fast, small")
- **Why**: Provides curve25519, ed25519, SHA-256. Consider if FIPS compliance needed.

#### Compression

**zstd**
- **Crates.io**: https://crates.io/crates/zstd
- **Purpose**: Zstandard compression (Facebook)
- **Why**: Excellent compression ratio with good performance. Default compression for chunk storage.

**lz4**
- **Crates.io**: https://crates.io/crates/lz4
- **Purpose**: Fast compression/decompression
- **Why**: Optional ultra-fast path for real-time operations. Worse ratio than zstd.

**snap**
- **Crates.io**: https://crates.io/crates/snap
- **Purpose**: Pure Rust Snappy implementation
- **Why**: Fast compression (~300 MB/sec), decompression (~800 MB/sec). Alternative fast-path codec.

#### Erasure Coding (Phase 4)

**solana-reed-solomon-erasure**
- **Crates.io**: https://crates.io/crates/solana-reed-solomon-erasure
- **Purpose**: Production-grade Reed-Solomon erasure coding
- **Why**: Actively maintained by Solana. Supports pure Rust and SIMD-accelerated variants.
- **Note**: Original `reed-solomon-erasure` crate seeking new maintainers; use Solana fork for production.

#### Serialisation

**serde**
- **Crates.io**: https://crates.io/crates/serde
- **Purpose**: Serialisation framework
- **Why**: Industry standard. Abstracts between formats (JSON, bincode, etc.).

**bincode**
- **Crates.io**: https://crates.io/crates/bincode
- **Purpose**: Compact binary serialisation
- **Why**: Metadata storage, inter-node communication if bandwidth constrained.

**rkyv**
- **Crates.io**: https://crates.io/crates/rkyv
- **Purpose**: Zero-copy deserialisation
- **Why**: High-performance metadata caching (Phase 3+). Faster than bincode in benchmarks.
- **Status**: Evaluate when metadata performance becomes critical

#### High-Performance I/O (Future)

**tokio-uring**
- **Crates.io**: https://crates.io/crates/tokio-uring
- **Purpose**: Linux io_uring integration with Tokio
- **Why**: True async file I/O (~60% improvement over epoll). Phase 2+ optimisation.

**glommio**
- **Crates.io**: https://crates.io/crates/glommio
- **Purpose**: Thread-per-core async runtime on io_uring
- **Why**: Higher throughput under contention. DataDog-maintained.
- **Status**: Evaluate if chunk I/O becomes bottleneck

### neonfs_fuse Crate (FUSE Filesystem)

| Crate | Purpose | Status |
|-------|---------|--------|
| `fuser` | FUSE filesystem interface | Recommended |
| `tokio` | Async runtime | Required |
| `rustler` | NIF bindings | Required |

**fuser**
- **Crates.io**: https://crates.io/crates/fuser
- **Purpose**: Full-featured FUSE implementation in pure Rust
- **Why**: Clean async-friendly API. Wraps libfuse up to 3.10.3. Preferred over legacy `fuse` crate.
- **Alternatives**:
  - `polyfuse`: Designed specifically for async/await syntax
  - `fuse3`: Async version, doesn't require libfuse for privileged mode

### neonfs-cli Crate (CLI Tool)

| Crate | Purpose | Status |
|-------|---------|--------|
| `erl_dist` | Erlang distribution protocol | Required |
| `erl_rpc` | RPC client for distribution | Required |
| `clap` | CLI argument parsing | Recommended |
| `tokio` | Async runtime | Required |
| `thiserror` | Error types for libraries | Recommended |
| `anyhow` | Error handling for applications | Recommended |

**erl_dist / erl_rpc**
- **Purpose**: Erlang distribution protocol implementation
- **Why**: CLI tool communicates with Elixir nodes via native distribution protocol

**clap**
- **Crates.io**: https://crates.io/crates/clap
- **Purpose**: Command-line argument parsing
- **Why**: Most popular in Rust ecosystem. Derive macro for ergonomic API, help generation, subcommands.
- **Alternative**: `argh` (lighter weight, follows Fuchsia conventions)

**thiserror / anyhow**
- **Purpose**: Error handling
- **Why**: `thiserror` for library code (custom error types), `anyhow` for application code (opaque errors with context). Both coexist well.

---

## OTP Built-in Modules

These modules come with OTP and require no additional dependencies.

### Currently in Use

| Module | Purpose | Usage Location |
|--------|---------|----------------|
| `:crypto` | Cryptographic operations | Cluster init, invite tokens, key generation |
| `:ets` | In-memory storage | ChunkIndex, FileIndex, VolumeRegistry |
| `:dets` | Disk-based storage | Persistence snapshots |
| `:file` | File operations | Atomic writes with sync |
| `:json` | JSON encoding/decoding | API responses, configuration |
| `:os` | System operations | Environment configuration |

#### :crypto

**Capabilities**:
- `crypto:strong_rand_bytes/1` - Cryptographically secure random bytes
- `crypto:mac(:hmac, :sha256, ...)` - HMAC-SHA256 for message authentication
- AES encryption, SHA hashing

**NeonFS Usage**:
- Generating cluster IDs, node IDs, master keys
- Computing HMAC-SHA256 signatures for invite tokens
- Listed in `extra_applications: [:crypto]`

#### :ets (Erlang Term Storage)

**Capabilities**:
- Fast in-memory key-value storage
- Table types: `:set`, `:bag`, `:ordered_set`
- `read_concurrency: true` for concurrent reads
- `ets:foldl/3` for iteration

**NeonFS Usage**:
- ChunkIndex: Fast chunk metadata lookups
- FileIndex: Index files by ID and path
- VolumeRegistry: Volume metadata storage
- Fast cache layer while Ra handles distributed consensus

**Gotchas**:
- Tables destroyed when owner process terminates (handled by Persistence GenServer)
- Non-persistent by default (mitigated with DETS snapshots)
- Must be `:public` mode for cross-process reads

#### :dets (Disk ETS)

**Capabilities**:
- Persistent disk-based storage
- `dets:to_ets/2` - Atomic load from disk to ETS
- `dets:sync/1` - Force flush to disk

**NeonFS Usage**:
- Persistence GenServer snapshots ETS to DETS for crash recovery
- Atomic write pattern: write to `.tmp`, sync, rename

**Gotchas**:
- Slower than ETS (disk I/O)
- Can suffer fragmentation over time
- Open files block access; must close before renaming

#### :json (OTP 27+)

**Capabilities**:
- `json:encode/1` - Encode Erlang terms to JSON
- `json:decode/1` - Decode JSON to Erlang terms
- Streaming encoder/decoder support

**NeonFS Usage**:
- API responses (S3, health endpoints)
- Configuration file parsing
- Audit log serialisation

**Benefits**:
- No external dependency required
- Well-tested OTP implementation
- Good performance for typical use cases

### Recommended for Future Phases

#### :pg (Process Groups) - Phase 2+

**Capabilities**:
- `pg:join/2` - Add process to named group
- `pg:get_members/2` - List processes cluster-wide
- Automatic cleanup when processes die

**NeonFS Usage**:
- Volume metadata cache invalidation broadcasts
- Service discovery (complement to Ra-based registry)
- Worker coordination (repair workers, scrubbers)

**Example**:
```elixir
:pg.get_members(:neonfs, {:volume, "documents", :metadata})
|> Enum.each(&send(&1, {:invalidate_cache, path}))
```

**Gotchas**:
- Eventually consistent, not strongly consistent like Ra
- Good for "best effort" broadcasts, not critical operations

#### :erpc (Enhanced RPC) - Phase 2+

**Capabilities**:
- `erpc:call/4` - Non-blocking remote call (OTP 24+)
- `erpc:send_request/4` + `erpc:receive_response/2` - Async pattern
- Better error handling than legacy `:rpc`

**NeonFS Usage**:
- Remote chunk reads during replication
- Read repair across nodes
- Cluster coordination

**Recommendation**: Prefer `:erpc` over `:rpc` for new code.

#### :counters / :atomics - Phase 3+

**Capabilities**:
- Lock-free atomic integer operations
- No GC pressure from intermediate maps
- Minimal lock contention

**NeonFS Usage**:
- Per-drive I/O counters for load tracking
- Global operation counters for monitoring

#### :persistent_term

**Capabilities**:
- Global hash table surviving GC
- Very fast reads
- Slow updates (rehash entire table)

**NeonFS Usage**:
- Already used via Ra for system registration
- Cluster configuration cache after startup

**Gotchas**:
- Avoid for frequently-changing data

### Not Needed

| Module | Reason |
|--------|--------|
| `:zlib` | Compression via Rust zstd (better performance) |
| `:ssl` | Network security via WireGuard mesh |
| `:mnesia` | Distributed consensus via Ra |
| `:timer` | GenServer timer patterns preferred |

---

## Dependency Management Strategy

### Principles

1. **Prefer OTP built-ins** when functionality matches (`:ets`, `:crypto`, `:pg`)
2. **Use well-maintained Hex packages** for complex functionality (Ra, Reactor, Broadway)
3. **Keep Rust for performance-critical paths** (chunking, compression, encryption)
4. **Avoid deprecated packages** (e.g., sodiumoxide → RustCrypto)

### Version Pinning

Pin major versions to avoid breaking changes:

```elixir
# mix.exs
{:ra, "~> 2.13"},           # Raft consensus
{:hlclock, "~> 1.0"},       # HLC timestamps
{:libcluster, "~> 3.5"},    # Cluster formation
{:broadway, "~> 1.0"},      # Data pipelines
{:reactor, "~> 0.17"},      # Saga orchestration
```

### Maintenance Monitoring

Monitor these packages for maintenance concerns:
- `reed-solomon-erasure` (original): Use Solana fork instead
- `sodiumoxide`: Deprecated, use RustCrypto

---

## Quick Reference: What to Add When

### Phase 1 (Foundation)
```
Hex: briefly, stream_data, mimic
Rust: fastcdc, sha2, zstd, fuser, clap, thiserror, anyhow
OTP: :json (for API responses, config)
```

### Phase 2 (Clustering)
```
Hex: libcluster, broadway, reactor
Rust: (no additions)
OTP: :pg, :erpc
```

### Phase 3 (Policies)
```
Hex: lru_cache, yaml_elixir
Rust: lz4 or snap (optional fast compression)
OTP: :counters (for I/O scheduler)
```

### Phase 4 (Erasure Coding)
```
Hex: (no additions)
Rust: solana-reed-solomon-erasure
OTP: (no additions)
```

### Phase 5 (Metadata Tiering)
```
Hex: hlclock (or custom HLC), libring (or custom ring)
Rust: (no additions — metadata NIFs extend existing neonfs_blob)
OTP: (no additions)
```

### Phase 6 (Security)
```
Hex: jose, joken, argon2_elixir
Rust: aes-gcm (already recommended for Phase 1)
OTP: (no additions)
```

### Phase 7 (APIs)
```
Hex: bandit, plug, grpc, msgpax (optional)
Rust: (no additions)
OTP: (no additions)
```

### Phase 8 (Operations)
```
Hex: telemetry_metrics_prometheus, prom_ex (optional)
Rust: tokio-uring (if I/O bottleneck identified)
OTP: (no additions)
```
