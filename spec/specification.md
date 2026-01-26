# NeonFS: A BEAM-Orchestrated Distributed Filesystem

## Overview

NeonFS is a distributed, content-addressed filesystem that combines the coordination strengths of Elixir/BEAM with the performance characteristics of Rust for low-level storage operations. It provides network-transparent storage with configurable durability, tiered storage, and multiple access methods including FUSE, S3-compatible API, and CIFS.

### Design Principles

1. **Separation of concerns**: Elixir handles coordination, policy, and APIs; Rust handles I/O, chunking, and cryptography
2. **Content-addressed storage**: All data stored as immutable, hash-identified chunks enabling deduplication and integrity verification
3. **Policy-driven replication**: Per-volume configuration of durability, performance, and cost tradeoffs
4. **Operational pragmatism**: Human-in-the-loop for ambiguous decisions; automation for routine operations

---

## Architecture at a Glance

```
┌───────────────────────────────────────────────────────────────┐
│                WireGuard Mesh (Tailscale/Headscale)           │
│                                                               │
│  ┌─────────────────────────────────────────────────────────┐  │
│  │                   Elixir Control Plane                  │  │
│  │  (Ra consensus, policy engine, API surfaces)            │  │
│  │                          │                              │  │
│  │            ┌─────────────┴─────────────┐                │  │
│  │      Rustler NIF                 Rustler NIF            │  │
│  └────────────┼───────────────────────────┼────────────────┘  │
│               │                           │                   │
│    ┌──────────┴──────────┐    ┌───────────┴───────────┐       │
│    │  neonfs_fuse crate  │    │  neonfs_blob crate    │       │
│    │  (FUSE driver)      │    │  (chunk storage)      │       │
│    └─────────────────────┘    └───────────────────────┘       │
└───────────────────────────────────────────────────────────────┘
```

All data flows through Elixir, providing a single code path for local and remote access, natural backpressure, and simplified reasoning about consistency.

---

## Specification Documents

The full specification is organised into the following documents. Each contains detailed design information for its topic area.

### Core Architecture

| Document | Description | When to read |
|----------|-------------|--------------|
| [Architecture](architecture.md) | System structure: Elixir control plane, Rust data plane, NIF integration, supervision trees, on-disk blob storage format, and the FUSE data path. | Implementing core system structure, understanding NIF boundaries, working on supervision trees. |
| [Data Model](data-model.md) | Core data structures: chunks (Rust and Elixir views), stripes, files, volumes. Includes chunking strategy and complete volume configuration examples. | Working with data structures, implementing new entity types, configuring volumes. |
| [Metadata](metadata.md) | Distributed metadata architecture: tiered storage (Ra, quorum, ephemeral), leaderless quorum model, HLC conflict resolution, consistent hashing, anti-entropy. | Implementing metadata operations, understanding consistency model, working on distributed state. |

### Cluster Behaviour

| Document | Description | When to read |
|----------|-------------|--------------|
| [Node Management](node-management.md) | Node lifecycle: states, state transitions, partition behaviour, clock synchronisation, escalation ladder, repair prioritisation, cost functions for read/write routing. | Node lifecycle code, failure handling, implementing read/write routing, partition recovery. |
| [Storage Tiering](storage-tiering.md) | Tier definitions (hot/warm/cold), drive state tracking, read path optimisation, power management (spin-down), promotion/demotion logic, caching strategy. | Drive management, tier migration, implementing caching, power management features. |
| [Replication](replication.md) | Write flows (replicated and erasure-coded), uncommitted chunk cleanup, quorum configurations, write hole mitigation, partial stripe handling, garbage collection. | Write path implementation, chunk replication, garbage collection, erasure coding. |

### Security and Access

| Document | Description | When to read |
|----------|-------------|--------------|
| [Security](security.md) | Network security recommendations, cluster authentication (invite tokens), key hierarchy, three encryption modes (none, server-side, envelope), deduplication/encryption interaction, identity and access control, threat model. | Auth implementation, encryption features, ACLs, security-sensitive code, threat modelling. |

### External Interfaces

| Document | Description | When to read |
|----------|-------------|--------------|
| [API Surfaces](api-surfaces.md) | External access methods: S3-compatible API, FUSE mount, Docker/Podman volume plugin, CSI driver (Kubernetes), CIFS/SMB via Samba. | Implementing external APIs, adding new access methods. |
| [Deployment](deployment.md) | CLI architecture (Rust binary using erl_dist/erl_rpc), daemon-CLI communication, cookie management, error handling, directory layout, systemd integration. | CLI implementation, packaging, installation, systemd setup. |

### Operations and Planning

| Document | Description | When to read |
|----------|-------------|--------------|
| [Operations](operations.md) | Day-to-day operations: cluster init, adding nodes, creating volumes, monitoring, decision points, audit logging, telemetry (metrics, tracing, logging), cluster upgrades, disaster recovery. | Operational tooling, observability, backup/restore, upgrade procedures. |
| [Implementation](implementation.md) | Implementation phases (1-7), technical dependencies (Elixir and Rust crates, external services), node and volume configuration reference. | Planning work, understanding dependencies, configuration options. |
| [Appendix](appendix.md) | Open questions (unresolved design decisions) and glossary of terms. | Terminology reference, understanding areas needing design decisions. |

---

## Quick Reference

### Key Concepts

- **Chunk**: Content-addressed block of data, identified by SHA-256 hash of original data
- **Volume**: Logical storage container with its own durability, tiering, and access policy
- **Tier**: Storage class (hot, warm, cold) based on media type and access patterns
- **Stripe**: Group of chunks encoded together with Reed-Solomon erasure coding

### Write Path (Simplified)

1. Client opens write stream
2. Elixir receives bytes, routes to Rust chunk engine
3. Rust splits into chunks, computes hashes, returns to Elixir
4. Elixir stores locally via Rust blob store, initiates replication
5. On completion, Elixir commits metadata atomically

### Read Path (Simplified)

1. FUSE request arrives at Rust driver
2. Rust asks Elixir: "read path X, offset Y, length Z"
3. Elixir looks up file metadata, determines needed chunks
4. Elixir fetches chunks (local via Rust NIF, or remote via RPC)
5. Elixir returns assembled data to FUSE driver

### Durability Options

| Type | Overhead | Use Case |
|------|----------|----------|
| Replication (factor 3) | 3x | Small files, fast access, simple |
| Erasure coding (10+4) | 1.4x | Large archives, storage efficiency |

### Encryption Modes

| Mode | Protection | Deduplication |
|------|------------|---------------|
| None | — | Full |
| Server-side | Data at rest | Within volume |
| Envelope | End-to-end per user | Limited |
