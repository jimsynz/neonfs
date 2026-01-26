# Appendix

This document contains open questions and the glossary.

## Open Questions

1. **Metadata storage backend:** SQLite vs RocksDB vs custom B-tree for per-node filesystem metadata. What are the performance characteristics for our access patterns?

2. **Chunk index sharding:** How to handle chunk index rebalancing when nodes join/leave? Consistent hashing with virtual nodes?

3. **Quotas:** Per-user and per-volume quota enforcement. Enforce at write time (blocks) or asynchronously (eventual)?

4. **Snapshots:** Volume-level snapshots as metadata-only operation (CoW makes this cheap). API design and retention policies?

5. **Dedup scope:** Cross-volume dedup (more savings, more complexity in GC) vs per-volume only (simpler)?

6. **Small file optimisation:** Pack many small files into single chunks to reduce metadata overhead? Or just accept the overhead?

7. ~~**Partial stripe writes:**~~ Resolved: Pad partial stripes with zeros and mark as partial in metadata. This maintains consistent durability semantics (all data gets full erasure coding protection) at the cost of storage efficiency. Future compaction can pack multiple partial stripes together to reclaim space. See [Replication - Partial Stripe Handling](replication.md#partial-stripe-handling).

8. **POSIX compliance level:** Goal is full POSIX compliance, but features are prioritised for incremental delivery. Initial version focuses on core read/write/directory operations. Deferred to later phases: hard links, extended attributes, extended ACLs (POSIX.1e), advisory locking (flock/fcntl). This reduces initial complexity and allows validating the core model before adding advanced features.

9. ~~**Concurrent writers:**~~ Resolved: Advisory locks (using quorum) are available for coordination but optional. If two writers modify the same file concurrently without holding a lock, both writes are rejected. This fails safe (no silent data loss) while allowing users to opt out of locking overhead when they know they're the only writer.

10. **Timestamp handling review:** Audit all uses of timestamps (TTLs, HLC, lease expiry, access statistics) for robustness against: daylight savings transitions, leap seconds, clock drift from poorly calibrated RTCs, and systems without hardware RTC (e.g., Raspberry Pi). Consider using monotonic clocks where wall-clock time isn't semantically required.

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
| HLC | Hybrid Logical Clock, combines wall clock with logical counter for ordering |
| LWW | Last-Writer-Wins, conflict resolution strategy where highest timestamp wins |
| StreamData | Property testing library for Elixir |
| proptest | Property testing library for Rust |
| cargo-fuzz | Fuzzing framework for Rust using libFuzzer |
| TestCluster | Elixir module for managing containerised test clusters |
