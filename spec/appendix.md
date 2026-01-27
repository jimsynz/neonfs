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

9. ~~**Concurrent writers:**~~ Resolved: The intent log provides automatic exclusive access for file writes. Before writing, a writer must acquire an exclusive intent via Ra; if another writer already holds an intent for the same file, the second writer receives a `:concurrent_modification` error. This fails safe (no silent data loss) without requiring explicit locking from applications. See [Metadata - Intent Log: Transaction Safety and Write Coordination](metadata.md#intent-log-transaction-safety-and-write-coordination).

10. **Timestamp handling review:** ~~HLC clock skew~~ Resolved: HLC timestamps are bounded by `max_clock_skew_ms` (default 1 second). Nodes with excessive skew are quarantined from writes. See [Metadata - Clock Skew Bounds](metadata.md#clock-skew-bounds). **Remaining:** Audit other timestamp uses (TTLs, lease expiry, access statistics) for robustness against: daylight savings transitions, clock drift from poorly calibrated RTCs, and systems without hardware RTC (e.g., Raspberry Pi). Consider using monotonic clocks where wall-clock time isn't semantically required.

11. ~~**Drive spin-down race conditions:**~~ Resolved: When all replicas of a chunk reside on spun-down drives, NeonFS races all available replicas in parallel and serves whichever responds first. This avoids arbitrary waits and naturally handles variable spin-up times. A drive state machine coordinates concurrent access, and multiple readers waiting for the same drive share the spin-up wait. See [Storage Tiering - Drive Spin-Up Coordination](storage-tiering.md#drive-spin-up-coordination).

12. ~~**I/O scheduling and fairness:**~~ Resolved: A GenStage-based scheduler coordinates all I/O operations. Priority queues ensure critical repairs happen before background tasks. Weighted fair queuing within each priority level prevents any single volume from monopolising I/O. The BEAM handles waiting processes efficiently, so no admission control or backpressure signalling is needed—clients simply wait until their operation completes. See [Storage Tiering - I/O Scheduling](storage-tiering.md#io-scheduling).

13. ~~**Intent log growth:**~~ Resolved: Completed, failed, and expired intents are archived to daily log files on the system volume (JSONL format with configurable retention), then removed from Ra state. Expired intents from crashed nodes are handled by any available node—most operations can be recovered or rolled back; only `write_ack: :local` writes require the original node. Physical chunk cleanup is delegated to GC. See [Metadata - Intent Cleanup and Archival](metadata.md#intent-cleanup-and-archival).

14. ~~**Metrics collection:**~~ Resolved: NeonFS uses Telemetry for instrumentation and exposes metrics via Prometheus endpoint. Defined metrics cover chunk operations, replication, repair, storage, cluster health, and FUSE operations. Service Level Indicators (SLIs) with thresholds for good/degraded/critical states. Health check endpoint for load balancers and Kubernetes probes. Example alerting rules provided. See [Observability](observability.md).

15. **S3 API consistency semantics**: The S3-compatible API needs defined consistency guarantees. NeonFS's quorum writes likely provide stronger consistency than S3's historical eventual consistency, but this should be explicitly documented. Key questions: LIST consistency after PUT/DELETE, read-after-write guarantees, behaviour during network partitions. Defer until core system is operational.

16. **Recovery procedures**: Operational runbooks for node failure, disk failure, quorum loss, split-brain resolution, and data corruption. Defer until system is deployed and real operational experience is gained.

17. **CSI driver timeout handling**: Kubernetes CSI operations have strict timeouts. Need to define behaviour when NeonFS can't meet deadlines (mount during degraded state, unmount with pending writes, volume creation during partition). Defer until CSI driver implementation.

---

## Glossary

| Term | Definition |
|------|------------|
| Chunk | Content-addressed block of data, identified by SHA-256 hash of original (uncompressed) data |
| Stripe | Group of chunks encoded together with erasure coding |
| Volume | Logical storage container with its own policy |
| Tier | Storage class (hot, warm, cold) based on media type |
| Ra | Raft consensus library for Erlang/Elixir |
| Reactor | Saga orchestration library for Elixir, handles step execution and rollback |
| Intent Log | Crash-safe record of in-flight multi-segment operations, stored in Ra |
| Active Write Refs | Set of write_ids currently using a chunk (created it or reusing it); protects chunks from GC during writes |
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
| Telemetry | Elixir library for metrics and instrumentation, ecosystem standard |
| SLI | Service Level Indicator, metric that measures service health |
| Failure Domain | Blast radius of a failure - drive, node, or zone level |
| Zone | Optional grouping of nodes by physical location (rack, site) for multi-site redundancy |
| StreamData | Property testing library for Elixir |
| proptest | Property testing library for Rust |
| cargo-fuzz | Fuzzing framework for Rust using libFuzzer |
| TestCluster | Elixir module for managing containerised test clusters |
