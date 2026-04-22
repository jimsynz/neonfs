# Log-Structured Write Absorption

- Date: 2026-04-22
- Scope: `neonfs_core` (write path, `WriteOperation`, `FileIndex`, new log module), `neonfs_client` (`ChunkReader` read-overlay), BlobStore (new entry shape)
- Issue: [#269](https://harton.dev/project-neon/neonfs/issues/269)
- Status: design sketch — not yet committed to implement

## Problem

Fsync-heavy workloads (OLTP databases, index files, page-level random writers) pay a compounded cost on the current write path:

1. **Chunk-level write amplification.** Chunks are content-addressed and immutable, with a 64 KiB minimum (`neonfs_core/native/neonfs_blob/src/chunking.rs`). An 8 KiB overwrite triggers read-covering-chunk → splice → re-hash (SHA-256) → re-compress (zstd) → new chunk stored. At 3× replication that's ~192 KiB of physical work per 8 KiB logical write. FastCDC-chunked files (>1 MiB) can be four times worse.

2. **Per-write quorum cost.** Every `write_file_at` issues a `QuorumCoordinator.quorum_write` at N=3/W=2 (`neonfs_core/lib/neon_fs/core/file_index.ex`). Parallel RPCs plus a W-of-N wait on the FileIndex update path per logical write.

The combined effect pushes fsync-per-op workloads well below what the hardware can do. An NVMe drive that handles 100K IOPS locally tops out at tens of IOPS through NeonFS not because the storage stack is slow but because every write touches SHA-256, zstd, and a quorum.

This design proposes an LSM-tree / ZFS-ZIL-shaped write-absorption layer that amortises both costs.

## Goals

1. Reduce per-write amplification on small in-place mutations by appending to a log rather than rewriting chunks.
2. Amortise quorum cost across concurrent writers via group commit.
3. Preserve the immutable content-addressed substrate — GC, scrub, anti-entropy, erasure coding, dedup, and tiering continue to operate on the compacted output.
4. Keep the feature per-volume opt-in so workloads that don't benefit from it don't pay for it.
5. Do not weaken any existing durability guarantee. `write_ack: :quorum` must still mean "acknowledged only after quorum durability".

Non-goals:

- Replacing chunk-based storage as the canonical store. The log is a buffer, not a new substrate.
- Supporting erasure-coded volumes in the first pass — erasure-coded streaming writes are separately tracked. Logged writes require replication.
- Supporting xattr / rename / delete inside a log segment. Those stay on the existing metadata path.

## Shape

Every logged volume has, per file that has seen any logged activity since the last compaction:

- Zero or more **sealed log segments** — content-addressed chunks containing frozen log entries, referenced from the file's metadata in a new `FileIndex` column (`log_segments`, ordered by sequence).
- At most one **active log segment** — an in-memory iolist of pending entries on the write-coordinator node, not yet flushed. Flush produces a sealed segment.

The existing `chunks` column on a `FileIndex` row continues to describe the base (compacted) file content. Reads merge: logged entries win wherever they overlap chunks.

### Log segment format

```
┌─────────────────────────┐
│ header (32 bytes)       │
│   magic    = "NLOG"     │
│   version  = u32        │
│   seq_base = u64        │
│   hlc_base = u64        │
│   file_id  = u128       │
│   entries  = u32        │
├─────────────────────────┤
│ entry 0                 │
│ entry 1                 │
│ ...                     │
└─────────────────────────┘
```

Each entry:

```
u64    offset
u32    length
u32    crc32c      -- of the payload
u64    seq_delta   -- sequence within this segment
u64    hlc_delta   -- HLC delta vs hlc_base
bytes  payload[length]
```

Constraints:

- **Segment size cap**: 4 MiB default, configurable per volume under `volume.log_absorption.segment_max_bytes`. 4 MiB matches the BlobStore's max chunk size (`CDC_MAX_SIZE = 1 MiB`) scaled to give useful batching headroom; a seal flushes a single chunk-sized write via the existing streaming path.
- **Entry size cap**: 1 MiB per entry. Writers larger than this bypass the log and hit the chunk-rewrite path directly — past that size the log overhead outweighs the benefit, and compaction-in-the-hot-path becomes a latency problem.
- **CRC**: per-entry crc32c so partial-segment reads (during recovery) can skip torn entries at the tail. We rely on the BlobStore's outer SHA-256 for end-to-end integrity of the sealed segment.

Segments are stored as BlobStore chunks — the existing replication, compression (off by default — log entries are usually incompressible and zstd won't help), encryption, and tiering infrastructure carry through unchanged. `log_absorption`-mode volumes default to no compression on log chunks and same encryption as the rest of the volume.

### Group commit

On the write-coordinator node (the node handling `write_file_at` for a given file), pending log entries accumulate in an in-process ETS table keyed by file ID. A small GenServer per volume (`NeonFS.Core.LogAbsorptionCoordinator`) flushes:

- when the accumulated payload crosses the segment-size cap, OR
- when the oldest pending entry is older than the group-commit window (1 ms default, configurable via `volume.log_absorption.commit_window_ms`), OR
- on explicit flush (fsync from FUSE/NFS, WebDAV `PUT` commit).

Flush does one quorum write to BlobStore (the sealed segment) and one quorum write to FileIndex (appending the new segment ref to `log_segments`). Both are in the existing hot path — no new RPC shapes required.

Concurrent writers to the same file share the flush: caller N+1 waits on caller N's flush if it arrives mid-batch. Latency floor per fsync caller is the window size; throughput scales with concurrency.

### Read path

`read_file_stream` and `ChunkReader.read_file_stream` gain a merge step:

1. Fetch the file's base chunk list as today.
2. Fetch the file's log segments list (already present in FileIndex row — no new RPC).
3. For the requested byte range, walk segments in reverse sequence order, collecting `(offset, length) → payload` for any entry whose range overlaps the request.
4. For any part of the request not covered by the log, fall through to chunk reads.
5. Emit bytes in request order.

Two optimisations worth noting up front:

- **Log bloom filter.** Each sealed segment carries a compact offset-range bitmap so the reader can skip entire segments that don't overlap the request range. ~64 bytes per segment. Cheap.
- **Client-side cache.** Log segments are immutable once sealed, so `ChunkReader` can cache them in the same chunk cache it already maintains. Hot random-read workloads on a recently-written log page hit RAM.

Reads on the write-coordinator node also see pending in-memory entries (not yet flushed). This is the same consistency the current `WriteBuffer` gives — reads immediately after writes, on the writing node, always see their own writes.

### Compaction

A background compactor per volume (`NeonFS.Core.LogAbsorptionCompactor`) triggers on:

- **Log size threshold**: per-file total sealed log size exceeds 32 MiB (default) or 25% of file size, whichever is smaller.
- **Staleness**: oldest sealed segment is older than 1 hour (default).
- **Explicit request**: admin command `neonfs volume compact <name> --file <path>`.

Compaction is per-file:

1. Acquire a compact claim via `IntentLog` keyed by `{:compact, volume_id, file_id}`. Conflicts with concurrent compactors; does not block writers (writers continue to append to the active log segment during compaction).
2. Read the base chunks + all sealed log segments up to a frozen cutoff sequence.
3. Stream the merged content through the incremental chunker (`NeonFS.Core.write_file_streamed`'s existing infrastructure) to produce new base chunks.
4. Atomically update `FileIndex`: replace `chunks` with the new base, prune `log_segments` up to the cutoff sequence.
5. GC picks up orphaned old chunks and compacted log segments on its next pass.

Writers that arrived during compaction land in new log segments (sequence > cutoff); these are preserved and applied on the next compaction cycle.

If compaction fails at any step before (4), no state change is visible — the file keeps its existing chunks + log.

## Design decisions

### Group commit vs leader-per-log

**Decision: group commit first. Leader-per-log deferred, not designed in.**

Group commit:

- Preserves the existing quorum model — N=3/W=2 continues to mean what it means.
- Layerable on top of the current `WriteOperation` + `QuorumCoordinator` without restructuring consensus.
- Latency floor = commit-window size. At 1 ms that's acceptable for OLTP (a consumer-grade NVMe is ~20 μs per fsync; a 1 ms floor trades some latency for 50× or better throughput amortisation).
- Failover is the existing FileIndex failover — another core node's LogAbsorptionCoordinator takes over on the next write.

Leader-per-log (Ceph primary-copy style) would give lower single-writer latency by skipping the quorum-wait on each batch, but adds:

- Proper leader election per file (or per log shard).
- Leader-failover with a consistent prefix cutover.
- A new consensus shape that doesn't fit anywhere else in the system.

That's a large amount of new machinery for a modest latency win. Build group commit, measure, revisit if the latency floor is a blocker.

### Log durability tier

**Decision: same replication factor and tier as the volume. A separate "log tier" deferred.**

ZFS SLOG and FoundationDB log servers tier the log onto dedicated low-latency storage. It works, but it needs:

- A separate tier/pool identified and sized per volume.
- A migration path from log tier to data tier during compaction (more IO work).
- Per-node hardware assumptions (NVMe-backed log drive).

None of that fits Raspberry-Pi-class clusters where the whole point is a uniform substrate. Volumes that need faster logs than their base tier provides should use a faster tier outright; there's no gain from log-tier asymmetry we can't get by just picking `hot` over `warm`.

If a future use case genuinely benefits from a separate log pool, the per-volume `log_absorption.tier` config key reserved in `Volume` config can be wired to point at a different BlobStore tier without breaking any on-disk shape.

### Per-file vs per-volume logs

**Decision: per-file. A per-volume shared log deferred.**

Per-file:

- File metadata (FileIndex row) owns its own log segments. No coordination needed across files.
- Compaction is per-file and can run independently.
- Read path trivially scoped — a read of file F never has to scan logs for file G.

Per-volume shared log:

- Fewer metadata writes (one `log_segments` append per volume instead of one per file).
- Much harder read path — every read has to scan the shared log for any entries touching this file.
- Concurrent writers across files serialise through the shared log's coordinator.

Per-file has a higher metadata baseline but its cost scales with actual mutation locality. Per-volume would only win when most writes hit one or a handful of hot files, which is already the scenario where per-file group-commit amortises well.

### Read ordering

**Decision: last-writer-wins by `(seq, hlc)` with `seq` as the primary key.**

An entry with a higher `seq` always wins over one with a lower `seq`. HLC is the tiebreaker for cross-coordinator races (two coordinators for the same file during a failover): the HLC distinguishes the two branches, and the lower-HLC loses on the eventual merge. `seq` alone isn't enough because it resets per segment; `(seg_seq_base + entry_seq_delta, hlc)` pair forms a total order.

## Crash and durability

### Durability model

`write_ack: :quorum` (the existing per-volume setting) governs when a logged write returns success to the caller. Options:

- `write_ack: :local` — return as soon as the coordinator has the entry in its in-memory buffer. Loses un-flushed entries on coordinator crash. Appropriate for speculative/scratch workloads.
- `write_ack: :quorum` — return only after the flush batch reaches quorum. The same guarantee as the current chunk-based path. Default.
- `write_ack: :all` — return only after all N replicas ACK the segment write. Same as today.

A per-operation override is worth considering (RocksDB's `WriteOptions.sync` maps to this) but not in scope for v1.

### Crash scenarios

**Coordinator crash with un-flushed entries.**

For `write_ack: :local`, un-flushed entries are lost. This is the expected semantics.

For `write_ack: :quorum`, un-flushed entries haven't returned success to the caller yet — so "losing" them is indistinguishable from the caller's RPC timing out. No corruption. The caller may or may not retry; either behaviour is correct.

**Coordinator crash mid-flush.**

The flush is two quorum writes: BlobStore (segment chunk) then FileIndex (segment ref append). Crash between them leaves an orphan chunk with no reference. GC reclaims it after a cutoff age. No corruption.

Crash after FileIndex ack but before the caller gets the success reply is the usual at-least-once semantics — the caller retries and the retry is idempotent at the FileIndex layer (same segment ref already present, or if not present, appended without harm because the entry `seq` is deterministic).

**Coordinator crash during compaction.**

`IntentLog` claim is released when the coordinator process dies. Another coordinator can pick up compaction. No partial state visible — compaction's final FileIndex update is atomic (as it is today for chunk writes).

**Replica crash during segment replication.**

Same as any chunk replication failure — the surviving replicas satisfy the quorum, and anti-entropy catches up the lagging replica when it returns.

**Torn segment at the tail.**

If a segment chunk was partially written to replica storage before crash (e.g. mid-write power loss on a replica), that replica's segment fails its SHA-256 verify on read, and the read falls through to another replica. The per-entry crc32c is an inner defence only for the recovery path that replays partial segments if we ever need one; today we don't, because the outer SHA-256 either validates the whole segment or rejects it.

### Consistency across concurrent protocols

Logged writes integrate with the existing DLM. An `flock`-holding FUSE writer and an NFS NLM-locked writer see the same lock state as today — the lock check happens before the write hits the log, not after.

Cross-protocol reads/writes on the same file work as today for the current coordinator. If the coordinator fails over mid-stream, the new coordinator begins a fresh log segment (with a new `seg_seq_base`); reads merging across the failover see both segments in `(seq, hlc)` order.

## Configuration surface

Per volume, under `volume.log_absorption`:

| Key | Default | Meaning |
|-----|---------|---------|
| `enabled` | `false` | Opt-in. Volumes with large sequential workloads should leave this off. |
| `segment_max_bytes` | `4_194_304` (4 MiB) | Sealed segment size cap. |
| `entry_max_bytes` | `1_048_576` (1 MiB) | Per-entry cap. Larger writes bypass the log. |
| `commit_window_ms` | `1` | Group commit window. Higher = more amortisation, higher latency floor. |
| `compact_size_threshold` | `33_554_432` (32 MiB) | Per-file log size that triggers compaction. |
| `compact_ratio_threshold` | `0.25` | Per-file log:file ratio that triggers compaction. |
| `compact_age_threshold_seconds` | `3600` | Staleness that triggers compaction regardless of size. |

All are live-adjustable via `neonfs volume update`.

## Rollout

1. **Phase 1**: design ACK (this doc).
2. **Phase 2**: log segment format + sealed-segment write path + reads merging a per-file pre-populated log (no actual writers routing through the log yet). Exercise the read merge with synthesized segments in tests.
3. **Phase 3**: route `write_file_at` through the coordinator on `log_absorption.enabled` volumes. Group commit. No compaction yet — accumulate logs indefinitely in tests, verify read merge correctness up to the segment/ratio/age limits.
4. **Phase 4**: compaction. Telemetry for log size, compaction cadence, per-flush batch size. Operator CLI for inspecting and forcing compaction.
5. **Phase 5**: benchmarking against Postgres pgbench and a small-random-write `fio` harness on a peer-cluster integration test. Volume config defaults tuned from measured data.

Each phase is one or more separate sub-issues when this design is accepted. No PR touches multiple phases.

## Open questions

1. **FastCDC chunk boundaries during compaction.** Compaction runs the merged stream through the incremental chunker. If the merged content is very similar to the existing base (common case: a small percentage of pages dirtied), FastCDC's content-defined boundaries should be mostly stable, giving good deduplication on the new base. Worth measuring before committing.

2. **Log reads over the TLS data plane.** Sealed log segments are BlobStore chunks — they go over the existing data plane. That's fine, but the reader has to fetch both the base chunks and the log segments for overlapping ranges. For a read-heavy workload on a file with a large log, this doubles the fetch work. Client-side caching should hide most of it; if not, log entries with very hot read traffic are a compaction signal.

3. **Interaction with ChunkCache invalidation.** The existing `ChunkCache` keys on chunk hash, which remains valid during the log phase — base chunks don't change. Log segments are new chunks with fresh hashes. No cache coherence problem. Confirmed by inspection of `NeonFS.Client.ChunkCache`; worth a property test in Phase 2.

4. **Quorum write ordering.** Today a `write_file_at` writes the chunk then updates FileIndex. For the log path, we write the sealed segment chunk then update `log_segments`. The ordering is the same; no new failure modes. But the flush batch may touch multiple files — today a batch is per-file, preserving that keeps failure handling simple. **Decision pending**: per-file or per-volume batch? Design leans per-file for simplicity.

5. **Snapshot integration.** DR snapshots (issues #322-#324) must include the log segments so restored snapshots match pre-snapshot file content. The mechanism is the same as including the chunks — follow the FileIndex row's `log_segments` column during snapshot assembly. Mentioned here so the DR design factors this in when it lands.

## References

- Issue: [#269](https://harton.dev/project-neon/neonfs/issues/269)
- Source: `neonfs_core/lib/neon_fs/core/write_operation.ex`, `file_index.ex`, `native/neonfs_blob/src/chunking.rs`, `intent_log.ex`
- Comparable systems: ZFS ZIL, LSM compaction (LevelDB/RocksDB), FoundationDB log servers
