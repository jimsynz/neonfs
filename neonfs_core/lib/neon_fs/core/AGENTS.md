# NeonFS Core — Agent Notes

Module-local notes for the Ra / QuorumCoordinator-backed managers that
live in this directory. Durable conventions; updated when behaviour
changes.

## QuorumCoordinator-backed indexes: cache coherence

Applies to `ChunkIndex`, `FileIndex`, `StripeIndex`.

These modules front `QuorumCoordinator.quorum_read/2` with a local
public ETS table (`:chunk_index`, `:file_index_by_id`, `:stripe_index`).
Unlike the Ra-backed managers (see #341), the underlying store is
leaderless and network-dependent, so the ETS table is genuinely useful
for avoiding per-call RPCs on this node — but it is **not a coherent
distributed cache**.

### Coherence model (as of #342)

The four dimensions investigated:

1. **Cross-node write visibility.** `QuorumCoordinator.quorum_write`
   dispatches `MetadataStore.write` to each replica in the ring — not
   `ChunkIndex.put`. So a write committed on node A replicates to B's
   `MetadataStore` (and disk) but **does not** update B's `:chunk_index`
   ETS. A subsequent read on B that hits the ETS serves the old value.

2. **Read-repair interaction.** `QuorumCoordinator.trigger_read_repair`
   targets the remote `MetadataStore` via `dispatch_write`. The stale
   replica's underlying store is healed; its ETS cache is not.

3. **Delete propagation.** `quorum_delete` writes a tombstone to each
   replica's `MetadataStore` but only clears the originating node's
   ETS. Remote nodes with the key cached continue to return it from
   ETS hits.

4. **Partition-heal behaviour.** There is no eviction or
   invalidation mechanism. Entries cached before the partition persist
   indefinitely afterwards unless the process restarts or the key is
   written again locally.

### Fix applied in #342

**Point reads (`get/1`, `exists?/1`) on all three indexes always go
through `QuorumCoordinator.quorum_read/2`.** The ETS table is no
longer consulted on the read path. Coherence for user-facing reads is
now backed by the quorum protocol — latest HLC wins, tombstones
resolve, read-repair triggers on divergence.

The ETS remains as a **write-through materialisation**:

- Local writes (`put`, `delete`) still insert/delete the local ETS
  entry on success.
- `get_from_quorum` still populates ETS on successful remote reads (a
  side-effect — nothing reads this back for point lookups, but it
  keeps list-operation visibility consistent with the previous
  behaviour on this node).
- `load_from_local_store` on boot still seeds ETS from locally-persisted
  `MetadataStore` segments.

Performance: every point read now does a quorum_read. For hot paths
that resolved thousands of chunks per file read, this is a measurable
regression. A follow-up (see the issue trail off #342) evaluates
TTL-invalidated caching or distributed cache invalidation for a
perf-recoverable fix — don't put the ETS-first check back in without
that design.

### Known limitations still open

**List operations** (`list_all/0`, `list_by_volume/1`,
`list_by_location/1`, `list_by_node/1`, `list_by_drive/2`,
`list_uncommitted/0`, `ChunkIndex.get_chunks_for_volume/1`) still
iterate the local ETS. They see:

- Entries written locally (via `put`, `update_locations`, etc.).
- Entries read via `get/1` / quorum read on this node (via the
  populate-on-read side-effect in `get_from_quorum`).
- Entries restored from local on-disk `MetadataStore` at boot.

They do **not** see entries written by other nodes that were never
read locally, and they **will** see entries that were deleted
elsewhere and not yet expunged locally. Callers treat these as
node-local maintenance views (scrub, GC, tiering — all per-node
schedulers). They are not safe for cluster-wide truth queries.

A follow-up is tracked to replace these with a cluster-wide iteration
API (scan via `MetadataStore.list_all/1` across replicas, or a
materialised aggregate).

**Internal write-path reads** (`FileIndex.fetch_file/2`, the
`:ets.lookup(:chunk_index, …)` in `ChunkIndex.handle_call` write-path
clauses) still read from ETS first. These are read-modify-write
sequences where staleness can cause a lost-update. HLC-LWW in
`QuorumCoordinator.quorum_write` masks the raw correctness issue, but
semantic-version bumps (e.g. `FileMeta.update/2`) are vulnerable. A
follow-up is tracked to route write-path reads through quorum too.

## General reminders for this directory

- `Process.flag(:trap_exit, true)` in `init/1` — without it,
  `terminate/2` is never called on supervisor shutdown.
- `:ra_not_available` fallbacks that write to ETS (still present in
  some legacy managers pending #341 slices) look like resilience but
  silently desynchronise the cluster — never add new ones.
- Ra `consistent_query` goes via the leader; `local_query` reads the
  local replica synchronously (see `RaSupervisor.local_query/2` once
  #345 lands). Prefer local_query for orchestration-layer reads; the
  state machine apply is synchronous so every replica has the same
  state after commit.
