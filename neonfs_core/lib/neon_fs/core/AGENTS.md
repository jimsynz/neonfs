# NeonFS Core — Agent Notes

Module-local notes for the Ra / per-volume metadata managers that
live in this directory. Durable conventions; updated when behaviour
changes.

## Per-volume metadata model (post #750 / #792)

Applies to `ChunkIndex`, `FileIndex`, `StripeIndex`.

Each volume has three index trees (`file_index`, `chunk_index`,
`stripe_index`) stored as ordinary content-addressed chunks under the
volume's `drive_locations`. The trees are reached from the
Ra-replicated *bootstrap pointer* (`MetadataStateMachine.get_volume_root/2`)
which carries the current `root_chunk_hash` and `drive_locations`.

The bootstrap pointer is consensus-strong; tree pages and data chunks
are eventually-consistent (replicated by background runners, see
below).

### Read paths

- `*Index.get/2`, `exists?/2` front the per-volume `MetadataReader`
  (`NeonFS.Core.Volume.MetadataReader`). MetadataReader walks
  bootstrap → root segment → index tree.
- Read-path cross-node fallback (#947): if the local node's drive
  doesn't have the canonical tree pages yet, MetadataReader RPCs the
  call to a remote node listed in `drive_locations`. Replicas
  converge as readers ask.

ETS tables (`:chunk_index`, `:file_index_by_id`, `:stripe_index`) are
**local materialisations** — populated by recent local writes and by
successful MetadataReader reads — used as fast paths for list
operations (`list_all/0`, `list_by_node/1`, etc). They do **not**
guarantee cluster-wide truth; the canonical view is the per-volume
index tree.

### Write paths

- `*Index.put/1`, `delete/1`, `update_*/2` front the per-volume
  `MetadataWriter` (`NeonFS.Core.Volume.MetadataWriter`). The writer
  runs the index-tree NIF op against a local drive's store handle,
  builds a new root segment, replicates the segment chunk via
  `ChunkReplicator`, then CAS-updates the bootstrap pointer through
  Ra.

### Background convergence

- **`Job.Runners.VolumeAntiEntropy` (#921)** walks each volume's
  `chunk_index`, asks every declared `location.node` whether the
  chunk exists locally, and enqueues `ReplicaRepair.repair_chunks/2`
  for gaps. Dispatched periodically by `VolumeAntiEntropyScheduler`
  (default 1h cadence) per `RootSegment.schedules.anti_entropy`.
- **`ReplicaRepair.repair_volume/2`** scans `ChunkIndex` for chunks
  below target replication factor and re-replicates. Periodic via
  `ReplicaRepairScheduler`.
- **`Job.Runners.Scrub`** reads each chunk on local drives, verifies
  the SHA-256 matches, flags bit-rot. Per-volume cadence in
  `RootSegment.schedules.scrub`.

### List operations

`list_all/0`, `list_by_volume/1`, `list_by_node/1`, etc still iterate
the local ETS. They see:

- Entries written locally.
- Entries observed via successful local reads.
- Entries restored from local DETS at boot.

They do **not** see entries written elsewhere that were never read
locally. Callers treat these as node-local maintenance views (scrub,
GC, tiering — all per-node schedulers). They are not safe for
cluster-wide truth queries. For cluster-wide reads use the canonical
`MetadataReader.range/5` path.

## General reminders for this directory

- `Process.flag(:trap_exit, true)` in `init/1` — without it,
  `terminate/2` is never called on supervisor shutdown.
- Ra `consistent_query` goes via the leader; `local_query` reads the
  local replica synchronously (`RaSupervisor.local_query/2`). The
  per-volume `MetadataReader` uses `consistent_query` for the
  bootstrap-pointer lookup so reads are linearisable against writes.
- Tree-page replication is currently per-write to a single local
  drive only — replicas catch up via anti-entropy or read-path
  fallback. See #903 / #955 for the fan-out-at-write-time follow-up.
