# Runbook: Restore from DR snapshot

DR (disaster-recovery) snapshots capture cluster metadata + CA material to the `_system` volume on a configurable cadence. They are the recovery mechanism for **catastrophic metadata events** — operator error (`rm -rf /` against a mounted volume, mass `volume delete`, accidental `cluster ca rotate` against a stale node set), Ra log corruption that survived replication, or a state-machine bug that mangled an index without crashing.

A DR snapshot is **not** a substitute for blob-storage replication or off-cluster backups of `cluster.json` and the CA. It captures *metadata*, not chunk bytes — restore only succeeds when the chunk blobs the snapshot references are still on the cluster's drives (or recoverable from elsewhere).

This runbook covers two scenarios:

- **Cluster-wide restore** — the entire metadata state needs to roll back to a known-good snapshot.
- **Partial restore** — a single volume's metadata needs to roll back without affecting the rest of the cluster.

> **Tooling note.** There is no first-class `neonfs dr snapshot restore` CLI today. Step [4](#4-apply-the-restore-engineering-applied) of every restore path is operator-applied with engineering oversight. Tracked as a follow-up — the runbook is published now so the diagnostic and decision steps don't have to be reconstructed under incident pressure.

## Symptom

- Bulk metadata loss observed: `neonfs volume list` is missing volumes that should be there, or `neonfs volume show <name>` reports a file count far below what the operator expects.
- `_system` volume index dump shows entries with corrupt fields (atom keys missing, integer fields holding strings).
- A `cluster ca rotate` against the wrong node set has revoked active node certs.
- Ra has applied a mutation that operators want to undo (e.g. an erroneous `cluster remove-node --force` against a still-needed peer).
- `neonfs cluster status` reports normal quorum but `neonfs dr snapshot list` shows a recent good snapshot whose `state_version` is several Ra log indexes behind the current state — the gap is the data loss to undo.

## Precondition

- A DR snapshot whose `created_at` predates the metadata event exists. `neonfs dr snapshot list` returns at least one snapshot newer than the cluster's current outage and older than the bad event. If the only snapshots are *after* the event, restore won't help — escalate.
- The `_system` volume itself is intact. Restore reads from `_system`'s `/dr/<id>/`; if `_system` is the index that was destroyed, the snapshot is unreachable from inside the cluster and you need an off-cluster copy of `_system` (LVM/ZFS snapshot, periodic `tar` of `/var/lib/neonfs/data/`).
- An off-cluster CA backup exists. The DR snapshot embeds a copy of the CA material under `/dr/<id>/ca/`, but it cannot be the only copy — the [CA-Rotation runbook](CA-Rotation.md) lists the off-cluster backup as a hard requirement.
- Blob storage on the cluster's drives is intact. A DR snapshot restores **metadata only** — restored `FileIndex` rows reference chunk hashes that must still exist as blobs on disk, otherwise reads will return `:all_replicas_failed`. Stop and verify drive health before restoring if any drives have been replaced or wiped since the snapshot was taken.
- Ra quorum is currently healthy (or you are intentionally rebuilding it as part of the recovery — see [Quorum-Loss runbook](Quorum-Loss.md) first if not).

## Procedure

### 1. Confirm a usable snapshot exists

```bash
neonfs dr snapshot list                       # newest first
neonfs dr snapshot show <id>                  # manifest summary
```

Pick the newest snapshot whose `created_at` predates the bad event. Note its `id` and `state_version` — both go in the post-mortem.

If the snapshot's `state_version` is `nil` or its `files` list is empty, the snapshot was captured during a degraded state — try the next-oldest, or escalate.

### 2. Decide cluster-wide vs partial restore

| Observation | Path |
|---|---|
| Multiple volumes missing or all index counts dramatically off | [Cluster-wide restore](#cluster-wide-restore) |
| One volume's `FileIndex` is the only thing wrong; other volumes are healthy | [Partial restore](#partial-restore-single-volume) |
| You aren't sure | [Cluster-wide restore](#cluster-wide-restore) — prefer the bigger-but-correct rollback to a partial restore that leaves indexes inconsistent. |

A wrong call here can compound the original event: a partial restore that misses a cross-volume reference (e.g. a chunk shared via dedup) can corrupt a previously-healthy volume. When in doubt, take the cluster-wide path and re-create any post-snapshot writes from external systems.

### Cluster-wide restore

#### 3a. Capture the current bad state

Before changing anything, capture a fresh DR snapshot of the *current* (broken) state. This is forensic: the post-mortem needs to compare what the indexes look like now versus the snapshot you're about to restore, so the broken state is reproducible.

```bash
neonfs dr snapshot create                     # writes to /dr/<now>/ on _system
```

Tag it as the bad-state reference in the incident channel.

#### 3b. Halt mutating traffic

Stop interface nodes so no client can write while the restore is in progress.

```bash
# On every interface host:
sudo systemctl stop neonfs-fuse neonfs-nfs neonfs-s3 neonfs-webdav neonfs-docker 2>/dev/null || true
```

Core nodes stay running — restore reads from `_system` on a core node and applies state-machine commands via Ra.

#### 3c. Verify off-cluster CA backup is current

```bash
ls -la /your/off-cluster/backup/path/tls/     # CA bundle, fingerprint, validity
```

If the on-disk CA differs from the snapshot's CA (`/dr/<id>/ca/ca.crt`), restore will reissue node certs against the snapshot's CA and any operator who has the off-cluster backup will still be able to validate them. If only the broken cluster has the post-rotation CA, document the chain you're about to resurrect before you commit.

#### 4. Apply the restore (engineering-applied)

There is no first-class CLI today. Restore is operator-applied with engineering oversight:

1. Identify the leader: `neonfs cluster status`.
2. On the leader, the snapshot directory is `$NEONFS_DATA_DIR/system/dr/<id>/`. Each `<index>.snapshot` file is a length-prefixed ETF stream the engineering team will replay through `MetadataStateMachine` Ra commands. Hand the file list and the snapshot manifest (`neonfs dr snapshot show <id>`) over for the replay step.
3. Engineering replays each index file via Ra commands, in this order: `volumes`, `services`, `segment_assignments`, `encryption_keys`, `volume_acls`, `chunks`, `stripes`, `files`, `escalations`, `s3_credentials`, `kv`. The order matches the dependency graph — a `FileIndex` row references a `ChunkIndex` row that references a `volumes` row.
4. The CA material under `/dr/<id>/ca/` is restored to `$NEONFS_TLS_DIR/` on each core node. Cluster-wide CA restore is the same shape as [CA-Rotation runbook §Emergency rotation](CA-Rotation.md#emergency-rotation-expiry-has-passed) step 7 — escalate if the snapshot CA differs from the live CA.

This step **must** be applied on every core node that holds a Ra replica. Skipping a follower means it will diverge from the leader on the next snapshot and propagate the divergence.

If any replay step fails mid-way: stop. Capture the index file that errored and the Ra error. Do not retry — a partial replay leaves the cluster in a state that's strictly worse than either the bad-state snapshot (3a) or the target snapshot. Escalate to engineering immediately.

#### 5. Bring interfaces back

```bash
# Each interface host:
sudo systemctl start neonfs-fuse              # or whichever package is installed
```

Each interface re-reads the (restored) `ServiceRegistry` and resumes serving.

### Partial restore (single volume)

#### 6a. Identify the volume's slice

`neonfs dr snapshot show <id>` returns the full manifest. Filter the `FileIndex` and `ChunkIndex` slices to only entries whose `volume_id` matches the volume to restore:

```bash
neonfs dr snapshot show <id> --json | \
  jq '.files[] | select(.path | startswith("files.snapshot")) | .path'
```

The volume's `volume_acls` row (if any) and its slice of `segment_assignments` also need to come along — the same engineering escalation step 4 applies.

#### 6b. Pause writes against the volume

Do not stop the cluster — only the target volume needs to be quiesced. Pausing per-volume traffic is interface-specific:

- **FUSE / NFS / SMB**: unmount the export(s) backed by the volume.
- **S3 / WebDAV**: revoke the credentials scoped to the volume (`neonfs s3 credential revoke …`) and wait for in-flight requests to drain.
- **Docker**: unmount any containers that have the volume in use; do not delete the volume.

If you cannot quiesce the volume cleanly, escalate — a partial restore against a volume that's still being written to will produce inconsistent indexes.

#### 6c. Apply the partial restore (engineering-applied)

Same as cluster-wide step 4, but the engineering team replays only the volume's slice of each index. The `volumes` and `services` rows do not change.

#### 6d. Resume the volume

Re-mount / re-issue credentials. First read should round-trip: `neonfs volume show <name>` reports the file count from the restored `FileIndex`, and a fresh client read of one of those files returns the expected bytes.

## Verification

The incident is resolved when:

- `neonfs cluster status` reports a healthy quorum with a leader elected.
- `neonfs volume list` shows every volume the snapshot manifested.
- `neonfs volume show <name>` reports file counts within `±0` of the snapshot's `state_version` per-volume slice. (If you took post-snapshot legitimate writes that were lost, the count will be lower; document this.)
- A fresh client read against each restored volume succeeds end-to-end. The read exercises `FileIndex → ChunkIndex → blob` so it catches any index slice that didn't make the replay.
- A fresh client write against each restored volume succeeds and survives a follow-up `neonfs dr snapshot create`.
- `neonfs dr snapshot list` shows the bad-state snapshot from step 3a plus the post-restore snapshot — both kept until the post-mortem closes.
- No `Logger.warning` entries from `MetadataStateMachine` in the last 30 minutes of any core node's journal.

## Escalation path

Escalate to engineering before step 4 in every restore path. The replay is operator-applied today and that won't change until a `neonfs dr snapshot restore` CLI lands. Additional escalation triggers:

- The newest pre-event snapshot is older than the cluster's RPO target (e.g. snapshot is from 3 days ago and the cadence is daily). Decide between "restore + accept the gap" and "rebuild from blob storage, accept full metadata regeneration cost" before applying step 4.
- The snapshot's CA does not match the live CA. Restoring the snapshot's CA invalidates every node cert issued under the live CA — every node needs a fresh cert before traffic resumes.
- Blob storage on a drive has been wiped or replaced since the snapshot was taken. Restored `FileIndex` rows will reference chunks that don't exist; the volume will appear correct in metadata but every read returns `:all_replicas_failed`.
- Mid-replay failure (step 4 partial). Do not retry — the cluster's state diverges from both the bad-state snapshot and the target snapshot on every retry attempt.
- The restore is being applied across an active partition (one site reachable, one not). Restoring on the reachable side and waiting for the partition to heal can split-brain Ra; engineering needs to coordinate which side wins.

Capture before escalating:

- `neonfs dr snapshot list --json` (which snapshots exist, when).
- `neonfs dr snapshot show <pre-event-id> --json` (manifest of the target snapshot).
- `neonfs dr snapshot show <bad-state-id> --json` (manifest of the bad state — taken at step 3a).
- `neonfs cluster status`, `neonfs cluster ca info`, `neonfs cluster ca list` from a core node.
- Last 500 lines of `journalctl -u neonfs-core` from each core node, plus the time of the metadata event itself.
- Off-cluster CA backup location and last-verified date.
- Timeline: first observation, decision to restore, snapshot picked, who's applying step 4.

After the incident, use the [Post-Mortem-Template.md](Post-Mortem-Template.md). DR-snapshot incidents always capture: the original event's blast radius (which volumes, which chunks, which Ra log range), why automated alerting did or didn't catch it, whether the snapshot cadence was sufficient (RPO actual vs target), and whether off-cluster backups would have been needed if the in-cluster `_system` volume had also been affected.

## Known limitations

- There is no `neonfs dr snapshot restore` CLI. Step 4 is operator-applied with engineering oversight; a first-class CLI is on the roadmap.
- Snapshots capture metadata only — not chunk blobs. A drive failure between snapshot and restore that hasn't been resolved by the cluster's replication / erasure-coding policy will show as restored-but-unreadable files.
- Snapshot cadence (set in `config :neonfs_core, NeonFS.Core.DRSnapshotScheduler, interval_ms: …`) is global — there is no per-volume cadence, so a high-write volume's RPO is the same as the rest of the cluster's. Tune the cadence to the most-volatile volume.
- Snapshot retention is grandfather-father-son (7 daily / 4 weekly / 12 monthly by default). A restore older than the monthly window is not available — pre-decide which incidents fall inside vs outside the retention envelope.
- Partial restore correctness depends on the operator correctly identifying every index slice the volume references. Cross-volume dedup, shared encryption keys, and shared segment-assignments make this non-obvious — when in doubt, prefer cluster-wide restore.

## References

- [Operator guide §Backup](../operator-guide.md#backup) — the broader backup story, of which DR snapshots are one piece.
- [`#322`](https://harton.dev/project-neon/neonfs/issues/322), [`#323`](https://harton.dev/project-neon/neonfs/issues/323), [`#324`](https://harton.dev/project-neon/neonfs/issues/324) — DR snapshot module, scheduler, and CLI as built today.
- [`#247`](https://harton.dev/project-neon/neonfs/issues/247) — DR snapshot epic.
- [Quorum-Loss runbook](Quorum-Loss.md) — when the metadata event has also broken Ra quorum and you need to recover quorum first.
- [CA-Rotation runbook](CA-Rotation.md) — when the snapshot's CA differs from the live CA.
- [Post-Mortem-Template.md](Post-Mortem-Template.md) — the template every restore incident closes against.
