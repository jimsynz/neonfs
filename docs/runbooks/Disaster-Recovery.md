# Runbook: Bootstrap-layer disaster recovery (`reconstruct-from-disk`)

The bootstrap layer's Ra state is unrecoverable but the underlying volume data on disk is intact. `neonfs cluster reconstruct-from-disk` walks every drive's `.neonfs-drive.json` identity file plus its root-segment chunks and rebuilds the bootstrap-layer Ra state from scratch.

This is a **last-resort** operation. Read it end-to-end before acting.

## When to use

All three must be true:

- The cluster's Ra logs are corrupted, missing, or otherwise unrecoverable on every node — typical after simultaneous loss of every Ra replica with no working backup. (See [Quorum-Loss](Quorum-Loss.md) before declaring this; ordinary quorum loss is recoverable without reconstruction.)
- The on-disk volume data — drive identity files plus root-segment chunks — is intact on at least one drive of each volume you want to recover.
- You have ruled out [DR-Snapshot-Restore](DR-Snapshot-Restore.md): a fresh snapshot restore is preferable when a snapshot exists, because it preserves the entire cluster state (ACLs, encryption keys, etc.), not just the volume registry.

If any of those is uncertain, **stop**. Reconstruction is destructive when applied to a healthy cluster — it overwrites the bootstrap-layer Ra state with whatever the algorithm finds on disk.

## What it does, exactly

`Reconstruct-from-disk` is an Elixir function plus a CLI subcommand. It runs locally on one core node and:

1. Reads the configured drive list (`:drives` in app env).
2. For each drive path, reads `<drive>/.neonfs-drive.json` (the [Drive.Identity](../../neonfs_core/lib/neon_fs/core/drive/identity.ex) file from #778). Drives whose `cluster_id` doesn't match the local cluster are skipped with a warning.
3. For each accepted drive, walks `<drive>/blobs/<tier>/**/*` and decodes each candidate chunk as an [ETF root segment](../../neonfs_core/lib/neon_fs/core/volume/root_segment.ex) (#780). Non-root chunks fail decode and are silently skipped — most chunks aren't root segments.
4. When the same `volume_id` has multiple root-segment chunks (old + new from a partially-GCd CoW history), the highest HLC wins. Same HLC + same hash on different drives produces a multi-replica `drive_locations` entry.
5. Submits the resulting `:register_drive` + `:register_volume_root` Ra commands via `RaSupervisor.command/1`. Per-command failures are aggregated and surfaced; the run does not abort on the first failure.

It does **not** reconstruct anything else: cluster identity, encryption keys, ACLs, IAM state, schedules, audit log — everything outside the bootstrap layer's `drives` + `volume_roots` tables stays whatever the running Ra state happens to contain. If the rest of the Ra state is also gone, restore it from a snapshot first (see [DR-Snapshot-Restore](DR-Snapshot-Restore.md)) and then run reconstruction to refresh the bootstrap layer.

## Precondition checklist

Tick every box before running anything:

- [ ] You have read this entire runbook.
- [ ] You have read [Quorum-Loss](Quorum-Loss.md) and confirmed that ordinary quorum-loss recovery does **not** apply.
- [ ] You have read [DR-Snapshot-Restore](DR-Snapshot-Restore.md) and confirmed no usable snapshot exists.
- [ ] You have shell access to a core node and the `neonfs` CLI is installed.
- [ ] The `neonfs-core` daemon on that node is running.
- [ ] Every drive path listed in the daemon's `:drives` config is mounted and readable.
- [ ] You have authorisation to overwrite the bootstrap-layer Ra state on this cluster.

## Procedure

### 1. Preview what will happen

Always preview first — `--dry-run` performs the discovery + decode pass without submitting anything to Ra.

```bash
neonfs cluster reconstruct-from-disk --dry-run --output json
```

Expected output shape:

```json
{
  "status": "preview",
  "drives": 3,
  "volumes": 7,
  "commands": 10,
  "commands_submitted": 0,
  "commands_failed": 0,
  "warnings": 0,
  "dry_run": true
}
```

Verify:

- `drives` matches the number of drives you expect this node to be hosting.
- `volumes` matches the number of distinct volumes you expect to recover.
- `commands` equals `drives + volumes` — one `register_drive` per drive plus one `register_volume_root` per volume.
- `warnings` is zero, or every warning is something you understand (foreign-cluster drives, unreadable identity files, foreign-cluster root segments).

If any of those are off, **stop**. Investigate the warnings before running the real submission. Common warnings:

- `:foreign_cluster` — the drive's `.neonfs-drive.json` has a different `cluster_id`. Probably an ex-cluster drive that got plugged in by mistake. Unmount it before re-running.
- `:identity_unreadable` — the identity file is missing or corrupted. The drive will not contribute to the reconstruction; if the drive holds the only copy of a volume's root segment, you have lost that volume.
- `:foreign_segment` — a chunk decoded as a root segment but with a foreign `cluster_id`. Usually leftovers from a previous cluster on the same disk. Reconstruction will skip it.
- `:chunk_unreadable` — an I/O error on a specific chunk. If repeated across many chunks, the drive is failing.

### 2. Submit the reconstruction

Once the preview looks right:

```bash
neonfs cluster reconstruct-from-disk --yes --output json
```

If the bootstrap layer's `volume_roots` is non-empty (e.g. you're recovering from partial Ra-state corruption rather than a full wipe), the daemon will refuse with a clear message. Re-run with `--overwrite-ra-state` once you have confirmed that overwriting the existing volume registry is the right call:

```bash
neonfs cluster reconstruct-from-disk --yes --overwrite-ra-state --output json
```

Expected output:

```json
{
  "status": "applied",
  "drives": 3,
  "volumes": 7,
  "commands": 10,
  "commands_submitted": 10,
  "commands_failed": 0,
  "warnings": 0,
  "dry_run": false
}
```

`commands_submitted` should equal `commands`. If `commands_failed > 0`, inspect the daemon log (`journalctl -u neonfs-core` or the equivalent) for the per-command failure reasons — typically Ra leader unavailable, in which case retry once Ra has elected a leader.

### 3. Verify recovery

```bash
# Volumes are visible:
neonfs volume list

# Each volume's metadata reads succeed:
for vol in $(neonfs volume list --output json | jq -r '.volumes[].name'); do
  echo "=== $vol ==="
  neonfs volume info "$vol"
done

# Sample-read a known file from each volume:
neonfs file get <volume>/<known-path> /tmp/probe-out
```

If a volume is missing from the list or its metadata reads fail, that volume's root-segment chunks were not decodable on any drive this node walked. Try running reconstruction on a different node that holds replicas of that volume.

## Limitations

- **Single-node walk.** Each `reconstruct-from-disk` invocation walks only the drives configured on the node it runs on. In a multi-node cluster you may need to invoke it on multiple nodes to recover every volume's root segment — the per-volume `drive_locations` only lists drives the running invocation found locally. Cross-node merge is currently the operator's job.
- **Drive registration is local-node-only.** `register_drive` commands tag the drive's `node` as the running node. If you ran reconstruction on the wrong node for a given drive, you will need to deregister and re-register the drive afterwards.
- **No data-plane reconstruction.** Files, chunks, ACLs, encryption keys, IAM state, audit log entries — none of these are touched by reconstruction. Only the bootstrap layer's volume + drive tables.
- **HLC tiebreak only.** When two root segments share the same HLC, the older one wins by ordering — this is fine for a CoW-clean cluster but can produce a slightly stale view if a fast-cycling write was in flight at the time the cluster died.

## Escalation

If reconstruction reports zero volumes recovered, every drive identity file is unreadable, or the `commands_failed` count is non-zero on every retry: stop. The volume data may be unrecoverable from this node. Engage the on-call escalation channel before running anything more invasive.

## See also

- [Quorum-Loss](Quorum-Loss.md) — the ordinary path for "no leader" symptoms. Almost always the right runbook before considering reconstruction.
- [DR-Snapshot-Restore](DR-Snapshot-Restore.md) — the preferred path when a snapshot exists.
- [Node-Down](Node-Down.md) — single-node failure recovery.
- Issue [#788](https://harton.dev/project-neon/neonfs/issues/788) — the per-volume metadata epic that introduced this recovery path.
