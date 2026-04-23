# Runbook: Drive failure

A core node's drive is failing or has failed. This runbook covers the diagnosis, evacuation, physical replacement, and re-join flow, including when to declare data loss and rely on replication-based recovery.

## Symptom

One or more of the following:

- Kernel logs (`dmesg`, `journalctl -k`) show I/O errors on the drive's block device: `Buffer I/O error`, `end_request: I/O error`, `medium error`.
- SMART alert fires (via `smartd`, node_exporter, or equivalent): reallocated sectors rising, pending sectors non-zero, or SMART overall-health assessment `FAILED`.
- `neonfs drive list` reports the drive as `unhealthy`, `evacuating (failed)`, or with a `last_error` timestamp.
- The BlobStore health check on the owning node emits warnings: `chunk write failed`, `chunk read failed`, or `fs stat failed for drive <id>`.
- Prometheus alerts on `neonfs_drive_errors_total` rising, or on filesystem-level `node_filesystem_readonly` flipping to `1` on the drive's mount.
- Operator is explicitly retiring hardware — no error, but the drive is going anyway.

If *every* drive on the node is erroring, you are likely looking at a host-level issue (backplane, controller, cable). In that case, move to [Node-Down](Node-Down.md) first and treat the drives as a node-level event.

## Precondition

- The cluster has quorum (majority of core nodes reachable). Evacuation uses quorum-replicated metadata changes, so quorum loss blocks this procedure — see the [Quorum-Loss runbook](Quorum-Loss.md) _(forthcoming)_ first.
- The affected volume's durability setting is either `replicated` with replication factor ≥ 2 or `erasure` with `data + parity` configured so that one chunk loss is survivable. Single-replica volumes with chunks only on the failing drive cannot recover without manual intervention — see [Escalation](#escalation-path).
- You have `neonfs` CLI access to a healthy node and shell / IPMI access to the affected node.

## Procedure

### 1. Confirm the drive is failing

```bash
# From the affected node, inspect the underlying block device.
sudo smartctl -a /dev/<device>

# Surface kernel I/O errors in the last hour.
sudo journalctl -k --since "1 hour ago" | grep -iE "I/O error|medium error|reallocated|end_request"

# Cross-check with what NeonFS sees.
neonfs drive list --node <node-name>
neonfs node status <node-name>
```

Capture the drive ID from `neonfs drive list` output — you will need it for every subsequent step. Note whether the drive is still readable (some errors are write-only; reads may still work) and its current capacity utilisation.

### 2. Classify the failure severity

| Class | Distinguishing signal | Evacuation path |
|---|---|---|
| **Degraded** — SMART warnings, sectors reallocating, but I/O still succeeds | SMART `Pre-fail` attributes rising; no I/O errors in kernel log yet | Graceful `drive evacuate` (step 3a) |
| **Failed read-write** — filesystem flipped to read-only, but reads still succeed | Kernel log shows remount `read-only`; writes fail, reads succeed | Graceful `drive evacuate` (step 3a); expect the source drive not to serve any writes during evacuation |
| **Failed hard** — no I/O succeeds, device disappeared from `/dev`, filesystem unmounted | Kernel shows `Buffer I/O error` on every access; `smartctl` can't communicate | No graceful path — jump to step 3b (declare data loss, rely on replication) |

If you're unsure between degraded and failed read-write, attempt a tiny test read: `sudo dd if=/var/lib/neonfs/drives/<id>/blobs/<any>/<file> of=/dev/null bs=64k count=1 iflag=direct 2>&1`. A clean read means the drive is still usable for evacuation.

### 3. Move chunks off the drive

#### 3a. Graceful evacuation (degraded / failed read-write)

```bash
# Starts the evacuation job. Same-tier destinations preferred; --any-tier
# falls back if the current tier has no capacity.
neonfs drive evacuate <drive-id> --node <node-name>
neonfs drive evacuate <drive-id> --node <node-name> --any-tier

# Watch the job.
neonfs job list --type drive_evacuate
neonfs job show <job-id>
```

Evacuation is chunk-by-chunk and respects volume durability — the source chunk is only deleted after the required replica count exists elsewhere. For a multi-TiB drive expect hours; the `neonfs_drive_evacuate_bytes_per_second` Prometheus metric gives the rate.

If the source drive starts erroring partway through evacuation, the job will attempt each remaining chunk once and mark unreadable ones as failed. Do not cancel the job in that case — let it drain so you know exactly which chunks are lost.

#### 3b. No evacuation possible (failed hard, data-loss call-out)

When the drive is unreadable, evacuation cannot move the chunks — they are already gone from this replica. Recovery relies on the surviving replicas on other nodes.

```bash
# Mark the drive as forcibly removed. Chunks on the drive are no longer
# tracked as "available here"; under-replicated chunks surface in
# cluster status.
neonfs drive remove <drive-id> --force
```

After the force-remove, NeonFS's GC and replication paths see the chunks as under-replicated and schedule re-replication onto surviving drives. The cluster remains readable for the volume from other replicas during this time, but write durability is reduced until re-replication completes.

> **Data-loss risk.** If the hard-failed drive held any chunk that was *only* on that drive (single-replica volume, or replicated volume that had not yet finished initial replication, or an erasure-coded stripe where a parallel loss has already happened), those chunks are gone. Check for under-replicated chunks before declaring the outage resolved — see step 6.

### 4. Remove the drive from the cluster

Once evacuation is either complete (`job show` reports `status: done` with `bytes_remaining: 0`) or the drive has been force-removed, the drive is no longer referenced:

```bash
neonfs drive list --node <node-name>   # drive should be absent
```

If the drive still appears, check `neonfs job show <job-id>` for an error — evacuation may have stalled on an I/O-errored chunk that needs force-removal (step 3b) to clear.

### 5. Physical replacement

Procedure depends on whether the host supports hot-swap.

#### 5a. Hot-swap capable

```bash
# Ensure the kernel has stopped using the device — evacuation + remove
# already removed NeonFS's reference, but double-check with:
lsof -n +D /var/lib/neonfs/drives/<id> 2>/dev/null
sudo umount /var/lib/neonfs/drives/<id>
```

Swap the disk. The hot-swap bay and controller will rescan; confirm the new device exists (`lsblk`, `/dev/disk/by-id/...`). Format and mount the replacement at the same path as the old drive:

```bash
sudo mkfs.xfs /dev/<new-device>
sudo mount /dev/<new-device> /var/lib/neonfs/drives/<id>
sudo chown neonfs:neonfs /var/lib/neonfs/drives/<id>
```

Persist the mount in `/etc/fstab` or the systemd mount unit (whichever the host uses).

#### 5b. Not hot-swap — offline replacement

Stop the NeonFS service on the node first:

```bash
sudo systemctl stop neonfs-core
```

Then power-cycle as needed, replace the drive, format+mount as above. Restart the service. The node rejoins the cluster and re-forms quorum with the surviving peers.

### 6. Re-add the replacement drive

```bash
neonfs drive add \
  --path /var/lib/neonfs/drives/<id> \
  --tier <same-tier-as-old-drive> \
  --capacity <size-of-new-disk> \
  --id <id>
```

Match the old drive's tier. Use the old drive ID if the drive is standing in for the same role — it keeps Prometheus labels stable. Use a new ID if you want to track the replacement separately (e.g. bigger disk taking on more capacity).

The cluster's rebalancer notices the new, empty drive and starts placing new chunks there. For faster re-balancing of existing chunks:

```bash
neonfs cluster rebalance --tier <tier> --threshold 0.05
neonfs cluster rebalance-status
```

### 7. Recover under-replicated chunks

```bash
neonfs cluster status --output json | jq '.under_replicated_chunks // empty'
neonfs job list --type replication
```

Under-replicated chunks — either from step 3b (force-remove) or from mid-evacuation I/O errors — are recovered automatically by the replication subsystem. It re-creates missing replicas onto surviving drives using the volume's durability rules. Watch `neonfs job list --type replication` until the count hits zero.

If the same chunk keeps failing to re-replicate (stuck in `retrying`), the source chunk is probably also unreadable (a second loss, or an erasure-coded stripe that can't be reconstructed because too many shards are missing). That is a genuine data-loss case — see [Escalation](#escalation-path).

## Verification

The incident is resolved when **all** of the following hold:

- `neonfs drive list` shows the replacement drive (or shows no drive at all if the failed one was being retired) in `healthy` state.
- `neonfs cluster status --output json | jq '.under_replicated_chunks // 0'` reports `0`.
- `neonfs job list --type replication` shows no pending or in-progress jobs.
- `neonfs job list --type drive_evacuate` shows the evacuation job in `done` state (not `failed` or `cancelled`).
- SMART on the replacement drive reports `PASSED` on the first scheduled scrub.
- A sample of reads and writes against volumes that had chunks on the failed drive succeed from a client.

## Escalation path

Stop and escalate to engineering before taking irreversible steps if any of the following hold:

- Step 7 shows chunks that cannot be re-replicated (replication job repeatedly retrying the same chunk). That is genuine data loss and needs a per-file impact analysis before deciding on recovery strategy.
- A second drive on the same node starts failing during evacuation. Running evacuation on a node with another failing drive can tip the node into quorum loss if that second drive fails hard.
- The failing drive is the only copy of any system-volume chunk (rare, indicates misconfiguration). Force-removing it can brick the cluster — engineering must assess first.
- Evacuation stalls with `bytes_remaining` unchanged for > 10 minutes and the job is not in `retrying`. This suggests a deadlock in the drive scheduler or the BlobStore, not a simple I/O error.

Before escalating, capture:

- Full output of `neonfs drive list`, `neonfs cluster status`, and `neonfs job show <evacuation-job-id>` (JSON).
- The last 200 lines of `journalctl -u neonfs-core` from the affected node and two surviving peers.
- `smartctl -x /dev/<device>` output.
- The timeline: first SMART warning, first I/O error, first operator response, outcome of each step attempted.

After the incident is fully resolved, write it up with the post-mortem template at [Post-Mortem-Template.md](Post-Mortem-Template.md) _(forthcoming)_.

## Known limitations

- There is no CLI subcommand to surface "chunks at risk" (chunks whose only replica is on a specific drive) as a preview before evacuation. Operators have to infer risk from under-replication counts and volume durability settings. Tracked as a follow-up.
- `neonfs drive remove --force` does not emit a structured list of chunks that were on the drive at the time of force-removal. For a post-mortem you have to cross-reference `neonfs job show` output against the replication jobs that follow.

## References

- [Operator guide — Drive management](../operator-guide.md#drive-management) (CLI surfaces for add/evacuate/remove).
- [CLI reference](../cli-reference.md#neonfs-drive) (full `drive` command options).
- [Node-Down runbook](Node-Down.md) (when the failure is host-wide, not just one drive).
