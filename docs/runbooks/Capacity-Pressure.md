# Runbook: Capacity pressure response

A cluster, tier, or drive is running out of space. This runbook covers the progression from early warning through immediate load-shedding to permanent capacity growth — and, critically, the decisions you should **not** make under pressure.

## Symptom

One or more of the following:

- `neonfs drive list --output json | jq '[.drives[] | select(.usage_ratio > 0.8)]'` returns non-empty rows.
- `neonfs cluster status` reports one or more drives in `usage: critical` or the node-level capacity metric above threshold.
- Prometheus alert on a drive-fullness or tier-fullness metric firing (`neonfs_drive_bytes_free / neonfs_drive_bytes_total` crossing the configured threshold).
- Writes to specific volumes start failing with `{:error, :no_available_drive}` or `{:error, :insufficient_capacity}`.
- Filesystem-level signals: kernel `ENOSPC` on the underlying device for `/var/lib/neonfs/drives/<id>`.

If writes are failing on **every** volume simultaneously, the cluster as a whole is out of any-tier capacity — jump straight to [Permanent capacity growth](#4-permanent-capacity-growth) and then come back for cleanup.

## Precondition

- Cluster has quorum (majority of core nodes reachable). Without quorum, neither metadata mutations nor tier migrations can make progress — see [Quorum-Loss runbook](Quorum-Loss.md) first.
- You have at least a rough picture of which volumes and tiers are full vs which have headroom. The first step captures this.
- Backups are up to date. Some of the remedial steps (aggressive GC, tier eviction) remove chunks that are expected to be reclaimable elsewhere; a recent backup is the failsafe.

## Procedure

### 1. Assess where the pressure lives

```bash
# Per-drive and per-tier utilisation across the cluster.
neonfs drive list
neonfs drive list --output json \
  | jq '[.drives[] | {node, id, tier, usage_ratio, bytes_free}] | sort_by(.usage_ratio) | reverse'

# Per-volume capacity footprint — needs list_files + per-file size.
# (No dedicated CLI today. The operator dashboard or cluster status
# summary is the first place to look.)
neonfs cluster status --output json | jq '.capacity'
```

Classify by scope:

| Scope | Example | Typical remediation |
|---|---|---|
| **One drive full, tier has headroom on other drives** | `nvme0` at 95%, but `nvme1` and `nvme2` have 40% free | Rebalance the tier (step 3) |
| **One tier full across every drive of that tier** | `hot` at 95% everywhere, `warm` at 50%, `cold` at 20% | Tier eviction (step 2a) + rebalance after eviction |
| **All tiers full** | Nothing has meaningful headroom | Permanent capacity growth (step 4) — there is no relief without new disks |
| **One node full, other nodes have room** | `node-2`'s drives all > 90%, `node-1` and `node-3` at 40% | Inter-node rebalance via `cluster rebalance` |

Record the pre-remediation usage numbers — they are your before picture for verification.

### 2. Immediate relief

Pick from the options below in the order they appear. Stop as soon as pressure is off (drives below the alert threshold) — over-remediating adds churn and masks the post-mortem.

#### 2a. Run garbage collection

GC reclaims chunks that no FileIndex entry references — orphans from aborted writes, deleted volumes, or pending-write log cleanup that did not complete.

```bash
neonfs gc collect
neonfs gc status                                  # watch the latest job
neonfs job list --type garbage_collection
```

GC is safe to run under pressure — it only touches chunks with no references. On a cluster that has not GC'd in a while, expect significant reclamation (chunks from the old-cert-rotation cycle, pre-release stress tests, etc.).

Scope to a specific volume when one volume's churn dominates:

```bash
neonfs gc collect --volume <volume-name>
```

#### 2b. Accelerate tier eviction

Hot → warm → cold eviction is driven per-volume by the volume's tiering policy. If hot is full but warm has room, lower the hot-tier target via the volume's config so more chunks demote:

```bash
neonfs volume update <volume-name> --initial-tier warm      # new writes land in warm
neonfs volume update <volume-name> --demotion-threshold 1   # demote after one access-idle window
```

_Note: The exact flag names for demotion thresholds depend on CLI version — check `neonfs volume update --help`._

Existing chunks demote over subsequent tier-management cycles; the effect is not immediate. If you need immediate hot-tier relief, combine eviction with a cross-tier drive evacuation: `neonfs drive evacuate <hot-drive-id> --any-tier` moves chunks to whichever tier has capacity, which is a more abrupt relief valve but correct when the alternative is ENOSPC.

#### 2c. Lower caching footprints

Volumes with `caching` enabled hold additional copies on nearby drives for read speed. Dropping the cache footprint under pressure frees capacity without deleting source data:

```bash
neonfs volume update <volume-name> --caching-enabled=false
```

This is reversible once the pressure clears. The cache rebuilds naturally as reads come back.

### 3. Rebalance

With pressure off one drive / tier, redistribute chunks so the remaining capacity is spread evenly:

```bash
neonfs cluster rebalance --tier <tier> --threshold 0.10
neonfs cluster rebalance-status
```

- `--threshold 0.10` means "move chunks when a drive is more than 10% above the tier average". Tighter threshold chases tighter balance but at the cost of more migration.
- Rebalance respects volume durability — a chunk is never deleted from its source until the required replica count exists elsewhere, matching `drive evacuate` semantics.

### 4. Permanent capacity growth

When the immediate-relief options are exhausted (GC has nothing left to reclaim, every tier is full), the cluster needs physical capacity.

#### 4a. Add a drive to an existing node

```bash
# On the affected node:
sudo mkfs.xfs /dev/<new-device>
sudo mkdir -p /var/lib/neonfs/drives/<id>
sudo mount /dev/<new-device> /var/lib/neonfs/drives/<id>
sudo chown neonfs:neonfs /var/lib/neonfs/drives/<id>

# From any CLI-equipped host:
neonfs drive add \
  --path /var/lib/neonfs/drives/<id> \
  --tier <tier-under-pressure> \
  --capacity <size> \
  --id <id>
```

Persist the mount (fstab or systemd mount unit) so the drive comes back after reboot.

The cluster starts placing new chunks on the fresh drive immediately. To spread existing chunks onto it, trigger a rebalance (step 3).

#### 4b. Add a new node

Appropriate when the existing nodes cannot take more drives, or when the capacity constraint is per-node (CPU, bandwidth) rather than raw disk. Follow the [Operator guide §Node join flow](../operator-guide.md#node-join-flow). Nodes join via invite token and pick up replication / rebalance responsibilities automatically.

### 5. Decisions to avoid under pressure

Capacity incidents are a high-pressure context where "quickly free some space" is tempting. Do not:

- **Delete volumes or files to reclaim space.** The deletion is visible to clients; the data is gone. Use eviction / tier demotion instead.
- **Lower volume durability** (replication factor, EC config). Reduces capacity pressure but trades durability for headroom — a permanent decision made under time pressure.
- **Lower retention policies** or force-delete old snapshots. Retention decisions should be made during scheduled capacity planning, not in response to an alert.
- **`drive remove --force`** as a cleanup shortcut. Force-removing a drive with resident chunks is a data-loss event unless you have confirmed the replicas exist elsewhere.

If any of the above feel tempting, treat the temptation as the trigger to escalate — someone fresh to the incident should sanity-check the plan before it runs.

## Verification

The incident is resolved when:

- `neonfs drive list --output json | jq '[.drives[] | select(.usage_ratio > 0.8)] | length'` returns `0` (or whatever threshold your alert fires at).
- Prometheus capacity alerts have cleared and stayed clear for at least one alert window (typically 5–15 min depending on config).
- Writes succeed on every volume under normal load. A synthetic large-file write to each volume is a good belt-and-braces check.
- `neonfs cluster rebalance-status` reports no in-progress rebalance, or the in-progress one is trending towards completion (not stuck).
- `neonfs job list --type garbage_collection` and `--type tier_migration` show no backlog from the incident window.

## Escalation path

Escalate to engineering if any of the following hold. Capture context before stopping:

- Capacity pressure persists after every option in steps 2 through 4 is exhausted. This typically means a volume has a growth rate that outpaces realistic capacity planning — a product-level issue that needs prioritisation, not just operator action.
- A rebalance or tier-migration job is stuck (no progress for > 15 minutes, no errors in the journal). This suggests a deadlock in the drive scheduler, not a pure capacity issue.
- ENOSPC at the filesystem level causes a core node to crash-loop. Once the BEAM cannot persist its Ra state, this escalates to a [Node-Down](Node-Down.md) / potentially [Quorum-Loss](Quorum-Loss.md) situation on top of the capacity one.
- `gc collect` refuses to run or returns `{:error, :busy}` persistently. The scheduler may be pinned on a stuck job; that is engineering-level.
- You find yourself considering any entry from the "Decisions to avoid" list. Stop and ask.

Capture before escalating:

- Full output of `neonfs drive list` and `neonfs cluster status` (JSON).
- `neonfs job list --type garbage_collection` and `--type tier_migration` and `--type rebalance` outputs.
- Last 200 lines of `journalctl -u neonfs-core` from each core node.
- Per-volume size and recent-growth data if available.
- Timeline: first alert, first operator action, outcome of each step attempted.

After the incident, use the post-mortem template at [Post-Mortem-Template.md](Post-Mortem-Template.md) (filled-in example: [Post-Mortem-Sample.md](Post-Mortem-Sample.md)). Capacity post-mortems are particularly valuable because they tend to surface missing capacity-planning rhythm (no forecasting, no trend dashboard, no quarterly capacity review) more than any one technical defect.

## Known limitations

- There is no per-volume capacity CLI (`neonfs volume status --output json | jq '.bytes_used'`). Per-volume footprint has to be derived from `list_files` output or a dashboard today. A future enhancement.
- `gc collect` does not emit a "bytes reclaimable" estimate before running. You find out how much space will come back only after the job completes. Tracked as a future enhancement.
- No automatic response-playbook: the cluster does not itself page on "projected to hit ENOSPC in N days" — this requires the capacity planner (tracked on [#321](https://harton.dev/project-neon/neonfs/issues/321)) which is still in flight. Today operators rely on alerts keyed to static thresholds.
- Tier-demotion flags on `volume update` vary by release; exact option names may differ from the examples here. Check `volume update --help` on the installed CLI.

## References

- [Operator guide §Drive management](../operator-guide.md#drive-management) (adding/removing/evacuating drives, rebalance).
- [Operator guide §Durability, tiering, compression](../operator-guide.md#durability-tiering-compression--when-to-change-defaults).
- [CLI reference §neonfs gc](../cli-reference.md#neonfs-gc), [§neonfs drive](../cli-reference.md#neonfs-drive), [§neonfs cluster](../cli-reference.md#neonfs-cluster).
- [Drive-Failure runbook](Drive-Failure.md) (when the capacity drop is caused by a drive going away, not by a volume growing).
- Capacity-planning forecaster: [#321](https://harton.dev/project-neon/neonfs/issues/321) (forthcoming).
