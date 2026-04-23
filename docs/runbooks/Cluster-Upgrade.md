# Runbook: Cluster upgrade (rolling + rollback)

A planned, scheduled upgrade of NeonFS — core nodes first, then interface nodes, one at a time, with rollback as the safety valve if the new version misbehaves. This runbook restates the [operator guide's §Upgrade procedures](../operator-guide.md#upgrade-procedures) mechanics as a precondition / procedure / verification / rollback flow suited to change-window execution.

## Symptom (trigger)

Unlike most runbooks in this directory, cluster upgrade is **not** triggered by an incident signal. Triggers are:

- Scheduled maintenance window.
- Security advisory requiring a pinned version bump.
- Release of a feature your cluster depends on.
- Bug fix in the [`CHANGELOG.md`](../../CHANGELOG.md) that resolves a running issue.

Treat this as a ruled procedure — you run it deliberately, with a rollback plan in hand, not reactively.

## Precondition

Before you start:

- **The cluster is healthy.** `neonfs cluster status` shows all expected nodes, Ra quorum intact, no in-progress migrations, no outstanding replication or scrub jobs. If any of these fail, resolve them first — upgrading a cluster that is mid-replication or mid-evacuation can strand the job partway through when the node in question rolls.
- **You have read the release notes.** Check [`CHANGELOG.md`](../../CHANGELOG.md) for the target version: breaking changes (`!` in the conventional-commit subject), state-machine version bumps, configuration-file changes, cookie or cert rotations required.
- **Backups are fresh.** Off-cluster backup of the CA material and a recent snapshot of `/var/lib/neonfs/meta/` on every core node. Rollback should be independent of this; the backup is the last-resort fallback if rollback itself fails.
- **You have a rollback path chosen.** Either the old Debian package version is still in the apt cache / available from the mirror, or you have the old container image tagged locally.
- **Maintenance window is scheduled.** Rolling upgrades are near-zero-downtime for clients, but mid-upgrade incidents can require a longer window; do not run upgrades against a live user base without one.
- **You have a peer-cluster smoke test plan.** Writes + reads against each interface after the upgrade, exercising at least one chunk larger than the volume's chunk size so the data plane is actually hit.

## Procedure

### 1. Pre-flight check

```bash
neonfs cluster status
neonfs node list
neonfs job list                     # no running jobs, or only recurring ones
neonfs cluster rebalance-status     # no in-progress rebalance
```

Every node expected, every one healthy, no job in a blocking state. If you see running replication, GC, tier-migration, or rebalance jobs, wait for them to finish before proceeding.

Record the pre-upgrade version:

```bash
neonfs --version                    # on a node or a CLI-only host
for host in <all-nodes>; do ssh "$host" neonfs --version; done
```

Put these in the change record — you'll compare against them for verification.

### 2. Upgrade core nodes one at a time

**Never upgrade more than one core node at a time.** The operator guide is emphatic on this because losing quorum stops metadata writes cluster-wide — a scenario the [Quorum-Loss runbook](Quorum-Loss.md) exists to recover from, but you do not want to cause deliberately.

For each core node, in order:

#### 2a. Drain interactive work on the node (optional)

Not required for core nodes unless the node hosts long-running jobs (scrub, large evacuation). If it does, let those finish or cancel them before proceeding.

#### 2b. Install the new version

Debian:

```bash
sudo apt update
sudo apt install "neonfs-core=<new-version>"
# apt install --reinstall if the version string resolves to the already-installed version
```

Container:

```bash
docker pull harton.dev/project-neon/neonfs/core:<new-version>
docker stop neonfs-core && docker rm neonfs-core
docker run -d --name neonfs-core <existing-flags> harton.dev/project-neon/neonfs/core:<new-version>
```

Kubernetes:

```bash
kubectl set image statefulset/neonfs-core \
  -n <namespace> core=harton.dev/project-neon/neonfs/core:<new-version>
kubectl rollout status statefulset/neonfs-core -n <namespace> --timeout=5m
```

(Note: with a StatefulSet, `rollout` handles one-at-a-time progression automatically if the update strategy is `RollingUpdate`. Set `partition` if you want explicit per-pod control.)

#### 2c. Restart and verify

```bash
sudo systemctl restart neonfs-core
# or, for containers, the restart is part of 2b

# Wait for the node to rejoin.
neonfs cluster status
neonfs node status <this-node>
```

The upgraded node reports the new version in `node status`. Ra quorum remains `healthy`. No `Ra register_service failed` or `ra_server_proc terminated abnormally` entries in `journalctl -u neonfs-core --since "5 minutes ago"`.

#### 2d. Let the cluster settle

Wait 1–2 minutes before moving to the next core node. During this window, watch for:

- `neonfs cluster status` drift indicators (any node unexpectedly showing as unhealthy).
- Journal spikes on the upgraded node (`Ra machine version bump applied` is expected; repeated `machine version mismatch` is not).
- Interface nodes losing and regaining connection to the upgraded core — expected briefly; should stabilise within 10s.

If any of these persist beyond the settle window, **stop**. The rest of the cluster is still on the old version. Consider [rollback](#rollback) before proceeding.

#### 2e. Repeat for each remaining core node

One at a time. The last core upgraded is the one you should verify most carefully — if any state-machine migration deferred until it joined, this is where you'd see it.

### 3. Upgrade interface nodes

Interface nodes are stateless. Replace the service, pool-level traffic shift handles client reconnection.

```bash
# Debian per node:
sudo apt install "neonfs-fuse=<new-version>"    # or neonfs-nfs / neonfs-s3 / neonfs-webdav / neonfs-docker
sudo systemctl restart neonfs-fuse

# Container per node:
docker pull harton.dev/project-neon/neonfs/fuse:<new-version>
docker stop neonfs-fuse && docker rm neonfs-fuse
docker run <flags> harton.dev/project-neon/neonfs/fuse:<new-version>
```

Interface upgrades can run in parallel across different services (FUSE and NFS at the same time is fine) and can even run in parallel across nodes of the same service as long as the load balancer / service mesh shifts traffic appropriately.

For zero-downtime interface upgrades, deploy the new version alongside the old (different host or different port), drain traffic at the load-balancer level, then decommission the old instance.

### 4. Major version upgrade (cross-major)

Cross-major upgrades may add steps this runbook does not cover — metadata migrations, cookie rotations, configuration-file shape changes. **Read the release notes.** If the release notes prescribe a specific sequence (e.g. "rotate CA before upgrade", "run migration X before restarting the first node"), follow that sequence; the generic "one node at a time" rule still applies, but the pre-flight gets longer.

Always practise a cross-major upgrade on a **non-production cluster** before touching production. If that is not possible in your environment, escalate the change plan for extra review before running.

### 5. Mid-upgrade incident handling

If something goes wrong mid-upgrade, treat the symptoms case by case:

| Observation | First response |
|---|---|
| One upgraded core node fails to rejoin | Stop upgrading further nodes. Investigate per [Node-Down](Node-Down.md). Decide between rollback of the failed node or continuing once it recovers. |
| `Ra machine version mismatch` persists | A state-machine migration did not complete cleanly. **Do not upgrade more nodes.** Escalate — this is engineering-level. |
| Metadata writes start hanging after an upgraded node rejoins | Quorum-level problem — see [Quorum-Loss](Quorum-Loss.md). Rollback the upgraded node first to try to restore quorum. |
| Interface node fails to reconnect to core post-upgrade | Typically a TLS / distribution issue; check [CA-Rotation](CA-Rotation.md) for cert expiry, check distribution port reachability. |
| Unexpected error logs from existing file reads/writes | Stop and investigate before upgrading more nodes. Upgrade-induced regressions in the data path need to be caught early. |

Rule of thumb: if two or more symptoms land at once, **stop the upgrade** and work the incident end-to-end before continuing.

### 6. Rollback

Rollback is an incident response within an upgrade — the upgraded new version is behaving badly, and you want the old version back.

#### Decision: is rollback safe?

| Situation | Is rollback safe? |
|---|---|
| No state-machine version bump in this release | Yes — old version reads its old state unchanged. |
| State-machine version bumped, migration is reversible | Yes — per the release notes; follow the reverse-migration sequence. |
| State-machine version bumped, migration is not reversible | **No.** Forward through the issue with engineering; do not roll back. |
| Some but not all nodes upgraded | Roll back just the upgraded ones. The mixed cluster is on a version Ra machine version it supports, assuming the target version is not a forward-incompatible bump. |

Read the target version's release notes for the "rollback" note. If there is no explicit note, treat it as reversible but verify with engineering before acting on a production cluster.

#### Rollback procedure (per node)

```bash
# Debian:
sudo systemctl stop neonfs-core
sudo apt install "neonfs-core=<old-version>"
# Pin if needed to prevent automatic upgrade re-application:
echo "neonfs-core hold" | sudo dpkg --set-selections

sudo systemctl start neonfs-core
neonfs cluster status       # rolled-back node rejoins with old version

# Container:
docker stop neonfs-core && docker rm neonfs-core
docker run <flags> harton.dev/project-neon/neonfs/core:<old-version>
```

Roll back in **reverse order of upgrade**: the most-recently-upgraded node first, so the cluster state transitions through the same intermediate states in reverse.

#### Post-rollback check

Same as [post-upgrade verification](#verification), but confirming the **old** version everywhere. Also:

- File an issue on [project-neon/neonfs](https://harton.dev/project-neon/neonfs/issues) capturing what misbehaved. Rollback-triggered issues are high-priority for the next release.
- Keep the rollback state pinned (Debian) or the rolled-back container images tagged (container) until engineering confirms a fix is in.

## Verification

The upgrade is resolved (or the rollback is resolved) when:

- `neonfs --version` on every node reports the target version.
- `neonfs cluster status` reports a healthy quorum.
- `neonfs node status <each-node>` shows every expected service running, drives reporting, health checks passing.
- A client smoke test per interface — at least one read, one write, one list — succeeds against volumes that exercise the data plane (i.e. not just metadata-only reads).
- `neonfs job list` shows no failed or stuck jobs from the upgrade window.
- No repeated error entries from the last 10 minutes in any node's `journalctl -u neonfs-core` (or the interface equivalent).

## Escalation path

Escalate to engineering if any of the following hold. Capture context before stopping:

- A state-machine migration fails or reports `machine version mismatch` persistently after the restart settle window.
- `neonfs cluster status` reports `quorum: degraded` or `leader: null` at any point during the upgrade and does not recover within 60s.
- Rollback steps (step 6) leave the cluster in a state the runbook or release notes do not describe — e.g. a node insists on its post-upgrade version despite the package being reinstalled.
- Two or more of the mid-upgrade symptoms (step 5 table) fire together.
- Data-plane reads or writes fail against volumes that worked pre-upgrade, even after a full-cluster roll through.

Capture before escalating:

- `neonfs --version` output from every node (pre- and post-upgrade / rollback).
- `neonfs cluster status --output json` (JSON).
- `journalctl -u neonfs-core --since "<upgrade-start-time>"` from every core node.
- Full release-notes text for the target version.
- Timeline: upgrade start, per-node upgrade completion, first symptom, first operator response.

After the incident, use the post-mortem template at [Post-Mortem-Template.md](Post-Mortem-Template.md) (filled-in example: [Post-Mortem-Sample.md](Post-Mortem-Sample.md)). Upgrade post-mortems are where "what made us land on this version" gets recorded — including who signed off and what validation was (or wasn't) done beforehand. That feedback shapes the next change-approval rhythm.

## Known limitations

- `neonfs --version` reports the local binary version but does not itself capture the cluster-wide version matrix — you have to query each node. A future enhancement could emit a cluster-wide version summary via `neonfs cluster status`.
- There is no CLI-level dry-run for an upgrade (no "check whether this version's state-machine migrations are forward-incompatible against our current state"). Release notes are the only source of that information today.
- Rollback is per-node-then-cluster-wide; there is no `neonfs cluster rollback` atomic operation. This is by design — rollback is an incident response and the per-node steps give the operator visibility — but the lack of cluster-wide affordance means the decision is made by the operator rather than guarded by tooling.

## References

- [Operator guide §Upgrade procedures](../operator-guide.md#upgrade-procedures) — the CLI mechanics this runbook wraps.
- [CHANGELOG.md](../../CHANGELOG.md) — release notes with breaking changes and state-machine version bumps.
- [CLI reference §neonfs cluster](../cli-reference.md#neonfs-cluster), [§neonfs node](../cli-reference.md#neonfs-node), [§neonfs job](../cli-reference.md#neonfs-job).
- [Node-Down runbook](Node-Down.md), [Quorum-Loss runbook](Quorum-Loss.md), [CA-Rotation runbook](CA-Rotation.md) — the runbooks you'll need on hand if mid-upgrade problems turn into their own incidents.
