# Runbook: Quorum loss

The cluster's Ra metadata quorum has broken: no leader is elected, metadata writes hang, and reads that touch recent state return stale data or time out. This is the most dangerous NeonFS incident class — every recovery path has trade-offs, and one of them is explicitly data-loss-capable. Read this runbook end-to-end before acting.

## Symptom

Every one of these is consistent with quorum loss, and in combination they confirm it:

- `neonfs cluster status` reports `leader: null` or `quorum: degraded` across multiple nodes.
- `neonfs node list` lists nodes but every one reports its role as `follower` (no leader).
- `neonfs volume create`, `mkdir`, and any other metadata-mutating CLI blocks and eventually times out.
- Core service logs on every surviving node show repeated `election timeout, starting new election` / `did not receive votes from majority` entries.
- Interface nodes return `{:error, :all_nodes_unreachable}` or `{:error, :timeout}` on writes.
- Reads of recently-written files succeed only if the data-plane can serve them from cached chunks — metadata lookups for new files fail.

A slow leader is **not** quorum loss. If `neonfs cluster status` reports a leader (even if that leader is under load), follow the operator guide's [Troubleshooting](../operator-guide.md#troubleshooting) entries instead. Quorum loss means there is no leader and no election can succeed.

## Precondition

- You have read the [data-loss warning](#data-loss-capable-operations) below **before** invoking any step in the Forced-reset path.
- You have `neonfs` CLI and shell access to every node you believe is still up, and out-of-band (IPMI/console) access to any node that is down or unreachable over the network.
- Backups or DR snapshots exist off-cluster. In the worst case the recovery path loses acknowledged writes; that is acceptable only if you have a fallback.
- You are authorised to declare data loss if the Forced-reset path turns out to be necessary. Quorum-loss recovery is a consequential decision; escalate to the change-approver before committing if you are not.

## Procedure

### 1. Confirm it is quorum loss, not a slow cluster

```bash
# From as many surviving nodes as you can reach:
for host in <n1> <n2> <n3>; do
  ssh "$host" neonfs cluster status --output json | jq '{leader, quorum, members}'
done
```

Every node reporting `leader: null` (or disagreeing on who the leader is and no one claiming the role) confirms quorum loss. A single node reporting `leader: null` while others report a valid leader is a partition-from-that-node problem — handle it via [Node-Down](Node-Down.md) or fix the network path first.

If every reachable node's service is down or times out entirely, move to [Node-Down](Node-Down.md) for each one first, then come back here.

### 2. Count reachable vs unreachable nodes

```bash
neonfs node list --output json | jq '[.nodes[] | {name, reachable, role}]'
```

You need to distinguish:

- **Majority reachable** — e.g. 2 of 3 nodes or 3 of 5 nodes are up. Ra should be able to elect a leader from this set. If it is not, that is a different bug — move to [Escalation](#escalation-path).
- **Minority reachable** — e.g. 1 of 3 nodes, or 2 of 5 nodes. No leader can be elected without the unreachable majority returning. Every recovery path from here is either "wait" or "risk data loss".

If you cannot tell (some nodes reachable but hung), treat them as unreachable for this count — a hung node contributes nothing to quorum.

### 3. Partition vs failure

Before picking a recovery path, determine whether the unreachable nodes are:

- **Partitioned** — the nodes are running and healthy but cannot reach their peers. Usually a network / firewall / DNS change. The nodes still hold their Ra state; once the partition heals they rejoin and the cluster forms normal quorum.
- **Failed** — the nodes are down (power, hardware, corrupted state). Their Ra state may or may not be recoverable.

Quick check for partitions from a reachable node:

```bash
# Can we reach the other peers at the distribution port?
for host in <unreachable1> <unreachable2>; do
  nc -zv -w 3 "$host" 4369   # EPMD
  # Distribution port — see NEONFS_DIST_PORT on the target
done
```

If EPMD responds but distribution is blocked, you are partitioned. If EPMD does not respond at all, the node is likely down (move to [Node-Down](Node-Down.md) for that node first).

### 4. Pick a recovery path

| Situation | Recovery path |
|---|---|
| **Majority reachable, but still no leader** | Diagnose why Ra cannot elect — engineering-level (see Escalation). Do **not** force anything. |
| **Minority reachable, unreachable majority is partitioned** | **Wait-it-out** (step 5). Fix the network, let the majority rejoin. |
| **Minority reachable, unreachable majority is failed and recoverable** | Try to bring them back (Node-Down per node), then fall into the majority-reachable case. |
| **Minority reachable, unreachable majority is failed and unrecoverable** | **Forced reset** (step 6) — this is data-loss-capable. See the warning below. |

### 5. Wait-it-out (majority partitioned)

This is the low-risk path. The cluster will resume when the partition heals.

```bash
# Reapply network / firewall / DNS fix. Verify with a distribution
# handshake test from a minority node:
ssh <minority-host> \
  "erl -name ping@<minority-host> -setcookie $(cat /var/lib/neonfs/.erlang.cookie) \
   -noshell -eval 'net_kernel:connect_node(<peer>@<peer-host>), halt().'"
```

Expect the cluster to form quorum within ~10 s of the partition healing. Watch `neonfs cluster status` on a reachable node until a leader is elected. Metadata writes unblock immediately once a leader is present.

During the wait, **do not** run any recovery command on the minority side. Any attempt to forcibly form quorum on the minority will cause a split-brain when the majority comes back.

### 6. Forced reset (minority-only, majority unrecoverable)

> **Data-loss warning.** The forced-reset path rebuilds the cluster's Ra quorum from the minority subset. Every metadata write acknowledged by the departed majority but not yet replicated to the surviving minority is lost. Clients whose writes were accepted-and-then-vanished will be unaware. This path is appropriate only when the alternative is **permanent** cluster unavailability and the majority is confirmed dead (not partitioned, not temporarily down).
>
> **Do not run the forced reset while the departed majority might still come back.** Split-brain is the worst outcome — two cluster halves, each thinking it is authoritative, diverging state with no automated reconciliation.

The forced-reset procedure is now a single CLI command (`neonfs cluster force-reset`, available since #473). The implementation:

1. Snapshot-extracts the survivor's local Ra state via `:ra.local_query/3` (no quorum required — that's what makes it usable post-quorum-loss).
2. Persists the snapshot to `$NEONFS_DATA_DIR/ra/force-reset-snapshots/<node>-<ts>.bin` so an operator can inspect it post-hoc.
3. Calls `:ra.force_delete_server/2` to destroy the survivor's Ra log and state.
4. Bootstraps a fresh single-node Ra cluster with the extracted state injected via `MetadataStateMachine.init/1`'s `:initial_state` option.

The new cluster has the **old metadata preserved** (volumes, ACLs, encryption keys, S3 credentials, service registry, segment assignments, KV) but a **fresh log + fresh membership** — every membership trace of the dropped majority is gone. State that's inherently in-flight (intent log, escalations, namespace claim locks) is dropped; operators retry those.

Procedure:

```bash
# 1. Stop NeonFS on every survivor that is NOT the chosen reset target.
#    Only one survivor should run the force-reset; the others rejoin
#    fresh after step 4.
for host in <survivor2> <survivor3>; do ssh "$host" sudo systemctl stop neonfs-core; done

# 2. Run force-reset on the chosen survivor. The CLI's safety gates
#    refuse anything that doesn't look like a real quorum-loss
#    scenario — see `neonfs cluster force-reset --help` for the
#    full list. `--yes-i-accept-data-loss` is required.
ssh <survivor1> \
  neonfs cluster force-reset \
    --keep <survivor1> \
    --min-unreachable-seconds 1800 \
    --yes-i-accept-data-loss

# 3. The CLI returns the snapshot path on success. Copy it
#    somewhere off-cluster as a paranoia backup before continuing.
ssh <survivor1> sudo cat /var/lib/neonfs/ra/force-reset-snapshots/<...>.bin \
  | scp - <off-cluster-host>:force-reset-snapshot.bin

# 4. Verify the survivor is now a single-node cluster.
ssh <survivor1> neonfs cluster status

# 5. Rejoin the other survivors (NOT the dead majority) as fresh
#    members. They have stale Ra state from the old cluster — wipe
#    it before rejoining.
for host in <survivor2> <survivor3>; do
  ssh "$host" sudo rm -rf /var/lib/neonfs/ra/<...>/
  ssh "$host" sudo systemctl start neonfs-core
  ssh "$host" neonfs cluster join <survivor1>
done

# 6. Trigger eager re-replication of chunks that lost replicas to
#    the departed majority (#687).
ssh <survivor1> neonfs cluster repair start

# 7. Verify. See step 7.
```

The first survivor to start becomes the effective leader. Its local Ra state at the time of `force-reset` is the canonical truth for the rebuilt cluster; anything any departed node had committed but not replicated to this one is lost.

### 7. Post-recovery verification

```bash
neonfs cluster status
neonfs volume list
# Pick a recent file from before the incident and verify it reads.
neonfs fs cat <volume> <path> | head -c 64 >/dev/null
```

The cluster is recovered when:

- `neonfs cluster status` reports a `leader` and `quorum: healthy`.
- `neonfs node list` shows every surviving node with a recent `last_seen`.
- A write succeeds (create a tiny test file, then delete it).
- A read of a pre-incident file returns bytes.

For forced-reset recoveries, additionally:

- `neonfs job list --type replication` shows replication reconciling newly under-replicated chunks as the cluster re-establishes durability.
- Any alerts on `neonfs_leader_changes_total` or `neonfs_ra_elections_total` settle back to a stable rate.

## Data-loss-capable operations

Every command in this list can cause or accelerate data loss when run during quorum loss. None of them are safe to run without explicit engineering approval:

- `neonfs drive remove --force` against a drive that was only on a departed majority node.
- `neonfs cluster ca revoke` against a node that might still be reachable over a healing partition.
- Directly editing `cluster.json` on any node.
- Invoking `:ra.force_delete_server/2` or other Ra force-* APIs via remote console.
- Starting NeonFS on a node whose drives are from a different cluster generation (orphaned data).

Treat the runbook order as a gate: do not run any of the above until step 6 explicitly calls for it.

## Verification

The incident is fully closed when:

- Step 7 assertions pass continuously for 30 minutes (no re-entry into election storms, no new leader changes).
- All pre-incident volumes are readable.
- Clients have been informed of any lost writes (if the forced-reset path was used).
- A post-mortem has been started — quorum loss always warrants one, regardless of the recovery path that resolved it.

## Escalation path

Escalate to engineering immediately if any of the following hold. Capture the required context **before** stopping:

- You cannot determine majority-vs-minority reachability confidently within 5 minutes.
- Ra cannot elect a leader despite a majority of nodes being reachable. This is not a standard quorum-loss failure mode and should not be recovered without understanding why.
- Step 5 (wait-it-out) is not resolving within 15 minutes after the partition is confirmed healed.
- The only path forward is step 6 (forced reset) and you have not done one on this cluster before.
- Any surviving node shows signs of corrupted Ra state — `ra_server_proc terminated abnormally`, `log snapshot mismatch`, repeated CRCs on `$NEONFS_DATA_DIR/ra/`.
- `cluster.json` on surviving nodes disagrees on membership composition — this means reconciliation is already ambiguous and a manual choice will be needed.

Capture before escalating:

- `neonfs cluster status --output json` from every reachable node.
- `neonfs node list --output json` from every reachable node.
- The last 500 lines of `journalctl -u neonfs-core` from every reachable node.
- `ls -la $NEONFS_DATA_DIR/ra/` and `$NEONFS_DATA_DIR/meta/cluster.json` from every reachable node.
- Network topology: which nodes reach which peers, firewall changes in the last 24 hours, DNS changes in the last 24 hours.
- The approximate time of the last successful metadata write (check any application log that would have caught it).

After the incident, use the post-mortem template at [Post-Mortem-Template.md](Post-Mortem-Template.md) (filled-in example: [Post-Mortem-Sample.md](Post-Mortem-Sample.md)). A quorum-loss post-mortem always captures: how quorum was lost, which path was taken, what data (if any) was lost, what alerting change would have surfaced it sooner, and what operator-facing-tooling gap would make the next one resolvable without engineering escalation.

## Known limitations

- There is no operator-facing CLI for the forced-reset path. Every forced reset today requires engineering involvement. Tracked as a follow-up — operators should not be running `:ra.force_delete_server/2` from a runbook without a proper CLI wrapper.
- `neonfs cluster status` does not yet surface the "which nodes voted for which candidate in the last election" view that would help diagnose why a majority-reachable cluster fails to elect. A future enhancement.
- There is no pre-flight check that warns "you are about to drop below quorum" when stopping a node for maintenance. Operators must count by hand against the current membership.

## References

- [Operator guide — Troubleshooting](../operator-guide.md#troubleshooting) ("Quorum loss" entry points here).
- [CLI reference](../cli-reference.md) (`cluster status`, `node list`, `volume *`).
- [Node-Down runbook](Node-Down.md) (when the root cause is one specific node being unreachable).
- Ra documentation: [rabbitmq/ra](https://github.com/rabbitmq/ra) (for engineering-level Ra recovery reference, not operator usage).
