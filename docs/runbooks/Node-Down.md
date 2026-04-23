# Runbook: Node down

A single NeonFS node — core or interface — has stopped responding. This runbook walks through detection, short-term recovery, and the longer-term decommission decision.

## Symptom

One or more of the following:

- `neonfs cluster status` reports the node as `unhealthy` or omits it from the members list.
- `neonfs node list` shows the node with a `last_seen` timestamp drifting into the past.
- `journalctl -u neonfs-core` on peers emits `Service node down, deregistering` entries for the missing node.
- Interface nodes return `{:error, :all_nodes_unreachable}` or RPC timeouts when that core was a preferred target.
- Prometheus alerts on `up{job="neonfs-core"}` firing against the affected host.

If every node in the cluster is missing from `neonfs cluster status` and metadata writes hang, you are looking at **[Quorum loss](Quorum-Loss.md)** instead — escalate there. This runbook is for a single-node outage.

## Precondition

- Cluster is otherwise healthy (majority of core nodes reachable; quorum present).
- The affected node was in the cluster long enough for its state — Ra log, `cluster.json`, blob storage — to be laid down. Nodes that never completed their join are not a "node down" case; see the operator guide's [Troubleshooting](../operator-guide.md#troubleshooting) "Orphaned data detected without cluster.json" entry.
- You have a shell session on at least one surviving peer, with the `fj` CLI or Forgejo token on hand for filing follow-up issues.

## Procedure

### 1. Confirm the outage is real

```bash
neonfs cluster status
neonfs node list
```

Record the full output — it is the first data point for the post-mortem. Repeat from a second surviving node to rule out a client-side view problem.

### 2. Distinguish the failure mode

Four classes of outage, each with a different recovery shape:

| Class | How to tell | Typical fix |
|---|---|---|
| **Process crashed, host up** | `ssh <host> systemctl status neonfs-core` reports `failed` or `activating` repeatedly | `systemctl restart` — see step 3a |
| **Host up, BEAM hung** | SSH works, systemd says `active (running)`, but the service does not respond to `epmd -names` or distribution port | `systemctl restart neonfs-core` to break the hang; capture a crash dump first (see step 6) |
| **Host down** | Network ping fails, console / hypervisor reports the host off | Boot the host back up; fall through to step 3a |
| **Network partition isolating the node** | Host is reachable from some peers but not others; `epmd -names` works on the host but not from all peers | Fix the network path (firewall, VLAN, routing); step 3a is then automatic |

If you cannot determine which class applies within a few minutes, move to **Escalation** below.

### 3. Short-term recovery

#### 3a. Process / host recovery

```bash
# On the affected node (after SSH access is restored)
sudo systemctl restart neonfs-core
# Or, for an interface service:
sudo systemctl restart neonfs-fuse   # or neonfs-nfs, neonfs-s3, neonfs-webdav, neonfs-docker
journalctl -u neonfs-core -f
```

The service rejoins automatically using `cluster.json` at `/var/lib/neonfs/cluster.json`. Expect to see formation log lines within ~10 s. If formation errors out with "Orphaned data detected without cluster.json" or similar, you are not in a pure "node down" case — stop here and follow the operator guide's entry for that error.

#### 3b. Container-deployed node

```bash
docker restart neonfs-core
# or
kubectl rollout restart deployment/neonfs-core -n <namespace>
kubectl logs -f -l app=neonfs-core -n <namespace>
```

For Kubernetes, the pod should come back under the same headless service entry so peer discovery continues to work. Check `publishNotReadyAddresses: true` is set on the headless service if it does not.

### 4. Short-term recovery check

```bash
# From any surviving peer
neonfs cluster status
neonfs node list
```

Expect the previously-down node back in the members list with a recent `last_seen`. Data writes should unblock within a few seconds of rejoin.

### 5. Decide whether to decommission

Once the node is back, decide whether it stays. Decommission if **any** apply:

- Hardware is failing (SMART warnings, kernel oops, ECC errors) and replacement is not imminent.
- The outage recurs within hours despite a successful restart.
- The node's role is being retired (scale-down, shrinking the cluster).

Otherwise leave it — re-joined nodes re-replicate missing chunks automatically over the next GC / scrub cycles.

### 6. Decommission procedure

Decommission is **not a single CLI command today**. It is a sequence:

1. **Evacuate each drive the node owns** — migrates chunks onto other replicas before the drive leaves the cluster.

    ```bash
    neonfs drive list --node <node-name>
    neonfs drive evacuate <drive-id>
    # Watch progress — block until every drive reports done:
    neonfs job list --type drive_evacuation
    ```

2. **Remove drives from the cluster registry** once evacuation is complete.

    ```bash
    neonfs drive remove <drive-id>
    ```

3. **Revoke the node's certificate** so it cannot rejoin.

    ```bash
    neonfs cluster ca revoke <node-name>
    neonfs cluster ca list
    ```

4. **Stop and uninstall the NeonFS services on the host.**

    ```bash
    sudo systemctl stop neonfs-core neonfs-fuse neonfs-nfs 2>/dev/null || true
    sudo apt-get purge neonfs-core neonfs-fuse neonfs-nfs   # or the relevant subset
    ```

5. **Remove the node's Ra membership.** There is no first-class CLI for this yet — see [Escalation](#escalation-path) below for the current workaround. Leaving the Ra membership in place is safe if the node will never come back (the cluster sees it as permanently down), but the membership count affects quorum maths: a 3-node cluster with one decommissioned node reported as `member` still needs 2 of the 3 for quorum.

### 7. Capture the data-loss risk call-out

If the down node held chunk replicas that have not yet re-replicated to surviving nodes, writes that land during the outage may be at risk if a second node fails before re-replication completes. Check:

```bash
neonfs cluster status --output json | jq '.under_replicated_chunks // empty'
neonfs job list --type replication
```

If `under_replicated_chunks` is non-zero, hold off on any other risky operation (a second decommission, a version upgrade, a CA rotation) until the replication jobs clear.

## Verification

The incident is resolved when **all** of the following are true:

- `neonfs cluster status` lists every expected node as `healthy` (or intentionally absent, for a completed decommission).
- `neonfs node status` on the recovered node shows every configured service running, drives reporting, and service-registry entries current.
- `neonfs job list --type replication` shows no pending or running replication jobs for chunks the down node held.
- No `Ra register_service failed` or `Service node down, deregistering` entries in the journal for the last five minutes.
- If interface services were affected, a test read and a test write against the affected volumes succeed from a client.

## Escalation path

Stop and escalate to engineering before taking irreversible steps if any of the following hold:

- You cannot determine the failure class after 10 minutes of investigation.
- Short-term recovery (step 3) fails more than twice in a row.
- `neonfs cluster status` shows `under_replicated_chunks > 0` and a second node begins reporting as unhealthy.
- You need to remove the node from Ra membership — there is no first-class CLI for this yet (open follow-up). Ra membership changes are consensus operations; mis-applying one can cause quorum loss.
- The down node's drives were mid-evacuation when it failed — evacuation may be partially applied, and resuming requires engineering knowledge of the drive-evacuation state machine.

Before escalating, capture:

- Full output of `neonfs cluster status` and `neonfs node list` (JSON mode).
- The last 200 lines of `journalctl -u neonfs-core` from the affected node (if reachable) and two surviving peers.
- Timestamps of: first symptom, first response action, outcome of each step attempted.
- The current NeonFS version (`neonfs --version` on any reachable node, or the package install record).

After the incident is fully resolved, follow up with a post-mortem using the template at [Post-Mortem-Template.md](Post-Mortem-Template.md) _(to land)_.

## Known limitations

- There is no CLI command to remove a Ra cluster member yet. The workaround is operator-applied and is what "escalate to engineering" in step 6 refers to.
- `neonfs cluster status --output json` does not yet include an `under_replicated_chunks` summary field; step 7 uses `jq` to pull it if present and otherwise falls back to counting replication jobs. This is tracked as a follow-up.

## References

- [Operator guide — Troubleshooting](../operator-guide.md#troubleshooting) (log locations, common failure modes, health checks).
- [CLI reference](../cli-reference.md) (`cluster`, `node`, `drive`, `job` command surfaces).
- [Service Discovery (wiki)](https://harton.dev/project-neon/neonfs/wiki/Service-Discovery) (why nodes show / don't show in `node list`).
