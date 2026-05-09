# Runbook: Certificate expiry and CA rotation

NeonFS's cluster CA signs every core and interface node certificate, and the certificates themselves secure Erlang distribution plus the TLS data plane. When the CA or a node certificate expires, the cluster loses both: nodes cannot talk to each other, and clients cannot reach interface nodes.

This runbook covers two scenarios:

- **Planned rotation** — the CA or a node cert is approaching expiry and you are rotating ahead of time. Low risk; the [Operator guide §Certificate authority rotation](../operator-guide.md#certificate-authority-rotation) documents the mechanics. This runbook frames them with pre-flight checks and verification for incident-response use.
- **Emergency rotation** — a cert has already expired; distribution is broken; nodes cannot talk. Higher risk; the cluster needs a jump-start before the normal CLI path will work.

## Symptom

### Planned rotation (no outage yet)

- `neonfs cluster ca info` reports `not_valid_after` less than 30 days from now.
- SMART alerting on TLS certificate expiry (if configured via Prometheus) fires below its warn threshold.
- Calendar or change-management reminder fires ("CA rotates annually, next: 2026-05-12").

### Emergency (expiry has passed)

- Every node's journal fills with TLS handshake failures: `cert_expired`, `bad certificate`, `unknown ca`.
- `neonfs cluster status` returns `{:error, :all_nodes_unreachable}` from an interface node.
- `neonfs cluster ca info` on a still-running node reports `valid: false` and `reason: expired`.
- Data-plane connections fail: `Router.data_call(…)` returns `{:error, :no_data_endpoint}` because the TLS pool cannot establish.
- Interface `/health` endpoints return 503 `tls_handshake_failed`.

## Precondition

- You have a **cluster CA backup off-cluster** at a known location. If you do not, stop: the CA is the root of trust. Losing it without a backup means every node needs a fresh identity, which is effectively a cluster rebuild. Capture what backups exist before you touch anything, and escalate.
- For a planned rotation, the cluster is otherwise healthy (all nodes reporting, Ra quorum intact).
- For emergency rotation, you have shell access to every core node. Interface nodes can be rotated later by rejoin after the core is healthy again.

## Procedure

### 1. Confirm expiry state and scope

```bash
neonfs cluster ca info
neonfs cluster ca list
```

- `ca info` shows the CA's `not_valid_before` / `not_valid_after`, its serial counter, and its subject. Note these — they go in the post-mortem.
- `ca list` shows every issued node certificate and its validity window.

If `ca info` itself cannot run because the CA service on the node has failed to start (expired CA blocks its own validation in some config paths), fall through to [Emergency rotation](#emergency-rotation-expiry-has-passed) below.

### 2. Decide: planned or emergency

| Observation | Path |
|---|---|
| `ca info` reports valid, more than 0 days left | [Planned rotation](#planned-rotation) |
| `ca info` reports valid, < 7 days left AND nodes reporting handshake warnings | [Planned rotation](#planned-rotation) — urgent but not yet down |
| `ca info` reports `valid: false` / `expired` OR nodes cannot talk via distribution OR `/health` returns handshake errors | [Emergency rotation](#emergency-rotation-expiry-has-passed) |

Once you are in the emergency path, do not attempt the planned-rotation CLI first — it requires working TLS to push the new material to every node. Emergency rotation re-establishes that trust first, then runs the normal rotation.

### Planned rotation

#### 3a. Verify off-cluster CA backup

Rotation re-signs every node cert and, for a full CA rotation, generates a new CA key. You want a snapshot of the current CA and certs **before** rotation starts, so a failed rotation can be rolled back.

```bash
# On a core node:
sudo tar czf /var/lib/neonfs/tls-backup-$(date -u +%Y%m%d-%H%M%S).tar.gz \
  -C /var/lib/neonfs/tls .
# Move the tarball off-cluster.
```

Keep the backup out of the cluster's normal data path — losing a cluster and its CA at the same time defeats the point.

#### 3b. Order core first, then interfaces

For a node-certificate rotation (issued by the existing CA), the CA itself does not change; only each node's cert is reissued. Rotate core nodes before interface nodes:

1. Each core node: the system renews its own cert during `ca rotate`; distribution reconnects automatically once each node presents its new cert.
2. Interface nodes: renewal requires the interface to reconnect to a core node that has already rotated, so it picks up the current CA chain.

For a full CA rotation (a new CA is generated and every cert reissued), the same ordering applies — core first so the interfaces always have a CA-authority they can reach.

```bash
# On any core node with CLI access:
neonfs cluster ca rotate
```

This drives the rolling reissue in four phases:

1. **Stage incoming CA.** Generates a new CA key + cert and writes them to the system volume under `_system/tls/incoming/`. The old CA is still active.
2. **Walk + reissue every node's cert.** For each node connected to the BEAM cluster, the orchestrator generates a fresh keypair, signs a node CSR against the *incoming* CA, and ships the new `node.crt` + `node.key` to that node via RPC. Atomic on-disk swap (temp-file + rename) means a crash mid-write leaves the existing cert intact.
3. **Distribute the dual-CA bundle.** Every node's `ca_bundle.crt` is regenerated to include both the old and the new CA, then the local `ssl_dist` listener is asked to refresh its trust store. During this window each node accepts inbound connections from peers that present *either* the old-CA-signed cert or the new-CA-signed cert.
4. **Finalize.** Promotes the staged incoming CA to active in the system volume, deletes the incoming tree. After this step the old CA is no longer trusted; only the new CA bundle is.

By default, the orchestrator stops between phase 3 and phase 4 with the rotation in `pending-finalize` state and prints the grace-window duration. This gives the dual-CA bundle time to settle on every node before the old CA is dropped — important if a node is briefly unreachable during phase 3 and would otherwise come back to find its peers no longer trust its old cert.

After the grace window passes, finalize the rotation:

```bash
# On any core node with CLI access:
neonfs cluster ca rotate --finalize
```

If you've explicitly verified every node picked up the dual-CA bundle in phase 3 and want to skip the grace window (for example: a planned rotation in a maintenance window, every node is reachable, every cert has been visibly reissued), pass `--no-wait` to the initial command and the finalize step runs as part of phase 4 in the same invocation:

```bash
neonfs cluster ca rotate --no-wait
```

`--no-wait` is faster but riskier: any node that didn't successfully install the dual-CA bundle in phase 3 will be unable to handshake with its peers immediately. Reach for it only when you have direct visibility into every node.

You can tune the documented grace window with `--grace-window-seconds N` (default `86400`, i.e. 24 hours). The flag affects the duration **printed** in the orchestrator's output; the operator is still the one who decides when to run `--finalize` based on the grace window having elapsed.

Audit-log events are written on the orchestrator node for each phase: `cluster_ca_rotate_started`, `cluster_ca_rotate_node_completed` (one per node), `cluster_ca_rotate_finalized`, and `cluster_ca_rotate_failed` if any phase aborts. Inspect them via `neonfs cluster ca status` or by querying the audit log directly during a post-mortem.

#### 3c. Verify post-rotation

After `--finalize` returns successfully:

```bash
neonfs cluster ca info                       # new not_valid_after ≥ rotation window
neonfs cluster ca list                       # every node's cert shows the new validity
neonfs cluster ca rotate --status            # rotation_in_progress: false, no incoming CA
neonfs cluster status                        # leader still elected, quorum healthy
neonfs node status <each-node>               # service reports running, health checks pass
```

From an interface node, perform a smoke-test read and a smoke-test write. If distribution handshakes are silently falling back to old certs you will see handshake retries in the journal; a real write flushing to disk confirms the new chain is end-to-end.

**Estimated downtime: none for the rolling reissue.** Phase 2 and 3 do not interrupt existing inter-node connections — `:ssl_dist` keeps using its already-handshaken sockets under the old trust store and only the *next* listener restart picks up the new bundle. Phase 4 (`--finalize`) atomically swaps the active CA in the system volume; existing connections continue, and a brief reconnect blip is possible if a peer re-handshakes within seconds of finalize.

#### 4. If a node fails to rotate

The orchestrator stops on the first failed node and writes a `cluster_ca_rotate_failed` audit event. The error message identifies the failed node and the failing RPC (typically `install_node_cert`, `regenerate_ca_bundle`, or `reload_listener`). The staged incoming CA stays in place — partial progress is **not** rolled back.

Diagnose:

```bash
# On the failed node:
sudo journalctl -u neonfs-core --since "10 minutes ago" | grep -iE "tls|cert|handshake"
# On a working core node:
neonfs cluster ca rotate --status     # confirms incoming CA is still staged
```

Two recovery paths depending on root cause:

**Per-node retry** — when the failure was transient (the node was briefly unreachable, hit a disk-full error that's now resolved, etc.) and the rest of the cluster reissued cleanly:

```bash
neonfs cluster ca rotate --node <node-name>
```

`--node` retries phase 2 + phase 3 for the named node only, against the **same** staged incoming CA. The orchestrator preserves the partial state from the original run, so other nodes' new certs are not regenerated. After the per-node retry succeeds, run `--finalize` (or re-run with `--no-wait`) to complete phase 4 across the cluster. The retry path expects the staged incoming CA to still exist; if you previously ran `--abort`, the retry will refuse and you must restart the rotation from scratch.

**Abort and restart** — when the failure indicates a bigger problem (the staged CA is corrupt, the original rotation never reached phase 3 cleanly, you want to start over):

```bash
neonfs cluster ca rotate --abort      # discards the staged incoming CA
neonfs cluster ca rotate              # starts again from phase 1
```

`--abort` removes the staged incoming CA and every cert it issued; subsequent rotations start fresh.

#### 4a. Offline-node handling

If a core or interface node is offline (Erlang distribution down) at the time of rotation, the orchestrator's BEAM-cluster walk skips it — the failure surfaces as either a missing `node_completed` audit event for that node or, if the node was *intermittently* reachable, an `install_node_cert_failed` mid-walk. There is no path that will reissue an offline node's cert.

The operator guide's design call (#501) for offline nodes is explicit: they must be **rejoined from scratch** after the rest of the cluster has rotated. The procedure:

1. Complete the rotation across the reachable nodes (`--finalize`).
2. Bring the offline node back to a clean state — remove its `$NEONFS_TLS_DIR/{node.crt,node.key,ca.crt,ca_bundle.crt,ssl_dist.conf}` so it cannot present the old cert. The node's data dir on the blob/meta side is preserved and re-bound on rejoin.
3. Issue a fresh invite token from any active core node (`neonfs cluster invite create --duration 1h`).
4. Re-run `neonfs cluster join --invite <token>` on the offline node. It enrols against the **new** CA, gets a fresh node cert, and returns to the cluster.

The "rejoin from scratch" step is the trade-off for not having any in-flight per-node cert distribution; the rotation orchestrator deliberately does not buffer "queued" nodes that are unreachable at rotation time.

### Emergency rotation (expiry has passed)

When handshakes are failing because the current CA or node certs are expired, the normal rotation CLI cannot establish the TLS channel it needs to push the new material. You must first re-establish trust on one node, then fan out.

> **Data-loss risk: low. Operational risk: high.** Emergency rotation temporarily disables TLS distribution verification on a single bootstrap node to re-issue certs, then re-enables. During that window the node is accepting any peer that presents itself — keep the cluster isolated from the network perimeter (firewall rules, air-gap) while the procedure runs.

#### 5. Isolate the cluster

```bash
# On each core host, block inbound distribution and data-plane ports from
# everything except the cluster's own subnet.
# Example (replace with your actual firewall tooling):
sudo iptables -A INPUT ! -s <cluster-subnet> -p tcp --dport <dist_port> -j DROP
sudo iptables -A INPUT ! -s <cluster-subnet> -p tcp --dport <data_plane_port> -j DROP
```

Keep this in place until step 10.

#### 6. Stop all interface nodes

```bash
# On every interface host:
sudo systemctl stop neonfs-fuse neonfs-nfs neonfs-s3 neonfs-webdav neonfs-docker 2>/dev/null || true
```

Interface nodes will not be able to reconnect until the core has fresh certs. Stopping them prevents handshake-retry storms from masking the real issue.

#### 7. Re-bootstrap the CA on one core node

`neonfs cluster ca emergency-bootstrap` runs entirely on the bootstrap node and never touches the system volume — every CA command that needs a working TLS chain is unreachable when the chain is the thing that's broken. The CLI installs CA material directly into `$NEONFS_TLS_DIR/`, regenerates the local node's own cert against the freshly-installed CA so distribution can come up, and writes a JSONL audit-log entry under `$NEONFS_DATA_DIR/audit/emergency-bootstrap.log`.

Pick the source first.

**With an off-cluster CA backup tarball** (the planned path — every cert keeps validating):

```bash
# On the chosen bootstrap node, with neonfs-core stopped:
sudo systemctl stop neonfs-core   # if not already stopped

NEONFS_TLS_DIR=/var/lib/neonfs/tls \
NEONFS_DATA_DIR=/var/lib/neonfs \
neonfs cluster ca emergency-bootstrap --from-backup /path/to/ca-backup.tar.gz
```

The CLI:
1. Refuses if `neonfs-core` is still listening on the distribution port (probed via `/run/neonfs/dist_port`). Stop the service first.
2. Validates the tarball (must contain `ca.crt`, `ca.key`, `serial`, `crl.pem`).
3. Refuses a foreign CA: the tarball's CA `subject CN` (after stripping the trailing ` CA`) must match this node's `cluster.json` `cluster_name`. If you're rebuilding with a backup of a different cluster, edit `cluster.json` first and re-run.
4. Atomically installs `ca.crt` / `ca.key` / `serial` / `crl.pem` (per-file rename + fsync).
5. Generates a fresh keypair and signs a node cert against the freshly-installed CA, atomically writing `node.crt` + `node.key` and bumping `serial`.

**Without a backup, with `--new-key`** (last-resort — every previously-issued cert is invalidated):

```bash
NEONFS_TLS_DIR=/var/lib/neonfs/tls \
NEONFS_DATA_DIR=/var/lib/neonfs \
neonfs cluster ca emergency-bootstrap --new-key --yes-i-accept-data-loss
```

`--new-key` mints a brand-new CA, advances its serial counter past the previous CA's last issued value (so reissued certs do not collide), and runs the same install + node-cert-regen pipeline. Every previously-issued cert is now untrusted. Operators MUST be ready to reissue every interface and core node cert via the subsequent `cluster ca rotate` (step 8). The `--yes-i-accept-data-loss` flag is a deliberate guard-rail; the CLI refuses to run `--new-key` without it.

Once emergency-bootstrap returns success, restart the bootstrap node:

```bash
sudo systemctl start neonfs-core
neonfs cluster ca info       # confirms new validity
```

#### 8. Fan out to the rest of the core

Once the bootstrap node is healthy with new certs:

```bash
neonfs cluster ca rotate
```

This takes the normal path now that TLS works between the bootstrap node and anywhere it can reach. Every other core node gets a new cert issued by the new CA.

If a core node cannot be reached by the bootstrap node (partition, its own cert so expired that its Erlang dist will not come up), restart it manually and let it pick up fresh certs on rejoin. If the node's own cert is the only blocker, delete `$NEONFS_TLS_DIR/node/*.crt` on that node and restart — it will request a fresh cert from the CA on startup.

#### 9. Bring the interfaces back

```bash
# Each interface node:
sudo systemctl start neonfs-fuse   # or whichever package is installed
```

Each interface will reconnect, be issued a cert by the new CA, and start serving traffic.

#### 10. Lift isolation and verify

Remove the firewall rules from step 5. Verify:

```bash
neonfs cluster ca info      # new validity
neonfs cluster ca list      # every node issued a new cert today
neonfs cluster status       # quorum healthy, leader present
neonfs node status <each>   # health checks pass
```

Smoke-test from outside the cluster perimeter: a client read and write via S3 / WebDAV / FUSE.

## Verification

The incident is resolved when:

- `neonfs cluster ca info` reports a validity window at least one rotation cycle in the future.
- Every node reported in `neonfs cluster ca list` has a cert signed by the current CA with a future `not_valid_after`.
- `neonfs cluster status` reports a healthy quorum.
- A fresh client read and a fresh client write succeed via each interface the cluster exposes.
- No TLS handshake-failure entries in the last 10 minutes of any node's journal.
- The off-cluster CA backup (step 3a) has been moved to the long-term backup location.

## Escalation path

Escalate to engineering if any of the following hold. Capture context before stopping:

- No off-cluster CA backup exists AND `--new-key` is not acceptable (every cert invalidated, every interface needs reissue). The trade-off between "regenerate from scratch (losing trust chain)" and "wait for key recovery from secure storage" needs a stakeholder decision — not a runbook decision.
- A node refuses to accept its new cert after rotation and cannot be forced to retry. Corrupted TLS state on disk (wrong permissions, partial write) can surface as "rotation succeeded but node still uses old cert" — diagnosing this is engineering-level.
- The cluster is multi-site and only one site is reachable for emergency rotation. Bringing fresh certs across a WAN partition during emergency rotation risks split-brain on the CA itself.
- Rotation succeeds but `neonfs cluster status` shows quorum loss immediately afterwards. This is unexpected and indicates rotation has unblocked a latent quorum problem that was previously masked by TLS errors.

Capture before escalating:

- `neonfs cluster ca info` and `neonfs cluster ca list` output (JSON).
- Last 500 lines of `journalctl -u neonfs-core` from the affected node(s).
- `ls -la $NEONFS_TLS_DIR/` on each reachable core node.
- Timeline: first handshake warning, first expiry failure, first operator action, outcome of each step attempted.

After the incident, use the post-mortem template at [Post-Mortem-Template.md](Post-Mortem-Template.md) (filled-in example: [Post-Mortem-Sample.md](Post-Mortem-Sample.md)). CA incidents always capture: what the rotation cadence was, what alerting was (or wasn't) in place, whether an off-cluster backup was tested, and what operator-facing-tooling gap made emergency recovery harder than it needed to be.

## Known limitations

- `neonfs cluster ca info` does not emit a machine-readable "expiring in N days" warning via telemetry — an operator monitoring dashboard gets visibility only if it polls and evaluates the validity window itself. Tracked as a future improvement.
- `ca rotate` has no dry-run mode. There is no way to preview which certs would be reissued before running the command for real.
- After `--finalize`, the new active CA cert is held in the system volume but is not pushed into each node's local `<tls_dir>/ca.crt` cache. Until that cache is refreshed (currently on the next listener restart), the on-disk single-CA copy remains stale even though the dual-CA bundle on disk is correct.
- The orchestrator's per-node walk uses `[Node.self() | Node.list()]`. A node that is genuinely offline is skipped and must be re-onboarded via `cluster join` after rotation completes; there is no buffered/deferred reissue.

## References

- [Operator guide §Certificate authority rotation](../operator-guide.md#certificate-authority-rotation) (CLI surfaces and the planned-rotation prerequisites).
- [Operator guide §Backup](../operator-guide.md#backup) (off-cluster CA backup guidance).
- [CLI reference §neonfs cluster](../cli-reference.md#neonfs-cluster) (`ca info / list / revoke / rotate`).
- [Node-Down runbook](Node-Down.md) (when the TLS symptoms are actually a single-node distribution failure, not a cluster-wide cert issue).
