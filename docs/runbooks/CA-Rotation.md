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

This:

1. (Full CA rotation only) Generates a new CA key and cert.
2. Reissues every known node's cert under the current (or new) CA.
3. Rolls distribution TLS to the new cert on each node.

Every node reports back in the CLI output as it finishes. A node that does not report back within the rotation timeout has failed — see [Step 4](#4-if-a-node-fails-to-rotate) below.

#### 3c. Verify post-rotation

```bash
neonfs cluster ca info                       # new not_valid_after ≥ rotation window
neonfs cluster ca list                       # every node's cert shows the new validity
neonfs cluster status                        # leader still elected, quorum healthy
neonfs node status <each-node>               # service reports running, health checks pass
```

From an interface node, perform a smoke-test read and a smoke-test write. If distribution handshakes are silently falling back to old certs you will see handshake retries in the journal; a real write flushing to disk confirms the new chain is end-to-end.

#### 4. If a node fails to rotate

- `ca rotate` exit non-zero, or a specific node is missing from the output.
- Check `neonfs node status <node>` — if it reports "cert expiry pending" or similar, its rotation has not completed.

Remediation:

```bash
# On the node itself:
sudo journalctl -u neonfs-core --since "10 minutes ago" | grep -iE "tls|cert|handshake"
# Retry rotation on just that node:
neonfs cluster ca rotate --node <node>        # if the CLI supports per-node retry
```

If per-node retry is not supported by the installed CLI version, stop and [escalate](#escalation-path). An offline node will fail to rotate; the operator guide flags this explicitly — they must be rejoined from scratch after the rest of the cluster has rotated.

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

There is no first-class CLI for "re-bootstrap the CA after expiry" today — every available CA command requires a working TLS chain. The workaround is operator-applied:

1. Stop `neonfs-core` on the chosen bootstrap node.
2. Regenerate the CA material manually (tooling: `openssl req -x509 -new …` against a replacement private key OR restore a pre-expiry CA backup that is still in-date), write it to `$NEONFS_TLS_DIR/ca/`.
3. Regenerate the bootstrap node's own cert against the new CA.
4. Start `neonfs-core` on the bootstrap node.
5. Verify: `neonfs cluster ca info` reports the new validity; the node's own distribution listener presents the new cert.

Because this step reaches inside `$NEONFS_TLS_DIR`, escalate to engineering for oversight — see [Escalation](#escalation-path). This is exactly the kind of hands-on-CA operation that should live behind a `neonfs cluster ca emergency-bootstrap` CLI, tracked as a follow-up.

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

- No off-cluster CA backup exists. The decision between "regenerate from scratch (losing trust chain)" and "wait for key recovery" needs a stakeholder decision — not a runbook decision.
- Emergency rotation step 7 (re-bootstrapping the CA on one node) is needed. This involves direct manipulation of `$NEONFS_TLS_DIR` and should have engineering oversight until a first-class CLI lands.
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

- There is no `neonfs cluster ca emergency-bootstrap` CLI for the post-expiry recovery case. Step 7 is operator-applied by reaching into `$NEONFS_TLS_DIR`; engineering oversight is required until that lands.
- `neonfs cluster ca info` does not emit a machine-readable "expiring in N days" warning via telemetry — an operator monitoring dashboard gets visibility only if it polls and evaluates the validity window itself. Tracked as a future improvement.
- `ca rotate` has no dry-run mode. There is no way to preview which certs would be reissued before running the command for real.
- Per-node retry for a partially-failed `ca rotate` may not be available in all CLI versions — check `neonfs cluster ca rotate --help` on your installed version.

## References

- [Operator guide §Certificate authority rotation](../operator-guide.md#certificate-authority-rotation) (CLI surfaces and the planned-rotation prerequisites).
- [Operator guide §Backup](../operator-guide.md#backup) (off-cluster CA backup guidance).
- [CLI reference §neonfs cluster](../cli-reference.md#neonfs-cluster) (`ca info / list / revoke / rotate`).
- [Node-Down runbook](Node-Down.md) (when the TLS symptoms are actually a single-node distribution failure, not a cluster-wide cert issue).
