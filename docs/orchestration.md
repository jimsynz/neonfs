# Orchestrated Cluster Deployment

This guide covers deploying NeonFS clusters with orchestrators — Docker Swarm and Kubernetes — using the autonomous formation feature. For standalone Docker Compose deployments with manual cluster init/join, see [deployment.md](deployment.md).

## Overview

Traditional NeonFS cluster setup requires manual steps: `cluster init` on the first node, `cluster invite` to generate a token, then `cluster join` on each additional node. This doesn't work with orchestrators where all replicas start simultaneously from the same configuration.

The **auto-bootstrap** feature implements the Consul/Nomad `bootstrap_expect` pattern: all nodes start with identical configuration, discover each other via Erlang distribution, wait for quorum, then deterministically elect an initialiser (the node with the lexicographically lowest name). The elected node runs `cluster init` and distributes invite tokens to the others. The entire process is automatic and idempotent.

### How formation works

1. **Precondition check** — If `cluster.json` already exists (a prior formation completed), Formation exits immediately. If orphaned data is detected without `cluster.json`, Formation refuses to start (operator must clean up or restore `cluster.json`).
2. **Peer connection** — Formation calls `Node.connect/1` on each bootstrap peer, retrying every 2 seconds until `bootstrap_expect` nodes are connected.
3. **Readiness wait** — Formation monitors the `:pg` process group `{:node, :ready}` to confirm each peer has fully started its supervision tree.
4. **Leader election** — All expected node names are sorted lexicographically. The lowest name becomes the init node.
5. **Cluster formation** — The init node runs `cluster init`, creates an invite token, and pushes it to each peer. Peers join the cluster and confirm back to the init node.
6. **Finish** — Quorum rings are rebuilt on all nodes, drives are persisted, and Formation exits normally.

On restart, Formation detects the existing `cluster.json` and exits immediately — the cluster is already formed.

### Environment variables

| Variable | Required | Default | Description |
|---|---|---|---|
| `NEONFS_AUTO_BOOTSTRAP` | No | `"false"` | Enable autonomous cluster formation |
| `NEONFS_CLUSTER_NAME` | When auto-bootstrap | — | Cluster name passed to `cluster init` |
| `NEONFS_BOOTSTRAP_EXPECT` | When auto-bootstrap | — | Number of core nodes to wait for before forming |
| `NEONFS_BOOTSTRAP_PEERS` | When auto-bootstrap | — | Comma-separated Erlang node names of all peers |
| `NEONFS_BOOTSTRAP_TIMEOUT` | No | `"300000"` | Max wait time in ms (5 min default) |

When `NEONFS_AUTO_BOOTSTRAP=true`, the three required variables are validated eagerly — a missing variable causes the release to crash at startup with a clear error message.

### Alternative: CLI flag

The core release also accepts `--auto-bootstrap` as a command-line flag:

```bash
bin/neonfs_core start --auto-bootstrap
```

This sets `NEONFS_AUTO_BOOTSTRAP=true` internally. The environment variables for cluster name, peers, and expect count are still required.

## Prerequisites

- Docker Engine 24+ with Buildx (for image builds)
- Docker Swarm mode (for Swarm deployments) or a Kubernetes cluster (for K8s deployments)
- Images built and available:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f bake.hcl --load core fuse cli
```

See [deployment.md](deployment.md) for full image build instructions.

## Docker Swarm

Docker Swarm provides built-in overlay networking, DNS-based service discovery, and replicated services — all of which map well to NeonFS auto-bootstrap.

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                     Docker Swarm                        │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │         neonfs-core service (3 replicas)        │    │
│  │                                                 │    │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐   │    │
│  │  │ core.1    │  │ core.2    │  │ core.3    │   │    │
│  │  │           │  │           │  │           │   │    │
│  │  │ Formation │  │ Formation │  │ Formation │   │    │
│  │  │ auto-elects init node (lowest name)     │   │    │
│  │  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘   │    │
│  │        └───────────────┼───────────────┘         │    │
│  │                   overlay network                │    │
│  └─────────────────────────────────────────────────┘    │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │         neonfs-fuse service (per host)          │    │
│  │  ┌───────────┐                                  │    │
│  │  │ fuse.1    │ → discovers core via bootstrap   │    │
│  │  └───────────┘                                  │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Stack file

```yaml
# neonfs-stack.yml
version: "3.8"

services:
  core:
    image: forgejo.dmz/project-neon/neonfs/core:latest
    hostname: "neonfs-core-{{.Task.Slot}}"
    environment:
      RELEASE_DISTRIBUTION: "name"
      RELEASE_NODE: "neonfs_core@neonfs-core-{{.Task.Slot}}"
      RELEASE_COOKIE_FILE: "/run/secrets/erlang_cookie"
      NEONFS_DATA_DIR: "/var/lib/neonfs/data"
      NEONFS_AUTO_BOOTSTRAP: "true"
      NEONFS_CLUSTER_NAME: "production"
      NEONFS_BOOTSTRAP_EXPECT: "3"
      NEONFS_BOOTSTRAP_PEERS: "neonfs_core@neonfs-core-1,neonfs_core@neonfs-core-2,neonfs_core@neonfs-core-3"
      NEONFS_BOOTSTRAP_TIMEOUT: "300000"
      ERL_AFLAGS: "-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9200"
    volumes:
      - core-data-{{.Task.Slot}}:/var/lib/neonfs
    secrets:
      - erlang_cookie
    networks:
      - neonfs
    ports:
      - target: 4369
        published: 4369
        protocol: tcp
        mode: host
      - target: 9100
        published: 9100
        protocol: tcp
        mode: host
    deploy:
      replicas: 3
      placement:
        max_replicas_per_node: 1
      restart_policy:
        condition: on-failure
        delay: 5s
      update_config:
        parallelism: 1
        delay: 30s
        order: stop-first

  fuse:
    image: forgejo.dmz/project-neon/neonfs/fuse:latest
    hostname: "neonfs-fuse-{{.Node.Hostname}}"
    environment:
      RELEASE_DISTRIBUTION: "name"
      RELEASE_NODE: "neonfs_fuse@neonfs-fuse-{{.Node.Hostname}}"
      RELEASE_COOKIE_FILE: "/run/secrets/erlang_cookie"
      NEONFS_CORE_NODE: "neonfs_core@neonfs-core-1"
    volumes:
      - fuse-data:/var/lib/neonfs
      - /mnt/neonfs:/mnt/neonfs:rshared
    devices:
      - /dev/fuse:/dev/fuse
    cap_add:
      - SYS_ADMIN
    secrets:
      - erlang_cookie
    networks:
      - neonfs
    deploy:
      mode: global
      restart_policy:
        condition: on-failure

volumes:
  core-data-1:
  core-data-2:
  core-data-3:
  fuse-data:

networks:
  neonfs:
    driver: overlay
    attachable: true

secrets:
  erlang_cookie:
    external: true
```

### Deploying

```bash
# Initialise Swarm (if not already done)
docker swarm init

# Create the Erlang cookie secret
openssl rand -hex 32 | docker secret create erlang_cookie -

# Deploy the stack
docker stack deploy -c neonfs-stack.yml neonfs

# Watch formation progress
docker service logs -f neonfs_core
```

All three core replicas start simultaneously. Formation on each node:
1. Connects to the other two peers via Erlang distribution over the overlay network
2. Waits for all three to report ready
3. Elects the node with the lowest name (`neonfs_core@neonfs-core-1`) as init node
4. Init node runs `cluster init`, distributes invites to peers 2 and 3
5. Peers join, cluster is formed

### Key considerations

**Stable hostnames.** Swarm's `{{.Task.Slot}}` template produces stable numeric suffixes (1, 2, 3) tied to the replica index. Combined with a predictable hostname pattern, this gives each core node a stable Erlang node name across restarts.

**Volume persistence.** Each core replica needs its own persistent volume. The `core-data-{{.Task.Slot}}` pattern ensures replica 1 always gets `core-data-1`, etc. On restart, the existing `cluster.json` is found and Formation skips — the cluster reforms from persisted state, not from scratch.

**Placement constraints.** `max_replicas_per_node: 1` ensures core replicas are spread across Swarm nodes for fault tolerance.

**Rolling updates.** `parallelism: 1` with `order: stop-first` ensures only one core node is restarted at a time during updates, maintaining Ra quorum.

**Port mode.** `mode: host` on the EPMD and distribution ports bypasses Swarm's routing mesh so Erlang distribution connects directly to the correct container.

### Scaling

To scale from 3 to 5 core nodes:

1. Update the stack file with `replicas: 5`, updated `NEONFS_BOOTSTRAP_PEERS` to include the new node names, and updated `NEONFS_BOOTSTRAP_EXPECT` to `"5"`.
2. Redeploy. The two new replicas won't have `cluster.json` — but Formation will see that the first three already have it and skip formation on those. The new replicas need to be joined manually via invite tokens, as Formation only handles initial cluster creation:

```bash
# On an existing core node, create an invite
docker exec $(docker ps -q -f name=neonfs_core.1) \
  /app/bin/neonfs_core rpc \
  "NeonFS.CLI.Handler.handle_command({:create_invite, 3600})"

# On the new node, join using the token
docker exec $(docker ps -q -f name=neonfs_core.4) \
  /app/bin/neonfs_core rpc \
  "NeonFS.CLI.Handler.handle_command({:join_cluster, \"<token>\", :\"neonfs_core@neonfs-core-1\"})"
```

## Kubernetes

Kubernetes StatefulSets provide stable network identities and persistent storage — both essential for NeonFS core nodes.

### Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Kubernetes Cluster                    │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │    StatefulSet: neonfs-core (3 replicas)        │    │
│  │    Headless Service: neonfs-core-headless       │    │
│  │                                                 │    │
│  │  ┌───────────┐  ┌───────────┐  ┌───────────┐   │    │
│  │  │ core-0    │  │ core-1    │  │ core-2    │   │    │
│  │  │ PVC: data │  │ PVC: data │  │ PVC: data │   │    │
│  │  │           │  │           │  │           │   │    │
│  │  │ Formation auto-elects init node             │   │    │
│  │  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘   │    │
│  │        └───────────────┼───────────────┘         │    │
│  │              headless DNS resolution              │    │
│  └─────────────────────────────────────────────────┘    │
│                                                         │
│  ┌─────────────────────────────────────────────────┐    │
│  │    DaemonSet: neonfs-fuse (per node)            │    │
│  │  ┌───────────┐  ┌───────────┐                   │    │
│  │  │ fuse      │  │ fuse      │                   │    │
│  │  └───────────┘  └───────────┘                   │    │
│  └─────────────────────────────────────────────────┘    │
└─────────────────────────────────────────────────────────┘
```

### Namespace and secrets

```yaml
# namespace.yml
apiVersion: v1
kind: Namespace
metadata:
  name: neonfs
---
# secret.yml
apiVersion: v1
kind: Secret
metadata:
  name: neonfs-erlang-cookie
  namespace: neonfs
type: Opaque
stringData:
  cookie: "CHANGE-ME-generate-with-openssl-rand-hex-32"
```

```bash
# Or generate the secret imperatively:
kubectl create namespace neonfs
kubectl -n neonfs create secret generic neonfs-erlang-cookie \
  --from-literal=cookie="$(openssl rand -hex 32)"
```

### Headless service

A headless service provides stable DNS names for each StatefulSet pod. Pod `neonfs-core-0` resolves as `neonfs-core-0.neonfs-core-headless.neonfs.svc.cluster.local`.

```yaml
# headless-service.yml
apiVersion: v1
kind: Service
metadata:
  name: neonfs-core-headless
  namespace: neonfs
  labels:
    app: neonfs-core
spec:
  clusterIP: None
  publishNotReadyAddresses: true
  selector:
    app: neonfs-core
  ports:
    - name: epmd
      port: 4369
      targetPort: 4369
    - name: dist
      port: 9100
      targetPort: 9100
```

> **`publishNotReadyAddresses: true`** is critical. Without it, Kubernetes won't create DNS records for pods until they pass readiness probes. Since Formation needs to connect to peers *before* the cluster is formed, all pods must be DNS-resolvable immediately.

### Core StatefulSet

```yaml
# core-statefulset.yml
apiVersion: apps/v1
kind: StatefulSet
metadata:
  name: neonfs-core
  namespace: neonfs
spec:
  serviceName: neonfs-core-headless
  replicas: 3
  podManagementPolicy: Parallel
  updateStrategy:
    type: RollingUpdate
    rollingUpdate:
      maxUnavailable: 1
  selector:
    matchLabels:
      app: neonfs-core
  template:
    metadata:
      labels:
        app: neonfs-core
    spec:
      terminationGracePeriodSeconds: 30
      containers:
        - name: core
          image: forgejo.dmz/project-neon/neonfs/core:latest
          env:
            - name: POD_NAME
              valueFrom:
                fieldRef:
                  fieldPath: metadata.name
            - name: RELEASE_DISTRIBUTION
              value: "name"
            - name: RELEASE_NODE
              value: "neonfs_core@$(POD_NAME).neonfs-core-headless.neonfs.svc.cluster.local"
            - name: RELEASE_COOKIE
              valueFrom:
                secretKeyRef:
                  name: neonfs-erlang-cookie
                  key: cookie
            - name: NEONFS_DATA_DIR
              value: "/var/lib/neonfs/data"
            - name: NEONFS_AUTO_BOOTSTRAP
              value: "true"
            - name: NEONFS_CLUSTER_NAME
              value: "production"
            - name: NEONFS_BOOTSTRAP_EXPECT
              value: "3"
            - name: NEONFS_BOOTSTRAP_PEERS
              value: >-
                neonfs_core@neonfs-core-0.neonfs-core-headless.neonfs.svc.cluster.local,
                neonfs_core@neonfs-core-1.neonfs-core-headless.neonfs.svc.cluster.local,
                neonfs_core@neonfs-core-2.neonfs-core-headless.neonfs.svc.cluster.local
            - name: NEONFS_BOOTSTRAP_TIMEOUT
              value: "300000"
            - name: ERL_AFLAGS
              value: "-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9200"
          ports:
            - name: epmd
              containerPort: 4369
            - name: dist
              containerPort: 9100
          volumeMounts:
            - name: data
              mountPath: /var/lib/neonfs
          livenessProbe:
            exec:
              command: ["/app/bin/neonfs_core", "pid"]
            initialDelaySeconds: 30
            periodSeconds: 10
          readinessProbe:
            exec:
              command: ["/app/bin/neonfs_core", "pid"]
            initialDelaySeconds: 10
            periodSeconds: 5
          resources:
            requests:
              memory: "512Mi"
              cpu: "250m"
            limits:
              memory: "2Gi"
              cpu: "2000m"
  volumeClaimTemplates:
    - metadata:
        name: data
      spec:
        accessModes: ["ReadWriteOnce"]
        resources:
          requests:
            storage: 50Gi
```

### Key considerations

**`podManagementPolicy: Parallel`** starts all pods simultaneously rather than sequentially. This is essential — Formation expects all peers to start roughly together.

**Stable DNS names.** StatefulSet pods get predictable names (`neonfs-core-0`, `neonfs-core-1`, `neonfs-core-2`) that combine with the headless service to form stable FQDNs. These are used directly as `NEONFS_BOOTSTRAP_PEERS`.

**Persistent volume claims.** Each pod gets its own PVC via `volumeClaimTemplates`. On restart, the pod reattaches to the same volume. If `cluster.json` exists from a prior formation, Formation exits immediately — no re-election or re-init occurs.

**Rolling updates.** `maxUnavailable: 1` ensures at most one core node is down during updates, preserving Ra quorum (2 of 3 remain).

**RELEASE_NODE with pod name.** The `$(POD_NAME)` substitution in `RELEASE_NODE` uses the downward API to construct the correct node name for each replica. Kubernetes performs variable expansion in env values, so `$(POD_NAME)` is resolved before the container starts.

### FUSE DaemonSet

```yaml
# fuse-daemonset.yml
apiVersion: apps/v1
kind: DaemonSet
metadata:
  name: neonfs-fuse
  namespace: neonfs
spec:
  selector:
    matchLabels:
      app: neonfs-fuse
  template:
    metadata:
      labels:
        app: neonfs-fuse
    spec:
      hostPID: false
      containers:
        - name: fuse
          image: forgejo.dmz/project-neon/neonfs/fuse:latest
          env:
            - name: NODE_NAME
              valueFrom:
                fieldRef:
                  fieldPath: spec.nodeName
            - name: RELEASE_DISTRIBUTION
              value: "name"
            - name: RELEASE_NODE
              value: "neonfs_fuse@$(NODE_NAME)"
            - name: RELEASE_COOKIE
              valueFrom:
                secretKeyRef:
                  name: neonfs-erlang-cookie
                  key: cookie
            - name: NEONFS_CORE_NODE
              value: "neonfs_core@neonfs-core-0.neonfs-core-headless.neonfs.svc.cluster.local"
          securityContext:
            privileged: true
          volumeMounts:
            - name: fuse-device
              mountPath: /dev/fuse
            - name: neonfs-mount
              mountPath: /mnt/neonfs
              mountPropagation: Bidirectional
          resources:
            requests:
              memory: "256Mi"
              cpu: "100m"
            limits:
              memory: "1Gi"
              cpu: "1000m"
      volumes:
        - name: fuse-device
          hostPath:
            path: /dev/fuse
        - name: neonfs-mount
          hostPath:
            path: /mnt/neonfs
            type: DirectoryOrCreate
```

FUSE nodes don't participate in auto-bootstrap — they connect to an existing core node and use service discovery to find the rest.

### Deploying

```bash
# Apply all manifests
kubectl apply -f namespace.yml
kubectl apply -f secret.yml
kubectl apply -f headless-service.yml
kubectl apply -f core-statefulset.yml

# Watch pods start and formation complete
kubectl -n neonfs get pods -w

# Check formation logs
kubectl -n neonfs logs -f neonfs-core-0

# Once cluster is formed, deploy FUSE
kubectl apply -f fuse-daemonset.yml
```

### Helm values (example)

If wrapping in a Helm chart, the key configurable values:

```yaml
# values.yml
core:
  replicas: 3
  image:
    repository: forgejo.dmz/project-neon/neonfs/core
    tag: latest
  storage:
    size: 50Gi
    storageClass: ""  # Use cluster default
  autoBootstrap:
    enabled: true
    clusterName: production
    timeout: 300000
  resources:
    requests:
      memory: 512Mi
      cpu: 250m
    limits:
      memory: 2Gi
      cpu: 2000m

fuse:
  enabled: true
  image:
    repository: forgejo.dmz/project-neon/neonfs/fuse
    tag: latest
  mountPath: /mnt/neonfs
  resources:
    requests:
      memory: 256Mi
      cpu: 100m
    limits:
      memory: 1Gi
      cpu: 1000m

erlangCookie:
  existingSecret: ""  # Set to use pre-existing secret
  # If empty, the chart generates one
```

The chart template would derive `NEONFS_BOOTSTRAP_PEERS` and `NEONFS_BOOTSTRAP_EXPECT` from `.Values.core.replicas`.

### CSI driver — exposing NeonFS volumes as PersistentVolumes

The `neonfs-csi` Helm chart at `deploy/charts/neonfs-csi/` installs a
Kubernetes CSI v1 driver so PVCs against a `StorageClass` provisioned
by `neonfs.csi.harton.dev` are backed by NeonFS volumes. The chart
ships:

- a Controller `Deployment` (default 2 replicas) running the CSI
  plugin alongside the upstream `external-provisioner`,
  `external-attacher`, and `external-resizer` sidecars;
- a Node `DaemonSet` running the plugin alongside the
  `node-driver-registrar` sidecar so kubelet can discover the
  per-node socket;
- a `CSIDriver` object declaring the driver's capabilities;
- a sample `StorageClass` keyed on `replication_factor` and `tier`.

Container image: `ghcr.io/jimsynz/neonfs/csi:<tag>` (also published
to the project's Forgejo registry).

```bash
helm install neonfs-csi ./deploy/charts/neonfs-csi \
  --namespace kube-system \
  --set bootstrap.value="$(cat /etc/neonfs/bootstrap-token)"
```

The cluster's NeonFS bootstrap token can be supplied either via
`bootstrap.value` (the chart creates the Secret) or
`bootstrap.existingSecret=<name>` to reference one created out of
band. Once installed:

```bash
kubectl apply -f - <<YAML
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: my-data
spec:
  accessModes: [ReadWriteMany]
  resources:
    requests:
      storage: 10Gi
  storageClassName: neonfs
YAML
```

See `deploy/charts/neonfs-csi/README.md` for the full values
reference and `tests/render.sh check` to verify the rendered
manifests against the snapshot.

## Operational notes

### Formation idempotency

Formation is idempotent and safe to re-trigger:

- **Node restart with existing data**: Formation finds `cluster.json`, exits immediately. The node rejoins the cluster via persisted Ra state.
- **Full cluster restart**: All nodes find their existing `cluster.json` and skip formation. Ra reforms the consensus group from persisted state.
- **Partial data loss**: If a node loses `cluster.json` but retains Ra/blob data, Formation detects orphaned data and refuses to start. This prevents accidental double-initialisation that would corrupt the cluster. The operator must either restore `cluster.json` from backup or clean the data directory.

### Monitoring formation

Formation emits a telemetry event on completion:

```
[:neonfs, :cluster, :formation, :complete]
```

With measurements `%{duration: integer()}` (milliseconds) and metadata `%{cluster_name: string(), node: atom()}`.

Key log messages to watch for:

| Log message | Meaning |
|---|---|
| `Auto-bootstrap: starting cluster formation` | Formation is running |
| `Auto-bootstrap: all peers connected` | Phase 2 complete |
| `Auto-bootstrap: all nodes ready, forming cluster` | Phase 3 complete, proceeding to elect |
| `Auto-bootstrap: this node is the init node` | This node was elected as initialiser |
| `Auto-bootstrap: received invite from init node` | This node received an invite |
| `Auto-bootstrap: formation complete` | Success |
| `Cluster state already exists, skipping formation` | Idempotent skip (normal on restart) |
| `Orphaned data detected without cluster.json` | Error — needs operator intervention |
| `Auto-bootstrap: timed out waiting for ...` | Peers didn't connect or become ready in time |

### Troubleshooting

**Formation times out waiting for peers.**

- Verify all pods/containers are running and healthy
- Check that EPMD (port 4369) and the distribution port range are reachable between pods
- Verify `NEONFS_BOOTSTRAP_PEERS` contains the correct node names for all replicas
- In Kubernetes, ensure the headless service has `publishNotReadyAddresses: true`
- Check that `RELEASE_COOKIE` matches on all nodes
- Increase `NEONFS_BOOTSTRAP_TIMEOUT` if startup is slow (e.g. large images, slow storage)

**Formation detects orphaned data.**

This means a node has Ra snapshots, populated DETS files, or blob chunk data but no `cluster.json`. Possible causes:
- `cluster.json` was accidentally deleted
- A partial formation was interrupted (rare — Formation is designed to be crash-safe)
- The volume was reused from a different cluster

To resolve:
- **Restore `cluster.json`** from backup if the node was previously part of a cluster
- **Clean the data directory** if starting fresh: remove `data/ra/`, `data/meta/*.dets`, and all files under `data/blobs/` prefix directories

**One node forms but others time out.**

This typically means the init node completed `cluster init` and `invite` but the invite messages didn't reach peers. Check:
- Erlang distribution is working (`Node.list()` shows connected peers)
- The `NEONFS_BOOTSTRAP_PEERS` list is consistent across all nodes
- No firewall rules blocking inter-node traffic on the distribution port range

**Bootstrap expect doesn't match actual replicas.**

`NEONFS_BOOTSTRAP_EXPECT` must match the number of replicas being started. If set to 3 but only 2 replicas exist, Formation will wait indefinitely (until timeout). If set to 2 but 3 replicas exist, the third replica won't be part of the initial formation — it must be joined manually via invite.
