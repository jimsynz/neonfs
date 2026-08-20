# neonfs-csi

Helm chart that installs the NeonFS CSI driver into a Kubernetes
cluster — a Controller `Deployment` for cluster-wide volume
lifecycle plus a Node `DaemonSet` for the per-node FUSE mount
lifecycle.

## Prerequisites

- Kubernetes ≥ 1.27 (the chart targets CSI v1.10 capabilities,
  including `VOLUME_CONDITION` and `GET_VOLUME`).
- A reachable NeonFS cluster, and an invite token minted with a redemption
  budget covering the nodes that will run NeonFS workloads.
- For `volumeMode: Block` PVCs only: a loadable `nbd` kernel module on each
  node, and `node.hostDevices.enabled=true`. See "Raw block volumes need host
  device access" below.

## How a pod obtains cluster identity

Identity is **per node, not per pod**. An init container — the same one in both
the controller `Deployment` and the node `DaemonSet` — redeems an invite into
the host's `stateDir` (`/var/lib/neonfs` by default, a `hostPath`) and exits.
The plugin containers then mount `tls/` and `meta/` from it read-only. A host
that already has credentials is skipped, so only the first NeonFS pod scheduled
onto a node spends a redemption.

Two consequences of that to plan for:

- **One redemption per node, not per pod.** Size `bootstrap.uses` against your
  node count, not your replica count. There is no automatic top-up: a cluster
  that grows past the budget needs a fresh invite, and pods on new nodes will
  fail to obtain identity until it has one. The chart refuses to install without
  `bootstrap.uses` for exactly this reason.
- **The host does not become a cluster member.** The init container writes
  credentials and exits; nothing runs on the node as a NeonFS node. Only the
  pods that later start there register as services.

Mint an invite for a 20-node cluster with:

```bash
neonfs cluster create-invite --expires 1h --uses 20
```

## Install

```bash
helm install neonfs-csi ./deploy/charts/neonfs-csi \
  --namespace kube-system \
  --set image.repository=harton.dev/project-neon/neonfs-csi \
  --set image.tag=0.1.0 \
  --set coreNode=neonfs_core@10.0.0.1 \
  --set bootstrap.uses=20 \
  --set bootstrap.value=$(cat /etc/neonfs/bootstrap-token)
```

Two addresses, deliberately separate because they are different kinds of thing:

- **`coreNode`** — core's Erlang **node name**, e.g. `neonfs_core@10.0.0.1`.
  Required. What the plugin dials over distribution; without it every pod looks
  for core on itself.
- **`joinVia`** — the `host:port` **HTTP endpoint** an invite is redeemed at.
  Defaults to `coreNode`'s host on port 9568, so most installs never set it.
  Set it when core's API is on another port, or when redemption should go to a
  different member than the plugin dials.

To use an existing Secret for the bootstrap token (recommended for
production):

```bash
kubectl create secret generic neonfs-csi-bootstrap \
  --namespace kube-system \
  --from-literal=token="$(cat /etc/neonfs/bootstrap-token)"

helm install neonfs-csi ./deploy/charts/neonfs-csi \
  --namespace kube-system \
  --set coreNode=neonfs_core@10.0.0.1 \
  --set bootstrap.existingSecret=neonfs-csi-bootstrap
```

`bootstrap.uses` is only required alongside `bootstrap.value` — with
`existingSecret` the budget is a property of the token you minted, and the chart
has no way to check it.

A deployment whose hosts already hold cluster credentials — provisioned out of
band, as the test rig does — needs no token at all. The init container finds the
credentials and exits without redeeming, so the chart installs with neither
`bootstrap` value set.

## The controller needs one host per replica

The plugin joins the cluster over Erlang distribution from the host's network
namespace, so `controller.hostNetwork` defaults to `true` and the two replicas
share one distribution port per host. A required `podAntiAffinity` on
`kubernetes.io/hostname` keeps them apart.

**On a single-node cluster the second replica stays `Pending` forever.** Set
`controller.replicaCount: 1` there. This is a deliberate trade: the alternative
is a replica that schedules and then cannot connect, which is harder to
diagnose than one that plainly never schedules.

## A pod without cluster identity refuses to start

If `tls/ssl_dist.conf` is absent the plugin exits rather than starting. It
previously came up on plain, unauthenticated distribution, never connected to
anything, and logged nothing that pointed at the cause. A `CrashLoopBackOff`
naming the missing file is the better failure — check the `provision-identity`
init container's logs and that `stateDir` is mounted.

`NEONFS_ALLOW_INSECURE_DIST=1` opts out, for a development release that never
joins a cluster.

## What gets installed

| Resource                 | Purpose                                                                 |
|--------------------------|-------------------------------------------------------------------------|
| `CSIDriver`              | Declares ownership of `neonfs.csi.harton.dev` to the kubelet.           |
| `Deployment` (controller)| Plugin + `external-provisioner` / `external-resizer` sidecars. |
| `DaemonSet` (node)       | Plugin + `node-driver-registrar` + `livenessprobe` sidecars on every node. |
| `ServiceAccount`         | One per role (controller, node).                                        |
| `ClusterRole` + binding  | RBAC scoped to what each plugin actually needs.                         |
| `Secret`                 | Bootstrap token (only created when `bootstrap.value` is set).           |
| init container           | `provision-identity`, in both workloads — redeems an invite into the host's `stateDir` and exits. |
| `StorageClass`           | Sample default class — set `storageClass.create=false` to manage out of band. |

## Raw block volumes need host device access

`volumeMode: Block` PVCs are off by default, because supporting them means
granting the node plugin the host's `/dev`:

```bash
helm install neonfs-csi ./deploy/charts/neonfs-csi \
  --set coreNode=neonfs_core@10.0.0.1 \
  --set node.hostDevices.enabled=true
```

Block staging attaches the volume with `nbd-client` and picks a free
`/dev/nbdX` by scanning, so it needs the directory rather than a named device —
which is why `/dev/fuse` can be a single `CharDevice` mount and this cannot.
The plugin container is privileged either way, so this widens what it can *see*
rather than what it is allowed to do.

Two host prerequisites the chart cannot supply:

- **A loadable `nbd` kernel module.** It and `/lib/modules` belong to the node.
  A node without it schedules the plugin fine and fails at `nbd-client` when a
  block volume is staged.
- **Enough `/dev/nbdX` devices.** The plugin scans `nbd0`–`nbd15`, so a node
  can serve at most sixteen attached block volumes, and `nbd_max_part` /
  `max_part` module parameters govern how many the kernel creates.

Mount-mode volumes need none of this; leave `node.hostDevices.enabled` at
`false` if you serve only filesystems.

## Upgrading the driver interrupts mounted volumes

The node plugin mounts each staged volume **inside its own pod**, so rolling
the `DaemonSet` — `helm upgrade`, an image bump, a plugin restart for any
reason — takes those mounts down with the pod.

The plugin records what it was serving and re-establishes its **staging**
mounts when it comes back, so a restart no longer leaves the node permanently
without them. It does **not** repair the bind mounts the kubelet already made
into running workload pods: those were bound from the previous mount, and they
keep returning `ENOTCONN` until the kubelet publishes the volume again, which
happens when the workload pod restarts.

So a driver upgrade is still a data-path event rather than a control-plane one.
Drain the workloads using NeonFS volumes from a node before the new plugin pod
lands on it, or expect to restart them afterwards.

## Tests

```bash
# Static lint (matches CI).
helm lint deploy/charts/neonfs-csi

# Snapshot test — `helm template` output vs the fixture.
deploy/charts/neonfs-csi/tests/render.sh check

# Refresh the fixture after intentional changes.
deploy/charts/neonfs-csi/tests/render.sh update
```
