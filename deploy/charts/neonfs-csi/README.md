# neonfs-csi

Helm chart that installs the NeonFS CSI driver into a Kubernetes
cluster — a Controller `Deployment` for cluster-wide volume
lifecycle plus a Node `DaemonSet` for the per-node FUSE mount
lifecycle.

## Prerequisites

- Kubernetes ≥ 1.27 (the chart targets CSI v1.10 capabilities,
  including `VOLUME_CONDITION` and `GET_VOLUME`).
- A reachable NeonFS cluster — provide the join token via
  `bootstrap.existingSecret` or `bootstrap.value`.

## Install

```bash
helm install neonfs-csi ./deploy/charts/neonfs-csi \
  --namespace kube-system \
  --set image.repository=harton.dev/project-neon/neonfs-csi \
  --set image.tag=0.1.0 \
  --set bootstrap.value=$(cat /etc/neonfs/bootstrap-token)
```

To use an existing Secret for the bootstrap token (recommended for
production):

```bash
kubectl create secret generic neonfs-csi-bootstrap \
  --namespace kube-system \
  --from-literal=token="$(cat /etc/neonfs/bootstrap-token)"

helm install neonfs-csi ./deploy/charts/neonfs-csi \
  --namespace kube-system \
  --set bootstrap.existingSecret=neonfs-csi-bootstrap
```

## What gets installed

| Resource                 | Purpose                                                                 |
|--------------------------|-------------------------------------------------------------------------|
| `CSIDriver`              | Declares ownership of `neonfs.csi.harton.dev` to the kubelet.           |
| `Deployment` (controller)| Plugin + `external-provisioner` / `external-resizer` sidecars. |
| `DaemonSet` (node)       | Plugin + `node-driver-registrar` + `livenessprobe` sidecars on every node. |
| `ServiceAccount`         | One per role (controller, node).                                        |
| `ClusterRole` + binding  | RBAC scoped to what each plugin actually needs.                         |
| `Secret`                 | Bootstrap token (only created when `bootstrap.value` is set).           |
| `StorageClass`           | Sample default class — set `storageClass.create=false` to manage out of band. |

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
