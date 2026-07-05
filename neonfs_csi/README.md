# neonfs_csi

A Kubernetes CSI (Container Storage Interface) driver for NeonFS,
written in Elixir. Pods get persistent volumes backed by replicated,
deduplicated, optionally encrypted cluster storage — dynamically
provisioned through a `StorageClass`, FUSE-mounted on the node, and
reachable over every other NeonFS interface at the same time.

The driver implements all three CSI services over a Unix domain socket:

- **Identity** — `GetPluginInfo`, `GetPluginCapabilities`, `Probe`.
- **Controller** — volume lifecycle (create/delete, publish/unpublish,
  capacity, capability validation) plus snapshots: `CreateSnapshot`,
  `DeleteSnapshot`, `ListSnapshots`, and creating volumes from
  snapshots or as clones of existing volumes.
- **Node** — stage/unstage and publish/unpublish with FUSE mounts, and
  volume health reporting (`VOLUME_CONDITION`) for pod scheduling.

Deployment is via the [Helm chart](../deploy/charts/neonfs-csi/): a
Controller `Deployment` with provisioner/attacher/resizer sidecars and
a Node `DaemonSet` with registrar and liveness-probe sidecars.

Depends on [`neonfs_client`](../neonfs_client/) only — it has **no
dependency on `neonfs_core`**.

The Identity and Controller services are covered by the upstream
[csi-sanity](https://github.com/kubernetes-csi/csi-test) conformance
suite (`test/integration/csi_sanity_test.exs`, tagged
`:requires_csi_sanity`). The remaining work on the epic
([#244](https://harton.dev/project-neon/neonfs/issues/244)) is a full
kind/k3d end-to-end test that provisions a PVC and reads data back
([#319](https://harton.dev/project-neon/neonfs/issues/319),
[#995](https://harton.dev/project-neon/neonfs/issues/995)), plus the
Controller conformance gaps csi-sanity surfaced
([#1458](https://harton.dev/project-neon/neonfs/issues/1458)).

## Install

```bash
helm install neonfs-csi ./deploy/charts/neonfs-csi \
  --namespace kube-system \
  --set bootstrap.value=$(cat /etc/neonfs/bootstrap-token)
```

See the [chart README](../deploy/charts/neonfs-csi/README.md) for
production patterns (existing Secrets, StorageClass management) and
[`docs/orchestration.md`](../docs/orchestration.md) for the wider
Kubernetes story.

## Implementation notes

- gRPC server bound to a Unix socket at the CSI-spec default paths:
  - Controller mode: `/var/lib/csi/sockets/pluginproxy/csi.sock`
  - Node mode: `/var/lib/kubelet/plugins/neonfs.csi.harton.dev/csi.sock`
- Vendored CSI protobuf definitions under `proto/csi.proto`; Elixir
  bindings generated into `lib/csi/csi.pb.ex` (regenerate with
  `mix csi.gen_proto`).
- Registers as `:csi` in the cluster service registry.

## Building and testing

```bash
mix deps.get
mix compile
mix test                          # unit tests (run without core)
mix check --no-retry              # full check suite
```

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
