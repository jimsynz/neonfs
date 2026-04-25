# neonfs_csi

Kubernetes Container Storage Interface (CSI) v1 plugin for NeonFS —
the Elixir gRPC server that a kubelet (Node mode) or external
provisioner sidecar (Controller mode) speaks to over a Unix domain
socket to provision and mount NeonFS volumes.

This package is the **first slice** of the CSI driver epic (#244):

- New OTP application `NeonFS.CSI.Application`.
- gRPC server bound to a Unix socket. Default paths follow the CSI
  spec:
  - Controller mode: `/var/lib/csi/sockets/pluginproxy/csi.sock`.
  - Node mode: `/var/lib/kubelet/plugins/neonfs.csi.harton.dev/csi.sock`.
- Vendored CSI v1.9 protobuf definitions under `proto/csi.proto`.
- Generated Elixir bindings under `lib/csi/csi.pb.ex` (regenerate with
  `mix csi.gen_proto`).
- Identity service (`GetPluginInfo`, `GetPluginCapabilities`, `Probe`)
  — required by every CSI plugin regardless of mode.
- Service registry registration as `:csi` so the cluster can discover
  the plugin.

## Out of scope (subsequent slices)

- Controller service RPCs (#314).
- Node service RPCs (#315).
- Volume health reporting (#316).
- Helm chart (#317).
- Container image / packaging (#318).
- End-to-end test on a kind/k3d cluster (#319).
