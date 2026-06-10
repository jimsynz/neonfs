# NeonFS.Containerd

A containerd content-store plugin for NeonFS. Configure containerd with
NeonFS as a proxy content store and image layers are stored in the
cluster — deduplicated (layers shared between images are stored once),
replicated, and visible to every node — instead of in each host's local
boltdb store.

Speaks the `containerd.services.content.v1.Content` gRPC protocol over
a Unix domain socket. Every content-store RPC is implemented:
`Read` and `Write` (both streaming), `Info`, `List`, `Update`,
`Delete`, `Status`, `ListStatuses`, and `Abort`, plus gRPC `Health.Check`
(reports `SERVING` when the cluster has quorum). Blob reads and writes
stream chunk by chunk — memory use is bounded regardless of layer size.

Depends on [`neonfs_client`](../neonfs_client/) only — it has **no
dependency on `neonfs_core`**.

## Wiring containerd

Point containerd at the plugin socket via `/etc/containerd/config.toml`:

```toml
[proxy_plugins]
  [proxy_plugins.neonfs]
    type = "content"
    address = "/run/neonfs/containerd.sock"
```

containerd dials that socket and uses NeonFS as a content store in
addition to (or instead of) the default boltdb store. Verify with:

```bash
ctr content ingest < layer.tar     # store a blob
ctr content get sha256:...         # read it back
```

## Configuration

Application env (`:neonfs_containerd`):

| Key | Default | Notes |
|-----|---------|-------|
| `:start_supervisor` | `true` | Disable for tests that spin up an isolated listener. |
| `:listener` | `:socket` | `:socket` for production UDS, `{:tcp, port}` for tests. |
| `:socket_path` | `/run/neonfs/containerd.sock` | Path containerd's `[proxy_plugins]` config dials. |
| `:register_service` | `true` | Register as `:containerd` in the cluster service registry. |

## Building and testing

```bash
mix deps.get
mix compile
mix test                          # unit tests (run without core)
mix check --no-retry              # full check suite
```

The [QEMU test rig](../test-rig/)'s acceptance suite drives a real
containerd through `ctr content ingest`/`get` against the plugin.

See [`docs/containerd.md`](../docs/containerd.md) for deployment
details.

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
