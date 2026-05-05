# NeonFS.Containerd

containerd content store plugin for NeonFS — speaks the
`containerd.services.content.v1.Content` gRPC protocol over a Unix
domain socket, so a `containerd` daemon configured with NeonFS as a
proxy content store dials this socket for image-layer reads and
writes.

This package is the package scaffold (issue #548). Concrete RPCs land
in dependent sub-issues — see the [`#196`
tracker](https://harton.dev/project-neon/neonfs/issues/196) for the
full breakdown.

## Status

| RPC          | State                  | Issue |
|--------------|------------------------|-------|
| `Status`     | empty in-progress list | #548 (this) |
| `ListStatuses` | empty in-progress list | #548 (this) |
| `Health.Check` | reports `SERVING` when the cluster has quorum | #548 (this) |
| `Read`       | `UNIMPLEMENTED` | #549 |
| `Write`      | `UNIMPLEMENTED` | #550 |
| `Info` / `List` / `Update` / `Delete` | `UNIMPLEMENTED` | #551 |
| `Abort`      | `UNIMPLEMENTED` | #552 |

## Configuration

Application env (`:neonfs_containerd`):

| Key | Default | Notes |
|-----|---------|-------|
| `:start_supervisor` | `true` | Disable for tests that spin up an isolated listener. |
| `:listener` | `:socket` | `:socket` for production UDS, `{:tcp, port}` for tests. |
| `:socket_path` | `/run/neonfs/containerd.sock` | Path containerd's `[proxy_plugins]` config dials. |
| `:register_service` | `true` | Register as `:containerd` in the cluster service registry. |

## Wiring containerd

Once the streaming RPCs land (#549 / #550), wire containerd to this
plugin via `/etc/containerd/config.toml`:

```toml
[proxy_plugins]
  [proxy_plugins.neonfs]
    type = "content"
    address = "/run/neonfs/containerd.sock"
```

containerd will dial that socket and use NeonFS as a content store
in addition to (or instead of) the default `boltdb` store.
