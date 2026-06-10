# NeonFS WebDAV

Serve NeonFS volumes over WebDAV — the protocol Finder, Windows
Explorer, GNOME Files, and rclone already speak. Map a network drive
to the cluster and drag files in; they land in the same namespace
served over FUSE, NFS, and S3.

Collections map to NeonFS volumes and resources to file paths. The
server supports collection locking (held in the cluster-wide
distributed lock manager, so WebDAV locks are honoured across every
protocol) and dead-property storage. Transfers stream chunk by chunk —
memory use is bounded regardless of file size.

The WebDAV protocol layer is the [`davy`](https://hex.pm/packages/davy)
library; this package supplies the NeonFS backend. It depends on
[`neonfs_client`](../neonfs_client/) only — it has **no dependency on
`neonfs_core`**.

## Architecture

```
Finder / Explorer / rclone ──→ neonfs_webdav (Bandit + WebdavServer.Plug)
                                      │
                                      │ NeonFS.Client.Router (metadata)
                                      │ TLS data plane (chunk bytes)
                                      ▼
                               neonfs_core (volumes, metadata, blob store)
```

## Building and testing

```bash
mix deps.get
mix compile
mix test                          # unit tests (run without core)
mix check --no-retry              # full check suite
```

End-to-end WebDAV tests live in `test/integration/`.

## Configuration

Environment variables (read at release startup via `config/runtime.exs`):

| Variable                     | Default                 | Description                        |
|------------------------------|-------------------------|------------------------------------|
| `NEONFS_CORE_NODE`           | `neonfs_core@localhost` | Core node to connect to            |
| `NEONFS_WEBDAV_PORT`         | `8081`                  | HTTP listen port                   |
| `NEONFS_WEBDAV_BIND`         | `0.0.0.0`               | HTTP bind address                  |
| `NEONFS_WEBDAV_METRICS`      | `false`                 | Enable Prometheus metrics endpoint |
| `NEONFS_WEBDAV_METRICS_PORT` | `9572`                  | Metrics endpoint port              |
| `NEONFS_WEBDAV_METRICS_BIND` | `0.0.0.0`               | Metrics endpoint bind address      |

Like every NeonFS node, the WebDAV node joins the cluster with an
invite token over TLS Erlang distribution — see the
[operator guide](../docs/operator-guide.md) for the join flow.

## Running

As an OTP release:

```bash
mix release neonfs_webdav
_build/prod/rel/neonfs_webdav/bin/neonfs_webdav start
```

Or as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../containers/bake.hcl --load webdav
```

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
