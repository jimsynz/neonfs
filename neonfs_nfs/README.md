# NeonFS NFS

Serve NeonFS volumes over NFSv3 — the network filesystem client that is
already built into Linux, macOS, and Windows. No agent to install on
the consuming machine: point `mount -t nfs` at a NeonFS NFS node and go.

The server is **pure Elixir** from the wire up — XDR codec, ONC RPC,
MOUNT, and the NFSv3 procedures come from the standalone
[`nfs_server`](../nfs_server/) library, with NLM v4 advisory locking
implemented here. No kernel NFS server, no native code. Locks taken
over NFS are held in the
cluster-wide distributed lock manager, so they are honoured by FUSE,
S3, and WebDAV clients too.

This package depends on [`neonfs_client`](../neonfs_client/) only — it
has **no dependency on `neonfs_core`**.

## How it works

A single TCP listener on port 2049 serves all exported volumes through
a virtual root:

```
/                        # synthetic read-only root (lists volumes)
/default/                # volume "default"
/photos/                 # volume "photos"
/photos/2024/img.jpg     # file in "photos" volume
```

Clients mount individual volumes (`mount -t nfs server:/photos /mnt/photos`)
or mount the root to browse available exports.

### Key modules

- `NeonFS.NFS.NFSv3Backend` — implements the `NFSServer.NFSv3.Backend`
  callbacks against `NeonFS.Client`
- `NeonFS.NFS.MountBackend` — implements the `NFSServer.Mount.Backend`
  callbacks against `ExportManager`
- `NeonFS.NFS.ExportManager` — starts the NFS listener and manages the
  volume export lifecycle
- `NeonFS.NFS.MetadataCache` — ETS-backed cache with event-driven
  invalidation
- `NeonFS.NFS.InodeTable` — bidirectional inode-to-path mapping

## Building and testing

```bash
mix deps.get
mix compile
mix test                          # unit tests (run without core)
mix check --no-retry              # full check suite
```

## Configuration

Environment variables (read at release startup via `config/runtime.exs`):

| Variable                  | Default                 | Description                        |
|---------------------------|-------------------------|------------------------------------|
| `NEONFS_CORE_NODE`        | `neonfs_core@localhost` | Core node to connect to            |
| `NEONFS_NFS_BIND`         | `0.0.0.0`               | NFS listener bind address          |
| `NEONFS_NFS_PORT`         | `2049`                  | NFS listener port                  |
| `NEONFS_NFS_METRICS`      | `false`                 | Enable Prometheus metrics endpoint |
| `NEONFS_NFS_METRICS_PORT` | `9570`                  | Metrics endpoint port              |
| `NEONFS_NFS_METRICS_BIND` | `0.0.0.0`               | Metrics endpoint bind address      |

## Running

As an OTP release:

```bash
mix release neonfs_nfs
_build/prod/rel/neonfs_nfs/bin/neonfs_nfs start
```

Or as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../containers/bake.hcl --load nfs
```

Then mount a volume from any NFS client:

```bash
mount -t nfs -o vers=3,tcp server:/volume_name /mnt/neonfs
```

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
