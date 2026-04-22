# neonfs_docker

Docker / Podman VolumeDriver plugin for NeonFS.

Implements the [Docker Volume Plugin v1 HTTP
protocol](https://docs.docker.com/engine/extend/plugins_volume/) over
a Unix socket. This package ships the package skeleton and the
non-mounting HTTP lifecycle endpoints; `Mount` / `Unmount` land in a
follow-up slice (see [issue #310](https://harton.dev/project-neon/neonfs/issues/310)).

## Endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /Plugin.Activate` | Handshake — advertises `VolumeDriver`. |
| `POST /VolumeDriver.Create` | Records a new volume locally, propagates to NeonFS core. |
| `POST /VolumeDriver.Remove` | Drops the local record (NeonFS volume itself is not deleted). |
| `POST /VolumeDriver.Get` | Looks up a single volume record. |
| `POST /VolumeDriver.List` | Lists all known volume records. |
| `POST /VolumeDriver.Path` | Returns the mountpoint (empty until Mount lands). |
| `POST /VolumeDriver.Capabilities` | Returns `{Scope: "local"}`. |

See [issue #243](https://harton.dev/project-neon/neonfs/issues/243) for
the full epic.
