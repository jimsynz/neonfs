# NeonFS Docker

A Docker / Podman VolumeDriver plugin for NeonFS. `docker volume create
-d neonfs` provisions a volume in the cluster; `docker run -v` mounts
it into a container via FUSE. Containers get replicated, deduplicated,
optionally encrypted storage that survives the host — and the same
files are reachable over NFS, S3, and every other NeonFS interface.

Implements the [Docker Volume Plugin v1 HTTP
protocol](https://docs.docker.com/engine/extend/plugins_volume/) over a
Unix socket. Depends on [`neonfs_client`](../neonfs_client/) only — it
has **no dependency on `neonfs_core`**.

## Usage

With the plugin running on the host (the omnibus Debian package
registers it at `/etc/docker/plugins/neonfs.spec`):

```bash
docker volume create -d neonfs my-volume
docker run -v my-volume:/data alpine sh -c 'echo hello > /data/greeting'
docker run -v my-volume:/data alpine cat /data/greeting
```

## Endpoints

| Endpoint | Purpose |
|----------|---------|
| `POST /Plugin.Activate` | Handshake — advertises `VolumeDriver`. |
| `POST /VolumeDriver.Create` | Creates the NeonFS volume (durability/policy options pass through). |
| `POST /VolumeDriver.Remove` | Drops the local record (the NeonFS volume itself is not deleted). |
| `POST /VolumeDriver.Mount` | FUSE-mounts the volume and returns the mountpoint. |
| `POST /VolumeDriver.Unmount` | Releases the mount when the last container detaches. |
| `POST /VolumeDriver.Path` | Returns the current mountpoint. |
| `POST /VolumeDriver.Get` / `List` | Volume lookup and enumeration. |
| `POST /VolumeDriver.Capabilities` | Returns `{Scope: "local"}`. |

## Building and testing

```bash
mix deps.get
mix compile
mix test                          # unit tests (run without core)
mix check --no-retry              # full check suite
```

End-to-end VolumeDriver tests live in `test/integration/`, and the
[QEMU test rig](../test-rig/)'s acceptance suite exercises the full
`docker volume create` → write → read-back path on a real host.

## Running

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../containers/bake.hcl --load docker
```

See [`docs/docker-plugin.md`](../docs/docker-plugin.md) for deployment,
configuration, and volume options.

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
