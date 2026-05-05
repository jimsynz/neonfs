# Docker VolumeDriver Plugin

The `neonfs_docker` package implements the Docker Volume Plugin v1 protocol so containers can mount NeonFS volumes directly with `docker volume create -d neonfs ...` / `docker run -v <name>:<path>`. The plugin runs as a host-local daemon that listens on a Unix socket at `/run/neonfs/docker.sock`. dockerd discovers it via a spec file at `/etc/docker/plugins/neonfs.spec` (installed by the .deb), and each `Mount` request lands a real FUSE mount against the corresponding NeonFS volume.

## Architecture

```
docker run -v myvol:/data ...
        │
        ▼
   dockerd  ──► /etc/docker/plugins/neonfs.spec
                              │  (resolves to)
                              ▼
                  /run/neonfs/docker.sock  (HTTP over UDS)
                              │
                              ▼
                  neonfs_docker (per-host)
                              │
              ┌───────────────┼───────────────┐
              │                               │
              ▼                               ▼
   NeonFS.Client.Router               NeonFS.FUSE.MountManager
   (volume create on core)            (FUSE mount on host)
```

The plugin depends only on `neonfs_client`. Volume metadata operations (create / remove / list) route to a core node via Erlang distribution; the actual FUSE mount is driven by a `NeonFS.FUSE.MountManager` instance — by default the plugin assumes co-location on the same BEAM node, but it can be pointed at a peer node via `NEONFS_FUSE_NODE`.

## Installation

### Debian / Ubuntu

```bash
# Install the common package once (provides the neonfs user, /var/lib/neonfs, TLS helpers).
sudo apt install ./neonfs-common_<version>_amd64.deb ./neonfs-cli_<version>_amd64.deb

# Then the plugin itself.
sudo apt install ./neonfs-docker_<version>_amd64.deb

sudo systemctl enable --now neonfs-docker.service
```

`neonfs-docker` conflicts with `neonfs-omnibus` (they ship the same release artifacts). Pick one.

### Container

```bash
docker run -d --name neonfs-docker \
  --restart unless-stopped \
  --network host \
  -v /run/neonfs:/run/neonfs \
  -v /etc/docker/plugins:/etc/docker/plugins \
  -v /var/lib/neonfs:/var/lib/neonfs \
  -e NEONFS_CORE_NODE=neonfs_core@neonfs-core.example \
  -e RELEASE_COOKIE=$(cat /var/lib/neonfs/.erlang.cookie) \
  ghcr.io/jimsynz/neonfs/docker:latest
```

The `--network host` requirement is the same as for the FUSE container — Erlang distribution needs a stable hostname/IP that matches the configured node name.

## Configuration

The defaults try to pick reasonable values from `/var/lib/neonfs/`. Override per-environment via `/etc/neonfs/neonfs.conf` (loaded by the systemd unit) or container env vars.

| Variable | Default | Purpose |
|----------|---------|---------|
| `NEONFS_CORE_NODE` | `neonfs_core@$(hostname -f)` | Erlang node name of the core to talk to. |
| `NEONFS_DOCKER_NODE` | `neonfs_docker@$(hostname -f)` | This node's name. |
| `NEONFS_DIST_PORT` | random 49152–65535, persisted | Erlang distribution listen port. |
| `RELEASE_COOKIE_PATH` | `/var/lib/neonfs/.erlang.cookie` | Where to read the shared cookie. |
| Application env `:neonfs_docker, :fuse_node` | `Node.self()` | Erlang node hosting `NeonFS.FUSE.MountManager`. Override when neonfs_docker and neonfs_fuse run as separate BEAMs on the same host. |
| Application env `:neonfs_docker, :mount_root` | `/var/lib/neonfs-docker/mounts` | Directory under which per-volume mount points are created. |
| Application env `:neonfs_docker, :socket_path` | `/run/neonfs/docker.sock` | Path of the plugin's Unix socket. The packaged `/etc/docker/plugins/neonfs.spec` points here; if you change this, update the spec file too. |
| Application env `:neonfs_docker, :listener` | `:socket` | Set to `{:tcp, port}` for testing. |

## Usage

```bash
# Create a NeonFS-backed Docker volume.
docker volume create -d neonfs --name myvol -o replication=3

# Mount it in a container.
docker run --rm -v myvol:/data alpine sh -c 'echo hi > /data/test && cat /data/test'

# Other containers on the same host share the same FUSE mount —
# the plugin ref-counts mounts and only tears down on the final
# Unmount.
docker run --rm -v myvol:/data alpine ls -la /data

# Tear it down.
docker volume rm myvol
```

`docker volume create -d neonfs` propagates `Opts` straight through to `NeonFS.Core.create_volume/2`, so the same per-volume settings (`replication`, `tier`, `compression`, `encryption`, etc.) apply.

## Health endpoint

The plugin exposes `GET /health` on the same Unix socket as the Docker protocol. It returns the standard NeonFS aggregated health JSON (HTTP 200 healthy, 503 otherwise) covering:

- `:docker_registrar` — service-registry registration
- `:docker_volume_store` — local volume record store
- `:docker_mount_tracker` — ref-counted FUSE mounts

Useful for orchestrator-side liveness checks against the plugin process even though the protocol itself is not designed for HTTP probes.

## Troubleshooting

### `docker volume create` returns "plugin not found"

dockerd discovers plugins by reading `*.spec` / `*.json` / `*.sock` files in `/run/docker/plugins/`, `/etc/docker/plugins/`, and `/usr/lib/docker/plugins/`. The .deb drops `/etc/docker/plugins/neonfs.spec` pointing at the daemon's socket. After installing or restarting:

```bash
sudo systemctl restart neonfs-docker.service
sudo systemctl restart docker             # optional, dockerd polls but a restart is faster
cat /etc/docker/plugins/neonfs.spec       # should contain unix:///run/neonfs/docker.sock
ls -l /run/neonfs/docker.sock             # the socket must exist and be readable by docker
```

If the socket exists but dockerd still can't reach it, check that the daemon's user (`neonfs`) and dockerd can both access `/run/neonfs/`. By default `RuntimeDirectory=neonfs` creates it as `neonfs:neonfs 0755`, which is readable by docker (root); if you've tightened that, dockerd needs read-execute on the directory and read-write on the socket.

### `Mount` returns `mount failed: ...`

Check the `neonfs_docker` daemon logs (`journalctl -u neonfs-docker -n 100`) and verify:

- The configured `:fuse_node` is reachable (`Node.list/0` includes it).
- `NeonFS.FUSE.MountManager` is running on that node.
- The mount root (`/var/lib/neonfs-docker/mounts/`) is writable.

### Stale mounts after a plugin crash

The plugin's `MountTracker.terminate/2` unmounts every active volume on shutdown, but a hard `kill -9` skips that. After a restart, leftover mounts will return `ENODEV`; clean them up with `fusermount3 -uz <path>` and `docker volume rm <name>`. The plugin will re-create them on the next `docker run`.

## See also

- Issue: [#243 — Implement Docker/Podman VolumeDriver plugin](https://harton.dev/project-neon/neonfs/issues/243)
- Reference: [Docker VolumeDriver plugin protocol](https://docs.docker.com/engine/extend/plugins_volume/)
