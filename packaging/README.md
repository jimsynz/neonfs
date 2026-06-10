# NeonFS Packaging

Debian packages and systemd integration for NeonFS — install with
`apt`, manage with `systemctl`, and read logs with `journalctl`, on
amd64 and arm64.

## Packages

| Package | Contains |
|---------|----------|
| `neonfs-core` | Storage daemon: chunks, metadata, cluster coordination |
| `neonfs-fuse` | FUSE filesystem daemon (POSIX mounts) |
| `neonfs-nfs` | NFSv3 server daemon |
| `neonfs-s3` | S3-compatible HTTP API daemon |
| `neonfs-webdav` | WebDAV server daemon |
| `neonfs-docker` | Docker / Podman VolumeDriver plugin daemon |
| `neonfs-containerd` | containerd content-store plugin daemon |
| `neonfs-omnibus` | All of the above in a single BEAM node |
| `neonfs-cli` | The `neonfs` admin CLI (installed automatically by every daemon package) |
| `neonfs-common` | Shared user/directory/configuration plumbing |

The services communicate via TLS Erlang distribution, authenticated by
the cluster's certificate authority. `neonfs-omnibus` conflicts with
the individual service packages and vice versa — pick split services
or omnibus, not both.

## Installing from the Debian repository

### Add the repository

```bash
# Download the repository signing key
curl -fsSL https://harton.dev/api/packages/project-neon/debian/repository.key \
  | sudo tee /etc/apt/keyrings/neonfs.asc > /dev/null

# Add the repository
echo "deb [signed-by=/etc/apt/keyrings/neonfs.asc] https://harton.dev/api/packages/project-neon/debian trixie main" \
  | sudo tee /etc/apt/sources.list.d/neonfs.list

# Update package index
sudo apt update
```

### Install packages

```bash
# All-in-one (for single-node deployments)
sudo apt install neonfs-omnibus

# Or individual services (for split deployments) — install what you need
sudo apt install neonfs-core
sudo apt install neonfs-fuse neonfs-nfs neonfs-s3 neonfs-webdav
sudo apt install neonfs-docker neonfs-containerd
```

## Deployment scenarios

### Single node, everything (omnibus)

All services run in a single BEAM node:

```bash
sudo apt install neonfs-omnibus
sudo systemctl enable --now neonfs-omnibus
```

Node name: `neonfs@localhost`

### Single node, split services

Core plus the interfaces you want, as separate processes on one host:

```bash
sudo apt install neonfs-core neonfs-fuse neonfs-nfs
sudo systemctl enable --now neonfs.target
```

Node names: `neonfs_core@localhost`, `neonfs_fuse@localhost`,
`neonfs_nfs@localhost`, …

### Multi-node

Core and access services run on different hosts.

**Storage node** (core only):
```bash
sudo apt install neonfs-core
sudo systemctl enable --now neonfs-core
```

**Access node** (any mix of interfaces, connecting to a remote core):
```bash
sudo apt install neonfs-fuse neonfs-nfs

# Update /etc/neonfs/neonfs.conf:
# NEONFS_CORE_NODE=neonfs_core@storage-host.domain

sudo systemctl enable --now neonfs-fuse neonfs-nfs
```

Make sure the Erlang distribution ports are reachable between nodes
(EPMD port 4369 + distribution ports 9100-9155). No shared secrets
need copying between hosts — the new node authenticates with a
single-use invite token (`neonfs cluster create-invite` on an existing
node, `neonfs cluster join` on the new one) and receives its
cluster-signed TLS certificate and credentials through the join
exchange.

The [operator guide](../docs/operator-guide.md) covers cluster
bootstrap and node joins from here.

## Service management

```bash
# Start/stop all installed services
sudo systemctl start neonfs.target
sudo systemctl stop neonfs.target

# Individual service control
sudo systemctl start neonfs-core
sudo systemctl status neonfs-core

# View logs
sudo journalctl -u neonfs-core -f
sudo journalctl -u neonfs-fuse -f
```

### Configuration override

Override default configuration using systemd drop-in files:

```bash
sudo systemctl edit neonfs-core
```

Example override:
```ini
[Service]
Environment=NEONFS_DATA_DIR=/mnt/storage/neonfs
Environment=RELEASE_LOG_LEVEL=debug
```

## Building packages locally

```bash
# Install nfpm (https://nfpm.goreleaser.com/)
# Then:
VERSION=0.1.0 ./packaging/build-debs.sh

# Output in ./dist/
ls dist/*.deb
```

## Directory structure

```
packaging/
├── nfpm/                  # nfpm package configurations (one per package above)
├── systemd/               # Units, daemon wrapper scripts, neonfs.target,
│                          # neonfs.conf, TLS helper (neonfs-tls-common.sh)
├── scripts/               # pre/post install/remove + systemd validation
├── build-debs.sh          # Build .deb packages locally
└── README.md
```

## Filesystem layout

| Path | Purpose |
|------|---------|
| `/usr/lib/neonfs/<service>/` | Release files (BEAM + NIFs) per service |
| `/usr/bin/neonfs` | CLI binary |
| `/usr/bin/neonfs-*-daemon` | Daemon wrapper scripts |
| `/etc/neonfs/neonfs.conf` | Environment configuration |
| `/var/lib/neonfs/` | Data directory (blobs, metadata, WAL) |
| `/var/lib/neonfs/tls/` | Local + cluster CA and node certificates |
| `/run/neonfs/` | Runtime state (PID files, node names, plugin sockets) |

## Security considerations

### Cluster-managed TLS

Node-to-node authentication is certificate-based, managed by the
cluster:

- On first boot, the daemon wrapper generates a local CA and node
  certificate, so Erlang distribution runs over TLS before the node
  has joined anything.
- `neonfs cluster init` creates the cluster CA; nodes joining via
  invite token submit a CSR and receive a cluster-signed certificate
  and credentials through the encrypted join exchange — there are no
  shared secrets to distribute by hand.
- Certificates live under `$NEONFS_TLS_DIR` (default
  `/var/lib/neonfs/tls`) and renew automatically.
- Bulk chunk transfer uses a separate mTLS data plane.

(An Erlang cookie file still exists per node as a mechanical
requirement of BEAM distribution, but it is generated and managed by
the daemon and the join flow — never copy it between hosts.)

### Network

For multi-node deployments, restrict EPMD (port 4369) and the
distribution ports (9100-9155) to cluster hosts with firewall rules —
TLS authenticates peers, but there is no reason to expose the ports
more widely.

### FUSE capabilities

The FUSE and omnibus services require `CAP_SYS_ADMIN` for mounting
filesystems. This is configured via `AmbientCapabilities` in their
systemd units.
