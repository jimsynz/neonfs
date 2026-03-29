# NeonFS Packaging

This directory contains packaging and deployment files for NeonFS.

## Architecture

NeonFS consists of several separate Erlang applications that can be deployed independently:

- **neonfs_core**: Storage daemon that manages data chunks, metadata, and cluster coordination
- **neonfs_fuse**: FUSE filesystem daemon that provides POSIX filesystem access via FUSE mounts
- **neonfs_nfs**: NFSv3 server daemon that provides NFS-compatible access
- **neonfs_omnibus**: All-in-one daemon that runs core, FUSE, and NFS in a single BEAM node

The separate services communicate via Erlang distribution using a shared cookie for authentication.

## Installing from the Debian Repository

NeonFS packages are published to the Forgejo Debian repository for amd64 and arm64.

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
# Individual services (for split deployments)
sudo apt install neonfs-core    # Storage node
sudo apt install neonfs-fuse    # FUSE mount node
sudo apt install neonfs-nfs     # NFS server node

# All-in-one (for single-node deployments)
sudo apt install neonfs-omnibus
```

All daemon packages automatically install `neonfs-cli` as a dependency. The `neonfs-omnibus` package conflicts with the individual service packages and vice versa.

## Directory Structure

```
packaging/
├── nfpm/                  # nfpm package configurations
│   ├── neonfs-cli.yaml
│   ├── neonfs-core.yaml
│   ├── neonfs-fuse.yaml
│   ├── neonfs-nfs.yaml
│   └── neonfs-omnibus.yaml
├── systemd/               # systemd integration files
│   ├── neonfs-core.service      # systemd unit for core storage daemon
│   ├── neonfs-fuse.service      # systemd unit for FUSE daemon
│   ├── neonfs-nfs.service       # systemd unit for NFS daemon
│   ├── neonfs-omnibus.service   # systemd unit for omnibus daemon
│   ├── neonfs.target            # systemd target to start all services
│   ├── neonfs-core-daemon       # Core daemon wrapper script
│   ├── neonfs-fuse-daemon       # FUSE daemon wrapper script
│   ├── neonfs-nfs-daemon        # NFS daemon wrapper script
│   ├── neonfs-omnibus-daemon    # Omnibus daemon wrapper script
│   └── neonfs.conf              # Environment configuration
├── scripts/               # Installation scripts
│   ├── pre-install.sh           # Create user and directories
│   ├── post-install.sh          # Reload systemd
│   ├── pre-remove.sh            # Stop services before removal
│   ├── post-remove.sh           # Clean up on purge
│   └── validate-systemd.sh      # Validate systemd unit files
├── build-debs.sh          # Build .deb packages locally
└── README.md
```

## Deployment Scenarios

### Single-Node Deployment (Omnibus)

All services run in a single BEAM node:

```bash
sudo apt install neonfs-omnibus
sudo systemctl enable --now neonfs-omnibus
```

Node name: `neonfs@localhost`

### Single-Node Deployment (Split Services)

Core, FUSE, and NFS as separate processes on the same host:

```bash
sudo apt install neonfs-core neonfs-fuse neonfs-nfs
sudo systemctl enable --now neonfs.target
```

Node names:
- Core: `neonfs_core@localhost`
- FUSE: `neonfs_fuse@localhost`
- NFS: `neonfs_nfs@localhost`

### Multi-Node Deployment

Core and access services run on different hosts.

**Storage node** (core only):
```bash
sudo apt install neonfs-core
sudo systemctl enable --now neonfs-core
```

**Mount/access node** (FUSE and/or NFS, connects to remote core):
```bash
sudo apt install neonfs-fuse neonfs-nfs

# Update /etc/neonfs/neonfs.conf:
# NEONFS_CORE_NODE=neonfs_core@storage-host.domain

sudo systemctl enable --now neonfs-fuse
sudo systemctl enable --now neonfs-nfs
```

Ensure:
- Both nodes have the same Erlang cookie at `/var/lib/neonfs/.erlang.cookie`
- Erlang distribution ports are accessible (EPMD port 4369 + distribution ports 9100-9155)

## Service Management

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

### Configuration Override

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

## Building Packages Locally

```bash
# Install nfpm (https://nfpm.goreleaser.com/)
# Then:
VERSION=0.1.0 ./packaging/build-debs.sh

# Output in ./dist/
ls dist/*.deb
```

## Filesystem Layout

| Path | Purpose |
|------|---------|
| `/usr/lib/neonfs/{core,fuse,nfs,omnibus}/` | Release files (BEAM + NIFs) |
| `/usr/bin/neonfs` | CLI binary |
| `/usr/bin/neonfs-*-daemon` | Daemon wrapper scripts |
| `/etc/neonfs/neonfs.conf` | Environment configuration |
| `/var/lib/neonfs/` | Data directory (blobs, metadata, WAL) |
| `/var/lib/neonfs/.erlang.cookie` | Erlang distribution cookie |
| `/run/neonfs/` | Runtime state (PID files, node names) |

## Security Considerations

### Erlang Cookie

The Erlang cookie at `/var/lib/neonfs/.erlang.cookie` authenticates node-to-node communication:

- Mode 600 (owner read/write only), owned by `neonfs:neonfs`
- Generated automatically by the core daemon on first start
- For multi-node deployments, copy the cookie to all nodes securely
- Never commit to version control

### FUSE Capabilities

The FUSE and omnibus services require `CAP_SYS_ADMIN` for mounting filesystems. This is configured via `AmbientCapabilities` in their systemd units.

### Network Security

For multi-node deployments:
- Erlang distribution traffic is unencrypted by default
- Restrict EPMD (port 4369) and distribution ports (9100-9155) with firewall rules
- Consider VPN or WireGuard for node-to-node communication
