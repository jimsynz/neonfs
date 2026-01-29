# NeonFS Packaging

This directory contains packaging and deployment files for NeonFS.

## Architecture

NeonFS consists of two separate Erlang applications that can be deployed independently:

- **neonfs_core**: Storage daemon that manages data chunks, metadata, and cluster coordination
- **neonfs_fuse**: FUSE filesystem daemon that provides POSIX filesystem access via FUSE mounts

The two services communicate via Erlang distribution using a shared cookie for authentication.

## Directory Structure

```
packaging/
├── systemd/           # systemd integration files
│   ├── neonfs-core.service    # systemd unit for core storage daemon
│   ├── neonfs-fuse.service    # systemd unit for FUSE daemon
│   ├── neonfs.target          # systemd target to start both services
│   ├── neonfs-core-daemon     # Core daemon wrapper script
│   ├── neonfs-fuse-daemon     # FUSE daemon wrapper script
│   └── neonfs.conf            # Environment configuration
└── scripts/           # Installation scripts
    ├── pre-install.sh         # Pre-installation setup script
    └── validate-systemd.sh    # Validate systemd unit files
```

## Deployment Scenarios

### Single-Node Deployment (Recommended for Phase 1)

Both services run on the same host, communicating via localhost:

```bash
# Start both services
sudo systemctl start neonfs.target

# Or start individually
sudo systemctl start neonfs-core
sudo systemctl start neonfs-fuse
```

Node names:
- Core: `neonfs_core@localhost`
- FUSE: `neonfs_fuse@localhost`

### Split Deployment (Future)

Core and FUSE services run on different hosts:

**Storage node** (runs core only):
```bash
sudo systemctl start neonfs-core
```

**Compute/mount node** (runs FUSE only, connects to remote core):
```bash
# Update /etc/neonfs/neonfs.conf:
# NEONFS_CORE_NODE=neonfs_core@storage-host.domain
sudo systemctl start neonfs-fuse
```

Ensure:
- Both nodes have the same Erlang cookie at `/var/lib/neonfs/.erlang.cookie`
- Erlang distribution ports are accessible (default: dynamic, configure EPMD port 4369 + distribution ports 9100-9155)

## systemd Integration

### Validation

Validate systemd unit files before deployment:

```bash
./scripts/validate-systemd.sh
```

This uses `systemd-analyze verify` to check for common configuration issues. The script gracefully skips validation if systemd is not available.

### Service Files

#### neonfs-core.service
systemd unit file for the core storage daemon.

Features:
- Type=notify for proper startup signalling
- Automatic restart on failure with 5-second backoff
- Security hardening with ProtectSystem, ProtectHome, NoNewPrivileges
- Manages data, metadata, and WAL directories

#### neonfs-fuse.service
systemd unit file for the FUSE filesystem daemon.

Features:
- Requires and starts after neonfs-core.service
- AmbientCapabilities=CAP_SYS_ADMIN for FUSE mounting
- Connects to core service via Erlang distribution

#### neonfs.target
systemd target that groups both services for convenient management.

Usage:
```bash
sudo systemctl start neonfs.target    # Start both services
sudo systemctl stop neonfs.target     # Stop both services
sudo systemctl enable neonfs.target   # Enable both at boot
```

#### Daemon Wrapper Scripts

**neonfs-core-daemon**: Core service wrapper
- Ensures data directories exist
- Generates Erlang cookie if missing
- Writes runtime information for CLI discovery

**neonfs-fuse-daemon**: FUSE service wrapper
- Verifies cookie exists (created by core)
- Writes runtime information
- Connects to core via NEONFS_CORE_NODE environment variable

#### neonfs.conf
Environment configuration file with default settings for both services.

Key variables:
- `NEONFS_CORE_NODE`: Core service node name
- `NEONFS_FUSE_NODE`: FUSE service node name
- `RELEASE_COOKIE_PATH`: Shared cookie path for authentication
- `NEONFS_DATA_DIR`: Data storage directory

### Installation Steps

#### Single-Node Installation (Both Services)

1. **Run pre-installation script** (as root):
   ```bash
   sudo ./scripts/pre-install.sh
   ```

2. **Install daemon wrappers**:
   ```bash
   sudo cp systemd/neonfs-core-daemon /usr/bin/
   sudo cp systemd/neonfs-fuse-daemon /usr/bin/
   sudo chmod 755 /usr/bin/neonfs-*-daemon
   ```

3. **Install systemd unit files**:
   ```bash
   sudo cp systemd/neonfs-core.service /etc/systemd/system/
   sudo cp systemd/neonfs-fuse.service /etc/systemd/system/
   sudo cp systemd/neonfs.target /etc/systemd/system/
   sudo chmod 644 /etc/systemd/system/neonfs-*
   sudo systemctl daemon-reload
   ```

4. **Install environment configuration**:
   ```bash
   sudo cp systemd/neonfs.conf /etc/neonfs/
   sudo chmod 644 /etc/neonfs/neonfs.conf
   ```

5. **Install Elixir releases**:
   ```bash
   # After building: MIX_ENV=prod mix release
   sudo mkdir -p /usr/lib/neonfs/core
   sudo mkdir -p /usr/lib/neonfs/fuse

   # Extract core release
   sudo tar -xzf neonfs_core/_build/prod/neonfs_core-*.tar.gz -C /usr/lib/neonfs/core

   # Extract fuse release
   sudo tar -xzf neonfs_fuse/_build/prod/neonfs_fuse-*.tar.gz -C /usr/lib/neonfs/fuse

   sudo chown -R root:root /usr/lib/neonfs
   ```

6. **Enable and start services**:
   ```bash
   sudo systemctl enable neonfs.target
   sudo systemctl start neonfs.target
   ```

#### Storage-Only Installation (Core Only)

For nodes that only provide storage (no FUSE mounts):

```bash
# Steps 1-5 same as above, but skip FUSE-related files
sudo systemctl enable neonfs-core
sudo systemctl start neonfs-core
```

#### Mount-Only Installation (FUSE Only)

For compute nodes that mount remote storage:

1. Install FUSE daemon wrapper and service unit
2. Update `/etc/neonfs/neonfs.conf` with remote core node name
3. Copy Erlang cookie from storage node to `/var/lib/neonfs/.erlang.cookie`
4. Ensure network connectivity to storage node
5. Start FUSE service: `sudo systemctl start neonfs-fuse`

### Service Management

#### Single-Node Commands

```bash
# Start all services
sudo systemctl start neonfs.target

# Stop all services
sudo systemctl stop neonfs.target

# Restart all services
sudo systemctl restart neonfs.target

# Check status
sudo systemctl status neonfs-core
sudo systemctl status neonfs-fuse

# View logs
sudo journalctl -u neonfs-core -f
sudo journalctl -u neonfs-fuse -f
```

#### Individual Service Commands

```bash
# Start/stop individual services
sudo systemctl start neonfs-core
sudo systemctl start neonfs-fuse
sudo systemctl stop neonfs-core
sudo systemctl stop neonfs-fuse

# Note: Stopping core will affect FUSE (FUSE depends on core)
```

### Configuration Override

Override default configuration using systemd drop-in files:

```bash
# For core service
sudo mkdir -p /etc/systemd/system/neonfs-core.service.d
sudo nano /etc/systemd/system/neonfs-core.service.d/override.conf
```

Example override:
```ini
[Service]
Environment=NEONFS_DATA_DIR=/mnt/storage/neonfs
Environment=RELEASE_LOG_LEVEL=debug
```

Then reload:
```bash
sudo systemctl daemon-reload
sudo systemctl restart neonfs-core
```

## Security Considerations

### Erlang Cookie

The Erlang cookie at `/var/lib/neonfs/.erlang.cookie` is used for authentication between nodes. Keep it secure:

- Mode: 600 (owner read/write only)
- Owner: neonfs:neonfs
- Never commit to version control
- Use unique cookie per cluster
- For split deployments, securely copy cookie to all nodes

### FUSE Capabilities

The FUSE service requires `CAP_SYS_ADMIN` capability for mounting filesystems. This is configured via `AmbientCapabilities` in the service unit. Ensure FUSE service is properly isolated:

- Runs as non-privileged neonfs user
- NoNewPrivileges=false (required for FUSE, but limits privilege escalation)
- ProtectSystem=strict, ProtectHome=true for filesystem protection

### Network Security

For split deployments:
- Erlang distribution traffic is not encrypted by default
- Use firewall rules to restrict access to EPMD (port 4369) and distribution ports (9100-9155)
- Consider VPN or WireGuard for multi-node communication
- Future: SSL distribution support for encrypted node-to-node communication

## Troubleshooting

### Services won't start

Check logs:
```bash
sudo journalctl -u neonfs-core -n 50
sudo journalctl -u neonfs-fuse -n 50
```

Common issues:
- Cookie permissions incorrect: `sudo chmod 600 /var/lib/neonfs/.erlang.cookie`
- Data directory permissions: `sudo chown -R neonfs:neonfs /var/lib/neonfs`
- FUSE not available: Check `/dev/fuse` exists and user in `fuse` group

### FUSE service can't connect to core

1. Verify core service is running: `sudo systemctl status neonfs-core`
2. Check node names match in config: `cat /etc/neonfs/neonfs.conf`
3. Test connectivity: `ping storage-host.domain` (for remote core)
4. Check cookie matches on both nodes
5. Verify EPMD is running: `epmd -names`

### CLI can't connect to daemon

1. Check runtime files: `ls -la /run/neonfs/`
2. Verify cookie exists: `ls -la /var/lib/neonfs/.erlang.cookie`
3. Check node names in runtime files match running services
4. Ensure CLI has access to cookie file

## Development Setup

For development, you can run releases manually without systemd:

```bash
# Terminal 1: Core service
cd neonfs_core
MIX_ENV=prod mix release
RELEASE_NODE=neonfs_core@localhost _build/prod/rel/neonfs_core/bin/neonfs_core start

# Terminal 2: FUSE service
cd neonfs_fuse
MIX_ENV=prod mix release
RELEASE_NODE=neonfs_fuse@localhost NEONFS_CORE_NODE=neonfs_core@localhost _build/prod/rel/neonfs_fuse/bin/neonfs_fuse start

# Terminal 3: CLI
cd neonfs-cli
cargo build --release
./target/release/neonfs cluster status
```

## References

- systemd.service(5): `man systemd.service`
- systemd.unit(5): `man systemd.unit`
- systemd.target(5): `man systemd.target`
- Erlang distribution: https://www.erlang.org/doc/reference_manual/distributed.html
- FUSE documentation: https://github.com/libfuse/libfuse
