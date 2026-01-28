# NeonFS Packaging

This directory contains packaging and deployment files for NeonFS.

## Directory Structure

```
packaging/
├── systemd/           # systemd integration files
│   ├── neonfs.service     # systemd unit file
│   ├── neonfs-daemon      # Daemon wrapper script
│   └── neonfs.conf        # Environment configuration
└── scripts/           # Installation scripts
    └── pre-install.sh     # Pre-installation setup script
```

## systemd Integration

### Files

#### neonfs.service
systemd unit file that defines the NeonFS service. Install to `/etc/systemd/system/neonfs.service`.

Features:
- Type=notify for proper startup signalling
- Automatic restart on failure with 5-second backoff
- RuntimeDirectory and StateDirectory managed by systemd
- Security hardening with ProtectSystem, ProtectHome, NoNewPrivileges

#### neonfs-daemon
Wrapper script that provides systemd-compatible start/stop commands. Install to `/usr/bin/neonfs-daemon`.

Responsibilities:
- Ensures data directories exist with proper structure
- Generates Erlang cookie if missing
- Writes runtime information for CLI discovery
- Invokes Elixir release commands

#### neonfs.conf
Environment configuration file with default settings. Install to `/etc/neonfs/neonfs.conf`.

Override by creating `/etc/systemd/system/neonfs.service.d/override.conf`:
```ini
[Service]
EnvironmentFile=/etc/neonfs/neonfs.conf
```

### Installation Steps

1. **Run pre-installation script** (as root):
   ```bash
   sudo ./scripts/pre-install.sh
   ```
   This creates the neonfs user and necessary directories.

2. **Install the daemon wrapper**:
   ```bash
   sudo cp systemd/neonfs-daemon /usr/bin/
   sudo chmod 755 /usr/bin/neonfs-daemon
   ```

3. **Install the systemd unit file**:
   ```bash
   sudo cp systemd/neonfs.service /etc/systemd/system/
   sudo chmod 644 /etc/systemd/system/neonfs.service
   sudo systemctl daemon-reload
   ```

4. **Install the environment configuration**:
   ```bash
   sudo cp systemd/neonfs.conf /etc/neonfs/
   sudo chmod 644 /etc/neonfs/neonfs.conf
   ```

5. **Install the Elixir release** to `/usr/lib/neonfs/`:
   ```bash
   # After building with: MIX_ENV=prod mix release
   sudo mkdir -p /usr/lib/neonfs
   sudo tar -xzf _build/prod/rel/neonfs/neonfs-*.tar.gz -C /usr/lib/neonfs
   sudo chown -R root:root /usr/lib/neonfs
   ```

6. **Enable and start the service**:
   ```bash
   sudo systemctl enable neonfs
   sudo systemctl start neonfs
   ```

### Service Management

```bash
# Start the service
sudo systemctl start neonfs

# Stop the service
sudo systemctl stop neonfs

# Restart the service
sudo systemctl restart neonfs

# Check status
sudo systemctl status neonfs

# View logs
sudo journalctl -u neonfs -f

# Enable at boot
sudo systemctl enable neonfs

# Disable at boot
sudo systemctl disable neonfs
```

## Directory Layout

After installation, NeonFS uses the following directories:

```
/etc/neonfs/              # Configuration
└── neonfs.conf               # Environment variables

/var/lib/neonfs/          # Persistent data (StateDirectory)
├── .erlang.cookie            # Erlang authentication cookie (mode 0600)
├── data/                     # Chunk storage
│   ├── hot/                  # Hot tier storage
│   ├── warm/                 # Warm tier storage
│   └── cold/                 # Cold tier storage
├── meta/                     # Metadata storage
│   ├── node.json             # Node identity
│   └── cluster.json          # Cluster membership
└── wal/                      # Write-ahead log

/run/neonfs/              # Runtime files (RuntimeDirectory)
├── node_name                 # Current node name (for CLI discovery)
└── daemon.pid                # PID file

/usr/lib/neonfs/          # Release installation
└── bin/neonfs                # Release executable
```

## Configuration

### Single-Node Deployment

For single-node deployments (Phase 1), use the default configuration:

```bash
# /etc/neonfs/neonfs.conf
RELEASE_NODE=neonfs@localhost
```

### Multi-Node Deployment (Phase 2+)

For multi-node clusters, each node needs a unique, addressable name:

```bash
# /etc/neonfs/neonfs.conf
RELEASE_NODE=neonfs@node1.example.com
```

**Recommendations:**
- Use Tailscale/Headscale MagicDNS for stable node names
- Or use static DNS names (not IPs) for resilience
- Ensure all nodes can resolve each other's names

### Environment Variables

The following environment variables can be configured in `neonfs.conf`:

| Variable | Default | Description |
|----------|---------|-------------|
| RELEASE_NODE | neonfs@localhost | Node name (must be unique in cluster) |
| RELEASE_COOKIE_PATH | /var/lib/neonfs/.erlang.cookie | Path to Erlang cookie |
| NEONFS_DATA_DIR | /var/lib/neonfs | Data directory root |
| RELEASE_ROOT | /usr/lib/neonfs | Release installation directory |
| ERL_MAX_PORTS | 65536 | Maximum BEAM ports |
| ERL_MAX_ETS_TABLES | 16384 | Maximum ETS tables |
| RELEASE_LOG_LEVEL | info | Log level (debug, info, warning, error) |

## Troubleshooting

### Service won't start

Check the logs:
```bash
sudo journalctl -u neonfs -n 50
```

Common issues:
- Permission errors: Check that `/var/lib/neonfs` is owned by `neonfs:neonfs`
- FUSE not available: Install fuse3 package
- Port conflicts: Check EPMD (port 4369) is available

### CLI can't connect to daemon

Verify the service is running:
```bash
sudo systemctl status neonfs
```

Check the cookie file exists and is readable:
```bash
sudo ls -la /var/lib/neonfs/.erlang.cookie
```

Verify node name is written:
```bash
cat /run/neonfs/node_name
```

### Permission denied errors

The CLI must run as the `neonfs` user or root to read the cookie:
```bash
sudo -u neonfs neonfs cluster status
# or
sudo neonfs cluster status
```

## Security Considerations

- The `.erlang.cookie` file is mode 0600 and owned by `neonfs:neonfs`
- Only root and the neonfs user can read the cookie
- The service runs with security hardening (NoNewPrivileges, ProtectSystem, ProtectHome)
- Data directory is mode 750, only accessible by neonfs user/group
- systemd manages RuntimeDirectory and StateDirectory permissions

## Future: Package Building

In later phases, these files will be integrated into:
- .deb packages (Debian/Ubuntu)
- .rpm packages (RHEL/CentOS/Fedora)
- Docker containers
- Snap packages

Package-specific hooks will use `pre-install.sh` during installation.
