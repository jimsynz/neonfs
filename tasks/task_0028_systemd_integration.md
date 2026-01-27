# Task 0028: Implement systemd Integration

## Status
Not Started

## Phase
1 - Foundation

## Description
Create systemd unit files and supporting scripts for running NeonFS as a system service. This includes the main daemon service, proper dependencies, and integration with systemd features.

## Acceptance Criteria
- [ ] `neonfs.service` systemd unit file
- [ ] Service runs as `neonfs` user
- [ ] Proper After/Wants dependencies
- [ ] Type=notify for proper startup signalling
- [ ] RuntimeDirectory and StateDirectory configured
- [ ] Environment variables for BEAM configuration
- [ ] Restart on failure with backoff
- [ ] `neonfs-daemon` wrapper script for release commands
- [ ] Pre-start script creates data directories if needed
- [ ] Graceful shutdown via ExecStop

## systemd Unit File
```ini
# /etc/systemd/system/neonfs.service
[Unit]
Description=NeonFS Distributed Filesystem Daemon
After=network.target
Documentation=https://neonfs.dev/docs

[Service]
Type=notify
User=neonfs
Group=neonfs
ExecStart=/usr/bin/neonfs-daemon start
ExecStop=/usr/bin/neonfs-daemon stop
Restart=on-failure
RestartSec=5
TimeoutStartSec=60
TimeoutStopSec=30

Environment=RELEASE_NODE=neonfs@localhost
Environment=RELEASE_COOKIE_PATH=/var/lib/neonfs/.erlang.cookie
Environment=NEONFS_DATA_DIR=/var/lib/neonfs

RuntimeDirectory=neonfs
StateDirectory=neonfs
ConfigurationDirectory=neonfs

[Install]
WantedBy=multi-user.target
```

## Testing Strategy
- Install service on test system
- `systemctl start neonfs` succeeds
- `systemctl status neonfs` shows active
- `systemctl stop neonfs` graceful shutdown
- Service restarts after crash
- Logs visible via `journalctl -u neonfs`

## Dependencies
- task_0026_elixir_supervision_tree
- task_0027_fuse_supervision_tree

## Files to Create
- `packaging/systemd/neonfs.service`
- `packaging/systemd/neonfs-daemon` (wrapper script)
- `packaging/systemd/neonfs.conf` (environment file)
- `packaging/scripts/pre-install.sh` (create user/dirs)

## Reference
- spec/deployment.md - systemd Integration section
- spec/deployment.md - Directory Layout

## Notes
The actual Elixir release configuration is a separate task. This focuses on the systemd integration assuming a release exists.
