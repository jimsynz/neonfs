# Task 0043: Split systemd Units for Core and FUSE

## Status

Complete

## Phase

1 (Foundation Addendum - must complete before Phase 2)

## Description

Currently there is a single `neonfs.service` systemd unit, but per the architecture spec, neonfs_core and neonfs_fuse are separate Erlang applications with separate releases. They should have separate systemd units with proper dependencies.

This matches the release configuration which defines two separate releases:
- `neonfs_core` release
- `neonfs_fuse` release

## Acceptance Criteria

- [x] `neonfs-core.service` unit for neonfs_core release
- [x] `neonfs-fuse.service` unit for neonfs_fuse release
- [x] `neonfs-fuse.service` has `After=neonfs-core.service` and `Requires=neonfs-core.service`
- [x] Separate daemon wrapper scripts for each service
- [x] Each service has its own node name (e.g., `neonfs_core@localhost`, `neonfs_fuse@localhost`)
- [x] Both share the same Erlang cookie for inter-node communication
- [x] FUSE service has additional capability for FUSE mounting (`AmbientCapabilities=CAP_SYS_ADMIN`)
- [x] `neonfs.target` groups both services for convenience (`systemctl start neonfs.target`)
- [x] Configuration file updated with both node names
- [x] Documentation for running single-node (both services) vs split deployment

## Implementation Notes

### neonfs-core.service

```ini
[Unit]
Description=NeonFS Core Storage Daemon
After=network.target
Wants=epmd.service
Documentation=https://neonfs.dev/docs

[Service]
Type=notify
User=neonfs
Group=neonfs
ExecStart=/usr/bin/neonfs-core-daemon start
ExecStop=/usr/bin/neonfs-core-daemon stop
Restart=on-failure
RestartSec=5

Environment=RELEASE_NODE=neonfs_core@localhost
Environment=RELEASE_COOKIE_PATH=/var/lib/neonfs/.erlang.cookie
Environment=NEONFS_DATA_DIR=/var/lib/neonfs

RuntimeDirectory=neonfs
StateDirectory=neonfs
ConfigurationDirectory=neonfs

# Security hardening
NoNewPrivileges=true
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/neonfs

[Install]
WantedBy=multi-user.target
```

### neonfs-fuse.service

```ini
[Unit]
Description=NeonFS FUSE Filesystem Daemon
After=network.target neonfs-core.service
Requires=neonfs-core.service
Documentation=https://neonfs.dev/docs

[Service]
Type=notify
User=neonfs
Group=neonfs
ExecStart=/usr/bin/neonfs-fuse-daemon start
ExecStop=/usr/bin/neonfs-fuse-daemon stop
Restart=on-failure
RestartSec=5

Environment=RELEASE_NODE=neonfs_fuse@localhost
Environment=RELEASE_COOKIE_PATH=/var/lib/neonfs/.erlang.cookie
Environment=NEONFS_CORE_NODE=neonfs_core@localhost

RuntimeDirectory=neonfs
StateDirectory=neonfs

# FUSE requires CAP_SYS_ADMIN for mounting
AmbientCapabilities=CAP_SYS_ADMIN
NoNewPrivileges=false
PrivateTmp=true
ProtectSystem=strict
ProtectHome=true
ReadWritePaths=/var/lib/neonfs /mnt

[Install]
WantedBy=multi-user.target
```

### neonfs.target

```ini
[Unit]
Description=NeonFS Distributed Filesystem
Documentation=https://neonfs.dev/docs
After=network.target

[Install]
WantedBy=multi-user.target
Requires=neonfs-core.service neonfs-fuse.service
```

### Handler Update for RPC

The FUSE handler currently checks `Code.ensure_loaded?` which only works if both apps are in the same node. Update to use `:rpc.call`:

```elixir
# In neonfs_core/lib/neon_fs/cli/handler.ex
defp with_fuse_manager(fun) do
  fuse_node = Application.get_env(:neonfs_core, :fuse_node, :"neonfs_fuse@localhost")
  case :rpc.call(fuse_node, NeonFS.FUSE.MountManager, :__info__, [:module]) do
    {:badrpc, _} -> {:error, :fuse_not_available}
    _ -> fun.(fuse_node)
  end
end
```

## Testing Strategy

1. Unit test: verify systemd unit files are valid (`systemd-analyze verify`)
2. Integration test: start both services, verify they connect
3. Test FUSE mounts work when both services running
4. Test core survives if FUSE service crashes
5. Test `systemctl start neonfs.target` brings up both

## Dependencies

- Task 0028 (systemd integration) - Complete
- Task 0029 (Elixir release) - Complete

## Files to Create/Modify

- `packaging/systemd/neonfs-core.service` (new)
- `packaging/systemd/neonfs-fuse.service` (new)
- `packaging/systemd/neonfs.target` (new)
- `packaging/systemd/neonfs-core-daemon` (new)
- `packaging/systemd/neonfs-fuse-daemon` (new)
- `packaging/systemd/neonfs.conf` (update with both node names)
- `packaging/systemd/neonfs.service` (delete or keep as alias)
- `packaging/systemd/neonfs-daemon` (delete or keep as alias)
- `neonfs_core/lib/neon_fs/cli/handler.ex` (update with_fuse_manager)
- `neonfs_core/config/runtime.exs` (add fuse_node config)

## Reference

- spec/architecture.md - separate Erlang nodes section
- systemd.unit(5) man page
- systemd.target(5) man page
