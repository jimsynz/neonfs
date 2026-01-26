# Deployment and CLI

This document describes the deployment architecture, CLI implementation, and system integration.

## Architecture Overview

NeonFS is deployed as a self-contained daemon with a separate Rust-based CLI for fast interaction.

```
┌─────────────────┐         ┌──────────────────────────────────┐
│   Rust CLI      │         │     Elixir Daemon (BEAM)         │
│   (neonfs)      │         │     (neonfs@localhost)           │
│                 │         │                                  │
│  ┌───────────┐  │  EPMD   │  ┌────────────────────────────┐  │
│  │ erl_rpc   │──┼─ port ──┼─▶│  NeonFS.CLI.Handler        │  │
│  │ client    │  │  4369   │  │  (RPC interface module)    │  │
│  └───────────┘  │         │  └────────────────────────────┘  │
│        │        │         │              │                   │
│        │        │  dist   │              ▼                   │
│        └────────┼─ port ──┼─▶ NeonFS.Volume.Registry        │
│                 │  ~9xxx  │   NeonFS.Cluster                 │
│                 │         │   NeonFS.Fuse                    │
└─────────────────┘         └──────────────────────────────────┘
```

**Design rationale:**

- **Rust CLI for fast startup**: BEAM startup is ~1-3 seconds; Rust CLI connects in 10-70ms
- **Erlang distribution**: Native to the Elixir ecosystem, no custom protocol needed
- **Local-only by default**: CLI runs on same machine as daemon, bound to localhost

## Server Daemon

The daemon is a standard Elixir release running as a BEAM node:

- **Node name**: `neonfs@localhost` (short name, localhost binding)
- **EPMD registration**: Daemon registers with Erlang Port Mapper Daemon on port 4369
- **Distribution port**: Dynamically assigned by EPMD (typically 9xxx range)

The daemon writes runtime information to well-known locations:

```
/run/neonfs/
├── node_name            # Current node name (e.g., "neonfs@localhost")
├── daemon.pid           # PID file for process management
└── daemon_port          # Optional: cached distribution port
```

## CLI Architecture

The CLI is a separate Rust binary (`neonfs`) using the `erl_dist` and `erl_rpc` crates to communicate with the daemon via Erlang distribution protocol.

**CLI crate structure:**

```
neonfs-cli/
├── Cargo.toml
└── src/
    ├── main.rs           # Entry point, clap argument parsing
    ├── daemon.rs         # DaemonConnection using erl_rpc
    ├── commands/
    │   ├── mod.rs
    │   ├── volume.rs     # volume list, create, delete, info
    │   ├── cluster.rs    # cluster init, join, status
    │   ├── node.rs       # node status, maintenance
    │   └── mount.rs      # mount, unmount, list mounts
    ├── term/
    │   ├── mod.rs
    │   ├── convert.rs    # FromTerm trait implementations
    │   └── types.rs      # VolumeInfo, ClusterInfo, etc.
    ├── output/
    │   ├── mod.rs
    │   ├── table.rs      # Table formatting
    │   └── json.rs       # JSON output mode (--json flag)
    └── error.rs          # CliError enum with user-friendly messages
```

**Connection flow:**

1. Read cookie from `/var/lib/neonfs/.erlang.cookie`
2. Connect to EPMD on `localhost:4369`
3. Query for `neonfs` node to get distribution port
4. Connect to daemon on distribution port
5. Perform Erlang distribution handshake with cookie
6. Call RPC functions on `NeonFS.CLI.Handler` module

**Typical latency:**

| Phase | Time |
|-------|------|
| Read cookie | < 1ms |
| EPMD lookup | 1-5ms |
| TCP connect | < 1ms |
| Distribution handshake | 5-10ms |
| RPC call | 1-50ms |
| **Total** | **10-70ms** |

## Communication Protocol

The CLI communicates with the daemon using Erlang's distribution protocol via the `erl_rpc` Rust crate. This enables calling Elixir functions directly.

**Daemon-side RPC interface:**

```elixir
defmodule NeonFS.CLI.Handler do
  @moduledoc """
  RPC handler for CLI commands. All functions return tagged tuples
  for easy pattern matching on the Rust side.
  """

  @spec list_volumes() :: {:ok, [map()]} | {:error, term()}
  def list_volumes do
    case NeonFS.Volume.Registry.list() do
      volumes when is_list(volumes) ->
        {:ok, Enum.map(volumes, &volume_to_map/1)}
      error ->
        {:error, error}
    end
  end

  @spec cluster_status() :: {:ok, map()}
  def cluster_status do
    {:ok, %{
      name: cluster_name(),
      nodes: node_statuses(),
      volumes: volume_summaries(),
      healthy: cluster_healthy?()
    }}
  end

  @spec mount(String.t(), String.t(), map()) :: {:ok, map()} | {:error, term()}
  def mount(volume_name, mount_point, opts) do
    NeonFS.Fuse.mount(volume_name, mount_point, opts)
  end

  # ... additional operations
end
```

**Response format:**

All functions return:
- `{:ok, data}` for success (data is a map or list of maps)
- `{:error, reason}` for failure (reason is an atom or string)
- `:ok` for void operations

This makes Rust-side pattern matching straightforward after term conversion.

## Cookie Management

Authentication uses Erlang's standard cookie mechanism.

**Cookie location:** `/var/lib/neonfs/.erlang.cookie`
**Permissions:** `0600`, owned by `neonfs` user

**Flow:**

1. During `neonfs cluster init`, daemon generates a cryptographically secure random cookie
2. Cookie is written to `/var/lib/neonfs/.erlang.cookie` with mode `0600`
3. CLI reads cookie from this file before connecting
4. CLI must run as the same user as daemon (or root) to read the cookie

This mirrors how `rabbitmqctl`, `riak-admin`, and other Erlang-based tools handle authentication.

## Error Handling

The CLI provides clear, actionable error messages:

| Scenario | Message |
|----------|---------|
| EPMD not running | "Cannot connect to NeonFS daemon. Is the service running?\n  Try: systemctl status neonfs" |
| Daemon not registered | "NeonFS daemon not found. The service may be starting.\n  Try again in a few seconds" |
| Cookie permission denied | "Permission denied reading cookie.\n  Run as neonfs user or use sudo" |
| Cookie not found | "NeonFS not initialised.\n  Run: neonfs cluster init" |
| Connection timeout | "Connection timed out. The daemon may be overloaded" |

## Directory Layout

```
/etc/neonfs/
└── daemon.conf          # Daemon configuration (node name, listen addresses)

/var/lib/neonfs/
├── .erlang.cookie       # Cookie file (mode 0600)
├── data/                # Chunk storage
│   ├── hot/
│   ├── warm/
│   └── cold/
├── meta/
│   └── node.json        # Node identity
└── wal/                 # Write-ahead log

/run/neonfs/
├── node_name            # Runtime node name (for CLI discovery)
└── daemon.pid           # PID file
```

## systemd Integration

```ini
# /etc/systemd/system/neonfs.service
[Unit]
Description=NeonFS Distributed Filesystem Daemon
After=network.target
Wants=epmd.service

[Service]
Type=notify
User=neonfs
Group=neonfs
ExecStart=/usr/bin/neonfs-daemon start
ExecStop=/usr/bin/neonfs-daemon stop
Restart=on-failure
RestartSec=5

Environment=RELEASE_NODE=neonfs@localhost
Environment=RELEASE_COOKIE_PATH=/var/lib/neonfs/.erlang.cookie
Environment=NEONFS_DATA_DIR=/var/lib/neonfs

RuntimeDirectory=neonfs
StateDirectory=neonfs

[Install]
WantedBy=multi-user.target
```

**Service management:**

```bash
# Start/stop
sudo systemctl start neonfs
sudo systemctl stop neonfs

# Check status
sudo systemctl status neonfs
journalctl -u neonfs

# Enable at boot
sudo systemctl enable neonfs
```
