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
│   ├── node.json        # Node identity
│   └── cluster.json     # Cluster membership (survives restarts)
└── wal/                 # Write-ahead log

/run/neonfs/
├── node_name            # Runtime node name (for CLI discovery)
└── daemon.pid           # PID file
```

## Cluster Bootstrap and Discovery

NeonFS clusters require explicit initialisation and peer discovery. This section describes how nodes bootstrap, join clusters, and reconnect after restarts.

### Cluster Initialisation

The first node creates the cluster:

```bash
$ neonfs cluster init --name home-lab
Initialising new cluster 'home-lab'...
- Generated cluster ID: clust_8x7k9m2p
- Generated master key (store securely!)
- Initialised Ra cluster with single node
- Cluster ready.

Create invite tokens with: neonfs cluster create-invite
```

```elixir
defmodule NeonFS.Cluster.Init do
  def init_cluster(name) do
    if cluster_state_exists?() do
      {:error, :already_initialised}
    else
      node_address = get_configured_node_address()
      cluster_id = generate_cluster_id()

      # Persist cluster identity
      save_cluster_state(%ClusterState{
        id: cluster_id,
        name: name,
        created_at: DateTime.utc_now(),
        this_node: %{
          name: node_address,
          id: generate_node_id()
        },
        known_peers: [],
        ra_cluster_members: [node_address]
      })

      # Bootstrap Ra with single node
      Ra.start_cluster(:neonfs_meta, [node_address])

      {:ok, cluster_id}
    end
  end
end
```

### Persistent Cluster State

Cluster membership is persisted locally to survive restarts:

```json
// /var/lib/neonfs/meta/cluster.json
{
  "cluster_id": "clust_8x7k9m2p",
  "cluster_name": "home-lab",
  "this_node": {
    "id": "node_3k8x9m2p",
    "name": "neonfs@node1.tail1234.ts.net",
    "joined_at": "2024-01-15T10:30:00Z"
  },
  "known_peers": [
    {"id": "node_7j2k8m1p", "name": "neonfs@node2.tail1234.ts.net", "last_seen": "2024-01-20T14:22:00Z"},
    {"id": "node_9x3k2m5p", "name": "neonfs@node3.tail1234.ts.net", "last_seen": "2024-01-20T14:22:00Z"}
  ],
  "ra_cluster_members": [
    "neonfs@node1.tail1234.ts.net",
    "neonfs@node2.tail1234.ts.net",
    "neonfs@node3.tail1234.ts.net"
  ]
}
```

### Node Startup and Reconnection

On startup, the daemon uses persisted state to reconnect:

```elixir
defmodule NeonFS.Cluster.Startup do
  def start do
    case load_cluster_state() do
      {:ok, state} ->
        reconnect_to_cluster(state)

      {:error, :not_found} ->
        # Not initialised - wait for cluster init or join
        {:ok, :awaiting_init}
    end
  end

  defp reconnect_to_cluster(state) do
    # Try to connect to known peers
    connected = state.known_peers
      |> Enum.map(fn peer -> {peer, try_connect(peer.name)} end)
      |> Enum.filter(fn {_peer, result} -> result == :ok end)

    case connected do
      [] ->
        handle_no_peers_reachable(state)

      _peers ->
        # Connected to some peers - rejoin Ra cluster
        rejoin_ra_cluster(state.ra_cluster_members)

        # Update peer list from Ra (authoritative source)
        sync_peers_from_ra()

        {:ok, :reconnected}
    end
  end

  defp handle_no_peers_reachable(state) do
    Logger.warning("No cluster peers reachable",
      known_peers: state.known_peers)

    # Don't serve requests until we can confirm cluster state
    # (prevents serving stale data during network partition)
    {:ok, :isolated, state.known_peers}
  end
end
```

### Join Flow with Persistence

When joining a cluster, peer information is persisted:

```elixir
def join_cluster(token, cluster_address) do
  # Connect to existing cluster node
  {:ok, cluster_info} = RPC.call(
    cluster_address,
    NeonFS.Cluster,
    :join_with_token,
    [token, Node.self()]
  )

  # Persist locally for restart recovery
  save_cluster_state(%ClusterState{
    id: cluster_info.cluster_id,
    name: cluster_info.cluster_name,
    this_node: %{
      id: cluster_info.assigned_node_id,
      name: Node.self(),
      joined_at: DateTime.utc_now()
    },
    known_peers: cluster_info.peers,
    ra_cluster_members: cluster_info.ra_members
  })

  # Join Ra consensus group
  Ra.add_member(:neonfs_meta, Node.self())

  {:ok, cluster_info}
end
```

### Peer List Synchronisation

The local peer list is periodically synced with Ra (the authoritative source):

```elixir
defmodule NeonFS.Cluster.PeerSync do
  use GenServer

  @sync_interval :timer.minutes(5)

  def handle_info(:sync, state) do
    # Get authoritative peer list from Ra
    case Ra.members(:neonfs_meta) do
      {:ok, ra_members} ->
        update_known_peers(ra_members)
        persist_cluster_state()

      {:error, _} ->
        # Ra unavailable - keep existing peer list
        :ok
    end

    schedule_sync()
    {:noreply, state}
  end
end
```

### Node Addressing

For multi-node clusters, nodes need addressable names. Use stable DNS names when possible:

```yaml
# /etc/neonfs/daemon.conf
cluster:
  # Tailscale MagicDNS (recommended - stable even if IP changes)
  node_address: neonfs@mynode.tail1234.ts.net

  # Or Headscale
  # node_address: neonfs@mynode.headscale.example.com

  # Or static IP (less resilient to network changes)
  # node_address: neonfs@192.168.1.10
```

**Why stable names matter:**

BEAM distribution uses node names for identity. If `neonfs@192.168.1.10` restarts as `neonfs@192.168.1.11` (DHCP change), the cluster sees it as a different node. Using Tailscale/Headscale DNS names avoids this issue.

### Handling Isolated Nodes

When a node can't reach any peers:

| Scenario | Behaviour |
|----------|-----------|
| Network partition | Node goes read-only; won't serve writes that need quorum |
| All other nodes down | Same as partition - can't distinguish |
| Single-node cluster | Normal operation (if `min_peers_for_operation: 0`) |

```elixir
defmodule NeonFS.Cluster.HealthCheck do
  def can_serve_writes? do
    connected_peers = count_connected_peers()
    min_required = Application.get_env(:neonfs, :min_peers_for_operation, 1)

    connected_peers >= min_required or single_node_cluster?()
  end
end
```

### CLI Commands

```bash
# Initialise new cluster (first node only)
$ neonfs cluster init --name home-lab
Cluster 'home-lab' initialised.
Cluster ID: clust_8x7k9m2p

# Check cluster status
$ neonfs cluster status
Cluster: home-lab (clust_8x7k9m2p)
This node: neonfs@node1.tail1234.ts.net (online)
Peers:
  neonfs@node2.tail1234.ts.net - online (last seen: 2s ago)
  neonfs@node3.tail1234.ts.net - online (last seen: 1s ago)
Ra consensus: healthy (leader: node2)

# List known peers
$ neonfs cluster peers
NAME                            STATUS    LAST SEEN
neonfs@node1.tail1234.ts.net    online    (this node)
neonfs@node2.tail1234.ts.net    online    2s ago
neonfs@node3.tail1234.ts.net    offline   3h ago

# Force peer list refresh from Ra
$ neonfs cluster sync-peers
Synchronised 3 peers from Ra consensus.

# Create invite for new node
$ neonfs cluster create-invite --expires 1h
Invite token: nfs_inv_7x8k2m9p...

# Join existing cluster (on new node)
$ neonfs cluster join --token nfs_inv_7x8k2m9p... --via neonfs@node1.tail1234.ts.net
Joined cluster 'home-lab'.
```

### Configuration

```yaml
# /etc/neonfs/daemon.conf
cluster:
  # This node's address (required for multi-node)
  node_address: neonfs@mynode.tail1234.ts.net

  # How often to sync peer list from Ra
  peer_sync_interval: 5m

  # Connection timeout when trying to reach peers on startup
  peer_connect_timeout: 10s

  # Minimum connected peers to serve write requests
  # Set to 0 for single-node clusters
  min_peers_for_operation: 1

  # How long to wait for peers before giving up on startup
  startup_peer_timeout: 30s
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
