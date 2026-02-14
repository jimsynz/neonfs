# Out-of-Band Data Transfer

NeonFS currently routes all inter-node communication — both control plane (Ra consensus, service discovery, metadata RPCs, event notifications) and data plane (chunk replication, chunk retrieval) — over Erlang distribution. This document describes an out-of-band data transfer layer that separates bulk chunk data onto dedicated TLS connections, preventing data transfers from interfering with latency-sensitive control plane traffic.

## Problem

Erlang distribution uses a **single TCP connection per node pair**. Every message between two nodes shares that connection:

- Ra heartbeats and leader election votes
- Service discovery updates and heartbeats
- Metadata quorum reads and writes
- Event notifications (`:pg` broadcasts)
- **Bulk chunk data** (256KB–1MB per chunk)

This creates two problems:

1. **Head-of-line blocking**: A 1MB chunk being serialised blocks all other messages to that node until transmission completes. Ra heartbeats delayed behind chunk transfers can trigger unnecessary leader elections, disrupting the entire cluster.

2. **Bandwidth contention**: Replicating a 10GB file means ~10,000 chunks flowing through distribution. Sustained data transfer pressure on the control plane channel increases latency for FUSE metadata operations (`getattr`, `lookup`, `readdir`), making the filesystem feel sluggish.

These problems scale with data volume and cluster activity. A single large write or background tier migration can degrade control plane responsiveness for all volumes.

## Design Principles

1. **Elixir orchestrates everything**: Data flows through Elixir on both sides — the transport changes, not the control model. Elixir decides what to transfer, when, and to whom; only the wire protocol for bulk data is different.
2. **Symmetric peer-to-peer**: Every node is both sender and receiver. The protocol uses the same code path regardless of which side initiated the connection. There is no client/server distinction after handshake.
3. **No new NIF dependencies**: Uses OTP's built-in `:ssl` module and `nimble_pool` (pure Elixir, zero deps). The project already has Rustler NIFs for blob storage and FUSE; adding another NIF boundary for networking increases build complexity and failure modes for marginal benefit.
4. **Discovery via existing infrastructure**: Nodes advertise their data transfer endpoints through the existing ServiceRegistry. No separate discovery mechanism (mDNS, multicast, etc.) is needed.
5. **Defence in depth**: Inter-node data transfer is secured with mutual TLS using a cluster-scoped certificate authority, independent of the network-level encryption (WireGuard) recommended for Erlang distribution.

## Architecture

```
┌─────────────────────────────────────────────────────────────────────┐
│                     Erlang Distribution (Control Plane)              │
│                                                                     │
│  Ra consensus, service discovery, metadata RPCs, event notifications│
└────────────────────────────────┬────────────────────────────────────┘
                                 │
                    Both channels between every node pair
                                 │
┌────────────────────────────────┴────────────────────────────────────┐
│                     TLS + {packet, 4} (Data Plane)                  │
│                                                                     │
│  Chunk replication, chunk retrieval, stripe distribution            │
└─────────────────────────────────────────────────────────────────────┘

Per node:

┌──────────────────────────────────────────────────────────────────┐
│                         NeonFS Node                              │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │  Elixir Control Plane                                      │  │
│  │                                                            │  │
│  │  Router.core_call/3  ──── distribution ───── peer control  │  │
│  │  Router.data_call/4  ─┐                                    │  │
│  │                       │                                    │  │
│  │  ┌────────────────────┴───────────────────┐                │  │
│  │  │ Transport                              │                │  │
│  │  │  .Listener  ─── :ssl ───── peer pools  │                │  │
│  │  │  .Pool      ─── :ssl ───── peer listeners               │  │
│  │  └────────────────────────────────────────┘                │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

## Transport: TLS with `{packet, 4}`

### Why Raw `:ssl`

OTP's `:ssl` module provides everything needed for encrypted peer-to-peer binary transfer without any external dependencies:

| Property | Mechanism |
|----------|-----------|
| **Automatic framing** | `{packet, 4}` — the BEAM runtime prepends a 4-byte length prefix on send and delivers complete frames on receive, all in C inside the socket driver |
| **Backpressure** | `{active, N}` — BEAM batch-delivers N frames then pauses until re-armed, preventing mailbox overflow |
| **Encryption + auth** | TLS 1.3 with mutual certificate verification against the cluster CA |
| **Symmetric sockets** | Both `:ssl.connect/3` and `:ssl.transport_accept/1` produce the same `sslsocket()` type — identical send/recv API regardless of which side initiated |
| **Rich messages** | `:erlang.term_to_binary/1` for serialisation — fast, native, supports all Erlang/Elixir types |

### Why Not HTTP/2

HTTP/2 (via Bandit + Req) was considered and rejected:

- **Client-server semantics**: HTTP/2 connections have an inherent client/server role. For P2P, each node would run both a server (Bandit) and client (Req) with different code paths — an asymmetry that raw `:ssl` avoids.
- **Unnecessary overhead**: HTTP framing, HPACK header compression, and stream management add complexity without benefit when the payload is opaque binary chunks.
- **Three new dependencies**: Bandit, Req, and Finch — versus zero for `:ssl` and one (`nimble_pool`) for connection pooling.

Bandit remains on the roadmap for the Phase 11 S3 API, where HTTP semantics are genuinely needed.

### Why Not QUIC

QUIC's main advantage — eliminating TCP head-of-line blocking at the transport layer — matters less on a LAN or WireGuard mesh where packet loss is minimal. The available Elixir QUIC libraries (`quicer`, `ex_quic`, `quichex`) are all NIF wrappers around C or Rust implementations, adding another native dependency for marginal benefit in this deployment context.

### Why Not UDP (Tsunami-style)

UDP-based bulk transfer protocols (Tsunami, UDT) solve a specific problem: TCP performance on long fat networks where high bandwidth × high latency creates a large bandwidth-delay product that exceeds TCP buffer sizes. On a LAN or WireGuard mesh — NeonFS's target deployment — RTT is sub-millisecond, TCP window scaling works correctly, and the BDP is small. Building reliability (acknowledgements, retransmission, ordering) on top of UDP would add complexity without solving a problem that exists in this context.

## Protocol

### Framing

All connections use `{packet, 4}` — the BEAM socket driver automatically handles length-prefix framing:

- **Sending**: `:ssl.send/2` prepends a 4-byte big-endian length prefix (supports frames up to 4GB)
- **Receiving**: the runtime reads the prefix, waits for the full frame, and delivers it as a single message

No application-level framing code is needed. Frames are complete by the time they reach the handler process.

### Message Format

Messages are serialised with `:erlang.term_to_binary/1` and deserialised with `:erlang.binary_to_term/2` (with the `:safe` option to reject unknown atoms). Each message is a tagged tuple:

**Requests:**

```elixir
{:put_chunk, request_ref, hash, volume_id, write_id, tier, chunk_bytes}
{:get_chunk, request_ref, hash, volume_id}
{:has_chunk, request_ref, hash}
```

**Responses:**

```elixir
{:ok, request_ref}                          # put_chunk success
{:ok, request_ref, chunk_bytes}             # get_chunk success
{:ok, request_ref, tier, size}              # has_chunk found
{:error, request_ref, :not_found}           # chunk not on this node
{:error, request_ref, :already_exists}      # put_chunk duplicate (idempotent, not an error)
{:error, request_ref, :insufficient_storage} # no space on target tier
```

The `request_ref` is a unique reference (via `make_ref()`) that correlates responses to requests. This allows multiple in-flight requests on a single connection — a lightweight form of multiplexing without the complexity of a full multiplexing protocol.

### Content Semantics

Chunks are already transformed (compressed, encrypted) by the blob store pipeline before transfer — the data plane moves opaque bytes. It does not interpret, decompress, or decrypt chunk content.

## Connection Pooling

### Why Pooling

A single connection per peer provides no concurrency — one transfer blocks the next. A pool of connections per peer gives true parallelism, with each connection potentially running on a different BEAM scheduler.

### NimblePool

Each peer gets a dedicated `NimblePool` instance managing a configurable number of persistent TLS connections. `NimblePool` is pure Elixir with zero dependencies and handles:

- Eager connection establishment at pool start
- Checkout/checkin with timeout
- Automatic replacement of dead connections (remove + re-init)
- Idle connection health checks via `handle_ping`
- Socket closure detection on idle connections via `handle_info`

```elixir
defmodule NeonFS.Transport.ConnPool do
  @behaviour NimblePool

  @impl NimblePool
  def init_pool(%{peer: peer, ssl_opts: ssl_opts} = pool_state) do
    {:ok, pool_state}
  end

  @impl NimblePool
  def init_worker(%{peer: {host, port}, ssl_opts: ssl_opts} = pool_state) do
    # Async connect to avoid blocking the pool process during TLS handshake
    {:async,
     fn ->
       {:ok, socket} = :ssl.connect(host, port, ssl_opts)
       socket
     end, pool_state}
  end

  @impl NimblePool
  def handle_checkout(:checkout, _from, socket, pool_state) do
    {:ok, socket, socket, pool_state}
  end

  @impl NimblePool
  def handle_checkin(socket, _from, _old_socket, pool_state) do
    {:ok, socket, pool_state}
  end

  @impl NimblePool
  def handle_info({:ssl_closed, _}, _socket) do
    {:remove, :closed}
  end

  def handle_info({:ssl_error, _, _reason}, _socket) do
    {:remove, :error}
  end

  @impl NimblePool
  def handle_ping(socket, pool_state) do
    # Verify connection is still alive
    case :ssl.connection_information(socket) do
      {:ok, _} -> {:ok, socket, pool_state}
      {:error, _} -> {:remove, :stale, pool_state}
    end
  end

  @impl NimblePool
  def terminate_worker(_reason, socket, pool_state) do
    :ssl.close(socket)
    {:ok, pool_state}
  end
end
```

### Pool Sizing

```elixir
NimblePool.start_link(
  worker: {NeonFS.Transport.ConnPool, %{peer: {host, port}, ssl_opts: opts}},
  pool_size: pool_size,          # connections per peer
  lazy: false,                   # establish all connections at startup
  worker_idle_timeout: 30_000    # health-check idle connections every 30s
)
```

Default pool size: **4 connections per peer**. This provides enough concurrency for typical replication workloads (writing 3 replicas concurrently, plus background tier migration) without excessive resource use. Configurable per deployment.

| Cluster Size | Connections per Peer | Total Connections per Node |
|:---:|:---:|:---:|
| 3 nodes | 4 | 8 |
| 5 nodes | 4 | 16 |
| 10 nodes | 4 | 36 |

### Transfer Flow

```elixir
def put_chunk(peer_node, hash, volume_id, write_id, tier, data) do
  pool = get_pool(peer_node)
  ref = make_ref()

  NimblePool.checkout!(pool, :checkout, fn socket ->
    msg = :erlang.term_to_binary(
      {:put_chunk, ref, hash, volume_id, write_id, tier, data}
    )
    :ok = :ssl.send(socket, msg)

    # Wait for response (connection uses {active, false} during checkout)
    {:ok, response_bytes} = :ssl.recv(socket, 0, 10_000)
    response = :erlang.binary_to_term(response_bytes, [:safe])

    {response, socket}
  end, 30_000)
end
```

## Listener

Each node runs a listener that accepts inbound connections from peers. The listener is a simple process that loops on `:ssl.transport_accept/1` and hands connections to handler processes.

```elixir
defmodule NeonFS.Transport.Listener do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    ssl_opts = [
      {:certs_keys, [%{certfile: opts[:certfile], keyfile: opts[:keyfile]}]},
      {:cacertfile, opts[:cacertfile]},
      {:verify, :verify_peer},
      {:fail_if_no_peer_cert, true},
      {:versions, [:"tlsv1.3"]},
      :binary,
      {:packet, 4},
      {:active, false},
      {:reuseaddr, true}
    ]

    {:ok, listen_socket} = :ssl.listen(opts[:port], ssl_opts)
    {:ok, {_addr, port}} = :ssl.sockname(listen_socket)

    # Spawn acceptor tasks
    for _ <- 1..opts[:num_acceptors] do
      Task.start_link(fn -> accept_loop(listen_socket) end)
    end

    {:ok, %{listen_socket: listen_socket, port: port}}
  end

  def get_port, do: GenServer.call(__MODULE__, :get_port)

  def handle_call(:get_port, _from, state) do
    {:reply, state.port, state}
  end

  defp accept_loop(listen_socket) do
    case :ssl.transport_accept(listen_socket) do
      {:ok, transport_socket} ->
        case :ssl.handshake(transport_socket, 10_000) do
          {:ok, socket} ->
            {:ok, pid} = NeonFS.Transport.Handler.start_link(socket: socket)
            :ssl.controlling_process(socket, pid)

          {:error, _reason} ->
            :ok
        end

      {:error, _reason} ->
        :ok
    end

    accept_loop(listen_socket)
  end
end
```

## Handler

The handler process owns an accepted socket and processes inbound requests. It calls directly into the local blob store to serve chunks.

```elixir
defmodule NeonFS.Transport.Handler do
  use GenServer

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts)
  end

  def init(opts) do
    socket = opts[:socket]
    :ssl.setopts(socket, [{:active, 10}])
    {:ok, %{socket: socket}}
  end

  # Complete frame received ({packet, 4} ensures this)
  def handle_info({:ssl, _socket, data}, state) do
    message = :erlang.binary_to_term(data, [:safe])
    handle_message(message, state)
  end

  # Re-arm active mode when batch drained
  def handle_info({:ssl_passive, socket}, state) do
    :ssl.setopts(socket, [{:active, 10}])
    {:noreply, state}
  end

  def handle_info({:ssl_closed, _socket}, state) do
    {:stop, :normal, state}
  end

  def handle_info({:ssl_error, _socket, _reason}, state) do
    {:stop, :normal, state}
  end

  defp handle_message({:put_chunk, ref, hash, volume_id, write_id, tier, data}, state) do
    result = NeonFS.BlobStore.store_chunk(hash, data, tier: tier)

    response = case result do
      {:ok, _} -> {:ok, ref}
      {:error, :already_exists} -> {:error, ref, :already_exists}
      {:error, :insufficient_storage} -> {:error, ref, :insufficient_storage}
    end

    :ssl.send(state.socket, :erlang.term_to_binary(response))
    {:noreply, state}
  end

  defp handle_message({:get_chunk, ref, hash, _volume_id}, state) do
    response = case NeonFS.BlobStore.read_chunk(hash) do
      {:ok, data} -> {:ok, ref, data}
      {:error, :not_found} -> {:error, ref, :not_found}
    end

    :ssl.send(state.socket, :erlang.term_to_binary(response))
    {:noreply, state}
  end

  defp handle_message({:has_chunk, ref, hash}, state) do
    response = case NeonFS.BlobStore.chunk_info(hash) do
      {:ok, %{tier: tier, size: size}} -> {:ok, ref, tier, size}
      {:error, :not_found} -> {:error, ref, :not_found}
    end

    :ssl.send(state.socket, :erlang.term_to_binary(response))
    {:noreply, state}
  end
end
```

## Security: Mutual TLS

### Cluster Certificate Authority

Each NeonFS cluster has its own certificate authority (CA), generated at cluster initialisation. The CA certificate and private key are stored in the **system volume** (`_system`), which is replicated to every node in the cluster (replication factor equals cluster size). This ensures:

- **No single point of failure**: The CA survives any single-node failure
- **Any core node can sign CSRs**: When a new node joins, whichever core node handles the join can issue a certificate
- **Consistent with trust model**: The cluster operates on full trust between nodes (see [Security — Accepted Risks](security.md#accepted-risks--cluster-node-trust)); storing the CA key on the system volume is no weaker than the existing trust boundary

Node-specific certificates and private keys are stored on the local filesystem — they are generated during the join process and never leave the node.

```
System volume (_system):
  /tls/ca.crt                # Cluster CA certificate (public)
  /tls/ca.key                # Cluster CA private key

Local filesystem per node:
  /var/lib/neonfs/tls/
  ├── ca.crt                 # Copy of CA cert (for TLS without system volume access)
  ├── node.crt               # This node's certificate (signed by CA)
  └── node.key               # This node's private key (generated locally)
```

A local copy of `ca.crt` is kept on each node's filesystem so that TLS verification works during startup before Ra has caught up and the system volume is available.

### Certificate Lifecycle

| Event | Action |
|-------|--------|
| `neonfs cluster init` | Generate CA keypair, store in system volume, issue certificate for first node |
| `neonfs cluster join` | Joining node generates keypair, sends CSR; core node signs with CA key from system volume, returns signed cert + CA cert |
| Node startup | Load `node.crt`, `node.key`, and `ca.crt` from local filesystem |
| Certificate approaching expiry | Node requests re-signing from any core node (automated, background) |
| `neonfs cluster decommission` | Optionally add certificate serial to CRL stored in system volume |

### TLS Configuration

Both listener and pool connections use the same TLS parameters:

```elixir
defp ssl_opts(role) do
  base = [
    {:certs_keys, [%{certfile: tls_path("node.crt"), keyfile: tls_path("node.key")}]},
    {:cacertfile, tls_path("ca.crt")},
    {:verify, :verify_peer},
    {:fail_if_no_peer_cert, true},
    {:versions, [:"tlsv1.3"]},
    :binary,
    {:packet, 4}
  ]

  case role do
    :listener -> base ++ [{:reuseaddr, true}]
    :connect -> base
  end
end
```

### Peer Identity Verification

mTLS provides mutual authentication — both sides verify the other's certificate against the cluster CA during the TLS handshake. The peer's certificate can be extracted post-handshake for audit logging:

```elixir
{:ok, der_cert} = :ssl.peercert(socket)
otp_cert = :public_key.pkix_decode_cert(der_cert, :otp)
```

## Service Discovery Integration

### ServiceInfo Extension

The existing `ServiceInfo` type is extended with the data transfer endpoint:

```elixir
%ServiceInfo{
  node: :"neonfs_core@storage1",
  type: :core,
  capabilities: [:storage, :metadata, :data_transfer],
  metadata: %{
    data_endpoint: {"10.0.1.5", 44831}   # {advertise_address, actual_port}
  }
}
```

### Registration Flow

1. Node starts the listener on port 0 (OS-assigned)
2. Node reads back the actual listening port via `:ssl.sockname/1`
3. Node registers with ServiceRegistry, including the data endpoint
4. Other nodes discover the endpoint via `NeonFS.Client.Discovery` (already cached in local ETS)
5. On discovering a new peer endpoint, the node creates a `NimblePool` for that peer

```elixir
# After listener starts
port = NeonFS.Transport.Listener.get_port()

NeonFS.Core.ServiceRegistry.register(%ServiceInfo{
  node: node(),
  type: :core,
  capabilities: [:storage, :metadata, :data_transfer],
  metadata: %{data_endpoint: {advertise_address(), port}}
})
```

### Router Integration

`NeonFS.Client.Router` gains a `data_call/4` function alongside the existing `core_call/3`:

```elixir
defmodule NeonFS.Client.Router do
  # Existing: routes metadata RPCs over Erlang distribution
  def core_call(module, function, args) do
    node = select_node()
    :erpc.call(node, module, function, args)
  end

  # New: routes chunk data over TLS data plane
  def data_call(node, operation, args, opts \\ []) do
    pool = NeonFS.Transport.PoolManager.get_pool(node)
    NeonFS.Transport.ConnPool.execute(pool, operation, args, opts)
  end
end
```

## Configuration

```yaml
# node.yaml
data_transfer:
  # Network interface to bind on.
  # Default: "::" (all interfaces, dual-stack IPv4+IPv6)
  bind: "::"

  # Listening port.
  # Default: 0 (OS-assigned, registered via ServiceRegistry)
  # Set to a fixed value if firewall rules require it.
  port: 0

  # Address to advertise in ServiceRegistry.
  # Default: "" (auto-detect from network interfaces)
  # Set explicitly when bind is "::" but only one interface should be
  # advertised (e.g., the WireGuard interface address).
  advertise: ""

  # Number of TLS connections per peer node.
  # Default: 4
  pool_size: 4

  # Number of acceptor processes for inbound connections.
  # Default: 4
  num_acceptors: 4

  # Idle connection health check interval (milliseconds).
  # Default: 30000
  worker_idle_timeout: 30000

  # TLS certificate paths (relative to data_dir, or absolute).
  tls:
    ca_cert: tls/ca.crt
    node_cert: tls/node.crt
    node_key: tls/node.key
```

### Port Selection

When `port: 0`, the OS assigns a random ephemeral port (typically 32768–60999 on Linux). The actual port is registered in ServiceRegistry and discovered dynamically by peers. This is the recommended default:

- No port configuration needed
- No port conflicts between co-located services
- Discovery handles finding the correct port

Set a fixed port only when firewall rules or network policies require predictable ports.

### Advertise Address

When `bind` is `::` or `0.0.0.0`, the node listens on all interfaces but must advertise a specific address that peers can reach. The auto-detection logic:

1. If `advertise` is set, use that value
2. If the node has a WireGuard interface, prefer that address
3. Otherwise, prefer the first non-loopback IPv4 address

## Integration with Write Flows

### Replicated Volume Write

The write flow from [Replication](replication.md) changes at step 2c — replication uses the data plane instead of distribution:

```
1. Client opens write stream, receives write_id
2. As bytes arrive:
   a. Chunk engine splits stream into chunks
   b. Each chunk written immediately to local storage via NIF
   c. Replication initiated in parallel:
      - Elixir selects target nodes (unchanged)
      - Chunk data sent via TLS data plane (NEW — was distribution RPC)
      - Metadata coordination still via distribution
   d. Chunks tagged with write_id, state: :uncommitted
3. Completion, abort — unchanged
```

### Erasure-Coded Volume Write

Stripe distribution uses the data plane:

```
When stripe complete (e.g., 10 data + 4 parity):
  - Compute parity chunks (unchanged)
  - Distribute all 14 chunks to different nodes via TLS data plane (NEW)
  - Tag with write_id, state: :uncommitted (via distribution)
```

### Read Path

Remote chunk retrieval uses the data plane:

```
1. FUSE/S3/CIFS request arrives
2. Elixir resolves file metadata → chunk list (via distribution, unchanged)
3. For each needed chunk:
   a. Check local blob store (NIF, unchanged)
   b. If not local: retrieve via TLS data plane from owning node (NEW)
4. Assemble and return data (unchanged)
```

### What Stays on Distribution

All control plane traffic remains on Erlang distribution:

- Ra consensus (heartbeats, log replication, leader election)
- Service discovery (registration, heartbeats, queries)
- Metadata operations (quorum reads/writes, intent log)
- Event notifications (`:pg` broadcasts)
- CLI RPC calls
- Node monitoring (`nodeup`/`nodedown`)

## Module Placement

| Module | Package | Role |
|--------|---------|------|
| `NeonFS.Transport.Listener` | `neonfs_client` | Accepts inbound peer connections |
| `NeonFS.Transport.Handler` | `neonfs_client` | Processes requests on accepted connections |
| `NeonFS.Transport.ConnPool` | `neonfs_client` | `NimblePool` behaviour for outbound connections |
| `NeonFS.Transport.PoolManager` | `neonfs_client` | Manages per-peer pools, creates pools on peer discovery |
| `NeonFS.Transport.TLS` | `neonfs_client` | Certificate generation, signing, path management |
| `NeonFS.Client.Router.data_call/4` | `neonfs_client` | Data plane routing (extends existing Router) |

All transport modules live in `neonfs_client` because both core and non-core nodes (FUSE, S3, etc.) need to send and receive chunk data. Each node runs both the listener (inbound) and pool manager (outbound) components.

## Dependencies

| Package | Role | Notes |
|---------|------|-------|
| `nimble_pool` | Connection pooling | Pure Elixir, zero dependencies |
| `:ssl` | TLS sockets | OTP built-in |
| `:public_key` | Certificate operations | OTP built-in |

## What This Spec Does NOT Cover

- **Chunk data format or transformation**: The data plane moves opaque bytes. Compression, encryption, and content addressing are handled by the blob store pipeline before data reaches the transfer layer.
- **Metadata transfer**: All metadata operations remain on Erlang distribution. This spec only covers bulk chunk data.
- **S3 API**: The Phase 11 S3 API uses HTTP/2 (Bandit) on a separate port with different routing, authentication, and endpoint semantics.
- **Bandwidth throttling**: Per-volume `max_replication_bandwidth` limits (defined in [Architecture](architecture.md)) are enforced at the Elixir orchestration layer, not in the transport. The data plane executes transfers; Elixir decides the rate.
- **System volume internals**: The system volume (`_system`) is specified separately in [System Volume](system-volume.md). This document only describes how the data transfer layer uses it for CA storage.

## Implementation Phase

This is the **Phase 9** deliverable, following the system volume (Phase 7) and cluster CA (Phase 8) prerequisites. Event notification follows in Phase 10. The `data_call/4` routing abstraction can be introduced early behind a feature flag — initially delegating to distribution, then switching to the TLS data plane when the transport layer is ready.

## Related Documents

- [Architecture](architecture.md) — System structure, backpressure, and per-volume limits
- [Replication](replication.md) — Write flows that use the data plane for chunk distribution
- [Discovery](discovery.md) — ServiceRegistry where data transfer endpoints are advertised
- [Security](security.md) — Cluster authentication, threat model, TLS distribution roadmap
- [Event Notification](pubsub.md) — Control plane events that remain on distribution
- [Node Management](node-management.md) — Cost functions for node selection
- [Cluster CA](cluster-ca.md) — Certificate authority lifecycle, issuance, revocation
- [System Volume](system-volume.md) — System volume specification, CA storage, audit logs
- [Metadata](metadata.md) — Intent log archival to system volume
