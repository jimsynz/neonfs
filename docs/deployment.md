# Docker Compose Deployment Guide

This guide explains how to deploy NeonFS using Docker Compose, covering both single-host and multi-host cluster topologies.

## Prerequisites

### Docker

- Docker Engine 24+ with the [Buildx plugin](https://docs.docker.com/build/buildx/) (ships by default with Docker Desktop and modern Docker CE packages)
- Docker Compose v2+

### Host requirements

- **Linux kernel with FUSE support.** The `fuse` kernel module must be loaded and `/dev/fuse` must exist on every host that runs a FUSE container:

  ```bash
  # Check for /dev/fuse
  ls -l /dev/fuse

  # Load the module if missing
  sudo modprobe fuse
  ```

- **AppArmor.** On Ubuntu/Debian hosts with AppArmor enabled, the FUSE container must run with `apparmor:unconfined` (shown in the compose examples below). Alternatively, write a custom AppArmor profile that permits FUSE operations.

## Building Images

NeonFS uses a [Docker Bake](https://docs.docker.com/build/bake/) file (`bake.hcl`) to build all container images. The build produces three runtime images:

| Target | Image | Description |
|--------|-------|-------------|
| `core` | `harton.dev/project-neon/neonfs/core` | Storage and metadata daemon |
| `fuse` | `harton.dev/project-neon/neonfs/fuse` | FUSE filesystem mount daemon |
| `cli`  | `harton.dev/project-neon/neonfs/cli`  | CLI client (`neonfs` binary) |

Build all three for the local Docker daemon:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f bake.hcl --load core fuse cli
```

> **Note:** The `--load` flag loads images into the local daemon. Multi-platform builds (the default `linux/amd64,linux/arm64`) don't support `--load`, so override `PLATFORMS` to a single architecture for local use.

You can tag images with a specific version:

```bash
TAG=0.1.0 PLATFORMS='linux/amd64' docker buildx bake -f bake.hcl --load core fuse cli
```

## Single-Host Deployment

This is the simplest topology: one core node and one FUSE node on the same machine, communicating over a shared Docker network using Erlang short names (`sname`).

### Compose file

```yaml
# docker-compose.yml
services:
  core:
    image: harton.dev/project-neon/neonfs/core:latest
    container_name: neonfs-core
    hostname: neonfs-core
    restart: unless-stopped
    environment:
      RELEASE_NODE: "neonfs_core@neonfs-core"
      RELEASE_COOKIE: "your-secret-cookie-here"
      NEONFS_DATA_DIR: "/var/lib/neonfs/data"
      NEONFS_FUSE_NODE: "neonfs_fuse@neonfs-fuse"
    volumes:
      - core-data:/var/lib/neonfs
    networks:
      - neonfs

  fuse:
    image: harton.dev/project-neon/neonfs/fuse:latest
    container_name: neonfs-fuse
    hostname: neonfs-fuse
    restart: unless-stopped
    depends_on:
      - core
    environment:
      RELEASE_NODE: "neonfs_fuse@neonfs-fuse"
      RELEASE_COOKIE: "your-secret-cookie-here"
      NEONFS_CORE_NODE: "neonfs_core@neonfs-core"
    volumes:
      - fuse-data:/var/lib/neonfs
      - /mnt/neonfs:/mnt/neonfs:rshared
    devices:
      - /dev/fuse:/dev/fuse
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
    networks:
      - neonfs

volumes:
  core-data:
  fuse-data:

networks:
  neonfs:
    driver: bridge
```

### How it works

- Both containers share the `neonfs` bridge network. Docker's built-in DNS resolves `neonfs-core` and `neonfs-fuse` to the correct container IPs, so Erlang short-name distribution (`sname`) works out of the box.
- `RELEASE_COOKIE` must match on all nodes. In production, generate a strong random value (e.g. `openssl rand -hex 32`).
- The core node stores metadata and blobs under `/var/lib/neonfs`, backed by the `core-data` named volume.
- The FUSE node mounts the filesystem at `/mnt/neonfs` inside the container. The `:rshared` mount propagation flag makes the FUSE mount visible to the host at `/mnt/neonfs`.

### Using a cookie file instead of an environment variable

If you prefer not to put the cookie in an environment variable, both releases will read from `/var/lib/neonfs/.erlang.cookie` when `RELEASE_COOKIE` is unset (see `rel/env.sh.eex`). Share a cookie file via a named volume or bind mount:

```yaml
services:
  core:
    # ...
    environment:
      RELEASE_NODE: "neonfs_core@neonfs-core"
      # No RELEASE_COOKIE — will read from cookie file
    volumes:
      - core-data:/var/lib/neonfs
      - ./cookie:/var/lib/neonfs/.erlang.cookie:ro

  fuse:
    # ...
    environment:
      RELEASE_NODE: "neonfs_fuse@neonfs-fuse"
      NEONFS_CORE_NODE: "neonfs_core@neonfs-core"
    volumes:
      - fuse-data:/var/lib/neonfs
      - ./cookie:/var/lib/neonfs/.erlang.cookie:ro
      - /mnt/neonfs:/mnt/neonfs:rshared
```

Create the cookie file with restricted permissions:

```bash
openssl rand -hex 32 > cookie
chmod 600 cookie
```

### Environment variables reference

#### Core node (`neonfs_core`)

| Variable | Default | Description |
|----------|---------|-------------|
| `RELEASE_NODE` | `neonfs_core@localhost` | Erlang node name |
| `RELEASE_COOKIE` | Read from `/var/lib/neonfs/.erlang.cookie` | Erlang distribution cookie |
| `RELEASE_DISTRIBUTION` | `sname` | Distribution mode (`sname` or `name`) |
| `NEONFS_DATA_DIR` | `/var/lib/neonfs/data` | Base directory for data, metadata, and Ra state |
| `NEONFS_FUSE_NODE` | `neonfs_fuse@localhost` | FUSE node name (for RPC calls to FUSE) |
| `NEONFS_DRIVES` | Single default drive at `{data_dir}/blobs` | Comma-separated drive specs (see [Storage Configuration](#storage-configuration)) |
| `NEONFS_ENABLE_RA` | `true` | Enable Ra consensus (set `false` only for debugging) |
| `NEONFS_PREFIX_DEPTH` | `2` | Blob store directory prefix depth |
| `NEONFS_SNAPSHOT_INTERVAL_MS` | `30000` | Ra snapshot interval in milliseconds |

#### FUSE node (`neonfs_fuse`)

| Variable | Default | Description |
|----------|---------|-------------|
| `RELEASE_NODE` | `neonfs_fuse@localhost` | Erlang node name |
| `RELEASE_COOKIE` | Read from `/var/lib/neonfs/.erlang.cookie` | Erlang distribution cookie |
| `RELEASE_DISTRIBUTION` | `sname` | Distribution mode (`sname` or `name`) |
| `NEONFS_CORE_NODE` | `neonfs_core@localhost` | Core node to connect to for bootstrap |
| `NEONFS_FUSERMOUNT_CMD` | `fusermount3` | Unmount command (`fusermount3` or `fusermount`) |

### Starting the cluster

```bash
# Start the containers
docker compose up -d

# Wait a few seconds for the core node to start, then initialise the cluster
docker compose exec core /app/bin/neonfs_core rpc \
  "NeonFS.CLI.Handler.handle_command({:cluster_init, \"my-cluster\"})"
```

Or use the CLI container:

```bash
docker run --rm --network neonfs \
  -e RELEASE_COOKIE=your-secret-cookie-here \
  harton.dev/project-neon/neonfs/cli:latest \
  --node neonfs_core@neonfs-core cluster init --name my-cluster
```

Check cluster status:

```bash
docker run --rm --network neonfs \
  -e RELEASE_COOKIE=your-secret-cookie-here \
  harton.dev/project-neon/neonfs/cli:latest \
  --node neonfs_core@neonfs-core cluster status
```

## Multi-Host Deployment

For production, run three or more core nodes across separate machines to form a Ra quorum. Each host runs its own compose stack.

### Key differences from single-host

1. **Long names (`name`) instead of short names (`sname`).** Erlang short names only work within a single hostname scope. Across machines, use fully-qualified node names.
2. **Nodes must be able to resolve each other's FQDNs.** Use DNS, `/etc/hosts`, or a service mesh.
3. **EPMD (port 4369) and the Erlang distribution port range must be reachable** between all nodes.

### Switching to long names

Override `RELEASE_DISTRIBUTION=name` and use FQDN-style node names:

```yaml
environment:
  RELEASE_DISTRIBUTION: "name"
  RELEASE_NODE: "neonfs_core@core1.neonfs.example.com"
```

No changes to the release scripts are needed — `env.sh.eex` sets `RELEASE_DISTRIBUTION=sname` as a default, but environment variables take precedence.

### Network connectivity

Erlang distribution requires:

- **EPMD** on TCP port 4369
- **Distribution ports** — by default Erlang picks a random high port. You can constrain the range with kernel configuration:

  ```yaml
  environment:
    ERL_AFLAGS: "-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9200"
  ```

  Then expose those ports in your compose file:

  ```yaml
  ports:
    - "4369:4369"
    - "9100-9200:9100-9200"
  ```

For multi-host networking, you have several options:

- **Docker Swarm overlay network** — built-in, works across hosts in a Swarm cluster
- **VPN mesh (Tailscale, WireGuard, etc.)** — nodes communicate over a flat private network; no special Docker networking needed
- **Host networking** — use `network_mode: host` to skip Docker's network namespace entirely

### Example: three-node core cluster

Each host runs a compose stack. The first host initialises the cluster; subsequent hosts join via invite tokens.

**Host 1** (`core1.neonfs.example.com`) — bootstrap node:

```yaml
# docker-compose.yml on host 1
services:
  core:
    image: harton.dev/project-neon/neonfs/core:latest
    container_name: neonfs-core
    hostname: core1.neonfs.example.com
    restart: unless-stopped
    network_mode: host
    environment:
      RELEASE_DISTRIBUTION: "name"
      RELEASE_NODE: "neonfs_core@core1.neonfs.example.com"
      RELEASE_COOKIE: "your-secret-cookie-here"
      NEONFS_DATA_DIR: "/var/lib/neonfs/data"
      ERL_AFLAGS: "-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9200"
    volumes:
      - /var/lib/neonfs:/var/lib/neonfs
```

Initialise the cluster on host 1:

```bash
docker compose exec core /app/bin/neonfs_core rpc \
  "NeonFS.CLI.Handler.handle_command({:cluster_init, \"my-cluster\"})"
```

Create an invite token:

```bash
docker compose exec core /app/bin/neonfs_core rpc \
  "NeonFS.CLI.Handler.handle_command({:create_invite, 3600})"
```

**Host 2** (`core2.neonfs.example.com`) — joins via invite:

```yaml
# docker-compose.yml on host 2
services:
  core:
    image: harton.dev/project-neon/neonfs/core:latest
    container_name: neonfs-core
    hostname: core2.neonfs.example.com
    restart: unless-stopped
    network_mode: host
    environment:
      RELEASE_DISTRIBUTION: "name"
      RELEASE_NODE: "neonfs_core@core2.neonfs.example.com"
      RELEASE_COOKIE: "your-secret-cookie-here"
      NEONFS_DATA_DIR: "/var/lib/neonfs/data"
      ERL_AFLAGS: "-kernel inet_dist_listen_min 9100 inet_dist_listen_max 9200"
    volumes:
      - /var/lib/neonfs:/var/lib/neonfs
```

Join the cluster using the token from host 1:

```bash
docker compose exec core /app/bin/neonfs_core rpc \
  "NeonFS.CLI.Handler.handle_command({:join_cluster, \"<token>\", \"neonfs_core@core1.neonfs.example.com\"})"
```

Repeat for host 3 with the appropriate hostname.

### Adding FUSE nodes on separate machines

FUSE nodes don't participate in Ra consensus — they only need connectivity to at least one core node. They can run on client machines that need to mount the filesystem.

```yaml
# docker-compose.yml on a FUSE client host
services:
  fuse:
    image: harton.dev/project-neon/neonfs/fuse:latest
    container_name: neonfs-fuse
    restart: unless-stopped
    network_mode: host
    environment:
      RELEASE_DISTRIBUTION: "name"
      RELEASE_NODE: "neonfs_fuse@fuse1.neonfs.example.com"
      RELEASE_COOKIE: "your-secret-cookie-here"
      NEONFS_CORE_NODE: "neonfs_core@core1.neonfs.example.com"
    volumes:
      - /var/lib/neonfs:/var/lib/neonfs
      - /mnt/neonfs:/mnt/neonfs:rshared
    devices:
      - /dev/fuse:/dev/fuse
    cap_add:
      - SYS_ADMIN
    security_opt:
      - apparmor:unconfined
```

The FUSE node's service discovery layer (`NeonFS.Client.Discovery`) will automatically find all core nodes in the cluster after connecting to the bootstrap node specified by `NEONFS_CORE_NODE`. If the bootstrap node goes down, the FUSE node will continue operating via any other core node it has discovered.

## Storage Configuration

### Drive tiers

Configure storage drives via the `NEONFS_DRIVES` environment variable on core nodes. Each drive is specified as `id:path:tier[:capacity]`, with multiple drives separated by commas:

```
NEONFS_DRIVES="nvme0:/data/nvme0:hot:1T,sata0:/data/sata0:cold:4T"
```

| Field | Description |
|-------|-------------|
| `id` | Unique identifier for the drive |
| `path` | Absolute path to the storage directory |
| `tier` | Storage tier: `hot`, `warm`, or `cold` |
| `capacity` | Optional capacity limit (0 = unlimited). Accepts raw bytes or human-readable suffixes: `M` (MiB, 1024^2), `G` (GiB, 1024^3), `T` (TiB, 1024^4). Suffixes are case-insensitive. Examples: `500G`, `1T`, `1.5T`, `100M`. |

At startup, NeonFS checks each drive's configured capacity against the actual partition size and logs a warning if the configured value exceeds the partition total.

When `NEONFS_DRIVES` is not set, a single default drive is created at `{NEONFS_DATA_DIR}/blobs` with tier `hot`.

### Data directory layout

The core node's data directory (`NEONFS_DATA_DIR`, default `/var/lib/neonfs/data`) contains:

```
/var/lib/neonfs/data/
  blobs/      # Default blob storage (when NEONFS_DRIVES is not set)
  meta/       # File and chunk metadata
  ra/         # Ra consensus log and snapshots
```

When using custom drives, blob data is stored at the paths specified in `NEONFS_DRIVES` rather than under the data directory.

### Volume mounts in compose

Mount the drive paths into the container when using custom drives:

```yaml
services:
  core:
    volumes:
      - /var/lib/neonfs:/var/lib/neonfs          # Data dir (metadata, Ra state)
      - /data/nvme0:/data/nvme0                   # Hot tier drive
      - /data/sata0:/data/sata0                   # Cold tier drive
    environment:
      NEONFS_DRIVES: "nvme0:/data/nvme0:hot:1T,sata0:/data/sata0:cold:4T"
```

## Operations

### Health checks

Check whether a node is running and responsive:

```bash
# Core node
docker compose exec core /app/bin/neonfs_core pid

# FUSE node
docker compose exec fuse /app/bin/neonfs_fuse pid
```

For a more detailed status check via the CLI:

```bash
docker run --rm --network neonfs \
  -e RELEASE_COOKIE=your-secret-cookie-here \
  harton.dev/project-neon/neonfs/cli:latest \
  --node neonfs_core@neonfs-core cluster status
```

### Logs

```bash
# Follow logs for all services
docker compose logs -f

# Follow logs for a specific service
docker compose logs -f core
docker compose logs -f fuse
```

Erlang/OTP log output goes to stdout by default in release mode, so Docker captures it automatically.

### Graceful shutdown

```bash
# Stop all services gracefully
docker compose down

# Stop a specific service
docker compose stop core
```

The Erlang release traps `SIGTERM` and performs a graceful shutdown — flushing pending writes, deregistering from the service registry, and persisting state. The default Docker stop timeout (10 seconds) should be sufficient; increase it with `--timeout` if you have large pending write buffers.

### Backup

Back up the core node's data volume to preserve cluster state:

```bash
# Stop the core node first for a consistent snapshot
docker compose stop core

# Back up the volume
docker run --rm -v core-data:/data -v $(pwd):/backup \
  debian:trixie tar czf /backup/neonfs-core-backup.tar.gz -C /data .

# Restart
docker compose start core
```

Key directories to back up:

- `/var/lib/neonfs/data/ra/` — Ra consensus log and snapshots (cluster membership, metadata)
- `/var/lib/neonfs/data/meta/` — file and chunk metadata
- `/var/lib/neonfs/data/blobs/` — stored blob data (or custom drive paths)

## Troubleshooting

### Cookie mismatch

**Symptom:** Nodes start but cannot connect. Logs show `** Connection attempt from node ... rejected` or `connection_rejected`.

**Fix:** Ensure `RELEASE_COOKIE` is identical on all nodes (core, FUSE, and CLI). If using a cookie file, verify the file contents match and are readable by the container user (`nobody`).

### EPMD connectivity

**Symptom:** Nodes cannot discover each other. `Node.connect/1` returns `false`.

**Fix:**
- Verify EPMD is running: `docker compose exec core epmd -names`
- Check that port 4369 is reachable between hosts
- Check that the distribution port range is open (if constrained via `ERL_AFLAGS`)
- Ensure hostnames resolve correctly: `docker compose exec core getent hosts neonfs-core`

### FUSE permissions

**Symptom:** FUSE node starts but cannot mount. Logs show `Operation not permitted` or `fuse: device not found`.

**Fix:**
- Verify `/dev/fuse` exists on the host: `ls -l /dev/fuse`
- Ensure the compose file includes `devices: ["/dev/fuse:/dev/fuse"]`
- Ensure `cap_add: [SYS_ADMIN]` is set
- On AppArmor systems, ensure `security_opt: ["apparmor:unconfined"]` is set
- Check that the `fuse` kernel module is loaded: `lsmod | grep fuse`

### FUSE mount not visible on host

**Symptom:** The FUSE mount works inside the container but `/mnt/neonfs` on the host is empty.

**Fix:** The volume mount needs shared propagation. Use `:rshared` on the bind mount:

```yaml
volumes:
  - /mnt/neonfs:/mnt/neonfs:rshared
```

### Distribution not starting

**Symptom:** Node starts but Erlang distribution is not active. `Node.self()` returns `:nonode@nohost`.

**Fix:**
- Check that `RELEASE_NODE` is set and contains an `@` sign
- For multi-host, ensure `RELEASE_DISTRIBUTION=name` is set
- Verify the hostname part of the node name is resolvable from within the container

### Core node unreachable from FUSE

**Symptom:** FUSE node logs `{:error, :all_nodes_unreachable}` or operations time out.

**Fix:**
- Verify `NEONFS_CORE_NODE` on the FUSE node matches the core node's `RELEASE_NODE`
- Check that the FUSE node can reach the core node: `docker compose exec fuse /app/bin/neonfs_fuse rpc "Node.connect(:\"neonfs_core@neonfs-core\")"`
- Allow time for service discovery to initialise — `NeonFS.Client.Connection`, `Discovery`, and `CostFunction` need a few seconds after startup to probe and cache core nodes
