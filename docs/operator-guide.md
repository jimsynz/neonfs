# NeonFS Operator Guide

This guide is for operators deploying and maintaining NeonFS clusters. It assumes Linux familiarity but no prior NeonFS knowledge.

For deployment-recipe detail, see the companion guides:

- [`deployment.md`](deployment.md) — Docker Compose single- and multi-host recipes.
- [`orchestration.md`](orchestration.md) — Docker Swarm and Kubernetes recipes with auto-bootstrap.
- [`cli-reference.md`](cli-reference.md) — generated `neonfs` command reference.

The specification and architecture documents in the [wiki](https://harton.dev/project-neon/neonfs/wiki) describe design intent in more depth.

## Topology overview

A NeonFS cluster is made up of two kinds of nodes:

- **Core nodes** — run the storage engine, metadata (Ra consensus), the cluster CA, and policy evaluation. They hold volumes' actual data on local drives. Three or more core nodes form the quorum.
- **Interface nodes** — expose data to clients. One interface package per protocol: `neonfs_fuse` (FUSE), `neonfs_nfs` (NFSv3), `neonfs_s3` (S3), `neonfs_webdav` (WebDAV), `neonfs_docker` (Docker VolumeDriver). Interface nodes are stateless — they connect to core nodes via Erlang distribution and fetch chunks over a dedicated TLS data plane.

For small deployments the `neonfs_omnibus` bundle runs core and all interfaces in one BEAM VM. Production deployments separate them so interfaces can scale horizontally without disturbing core quorum.

## Installation

NeonFS ships as Debian packages, OCI container images, and a source-build fallback.

### Debian packages (recommended for bare-metal and VMs)

Packages are built from `packaging/nfpm/*.yaml` and published per release. Each package installs a release into `/usr/lib/neonfs/<name>/`, a systemd unit, a runtime wrapper in `/usr/bin/neonfs-<name>-daemon`, and a shared environment file at `/etc/neonfs/neonfs.conf`.

Available packages:

| Package | Role |
|---------|------|
| `neonfs-common` | Shared user/group, data directory (`/var/lib/neonfs`), and environment file. Dependency of every other package. |
| `neonfs-cli` | The `neonfs` command-line tool. Install on any host that administers the cluster. |
| `neonfs-core` | Storage and metadata daemon. Install on each core node. |
| `neonfs-fuse` | FUSE mount daemon. |
| `neonfs-nfs` | NFSv3 + NLM server. |
| `neonfs-s3` | S3-compatible HTTP gateway. |
| `neonfs-webdav` | WebDAV server. |
| `neonfs-docker` | Docker VolumeDriver plugin (HTTP over UNIX socket). |
| `neonfs-omnibus` | All-in-one bundle. **Conflicts with every other `neonfs-*` package** — pick omnibus *or* split services, not both. |

Install a split-service core node:

```bash
apt install neonfs-cli neonfs-core
systemctl enable --now neonfs-core
```

Install an interface node on a separate host:

```bash
apt install neonfs-cli neonfs-fuse
systemctl enable --now neonfs-fuse
```

Install the omnibus bundle on a single-host deployment:

```bash
apt install neonfs-cli neonfs-omnibus
systemctl enable --now neonfs-omnibus
```

Systemd unit names follow the package name: `neonfs-core.service`, `neonfs-fuse.service`, etc. A `neonfs.target` groups them so `systemctl start neonfs.target` starts everything enabled on the host.

### Container images

OCI images are published for each release at `harton.dev/project-neon/neonfs/<name>`. For local testing build them yourself:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f bake.hcl --load core fuse nfs s3 webdav docker cli
```

The `--load` flag places images into the local Docker daemon. Multi-arch manifest lists use the release pipeline (`.forgejo/workflows/release.yml`).

See [`deployment.md`](deployment.md) for Docker Compose recipes and [`orchestration.md`](orchestration.md) for Docker Swarm / Kubernetes.

### Source build

Development environments build from source:

```bash
mix deps.get
MIX_ENV=prod mix release       # produces _build/prod/rel/neonfs_core (and per-package)
```

Requires Elixir 1.19 (OTP 28) and Rust 1.93 — see `.tool-versions`.

### Omnibus vs split-service topologies

| Topology | Use when |
|----------|----------|
| **Omnibus** — one BEAM per host running core + interfaces | Single-host lab deployments, small homelab clusters, developer workstations. |
| **Split-service** — each interface in its own BEAM on its own host | Production clusters that need independent scaling, protocol-specific hardening, or failure-domain isolation between storage and protocols. |

Both topologies use the same configuration file (`/etc/neonfs/neonfs.conf`) and the same `neonfs` CLI. Switching requires reinstalling packages (omnibus conflicts with split-service packages) — plan the topology before first bootstrap.

## Configuration file

All Debian packages read `/etc/neonfs/neonfs.conf`. The file is a systemd `EnvironmentFile` — shell-style `KEY=value` lines, no exports, no quoting except for literal whitespace.

Key variables:

| Variable | Default | Purpose |
|----------|---------|---------|
| `NEONFS_DATA_DIR` | `/var/lib/neonfs` | Base data directory (drives, metadata, Ra state, TLS material). |
| `NEONFS_TLS_DIR` | `/var/lib/neonfs/tls` | Directory the cluster CA writes certificates into. |
| `NEONFS_CORE_NODE` | `neonfs_core@<host>` | Core node name advertised on the local host (used by interfaces for bootstrap). |
| `NEONFS_DIST_PORT` | Auto-generated, persisted to `/var/lib/neonfs/meta/dist_port` | Erlang distribution port. Pin explicitly in firewalled environments. |
| `RELEASE_COOKIE_PATH` | `/var/lib/neonfs/.erlang.cookie` | File holding the Erlang distribution cookie. Must be identical on every node. |
| `RELEASE_LOG_LEVEL` | `info` | BEAM log level (`debug`, `info`, `warning`, `error`). |
| `ERL_MAX_PORTS` | `65536` | BEAM port-table cap. Raise if many concurrent clients. |
| `NEONFS_S3_BIND` / `NEONFS_S3_PORT` | `0.0.0.0` / `8080` | S3 gateway listener. |

Service-specific environment variables for FUSE, NFS, S3, WebDAV, and Docker are documented in `packaging/systemd/neonfs.conf` (commented-out examples) and in [`deployment.md`](deployment.md).

Override per-service by dropping files into `/etc/systemd/system/neonfs-<svc>.service.d/` — standard systemd drop-in practice.

## Initial cluster bootstrap

A fresh cluster needs three steps: pick the bootstrap node, initialise it, and (optionally) make sure TLS material is ready before interface nodes join.

### First-node checklist

Before running `cluster init` on the chosen bootstrap node, confirm:

- [ ] `neonfs-common`, `neonfs-cli`, and `neonfs-core` (or omnibus) installed; `neonfs-core` service running.
- [ ] `/var/lib/neonfs` is on the disk you intend to use for metadata (separate from blob storage drives).
- [ ] `/var/lib/neonfs/.erlang.cookie` exists and has mode `0600`. Generate with `openssl rand -hex 32 > /var/lib/neonfs/.erlang.cookie && chmod 600 /var/lib/neonfs/.erlang.cookie && chown neonfs:neonfs /var/lib/neonfs/.erlang.cookie`.
- [ ] Hostname is stable and resolvable from every future cluster peer (DNS, `/etc/hosts`, or service mesh).
- [ ] The Erlang distribution port (see `NEONFS_DIST_PORT`) and EPMD (`4369`) are open between all prospective core nodes.
- [ ] Time is synchronised (chrony/ntp). NeonFS uses hybrid logical clocks, but wall-clock drift still trips operators when reading logs.

### `cluster init`

On the first node:

```bash
neonfs cluster init --name production
```

This:

1. Generates the cluster ID and persists `cluster.json`.
2. Starts Ra as a single-member quorum.
3. Generates a self-signed cluster CA (ECDSA P-256), stored under `$NEONFS_TLS_DIR/ca/`.
4. Issues the first core node's TLS certificate and rotates Erlang distribution onto it.
5. Registers the local node in the service registry.

Verify:

```bash
neonfs cluster status
neonfs cluster ca info
```

The CA command prints the subject, algorithm, not-before/not-after, and the serial counter — worth noting at bootstrap time so future rotations have a reference point.

### Auto-bootstrap (orchestrated deployments)

Docker Swarm, Kubernetes, and Nomad can't easily run a single manual `cluster init`. Core releases support the Consul/Nomad `bootstrap_expect` pattern — set these variables on every core replica:

```
NEONFS_AUTO_BOOTSTRAP=true
NEONFS_CLUSTER_NAME=production
NEONFS_BOOTSTRAP_EXPECT=3
NEONFS_BOOTSTRAP_PEERS=neonfs_core@node-1,neonfs_core@node-2,neonfs_core@node-3
```

All three replicas start simultaneously, discover each other, sort names lexicographically, the lowest runs `cluster init`, and the rest join via an automatically generated invite token. See [`orchestration.md`](orchestration.md) for the full mechanism and failure modes.

## Node join flow

Additional core nodes (and all interface nodes) join an existing cluster using an invite token.

### Create an invite

On any initialised core node:

```bash
neonfs cluster create-invite --expires 1h
```

Returns a token and a `via` address (host:port reachable from the joining node). Tokens are single-use and time-bounded — default one hour, configurable per-invite.

### Join a core node

On the new host, after `neonfs-core` is installed and running:

```bash
neonfs cluster join \
  --token <invite-token> \
  --via node-1.example.com:9568
```

This:

1. Connects to the inviting node's data-plane listener on `--via`.
2. Presents the token and has the inviter issue a TLS certificate for this node via the cluster CA.
3. Rotates Erlang distribution onto the new certificate.
4. Adds this node to Ra and the service registry.
5. Rebuilds the quorum metadata ring on every existing core so the new member is included.

When the command returns, `neonfs cluster status` should list the new node and `neonfs node list` should show it as a core member.

### Join an interface node

Interface packages (`neonfs-fuse`, `neonfs-nfs`, `neonfs-s3`, `neonfs-webdav`, `neonfs-docker`) don't run `cluster join` — they register automatically via `NeonFS.Client.Registrar` as soon as they connect to a core node. The prerequisites are:

- The same `RELEASE_COOKIE` as the core cluster, at `/var/lib/neonfs/.erlang.cookie`.
- `NEONFS_CORE_NODE` set to the Erlang node name of any core member the interface can reach (for example `neonfs_core@node-1.example.com`).
- EPMD and the Erlang distribution port reachable to that core node.

Once those are in place, enable the service and it registers itself on startup:

```bash
systemctl enable --now neonfs-fuse
```

The interface connects to the configured bootstrap core node, `NeonFS.Client.Discovery` fetches the full list of core members, and `NeonFS.Client.CostFunction` picks the best node for each subsequent RPC.

### Verifying health post-join

The joining node is healthy when:

- `neonfs cluster status` lists it with its expected type and the correct cluster ID.
- `neonfs node status <name>` shows its services, drives (for core), and service-registry entries.
- The interface's health endpoint reports `ok` (each interface exposes one — see each package's `health_check.ex`).
- The service's log shows `Auto-bootstrap: formation complete` (orchestrated) or `cluster join complete` (manual), followed by `ready`.

Repeat the join flow for each additional node. Three core nodes is the minimum for tolerating a single-node failure (Ra quorum = 2); five tolerates two failures.

## Volume management

Volumes are the primary unit of isolation in NeonFS — each has its own durability, tiering, compression, encryption, ACL, and supervision tree.

### Creating a volume

```bash
neonfs volume create my-data \
  --replicas 3 \
  --compression zstd \
  --encryption server-side \
  --atime-mode relatime \
  --scrub-interval 86400
```

Key options (see `neonfs volume create --help` for the complete list):

| Flag | Meaning |
|------|---------|
| `--replicas N` | Replicas per chunk. Three is a typical default; one is acceptable only for scratch volumes. |
| `--compression {none,zstd}` | Server-side compression. `zstd` is usually the right default. |
| `--encryption {none,server-side}` | Server-side AES-256-GCM encryption with per-volume keys. |
| `--atime-mode {noatime,relatime}` | Governs access-time updates. `noatime` reduces metadata writes dramatically. |
| `--scrub-interval SECONDS` | How often the volume's full integrity scan runs. |

Reed–Solomon erasure coding is supported at the volume-config level but not yet exposed through the `volume create` CLI — see the wiki's [Implementation](https://harton.dev/project-neon/neonfs/wiki/Implementation) page for the current status.

### Updating a volume

Most volume settings are live-adjustable:

```bash
neonfs volume update my-data --compression zstd --write-ack quorum
neonfs volume update my-data --initial-tier warm --promotion-threshold 5
neonfs volume update my-data --cache-transformed false --cache-remote true
```

Durability settings that change replication or erasure coding trigger background migration — plan the change during a quiet period and watch `neonfs cluster status` for migration progress.

### Listing and inspecting volumes

```bash
neonfs volume list
neonfs volume show my-data
neonfs volume list --all              # include internal volumes (e.g. _system)
```

### Key rotation

For encrypted volumes, start a background rotation with:

```bash
neonfs volume rotate-key my-data
neonfs volume rotation-status my-data
```

Rotation is online — the volume stays readable and writable throughout. `rotation-status` reports how many chunks have been migrated to the new key.

For scheduled-cadence and suspected-compromise rotation flows (with backup-lifecycle guidance for ciphertext under the old key), see the [Key-Rotation runbook](runbooks/Key-Rotation.md).

### Deleting a volume

```bash
neonfs volume delete my-data --force
```

Deletion removes the volume's metadata immediately; chunk reclamation happens asynchronously via the garbage collector (`neonfs gc`).

### Durability, tiering, compression — when to change defaults

- **Replication factor 3, zstd, server-side encryption** is a safe default for general-purpose storage.
- Drop replication to 1 (or use erasure coding for better storage efficiency) only when you understand the failure model.
- Turn compression off only for already-compressed data (video, archives). The CPU cost of zstd is usually a net win.
- Encryption costs ~5–15% throughput on current hardware; turn it off only when the platform provides encryption-at-rest elsewhere.
- `--initial-tier warm` or `cold` is appropriate for archival data; otherwise leave at `hot` and let access-based promotion/demotion work.

## Drive management

Drives are the physical storage units that hold blob chunks. Each core node owns zero or more drives, each assigned to one tier (`hot`, `warm`, `cold`).

A fresh node starts with **no drives configured** — the daemon refuses to accept writes until at least one drive is registered. Run `neonfs drive add ...` after `cluster init` to attach storage. This is deliberate: it forces operators to think about disk topology before going live, and it avoids the data-directory layout footgun where the auto-registered default drive both doubled the on-disk path (`<data_dir>/blobs/blobs/...`) and silently held the only metadata replicas in single-node deployments.

### Adding a drive

```bash
neonfs drive add \
  --path /data/nvme0 \
  --tier hot \
  --capacity 1T \
  --id nvme0
```

| Flag | Description |
|------|-------------|
| `--path` | Absolute path to the storage directory. Must exist and be writable by the `neonfs` user. |
| `--tier` | `hot` (NVMe), `warm` (SATA SSD), or `cold` (HDD). |
| `--capacity` | Capacity limit. Accepts raw bytes or suffixes (`M`, `G`, `T`). `0` means unlimited. |
| `--id` | Unique drive identifier. Auto-generated from the path if omitted. |

The node checks the configured capacity against the partition's real size at startup and logs a warning if the configured value exceeds what's available.

### Listing drives

```bash
neonfs drive list
neonfs drive list --node node-2
```

Shows each drive's tier, capacity, current usage, and state (healthy, evacuating, failed).

### Evacuating a drive

Graceful removal — copy all chunks on the drive to other drives in the same tier, then mark the drive empty:

```bash
neonfs drive evacuate nvme0 --node node-2
neonfs drive evacuate nvme0 --any-tier   # allow migration across tiers if same-tier capacity is unavailable
```

Evacuation is a background job tracked by `neonfs job list --type drive_evacuate` and `neonfs job show <id>`. It respects volume durability constraints — a chunk is only deleted from the source drive once the required number of replicas exist elsewhere.

### Removing a drive

After a successful evacuation, or when a drive has failed and its data is recoverable elsewhere:

```bash
neonfs drive remove nvme0
```

If the drive still contains chunks, the command is refused. Force-removal loses the chunks on that drive:

```bash
neonfs drive remove nvme0 --force
```

Force-remove a drive with data only when:

- The drive is physically dead and its data is available elsewhere via replication/erasure coding, or
- You have already evacuated it and the state is stale for some reason, or
- The cluster is being decommissioned.

### Rebalancing

When drive capacities diverge (new drive added to a tier that's already busy), rebalance to spread load evenly:

```bash
neonfs cluster rebalance --tier hot --threshold 0.10
neonfs cluster rebalance-status
```

`--threshold 0.10` means "only move chunks when a drive's usage is more than 10% above the tier average". Raise for less aggressive rebalancing; lower to chase tighter balance at the cost of more migration.

### Manual recovery (`cluster.json`)

Drive configuration lives in `cluster.json` at `$NEONFS_DATA_DIR/meta/cluster.json`. If a node's drive config is corrupt and the CLI won't start, you can edit this file directly:

1. `systemctl stop neonfs-core` (or omnibus).
2. Edit `cluster.json`, adjust the `drives` array as needed. See [`deployment.md`](deployment.md#cluster-json-drives-format) for the schema.
3. `systemctl start neonfs-core`.

This is the break-glass path. Prefer the CLI whenever it's available.

## Upgrade procedures

NeonFS supports rolling upgrades within the same major version. Cross-major upgrades need the release notes — check `CHANGELOG.md` for breaking changes before starting.

For change-window execution with pre-flight / verification / mid-upgrade handling / rollback decision tree, see the [Cluster-Upgrade runbook](runbooks/Cluster-Upgrade.md).

### Rolling upgrade (core nodes)

1. **Pre-flight**: confirm `neonfs cluster status` shows all nodes healthy and Ra quorum is intact.
2. **Upgrade one node**: drain it of interactive work if needed, then `apt install neonfs-core=<new-version>` (or pull the new container image and restart).
3. **Restart**: `systemctl restart neonfs-core`. The node rejoins Ra with its persisted state; no re-init happens.
4. **Verify**: `neonfs cluster status` — the upgraded node reports the new version, Ra quorum remains `healthy`.
5. **Wait** a minute or two for drift indicators to settle, then move to the next node.

**Never upgrade more than one core node at a time.** Losing quorum stops metadata writes cluster-wide.

### Rolling upgrade (interface nodes)

Interface nodes are stateless — upgrade by replacing the service. In-flight requests drain during `systemctl stop`; clients reconnect transparently via service discovery.

For zero-downtime upgrades, deploy the new version alongside the old one (different host or different port) and shift traffic at the load-balancer level before decommissioning the old instance.

### Major version upgrades

Cross-major upgrades may require:

- Metadata migrations (documented in that version's release notes).
- Cookie or certificate rotations.
- Configuration-file changes.

The release notes spell this out per-release. If the upgrade path is non-trivial, practice on a test cluster before touching production.

### Rollback

If an upgrade misbehaves:

1. Stop the upgraded node: `systemctl stop neonfs-core`.
2. Downgrade the package: `apt install neonfs-core=<old-version>` (pin if needed).
3. Restart and verify `cluster status`.

Ra state, cluster CA material, and volume metadata are forward/backward compatible within a major version. If a release introduces a state-machine version bump, rollback is safe only up to the version prior to the bump — the release notes will call this out.

## Certificate authority rotation

The cluster CA issues every core and interface node certificate. Rotation is supported but non-trivial — plan it against the CA validity window shown by `neonfs cluster ca info`.

```bash
neonfs cluster ca info
neonfs cluster ca list
neonfs cluster ca revoke node-2
neonfs cluster ca rotate
```

`ca rotate` generates a new CA and reissues every node's certificate. Each node must be healthy throughout — offline nodes will fail to rotate and must be rejoined from scratch after.

For planned rotation pre-flight and post-expiry emergency recovery, see the [CA-Rotation runbook](runbooks/CA-Rotation.md).

## Troubleshooting

### Log locations

- **Debian install**: systemd journal — `journalctl -u neonfs-core` (or `neonfs-fuse`, `neonfs-nfs`, etc.). Follow with `-f`.
- **Container install**: `docker logs <container>` or `kubectl logs <pod>`. BEAM writes to stdout, Docker captures it.
- **Core internal state**: `$NEONFS_DATA_DIR/ra/` (Ra log + snapshots) and `$NEONFS_DATA_DIR/meta/` (quorum indexes, cluster.json).

Raise verbosity by setting `RELEASE_LOG_LEVEL=debug` in `/etc/neonfs/neonfs.conf` and restarting the service.

### Health checks

Each service registers health checks with `NeonFS.Client.HealthCheck` (readable via the interface's `/health` endpoint, where applicable, and included in `neonfs node status`).

```bash
neonfs cluster status
neonfs node status          # this host
neonfs node status <name>   # specific host
neonfs node list
```

A healthy cluster reports:

- `neonfs cluster status` — all expected core nodes, Ra quorum `healthy`, no in-progress migrations that shouldn't be there.
- `neonfs node status` — every configured service running, drives reporting, service-registry entries current.

### Common failure modes

**Cookie mismatch.** Nodes start but refuse to connect, logs say `Connection attempt from node ... rejected`.

Every node must share the same Erlang cookie. Verify `/var/lib/neonfs/.erlang.cookie` contents match; check permissions are `0600` and owner is `neonfs:neonfs`.

**EPMD / distribution port unreachable.** `Node.connect/1` returns `false`, or auto-bootstrap times out.

Check EPMD is up (`epmd -names` on the host), the distribution port is open in the firewall, and hostnames resolve between all peers. In Kubernetes the headless service needs `publishNotReadyAddresses: true` or formation can't find unstarted peers.

**FUSE permissions.** FUSE service starts, mount fails with `Operation not permitted`.

Container deployments must pass `--device /dev/fuse`, add `SYS_ADMIN`, and on AppArmor hosts set `apparmor:unconfined`. See [`deployment.md`](deployment.md#fuse-permissions) for the compose equivalent. Debian installs: confirm the `fuse` kernel module is loaded (`lsmod | grep fuse`) and `/dev/fuse` exists.

**Auto-bootstrap timed out.** Formation waited `NEONFS_BOOTSTRAP_TIMEOUT` ms without forming.

Check every peer is reachable (EPMD + distribution ports), `NEONFS_BOOTSTRAP_EXPECT` matches the actual replica count, and `NEONFS_BOOTSTRAP_PEERS` has the right node names. See [`orchestration.md`](orchestration.md#troubleshooting) for the per-log-message table.

**Orphaned data detected without cluster.json.** Formation refused to start because it found Ra or blob data on a node that has no `cluster.json`.

Either restore `cluster.json` from backup (if this node was previously part of the cluster) or clean the data directory (if starting fresh): remove `data/ra/`, `data/meta/*.dets`, and files under `data/blobs/` prefix directories.

**Interface node can't reach core.** Logs show `{:error, :all_nodes_unreachable}` or RPC timeouts.

Confirm `NEONFS_CORE_NODE` on the interface matches the core node's actual `RELEASE_NODE`; EPMD and the distribution port range are reachable; service discovery has had time to probe (`NeonFS.Client.Connection` / `Discovery` / `CostFunction` need a few seconds after startup).

**Drive warnings on startup.** Configured capacity exceeds the partition's real size.

Fix the capacity in `cluster.json` or use `neonfs drive remove` + `drive add` with a corrected value.

**Quorum loss.** Ra reports no leader; metadata writes hang.

Inspect which nodes are up; if a majority is reachable, Ra will elect a new leader within seconds. If a minority is reachable, decide between waiting for the minority to come back or (risky, data-loss-capable) a manual reset. See the [Quorum-Loss runbook](runbooks/Quorum-Loss.md) and the [operational runbooks index](runbooks/README.md) for other incident procedures.

**Single node down.** One node has stopped responding but the cluster otherwise has quorum.

See the [Node-Down runbook](runbooks/Node-Down.md) for the full diagnosis, recovery, and decommission-decision flow.

### Backup

Every core node's `/var/lib/neonfs` is worth backing up. Minimally:

- `meta/` — file and chunk metadata, including `cluster.json`.
- `ra/` — Ra log and snapshots (cluster membership, DLM, service registry).
- `tls/` — cluster CA and node certificates. **Back this up off-cluster** — losing the CA means every node needs a fresh identity.
- Blob storage (default `blobs/` or custom drive paths) — by far the largest. If you have replication or erasure coding, per-node backups are optional for this.

Backup strategy varies by cluster size. Small homelab clusters: stop a core node, `tar czf` its data directory, start it again. Production: arrange LVM or ZFS snapshots so the daemon never stops.

For metadata-only recovery (catastrophic operator error, Ra log corruption, accidental `volume delete`), NeonFS captures periodic point-in-time DR snapshots of cluster metadata + CA material to the `_system` volume — see `neonfs dr snapshot list / show` and the [DR-Snapshot-Restore runbook](runbooks/DR-Snapshot-Restore.md). DR snapshots are not a substitute for blob-storage replication; they cover the metadata indexes, not chunk bytes.

Proper incremental per-volume backups are a work-in-progress feature — see the [backup/restore issue](https://harton.dev/project-neon/neonfs/issues/248).

## Further reading

- [`deployment.md`](deployment.md) — Docker Compose single- and multi-host recipes, environment variables, drive management details.
- [`orchestration.md`](orchestration.md) — Docker Swarm and Kubernetes with auto-bootstrap Formation.
- [`cli-reference.md`](cli-reference.md) — complete `neonfs` CLI reference, auto-generated from `--help`.
- [Specification](https://harton.dev/project-neon/neonfs/wiki/Specification), [Architecture](https://harton.dev/project-neon/neonfs/wiki/Architecture), and [Service Discovery](https://harton.dev/project-neon/neonfs/wiki/Service-Discovery) in the wiki — design intent.
