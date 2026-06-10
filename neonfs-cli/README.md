# NeonFS CLI

`neonfs` — the command-line interface for administering NeonFS
clusters, from first bootstrap to drive evacuations and disaster
recovery.

It's a single static Rust binary that speaks the Erlang distribution
protocol directly (TLS included) — no BEAM, no Elixir runtime, nothing
to install on the admin machine beyond the binary itself. It connects
to a running `neonfs_core` node as just another cluster peer.

## Command surface

| Group | Covers |
|-------|--------|
| `cluster`, `node` | Bootstrap (`cluster init`), invite-token joins, status, membership |
| `drive` | Register, list, evacuate, and remove storage drives |
| `volume` | Create, configure (durability/tiering/compression/encryption), key rotation, delete |
| `fuse`, `nfs`, `s3` | Mount/export lifecycle and S3 credential management |
| `acl`, `audit` | Access control entries and the audit log |
| `gc`, `scrub`, `repair`, `job`, `worker`, `escalation` | Background maintenance: status, triggering, tuning |
| `backup`, `dr` | Backups and disaster-recovery snapshots |

The full reference with every flag lives in
[`docs/cli-reference.md`](../docs/cli-reference.md).

## Usage

The CLI connects to the daemon named by the `NEONFS_NODE` environment
variable, falling back to the node name the local daemon records in
`/run/neonfs/core_node_name`:

```bash
neonfs cluster status
neonfs volume create my-volume --replicas 3
neonfs fuse mount my-volume /mnt/neonfs
neonfs drive list
NEONFS_NODE=neonfs_core@other-host neonfs cluster status   # target another node
```

On a host with a NeonFS Debian package installed, `neonfs` is already
on `$PATH` and targets the local daemon with no configuration.

## Building

```bash
cargo build --release        # binary at target/release/neonfs-cli
```

Or as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../containers/bake.hcl --load cli
```

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
