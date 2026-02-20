# NeonFS CLI

Command-line interface for managing NeonFS clusters.

The CLI connects to a running `neonfs_core` node via Erlang distribution to
perform administrative operations.

## Commands

- **Cluster** — initialise, join (via invite token), node status
- **Volume** — create, list, delete, configure durability/tiering/encryption
- **Mount** — mount and unmount volumes via FUSE nodes

## Building

```bash
mix deps.get
mix compile
```

Or build as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../bake.hcl --load cli
```

## Usage

The CLI escript connects to the core node specified by `--node` (or the
`NEONFS_NODE` environment variable):

```bash
./neonfs_cli --node neonfs_core@hostname cluster status
./neonfs_cli volume create my_volume --replication-factor 3
./neonfs_cli mount my_volume /mnt/neonfs
```

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
