# NeonFS CLI

Command-line interface for managing NeonFS clusters.

The CLI connects to a running `neonfs_core` node via Erlang distribution to
perform administrative operations.

## Commands

- **Cluster** — initialise, join (via invite token), node status
- **Volume** — create, list, delete, configure durability/tiering/encryption
- **Fuse** — mount and unmount volumes via FUSE nodes
- **Nfs** — export and unexport volumes via NFS nodes

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
./neonfs_cli fuse mount my_volume /mnt/neonfs
./neonfs_cli nfs mount my_volume
```

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
