# NeonFS Core

Storage engine, metadata management, and cluster coordination for NeonFS.

This is the main server-side package. It manages chunk storage via Rust NIFs,
cluster state via Ra consensus, and exposes services that other NeonFS nodes
(FUSE, S3, etc.) consume through `neonfs_client`.

## Responsibilities

- **Chunk storage** — content-addressed blob storage with compression, encryption,
  and tiered placement (hot/warm/cold), implemented as Rust NIFs via Rustler
- **Metadata** — file, chunk, and stripe index management backed by leaderless
  quorum-replicated BlobStore
- **Cluster coordination** — Ra-backed service registry, volume registry, and
  node membership
- **Volume management** — per-volume supervision trees with configurable
  durability, tiering, compression, and encryption policies
- **Replication** — pipelined chunk replication with configurable write
  acknowledgement (local, quorum, or all replicas)
- **Erasure coding** — Reed-Solomon encoding for storage-efficient durability
- **Garbage collection** — mark-and-sweep with active write protection

## Building

```bash
mix deps.get
mix compile    # compiles Elixir and Rust NIFs
```

Rust toolchain (1.93+) is required for the NIF crates in `native/`.

## Testing

```bash
mix test                          # unit tests
mix test path/to/test.exs:42     # specific test
mix check --no-retry              # full check suite (credo, dialyzer, etc.)
```

## Running

As an OTP release:

```bash
mix release neonfs_core
_build/prod/rel/neonfs_core/bin/neonfs_core start
```

Or as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../bake.hcl --load core
```

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
