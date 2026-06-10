# NeonFS Core

The storage engine and control plane of [NeonFS](../README.md) — every
byte stored in a cluster and every piece of metadata about it flows
through this package.

Core nodes do the heavy lifting so interface nodes (FUSE, NFS, S3,
WebDAV, …) can stay thin: they hold the drives, run the consensus
machinery, enforce policy, and expose services that the rest of the
cluster discovers through `neonfs_client`.

## What lives here

- **Chunk storage** — content-addressed blob storage with FastCDC
  chunking, SHA-256 addressing, Zstandard compression, AES-256-GCM
  encryption, and tiered placement (hot/warm/cold), implemented as Rust
  NIFs via Rustler. A single NIF boundary crossing per chunk.
- **Metadata** — file, chunk, and stripe indexes on a leaderless
  quorum-replicated blob store (R+W>N reads/writes, hybrid logical
  clocks for conflict resolution).
- **Cluster coordination** — Ra (Raft) consensus backing the service
  registry, volume registry, and node membership.
- **Cluster CA** — a self-signed ECDSA P-256 certificate authority:
  CSR signing during node join, automatic certificate renewal, mTLS on
  the data plane, and TLS Erlang distribution.
- **Volume management** — per-volume supervision trees with independent
  durability, tiering, compression, encryption, and ACL policies.
- **Durability** — pipelined replication with configurable write
  acknowledgement (local, quorum, or all replicas), or Reed–Solomon
  erasure coding for storage-efficient redundancy.
- **Self-healing** — scrubbing, repair, anti-entropy, drive evacuation,
  and mark-and-sweep garbage collection with active-write protection.

## Building

```bash
mix deps.get
mix compile    # compiles Elixir and the Rust NIF crates in native/
```

A Rust toolchain (1.93+) is required for the NIFs.

## Testing

```bash
mix test                          # unit + property tests
mix test path/to/test.exs:42      # a specific test
mix check --no-retry              # full check suite (credo, dialyzer, …)
```

## Running

As an OTP release:

```bash
mix release neonfs_core
_build/prod/rel/neonfs_core/bin/neonfs_core start
```

Or as a container:

```bash
PLATFORMS='linux/amd64' docker buildx bake -f ../containers/bake.hcl --load core
```

For real deployments, install the `neonfs-core` Debian package — see
[`packaging/README.md`](../packaging/README.md) and the
[operator guide](../docs/operator-guide.md).

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
