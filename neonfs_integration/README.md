# NeonFS Integration

Multi-node integration test suite for NeonFS. Every test here boots a
real cluster of BEAM peer nodes — actual Ra consensus, actual
replication, actual network partitions — and asserts the cluster
behaves correctly: this is where claims like "survives node failure"
and "heals partitions" are proven, not just stated.

The suite is scoped (#582) to cross-node cluster correctness scenarios
that genuinely need a peer cluster spanning multiple core nodes.
Per-interface integration tests (FUSE, NFS, S3, WebDAV, Docker, CSI,
...) live with their owning packages and pull peer-cluster scaffolding
in via [`neonfs_test_support`](../neonfs_test_support/).

## What it tests

- **Cluster formation** — node join, invite-token redemption, service
  registry, Ra consensus, autonomous bootstrap.
- **Replication, partition, quorum, failure recovery** — partitioning
  the cluster, killing nodes, anti-entropy, escalation, partition
  healing.
- **Cross-node correctness** — ACLs, audit log, encryption, erasure
  coding, key rotation, event notification, drive eviction,
  metadata tiering, namespace coordinator (`claim_path`,
  `claim_subtree`, `claim_rename`), DR snapshots.
- **Profilers** — app start, supervisor child timings (`@tag :profile`).
- **Benchmarks** — metadata write-throughput baseline (`@tag :benchmark`,
  excluded by default; run with `--include benchmark`). See #1291/#1292.

What's *not* here any more:

- FUSE end-to-end → `neonfs_fuse/test/integration/`
- S3 end-to-end + streaming-RSS / process-heap profile →
  `neonfs_s3/test/integration/`
- WebDAV end-to-end → `neonfs_webdav/test/integration/`
- Docker VolumeDriver end-to-end → `neonfs_docker/test/integration/`

## Running

```bash
mix test
```

Distribution is started automatically in `test_helper.exs` — no need
for `elixir --sname`.

The full suite can take several minutes. Save output to a file for
inspection:

```bash
mix test 2>&1 | tee /tmp/neonfs_integration.txt
```

## Test infrastructure

The peer-cluster scaffolding (`PeerCluster`, `ClusterCase`,
`EventCollector`, `LoopbackDevice`, `TelemetryForwarder`,
`AppProfiler`, `SupervisorStartTimer`, `MetadataBench`,
`PeerClusterTelemetry`) lives
in [`neonfs_test_support`](../neonfs_test_support/) so it can be
reused by every interface package. Reach for `alias
NeonFS.TestSupport.PeerCluster` (and friends) at the top of any new
test that needs a peer cluster.

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
