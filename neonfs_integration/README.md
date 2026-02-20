# NeonFS Integration

Multi-node integration test suite for NeonFS.

This package depends on all other NeonFS packages and uses `PeerCluster` to
spawn real Erlang peer nodes, testing the full distributed system end-to-end.

## What It Tests

- **Cluster formation** — node join, service registry, Ra consensus
- **FUSE handler** — full FUSE-to-core round-trip via Erlang distribution
- **Mount manager** — volume mount/unmount lifecycle
- **Failure scenarios** — node restart, node failure, recovery
- **Service discovery** — client connection, discovery caching, routing failover

## Running

```bash
mix test
```

Distribution is started automatically in `test_helper.exs` — no need for
`elixir --sname`.

The full suite can take several minutes. Save output to a file for inspection:

```bash
mix test 2>&1 | tee /tmp/neonfs_integration.txt
```

## Test Infrastructure

### PeerCluster

`NeonFS.Integration.PeerCluster` spawns real peer nodes for each test, providing
isolated multi-node environments. Tests use `NeonFS.Integration.TestCase`
helpers to start specific subsystems.

## Licence

Apache-2.0 — see [LICENSE](LICENSE) for details.
