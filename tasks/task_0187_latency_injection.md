# Task 0187: Latency Injection via tc/netem

## Status
Blocked — premature; deferring chaos testing to a later stage

## Phase
Gap Analysis — H-4 (4/7)

## Description
Add network latency injection capabilities to `TestCluster` using Linux
`tc` (traffic control) with `netem` (network emulator). This allows tests
to simulate slow network links between specific node pairs.

## Acceptance Criteria
- [ ] `TestCluster.inject_latency/3` adds latency between two containers: `inject_latency(cluster, node_a, node_b, delay_ms: 100)`
- [ ] Latency is bidirectional by default (both directions affected)
- [ ] `inject_latency/3` supports options: `delay_ms`, `jitter_ms`, `correlation` (percentage)
- [ ] `TestCluster.remove_latency/3` removes injected latency between two containers
- [ ] `TestCluster.inject_latency_all/2` adds latency between all node pairs
- [ ] `TestCluster.remove_latency_all/1` removes all injected latency
- [ ] Implementation uses `tc qdisc add dev eth0 root netem delay <ms>ms` inside containers
- [ ] Per-peer targeting uses `tc filter` with destination IP matching
- [ ] Containers must have `NET_ADMIN` capability (added in `TestCluster.start/1`)
- [ ] `iproute2` package available in container images (or installed via exec)
- [ ] Latency injection verified by measuring actual round-trip time
- [ ] Unit test: inject 200ms latency, verify RPC call takes >= 200ms
- [ ] Unit test: remove latency, verify RPC call returns to normal speed
- [ ] Unit test: per-peer latency affects only targeted pair
- [ ] Tests tagged `@tag :chaos`
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_integration/test/neonfs/integration/latency_injection_test.exs`:
  - Start a 3-node cluster with `setup_cluster/1`
  - Inject 200ms latency between nodes 1 and 2
  - Measure round-trip time of an RPC call between nodes 1 and 2
  - Verify it's >= 200ms (with some tolerance)
  - Verify RPC between nodes 1 and 3 is unaffected
  - Remove latency and verify normal speed

## Dependencies
- Task 0185 (TestCluster container lifecycle)

## Files to Create/Modify
- `neonfs_integration/lib/neonfs/integration/test_cluster.ex` (modify — add latency injection)
- `neonfs_integration/test/neonfs/integration/latency_injection_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-4
- `spec/testing.md` lines 770–800 (latency injection)
- Linux `tc-netem` man page
- `iproute2` documentation

## Notes
The `tc netem` commands:

```bash
# Add 100ms delay with 20ms jitter on eth0
tc qdisc add dev eth0 root netem delay 100ms 20ms 25%

# Remove
tc qdisc del dev eth0 root
```

For per-peer targeting (latency only to a specific destination):

```bash
# Create a prio qdisc with 3 bands
tc qdisc add dev eth0 root handle 1: prio bands 3

# Add netem to band 1
tc qdisc add dev eth0 parent 1:1 handle 10: netem delay 100ms

# Filter traffic to specific IP to band 1
tc filter add dev eth0 parent 1:0 protocol ip u32 match ip dst 172.18.0.3/32 flowid 1:1
```

Containers need `--cap-add=NET_ADMIN` to modify network configuration.
Update `TestCluster.start/1` to add this capability.

The NeonFS container images (from `bake.hcl`) may not include `iproute2`
by default. Either:
1. Add `iproute2` to the Containerfile
2. Install it via `Container.exec/2` at cluster start time
3. Use a sidecar container approach

Option 1 is cleanest for testing but adds to production image size. Option 2
works but is slower. Consider adding it only to a test-tagged image variant.
