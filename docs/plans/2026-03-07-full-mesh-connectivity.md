# Full-Mesh Client Connectivity Plan

- Date: 2026-03-07
- Scope: `neonfs_client`, `neonfs_nfs`, `neonfs_fuse`

## Problem

Non-core nodes bootstrap to a core node, but they do not actively converge toward a full BEAM mesh after service discovery succeeds. In Swarm this leaves visible nodes with partial connectivity, which triggers OTP `global` overlapping partition protection and breaks stable service registration.

## Goals

1. Keep visible nodes converging toward a full mesh after bootstrap.
2. Retry connections when nodes disconnect or become reachable later.
3. Re-register non-core services after reconnects so `ServiceRegistry` recovers automatically.

## Planned Changes

1. Extend `NeonFS.Client.Connection` to reconcile a desired node set instead of only static bootstrap nodes.
2. Have `NeonFS.Client.Discovery` feed discovered service nodes back into `Connection` after refreshes and cache invalidations.
3. Add a reusable client registrar process for periodic service registration refresh in `neonfs_nfs` and `neonfs_fuse`.
4. Add unit tests for desired-node reconciliation, disconnect handling, and registrar retry behaviour.

## Risks

- Full-mesh code will not help if Swarm naming, EPMD, or distribution ports are still wrong.
- `connected_core_node/0` must keep returning only core nodes once non-core nodes are also connected.
- Registration retries must avoid noisy logs during startup or partitions.

## Verification

1. `neonfs_client` tests cover reconciliation and retry logic.
2. `neonfs_nfs` and `neonfs_fuse` still pass their local test suites.
3. In Swarm, each visible node eventually sees the other five nodes in `Node.list()` and re-registers after a container restart.
