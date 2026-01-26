# Operations

This document covers day-to-day operations, cluster upgrades, disaster recovery, monitoring, and telemetry.

## Cluster Initialisation

```bash
$ neonfs cluster init --name my-cluster

Generating cluster identity...
Generating master encryption key...

Store this master key securely (needed for recovery):
  Master key: bfs_mk_7x8k2m9p...

  Options:
  - Save to password manager
  - Store in external secret manager (--vault-url)
  - Derive from passphrase at startup (--passphrase-mode)

Cluster 'my-cluster' initialised.

Add nodes with:
  $ neonfs cluster create-invite
```

## Adding Nodes

```bash
# On existing node
$ neonfs cluster create-invite --expires 1h
Invite token: bfs_inv_7x8k2m...

# On new node
$ neonfs cluster join --token bfs_inv_7x8k2m...
Connecting to cluster...
Joining BEAM cluster...
Joining Ra consensus...
Syncing metadata...
Node successfully joined cluster 'my-cluster'

# Verify
$ neonfs cluster status
Cluster: my-cluster
Nodes:
  node1 (this node)  online   leader
  node2              online   follower
  node3              online   follower
```

## Creating Volumes

```bash
$ neonfs volume create documents --policy documents-policy
Volume 'documents' created

$ neonfs volume create media --policy media-archive-policy
Volume 'media' created
```

## Monitoring

Key dashboards to build:

- **Cluster health**: Node states, Ra leader, consensus latency
- **Storage capacity**: Per-node, per-tier usage and projections
- **Replication status**: Under-replicated chunks, repair progress
- **Performance**: Read/write latencies, throughput, cache hit rates
- **Drive health**: Spin states, SMART data, error rates

## Decision Points

Events requiring operator input (CLI, web UI, or agent):

```
┌────────────────────────────────────────────────────────────────┐
│ DECISION REQUIRED: Node Unreachable                            │
├────────────────────────────────────────────────────────────────┤
│ Node: node3                                                    │
│ Unreachable since: 2h 15m ago                                  │
│                                                                │
│ Affected chunks: 145,000                                       │
│ Current durability:                                            │
│   - Fully satisfied: 94%                                       │
│   - Degraded: 5.9%                                             │
│   - At risk: 0.1%                                              │
│                                                                │
│ Repair estimate:                                               │
│   - Data to move: 2.3 TB                                       │
│   - Storage required: 2.3 TB (available: 4.1 TB)               │
│                                                                │
│ Options:                                                       │
│   [1] Wait - Continue monitoring                               │
│   [2] Partial repair - Fix at-risk chunks only (~50 GB)        │
│   [3] Full repair - Assume dead, complete resilver             │
│   [4] Maintenance - I'm working on it, remind in __ hours      │
└────────────────────────────────────────────────────────────────┘
```

## Audit Logging

Security-relevant events are logged for operational visibility and debugging. These are operational logs, not tamper-proof forensic records.

```elixir
%AuditEvent{
  timestamp: ~U[2025-01-15T10:30:00Z],
  event_type: :node_joined,
  node: :node5,
  details: %{invite_token_hash: "abc123..."}
}
```

Events logged:
- Node join/leave
- Volume create/delete
- Access control changes
- Repair operations
- Administrative actions

**Limitations:**

Audit logs are stored within the cluster. Given the BEAM trust model (any node has full cluster access), a compromised node can modify or delete audit entries. These logs are useful for:
- Operational debugging
- Understanding cluster history
- Detecting accidental misconfigurations

They are **not** suitable for:
- Forensic evidence after a breach
- Compliance requiring tamper-proof audit trails
- Detecting malicious insider activity

**For tamper-resistant audit trails**: Ship logs to an external system (syslog, SIEM, append-only S3 bucket) that cluster nodes cannot modify. This is a deployment concern, not something NeonFS provides.

## Telemetry and Observability

Both Elixir and Rust components emit telemetry for monitoring and debugging.

### Metrics (Prometheus/StatsD)

Elixir control plane:
```
# Cluster state
neonfs_nodes_total{state="online|degraded|unreachable"}
neonfs_ra_term
neonfs_ra_commit_index

# Metadata operations
neonfs_metadata_ops_total{op="read|write|delete"}
neonfs_metadata_op_duration_seconds{op, quantile}

# Replication
neonfs_replication_queue_depth
neonfs_replication_lag_seconds
neonfs_chunks_under_replicated
```

Rust data plane:
```
# Storage I/O
neonfs_blob_read_bytes_total{node, tier}
neonfs_blob_write_bytes_total{node, tier}
neonfs_blob_read_duration_seconds{tier, quantile}
neonfs_blob_write_duration_seconds{tier, quantile}

# Chunking
neonfs_chunking_duration_seconds{quantile}
neonfs_chunks_created_total

# FUSE operations
neonfs_fuse_ops_total{op="read|write|lookup|readdir|..."}
neonfs_fuse_op_duration_seconds{op, quantile}
neonfs_fuse_errors_total{op, error_type}

# Drive state
neonfs_drive_state{node, path, state="active|standby|spinning_up"}
neonfs_drive_spinups_total{node, path}
```

### Distributed Tracing

Traces span both Elixir and Rust:

```
[Client write request]
  └─ [Elixir: receive request]
       ├─ [Rust: chunk data] ─── chunk_count, total_bytes
       │    └─ [Rust: hash chunks]
       ├─ [Elixir: check dedup]
       ├─ [Rust: write local blob] ─── tier, duration
       ├─ [Elixir: initiate replication]
       │    ├─ [RPC to node2]
       │    │    └─ [Rust: write blob on node2]
       │    └─ [RPC to node3]
       │         └─ [Rust: write blob on node3]
       └─ [Elixir: commit metadata]
```

Use OpenTelemetry for cross-language trace context propagation.

### Structured Logging

Both components emit JSON-structured logs:

```json
{
  "timestamp": "2025-01-15T10:30:00.123Z",
  "level": "info",
  "component": "rust.blob_store",
  "node": "node1",
  "event": "chunk_written",
  "chunk_hash": "sha256:abc123...",
  "tier": "hot",
  "size_bytes": 262144,
  "duration_ms": 12,
  "trace_id": "abc123",
  "span_id": "def456"
}
```

### Debug Endpoints

Each node exposes debug endpoints (authenticated):

```
GET /debug/ra_state       # Ra cluster state
GET /debug/pending_writes # In-flight writes
GET /debug/replication    # Replication queue
GET /debug/drives         # Drive states and health
GET /debug/cache          # Cache hit rates, contents
```

---

## Cluster Upgrades

### Version Compatibility

Upgrading a running cluster requires careful coordination. Nodes running different versions must interoperate during rolling upgrades.

**Metadata versioning:**

All persistent metadata structures include a schema version:

```elixir
%FileMeta{
  schema_version: 2,
  id: "...",
  # ... fields
}

%ChunkMeta{
  schema_version: 1,
  hash: "...",
  # ... fields
}
```

**Migration approach:**

Similar to database migrations, metadata schema changes are versioned and applied incrementally:

```elixir
defmodule NeonFS.Migrations do
  # Migrations are numbered and applied in order
  @migrations [
    {1, NeonFS.Migrations.V1_InitialSchema},
    {2, NeonFS.Migrations.V2_AddHLCTimestamps},
    {3, NeonFS.Migrations.V3_SplitDirectoryEntries}
  ]

  def migrate(from_version, to_version) do
    @migrations
    |> Enum.filter(fn {v, _} -> v > from_version and v <= to_version end)
    |> Enum.each(fn {_v, module} -> module.up() end)
  end
end
```

**Rolling upgrade protocol:**

```
1. New version deployed to one node, joins cluster
2. New node advertises its version capabilities
3. Cluster operates in "compatibility mode" (lowest common version)
4. Once all nodes upgraded, leader triggers migration
5. Migration runs as background operation
6. Cluster exits compatibility mode
```

**Design requirements (to be detailed during implementation):**

- Backwards-compatible wire protocol changes (additive only, or versioned)
- Metadata readers must handle older schema versions
- Migrations must be resumable (crash-safe)
- Rollback path for failed upgrades
- Version skew limits (e.g., max 1 major version difference between nodes)

This area requires detailed design before Phase 2 (clustering) to ensure upgrades are possible from the start.

---

## Disaster Recovery

### Recovery Scenarios

**Single node failure:**

1. Cluster detects node unreachable
2. Escalation ladder determines timing
3. When repair triggered, chunks re-replicated from surviving copies
4. Prioritised by risk tier and volume configuration

**Multiple node failure (quorum intact):**

1. Same as single node, parallelised
2. May trigger capacity warnings if insufficient space
3. Repair rate-limited to avoid overwhelming remaining nodes

**Catastrophic failure (single survivor):**

1. Surviving node continues serving local data (degraded mode)
2. New nodes join via invite token
3. Data re-replicated from survivor
4. Once quorum restored, normal operation resumes

**Total loss (restore from backup):**

1. Initialise new cluster
2. Restore metadata from backup
3. Restore chunks from external backup (S3, tape, etc.)
4. Verify integrity via scrubbing

### Backup Strategy

NeonFS replication is not backup — it protects against node failure, not against accidental deletion, corruption, or ransomware.

**Recommended backup approach:**

- Regular snapshots of metadata (Ra state)
- Periodic export of chunks to external storage (S3-compatible)
- Test restores periodically

```bash
# Export volume to external S3
$ neonfs backup create documents \
    --destination s3://backup-bucket/neonfs/ \
    --schedule daily

# Restore
$ neonfs backup restore documents \
    --source s3://backup-bucket/neonfs/documents-2025-01-15/
```

### Suspected Node Compromise

Due to BEAM's full-trust distribution model, if a node is suspected of compromise, the entire cluster should be considered compromised. The compromised node had access to:

- All chunk data across the cluster
- Decryption keys for server-side encrypted volumes
- All metadata (file paths, permissions, user definitions)
- Ability to modify or delete audit logs stored within the cluster

**Response steps:**

1. **Isolate immediately**: Remove the suspected node from the network (WireGuard/VPN/firewall) to prevent further access
2. **Remove from cluster**: Use Ra to remove the node from cluster membership
3. **Rotate Erlang cookie**: Generate a new cookie and distribute to all remaining nodes (requires cluster restart)
4. **Rotate encryption keys**: Generate new volume encryption keys and re-encrypt affected data if server-side encryption was in use
5. **Review external audit logs**: Logs within NeonFS may have been tampered with; rely only on logs shipped to external systems
6. **Assess data exposure**: Assume all data was exfiltrated; notify affected users if applicable
7. **Consider full restore**: If data tampering is suspected, restore from a known-good backup predating the compromise

**Prevention:**

- Run nodes only on trusted, isolated networks (WireGuard mesh recommended)
- Minimise the number of people with physical/root access to nodes
- Ship audit logs to external, append-only storage that cluster nodes cannot modify
- Monitor for unexpected node join attempts or Ra membership changes

This is a fundamental limitation of Erlang distribution. More granular trust boundaries would require abandoning BEAM's operational benefits (hot code reload, distributed shell, cluster-wide debugging).
