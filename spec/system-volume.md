# System Volume

The system volume (`_system`) is a special-purpose volume that stores cluster-wide operational data. It is created automatically during cluster initialisation and replicated to every node. Unlike user volumes, it is managed by the cluster itself and cannot be deleted, renamed, or reconfigured by operators.

## Purpose

NeonFS needs a durable, replicated store for cluster-wide data that doesn't belong in Ra (too large, append-only, or not consensus-critical) but must survive node failures and be accessible from any node. Examples:

- **TLS certificate authority**: CA certificate and private key for the data transfer plane's mTLS (see [Data Transfer](data-transfer.md))
- **Intent log archives**: Completed and expired intents archived from Ra to keep the state machine lean (see [Metadata — Intent Cleanup](metadata.md#intent-cleanup-and-archival))
- **Audit logs**: Security-relevant events in append-only JSONL format
- **Disaster recovery snapshots**: Cluster state snapshots for recovery

Storing this data in Ra would bloat the consensus log. Storing it on a single node's local filesystem would create a single point of failure. The system volume provides the middle ground: replicated for durability, accessible from any node, using the same storage infrastructure as user volumes.

## Properties

| Property | Value | Rationale |
|----------|-------|-----------|
| **Name** | `_system` | Leading underscore signals internal use; reserved, cannot be used by operators |
| **ID** | Deterministic, derived at init | Not random — every node generates the same ID from cluster identity |
| **Replication factor** | Cluster size (all nodes) | Every node has a complete copy; survives N-1 simultaneous failures |
| **Write acknowledgement** | `:quorum` | Writes succeed with majority acknowledgement; balances durability and availability |
| **Durability type** | `:replicate` | Never erasure-coded — full copies on every node simplify access and recovery |
| **Encryption** | `:none` | The system volume stores the CA key used for data plane encryption; encrypting it would create a circular dependency. The cluster trust model already assumes all nodes are trusted. |
| **Compression** | `:zstd` level 3 | Audit logs and JSONL archives compress well; certificates and keys are small enough that it doesn't matter |
| **Initial tier** | `:hot` | Operational data should be on the fastest available storage |
| **Deletable** | No | Deletion is rejected by VolumeRegistry |
| **Renameable** | No | Name is fixed |
| **Listable** | Hidden from `neonfs volume list` by default | Visible with `--all` or `--system` flag |

## Replication Behaviour

### Factor Equals Cluster Size

The system volume's replication factor is always equal to the number of nodes in the cluster. When nodes join or leave, the replication factor is adjusted automatically:

| Event | Action |
|-------|--------|
| Node joins cluster | Replication factor incremented; new node receives full copy |
| Node decommissioned | Replication factor decremented after data migrated off |
| Node declared dead | Replication factor remains (target); repair creates copies on remaining nodes if possible |

This means every node always has a local copy of the system volume's data. No remote reads are needed — any node can serve system volume content from local storage.

### Consistency

Writes use quorum acknowledgement (`write_ack: :quorum`), so a majority of nodes must confirm before a write succeeds. This prevents split-brain writes while allowing progress when a minority of nodes are unavailable.

Reads are always local — since every node has a full replica, there is no need for quorum reads. Consistency is maintained through the normal replication write path.

### Startup Ordering

The system volume must be available before other subsystems that depend on it (e.g., the data transfer TLS layer). During node startup:

1. Ra cluster joins and catches up
2. System volume replica verifies/repairs from Ra state
3. Local TLS certificates loaded from filesystem (cached copy of CA cert from system volume)
4. Data transfer listener starts
5. Service registration occurs

The local filesystem cache of `ca.crt` (described in [Data Transfer — Certificate Lifecycle](data-transfer.md#certificate-lifecycle)) allows the TLS layer to start even if the system volume is briefly unavailable during Ra catch-up.

## Directory Layout

```
_system/
├── tls/
│   ├── ca.crt                  # Cluster CA certificate
│   ├── ca.key                  # Cluster CA private key
│   └── crl.pem                 # Certificate revocation list (optional)
├── audit/
│   ├── intents/
│   │   ├── 2026-02-14.jsonl    # Daily intent log archives
│   │   └── ...
│   └── security/
│       ├── 2026-02-14.jsonl    # Security audit events
│       └── ...
├── snapshots/
│   └── ...                     # DR snapshots (Phase 12)
└── cluster/
    └── identity.json           # Cluster name, init timestamp, version
```

### Path Conventions

- Paths within the system volume use forward slashes, like any other volume
- Date-partitioned files use ISO 8601 format (`YYYY-MM-DD`)
- Log files use JSONL format (one JSON object per line) for append-friendliness
- Directories are created on demand when first written

## Lifecycle

### Creation

The system volume is created as the first action during `neonfs cluster init`, before any other volumes or cluster operations:

```elixir
def init_cluster(cluster_name, opts) do
  # 1. Initialise Ra cluster with single member
  :ok = RaSupervisor.start_ra_cluster()

  # 2. Create system volume (replication factor = 1 for initial single node)
  {:ok, _system_vol} = VolumeRegistry.create_system_volume()

  # 3. Generate cluster CA and store in system volume
  {:ok, ca_cert, ca_key} = NeonFS.Transport.TLS.generate_ca(cluster_name)
  :ok = write_system_file("/tls/ca.crt", ca_cert)
  :ok = write_system_file("/tls/ca.key", ca_key)

  # 4. Issue certificate for this node
  {:ok, node_cert, node_key} = NeonFS.Transport.TLS.issue_node_cert(ca_cert, ca_key)
  :ok = write_local_tls(ca_cert, node_cert, node_key)

  # 5. Store cluster identity
  :ok = write_system_file("/cluster/identity.json", encode_identity(cluster_name))

  # 6. Generate master encryption key, register first node, etc.
  ...
end
```

### Node Join

When a new node joins, the system volume's replication factor is incremented and the new node receives a full copy:

```elixir
def handle_node_join(new_node) do
  # 1. Validate invite token (existing flow)
  ...

  # 2. Increment system volume replication factor
  :ok = VolumeRegistry.adjust_system_volume_replication(cluster_size())

  # 3. Sign certificate for new node
  ca_key = read_system_file("/tls/ca.key")
  ca_cert = read_system_file("/tls/ca.crt")
  {:ok, node_cert} = NeonFS.Transport.TLS.sign_csr(csr, ca_cert, ca_key)

  # 4. Return cert + CA cert to joining node
  # (new node stores these locally and also receives system volume replica)
  {:ok, %{node_cert: node_cert, ca_cert: ca_cert}}
end
```

### Node Decommission

When a node is decommissioned, the replication factor is decremented:

```elixir
def handle_node_decommission(node) do
  # 1. Migrate data off node (existing flow)
  ...

  # 2. Decrement system volume replication factor
  :ok = VolumeRegistry.adjust_system_volume_replication(cluster_size())

  # 3. Optionally revoke node's certificate
  :ok = append_to_crl(node_cert_serial)
end
```

## VolumeRegistry Integration

The system volume is a `Volume` struct with special handling in `VolumeRegistry`:

```elixir
defmodule NeonFS.Core.VolumeRegistry do
  @system_volume_name "_system"

  # System volume uses a deterministic ID derived from cluster identity
  defp system_volume_id do
    :crypto.hash(:sha256, "neonfs:system_volume:#{cluster_name()}")
    |> Base.encode16(case: :lower)
    |> binary_part(0, 32)
  end

  @doc """
  Creates the system volume. Called once during cluster init.
  """
  def create_system_volume do
    volume = %Volume{
      id: system_volume_id(),
      name: @system_volume_name,
      owner: :system,
      durability: %{type: :replicate, factor: 1, min_copies: 1},
      write_ack: :quorum,
      repair_priority: :critical,
      tiering: %{initial_tier: :hot, promotion_threshold: 0, demotion_delay: nil},
      compression: %{algorithm: :zstd, level: 3, min_size: 0},
      encryption: %{mode: :none},
      caching: %{
        transformed_chunks: false,
        reconstructed_stripes: false,
        remote_chunks: false,
        max_memory: 0
      },
      verification: %{on_read: :always, scrub_interval: :timer.hours(24)},
      system: true
    }

    # Write via Ra — same path as user volumes
    do_create(volume)
  end

  @doc """
  Adjusts the system volume replication factor to match cluster size.
  """
  def adjust_system_volume_replication(new_cluster_size) do
    {:ok, volume} = get_by_name(@system_volume_name)
    updated = put_in(volume.durability.factor, new_cluster_size)
    do_update(updated)
  end

  # Guard: system volume cannot be deleted
  def delete(@system_volume_name), do: {:error, :system_volume}
  def delete(id) do
    case get(id) do
      {:ok, %Volume{system: true}} -> {:error, :system_volume}
      {:ok, _volume} -> do_delete(id)
      error -> error
    end
  end

  # Guard: system volume cannot be renamed
  def rename(@system_volume_name, _new_name), do: {:error, :system_volume}

  @doc """
  Lists user volumes (excludes system volume by default).
  """
  def list(opts \\ []) do
    volumes = do_list()

    if Keyword.get(opts, :include_system, false) do
      volumes
    else
      Enum.reject(volumes, & &1.system)
    end
  end
end
```

### Volume Struct Extension

The `Volume` struct gains a `system` boolean field:

```elixir
defmodule NeonFS.Core.Volume do
  defstruct [
    :id, :name, :owner,
    :durability, :write_ack, :repair_priority,
    :tiering, :compression, :encryption,
    :caching, :verification,
    :logical_size, :physical_size, :chunk_count,
    system: false    # true only for _system volume
  ]
end
```

This field is set at creation time and cannot be changed. User-created volumes always have `system: false`.

## Access API

The system volume is accessed through the same chunk-based storage pipeline as user volumes. Convenience functions wrap the volume operations for common system volume patterns:

```elixir
defmodule NeonFS.Core.SystemVolume do
  @volume_name "_system"

  @doc """
  Reads a file from the system volume.
  """
  @spec read(String.t()) :: {:ok, binary()} | {:error, term()}
  def read(path) do
    NeonFS.Core.ReadOperation.read(@volume_name, path)
  end

  @doc """
  Writes a file to the system volume.
  """
  @spec write(String.t(), binary()) :: :ok | {:error, term()}
  def write(path, content) do
    NeonFS.Core.WriteOperation.write(@volume_name, path, content)
  end

  @doc """
  Appends to a file on the system volume.
  Used for audit logs and other append-only data.
  """
  @spec append(String.t(), binary()) :: :ok | {:error, term()}
  def append(path, content) do
    NeonFS.Core.WriteOperation.append(@volume_name, path, content)
  end

  @doc """
  Lists files under a path on the system volume.
  """
  @spec list(String.t()) :: {:ok, [String.t()]} | {:error, term()}
  def list(path) do
    NeonFS.Core.ReadOperation.list_directory(@volume_name, path)
  end
end
```

## Log Retention

Append-only data (audit logs, intent archives) grows indefinitely without cleanup. A background process prunes old entries:

```yaml
# node.yaml
system_volume:
  retention:
    intent_log_days: 90        # Keep intent archives for 90 days
    security_audit_days: 365   # Keep security audit logs for 1 year
    snapshot_count: 5          # Keep last 5 DR snapshots
```

```elixir
defmodule NeonFS.Core.SystemVolume.Retention do
  @doc """
  Deletes log files older than the configured retention period.
  Runs daily via a scheduled task.
  """
  def prune do
    prune_directory("/audit/intents", retention_days(:intent_log_days, 90))
    prune_directory("/audit/security", retention_days(:security_audit_days, 365))
    prune_snapshots(retention_count(:snapshot_count, 5))
  end

  defp prune_directory(path, max_age_days) do
    cutoff = Date.utc_today() |> Date.add(-max_age_days)

    {:ok, files} = SystemVolume.list(path)

    files
    |> Enum.filter(fn file -> file_date(file) < cutoff end)
    |> Enum.each(fn file -> SystemVolume.delete(Path.join(path, file)) end)
  end
end
```

## What This Spec Does NOT Cover

- **System volume as a FUSE mount**: The system volume is not mounted via FUSE. It is accessed programmatically through `NeonFS.Core.SystemVolume` functions.
- **Operator write access**: Operators cannot write directly to the system volume. All writes go through NeonFS internal APIs.
- **Backup of system volume**: DR snapshot and backup procedures are a Phase 12 concern. The system volume itself may be part of the data that gets backed up.
- **Quota or size limits**: The system volume has no size quota. In practice, its contents (certificates, logs, snapshots) are small relative to user data.

## Implementation Phase

The system volume is **Phase 7**, a prerequisite for the cluster CA (Phase 8) and data transfer (Phase 9).

## Related Documents

- [Data Transfer](data-transfer.md) — Cluster CA stored in system volume
- [Metadata](metadata.md) — Intent log archival to system volume
- [Data Model](data-model.md) — Volume type definition
- [Security](security.md) — Cluster authentication, key hierarchy
- [Operations](operations.md) — Cluster initialisation, DR snapshots
- [Implementation](implementation.md) — Phase roadmap
