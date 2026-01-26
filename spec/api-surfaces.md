# API Surfaces

This document describes the external interfaces for accessing NeonFS: S3, FUSE, Docker/Podman, CSI, and CIFS.

## S3-Compatible API

Standard S3 operations mapped to NeonFS:

| S3 Operation | NeonFS Mapping |
|--------------|----------------|
| CreateBucket | Create volume |
| PutObject | Write file |
| GetObject | Read file |
| DeleteObject | Delete file (CoW, GC later) |
| ListObjects | Directory listing |
| HeadObject | File metadata |

Authentication via S3 signature v4 mapped to NeonFS users.

## FUSE Mount

Local filesystem access via FUSE driver:

```bash
$ neonfs mount /mnt/cluster --volume documents
```

Supports standard POSIX operations.

**Deferred features (planned for future implementation):**
- Hard links
- Extended attributes (xattr)
- Extended ACLs (POSIX.1e)
- Advisory locking (flock/fcntl)

These are deferred to reduce initial complexity and validate the core model first, not due to technical limitations. See [Appendix - Open Questions](appendix.md#open-questions) for POSIX compliance planning.

**Concurrent writers:**

Advisory locks (flock/fcntl) are deferred to a later phase, but the system handles concurrent writes safely:
- If two writers modify the same file concurrently without a lock, both writes are rejected
- No silent data loss from write conflicts
- Writers who know they're alone can proceed without locking overhead
- Once advisory locks are implemented, writers can coordinate explicitly

## Docker/Podman Volume Plugin

HTTP-based plugin protocol:

```
POST /VolumeDriver.Create   → Create subvolume
POST /VolumeDriver.Mount    → FUSE mount at container path
POST /VolumeDriver.Unmount  → Unmount
POST /VolumeDriver.Remove   → Delete subvolume (optional)
```

Usage:

```bash
$ docker volume create -d neonfs -o volume=mydata myvolume
$ docker run -v myvolume:/data myimage
```

## CSI Driver (Kubernetes)

gRPC interface implementing Container Storage Interface:

**Controller Service:**
- CreateVolume / DeleteVolume
- ControllerPublishVolume / ControllerUnpublishVolume

**Node Service:**
- NodeStageVolume / NodeUnstageVolume
- NodePublishVolume / NodeUnpublishVolume

## CIFS/SMB (via Samba)

Samba VFS module translates SMB operations to NeonFS API calls. Enables Windows client access and macOS Finder integration.

## Degraded Mode Behaviour

When a node is in a minority partition (or otherwise unable to reach quorum for writes), it enters a degraded read-only mode. Each access protocol handles this differently:

### FUSE

Write operations return `EROFS` (read-only filesystem error). No kernel-level remount is needed — the FUSE driver simply returns the appropriate error code for write syscalls while continuing to serve reads.

```
open(O_WRONLY)  → EROFS
write()         → EROFS
truncate()      → EROFS
mkdir()         → EROFS
unlink()        → EROFS
read()          → works (local data)
readdir()       → works (cached metadata)
stat()          → works
```

Applications see standard "read-only filesystem" errors and can handle them appropriately.

### S3-Compatible API

Write operations return HTTP errors with headers indicating cluster status:

| Operation | Response | Headers |
|-----------|----------|---------|
| PUT, POST, DELETE | `503 Service Unavailable` | `Retry-After: 30`, `X-NeonFS-Status: partition-minority` |
| GET, HEAD, LIST | `200 OK` (if data available locally) | `X-NeonFS-Status: partition-minority` |

**Health endpoint:**

```
GET /health
{
  "status": "degraded",
  "writable": false,
  "readable": true,
  "reason": "partition-minority",
  "quorum_reachable": false
}
```

Load balancers should check this endpoint and route write traffic to healthy nodes.

### Docker/Podman Volume Plugin

Uses FUSE underneath, so containers see `EROFS` errors for writes. The plugin health check reports degraded status:

```
GET /health
{"status": "degraded", "writable": false}
```

Container orchestrators can use this to avoid scheduling write workloads on affected nodes.

### CSI Driver (Kubernetes)

The CSI driver reports node conditions via the standard CSI health mechanisms:

- `NodeGetVolumeStats` returns `ABNORMAL` volume condition
- Kubernetes can use this for pod scheduling decisions
- Underlying FUSE mount returns `EROFS` for writes

### CIFS/SMB

The Samba VFS module returns appropriate NT status codes for write operations:

| Operation | NT Status |
|-----------|-----------|
| Create (write) | `STATUS_MEDIA_WRITE_PROTECTED` |
| Write | `STATUS_MEDIA_WRITE_PROTECTED` |
| Delete | `STATUS_MEDIA_WRITE_PROTECTED` |
| Read | Success |

Windows clients display "The media is write protected" or similar messages. This is less elegant than FUSE's `EROFS` but functionally equivalent.

### Recovery

When the partition heals and quorum is restored:
- Write operations automatically start succeeding
- No manual intervention required
- In-flight operations that received errors should be retried by clients
- Health endpoints return to normal status

## Data Unavailability

When required chunks are completely unavailable (all replicas on unreachable nodes), read operations fail. This is distinct from degraded mode where local data remains accessible.

### Detection

A read fails with unavailability when:
- **Replicated data**: All replica nodes are unreachable
- **Erasure-coded data**: Fewer than K chunks available (can't reconstruct)

```elixir
def read_chunk(hash) do
  locations = get_chunk_locations(hash)
  available = Enum.filter(locations, &node_reachable?(&1.node))

  case {volume.durability.type, length(available)} do
    {:replicate, 0} ->
      {:error, :unavailable, "No replicas reachable"}

    {:erasure, n} when n < volume.durability.data_chunks ->
      {:error, :unavailable, "Insufficient chunks for reconstruction (#{n}/#{volume.durability.data_chunks})"}

    _ ->
      # Can serve from available replicas or reconstruct
      fetch_from_available(hash, available)
  end
end
```

### Per-Protocol Error Handling

| Protocol | Error | Details |
|----------|-------|---------|
| FUSE | `EIO` | I/O error; application sees read failure |
| S3 | `503 Service Unavailable` | With `Retry-After` header and `X-NeonFS-Status: data-unavailable` |
| CIFS/SMB | `STATUS_IO_DEVICE_ERROR` | Windows shows "The device is not ready" or similar |
| CSI | Volume reported unhealthy | Pod may be rescheduled |

### Partial File Availability

A file may be partially available if some chunks are reachable and others aren't:

```
File: report.pdf (3 chunks)
  Chunk 0: available (node1, node2)
  Chunk 1: unavailable (only on node3, node3 down)
  Chunk 2: available (node1, node4)
```

**Behaviour:**
- Reads within available ranges succeed
- Reads spanning unavailable chunks fail with `EIO` / `503`
- `stat()` / `HeadObject` succeed (metadata is separate from chunk data)
- File size and attributes remain visible

This allows applications to potentially recover partial data or make informed decisions about waiting vs failing.

### Erasure Code Reconstruction

For erasure-coded volumes, data remains available as long as enough chunks exist to reconstruct:

```
10+4 erasure coding (need any 10 of 14 chunks):

Scenario A: 3 nodes down, but still have 11 chunks → Reconstructable ✓
Scenario B: 5 nodes down, only 9 chunks available → Unavailable ✗
```

Reconstruction is transparent to clients but slower than direct reads. The I/O scheduler prioritises reconstruction at user-read priority.

### Client Guidance

**Retry behaviour:**
- `503` / `EIO` for unavailability should be retried with backoff
- Include `Retry-After` header in S3 responses based on expected node recovery
- Don't retry indefinitely — if nodes are dead (not just partitioned), data may need repair first

**Timeout guidance:**
- Transient unavailability (node restart, brief partition): seconds to minutes
- Extended unavailability (hardware failure): may require repair process to complete

### Monitoring and Alerts

Unavailability events should trigger alerts:

```
neonfs_chunks_unavailable{volume="documents"} gauge
neonfs_read_unavailable_total{volume, reason} counter
neonfs_files_partially_available{volume} gauge
```

Operators can then decide whether to:
1. Wait for nodes to recover
2. Mark nodes as dead and trigger repair
3. Restore from backup if data is permanently lost
