# NeonFS User Guide

This guide is for engineers and power users who consume a running NeonFS cluster. It assumes someone else is running the cluster — if that's you as well, start with [`operator-guide.md`](operator-guide.md).

## What you get

A NeonFS cluster exposes data through a set of network protocols:

- **FUSE** — a Linux mount point backed by the cluster.
- **NFSv3 with NLM v4** — classic NFS exports with advisory file locking.
- **S3-compatible HTTP API** — bucket/object access, multipart uploads, virtual-hosted-style addressing.
- **WebDAV** — HTTP-based filesystem with collection locks and dead properties.

Every protocol lands on the same underlying data. A file written over S3 is visible to FUSE and NFS clients (subject to path conventions); a file locked over NFS blocks writers on FUSE. Pick the protocol that fits your workload, not your data.

Your operator configures **volumes** — the unit of isolation that controls durability, tiering, compression, encryption, and ACLs. Different applications on the same cluster often use different volumes.

## CLI basics

The `neonfs` CLI drives everything users and operators do. Full reference: [`cli-reference.md`](cli-reference.md).

User-relevant commands:

- `neonfs volume list` / `neonfs volume show <name>` — see the volumes you can access.
- `neonfs acl show <volume>` — see who has what permissions on a volume.
- `neonfs acl get-file <volume> <path>` — inspect POSIX mode / owner / group on a file.
- `neonfs fuse list` — see active FUSE mounts on this host.
- `neonfs nfs list` — see NFS exports this cluster advertises.
- `neonfs s3 list-credentials` / `neonfs s3 show-credential <access-key-id>` — inspect your S3 credentials.
- `neonfs s3 bucket list` — see which volumes are reachable via S3.

Output defaults to tables. Add `--output json` for machine-readable output.

```bash
neonfs volume list --output json | jq '.[] | .name'
```

The CLI talks to its local node's control socket — it works from any host where the `neonfs-cli` package is installed and the local service is reachable.

## Choosing an access method

Pick by workload, not preference:

| Workload | Use | Why |
|----------|-----|-----|
| Local app needs POSIX filesystem semantics (mmap, random writes, locks) | **FUSE** | Closest to a real disk. Kernel-cached. mmap works within single-reader/writer constraints. |
| Legacy NFS clients, cross-host shared home directories, simple Linux sharing | **NFSv3** | Widely supported, works through normal mount tooling (`mount -t nfs`), NLM provides advisory locking. |
| Applications that already speak S3 (backups, data-science, Spark, Kafka connect) | **S3** | No mount. Signed HTTP. Plays well with IAM-style credentials and lifecycle rules. |
| Cross-platform editing (macOS Finder, Windows Explorer, Linux file managers) | **WebDAV** | Mount-and-forget over HTTPS. Handles collection locks and dead properties for desktop clients. |
| Docker volumes for containerised apps | **FUSE** or **Docker plugin** | The [`neonfs_docker`](../neonfs_docker/) VolumeDriver plugin manages FUSE mounts per container. |

You can mix freely. A common pattern: applications write via S3, ops staff read via FUSE for debugging.

### FUSE

The operator mounts volumes with `neonfs fuse mount <volume> <mount-point>`:

```bash
neonfs fuse mount my-data /mnt/my-data
```

Once mounted, the path is a regular directory tree:

```bash
ls /mnt/my-data
cp ~/work/report.pdf /mnt/my-data/reports/
```

Behaviour notes:

- **POSIX mode bits and ownership** are honoured — `chmod` / `chown` do what you expect, subject to the volume's ACL.
- **Access times**: unless the operator chose `atime-mode relatime`, `atime` updates are suppressed (`noatime`). Don't build tooling that depends on atime without checking first.
- **File creation time** (`btime` / `crtime`) is preserved and returned via `statx`. The POSIX `ctime` ("change time") reflects metadata changes as usual.
- **Locks** (`flock`, `fcntl`) are advisory and local to the mount. If you need cross-host locking, use NFS + NLM.
- **Unmount**: `neonfs fuse unmount /mnt/my-data` or `fusermount3 -u /mnt/my-data`.

The FUSE client fetches chunks lazily over the TLS data plane; the first read of a large file touches the network, subsequent reads within the local cache window are served locally.

### NFSv3 / NLM v4

Your operator publishes volumes as NFS exports with `neonfs nfs export <volume>`. Client-side, mount with standard Linux tooling:

```bash
sudo mount -t nfs nfs-host.example.com:/my-data /mnt/my-data
```

Notes specific to NeonFS's NFS implementation:

- **Protocol**: NFSv3 only. NFSv4 is not exposed.
- **Locking**: NLM v4 — advisory byte-range locks (`flock`-style). Use `lockd` on Linux clients; macOS, BSD, and Windows clients speak NLM too.
- **Root squash / ACLs** are enforced via the volume's ACL plus the file's POSIX mode. The operator controls which UIDs/GIDs can see what; see the [ACLs](#acls-and-permissions) section.
- **Async vs sync**: NeonFS respects `COMMIT` per the NFSv3 spec — `sync` mounts get proper commit semantics; `async` mounts still work but commit on the server side is best-effort between explicit `COMMIT` calls.

### S3

Your operator issues S3 credentials per identity (`neonfs s3 credential create`). Each credential is an `access_key_id` / `secret_access_key` pair. NeonFS speaks SigV4; any S3 client library will work.

```python
import boto3
s3 = boto3.client(
    "s3",
    endpoint_url="https://s3.neonfs.example.com",
    aws_access_key_id="AKIA...",
    aws_secret_access_key="...",
    region_name="neonfs",
)
s3.list_buckets()
s3.put_object(Bucket="my-data", Key="reports/april.pdf", Body=open("report.pdf", "rb"))
```

Behaviour notes:

- **Buckets map one-to-one with volumes** — the bucket name is the volume name. `neonfs s3 bucket list` shows what's visible.
- **Addressing**: virtual-hosted-style (`my-data.s3.neonfs.example.com`) and path-style (`s3.neonfs.example.com/my-data/...`) are both supported.
- **Multipart uploads**: fully supported, including `ListParts`, `AbortMultipartUpload`, and parallel part uploads.
- **Conditional requests**: `If-Match`, `If-None-Match`, `If-Modified-Since`, `If-Unmodified-Since` per RFC 7232.
- **TLS**: use the cluster's public CA if your operator published one, or disable verification only in trusted test environments.

Large uploads automatically stream through to the storage engine — the S3 gateway never holds a whole object in memory, so uploads of many gibibytes work even when the gateway has modest RAM.

### WebDAV

Mount from macOS Finder (`Go → Connect to Server → https://webdav.example.com/my-data`), Windows Explorer (`\\webdav.example.com\my-data`), or Linux (`davfs2`, `cadaver`, `gvfs`):

```bash
# Linux CLI
cadaver https://webdav.neonfs.example.com/my-data
```

Behaviour notes:

- **Authentication**: HTTP Basic over TLS. Credentials are separate from S3 credentials — your operator configures WebDAV principals.
- **Locking**: collection (directory) locks with RFC 4918 `LOCK`/`UNLOCK`. Depth `0` and `infinity` locks are supported.
- **Dead properties**: arbitrary custom properties persist across calls — macOS Finder and Windows Explorer rely on this for metadata like creation dates and icons.
- **Copy/Move**: atomic at the server when both source and destination are on the same volume.

## Volume choices to discuss with your operator

When asking for a new volume, be explicit about these trade-offs. The operator uses `neonfs volume create` (see [`operator-guide.md`](operator-guide.md#volume-management)) — your job is to say what you need.

### Durability

- **3× replication** is the usual default — tolerates two node losses, fast recovery, simple mental model.
- **Erasure coding** trades lower storage overhead for slower recovery and more compute on reads. Good for cold archives; less good for hot working sets.
- **1× replication** is only appropriate for scratch or reproducible data. A single disk failure loses the volume.

### Tiering

- **Hot** (NVMe) — latency-sensitive workloads, databases, index files.
- **Warm** (SATA SSD) — general-purpose reads and writes, home directories, user content.
- **Cold** (HDD, optionally spin-down) — archival, infrequently accessed data.

Data can move between tiers automatically — ask the operator about promotion thresholds (accesses per hour that triggers a move up) and demotion delays (hours of inactivity that trigger a move down) if the defaults don't suit your workload.

### Compression

Leave `zstd` on unless your data is already compressed (video, archives, encrypted blobs). The CPU cost is negligible compared to the bandwidth savings.

### Encryption

Server-side encryption (AES-256-GCM) costs a small CPU overhead per chunk. Turn it on for anything sensitive. Key rotation is online — ask the operator to schedule periodic rotations per compliance needs.

### Access-time tracking

`noatime` (default) suppresses access-time updates. Pick `relatime` only if a tool genuinely needs atime behaviour (rare). `strictatime` is not supported — the metadata write amplification would be prohibitive.

## Credentials and authentication

### S3

- **Creation**: operator runs `neonfs s3 create-credential --user <your-identity>`. They hand you back an `access_key_id` and `secret_access_key`.
- **Listing your credentials**: `neonfs s3 list-credentials --user <you>` (if the cluster's policy permits you to see them).
- **Rotation**: `neonfs s3 rotate-credential <access_key_id>` issues a new `secret_access_key` for an existing key ID. Clients must pick up the new secret; rotate callers one-by-one.
- **Deletion**: `neonfs s3 delete-credential <access_key_id>` immediately revokes.

Store credentials in `~/.aws/credentials` with a named profile, or inject via environment (`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, `AWS_REGION=neonfs`). Do **not** commit them to source control.

### NFS

NFSv3 authenticates by UID/GID, not by password. Your identity on the client is what the server sees. The volume's ACL (below) gates access per-UID/GID. Ask your operator to grant your UID access to the volumes you need.

### WebDAV

HTTP Basic authentication over TLS, username/password. The operator provisions principals and hands you credentials; rotate by asking for a new password.

### FUSE

FUSE mounts inherit the UID/GID of the mounting process. The volume ACL gates per-UID/GID access just like NFS.

## ACLs and permissions

NeonFS uses a two-layer permissions model:

1. **Volume ACL** — who can access the volume at all. Grants `read`, `write`, or `admin` to a principal (UID or GID).
2. **POSIX mode bits** — within a volume, standard Unix permissions on each file/directory.

Volume ACL (operator-managed, readable by users):

```bash
neonfs acl show my-data
```

File-level POSIX bits:

```bash
neonfs acl get-file my-data /reports/april.pdf         # inspect
neonfs acl set-file my-data /reports/april.pdf --mode 644 --uid 1000
```

Cross-protocol consistency:

- **FUSE** — honours POSIX bits directly. `chmod`/`chown` work.
- **NFSv3** — honours POSIX bits; the volume ACL maps to exports.
- **S3** — `PutObjectAcl` maps to the underlying POSIX bits; object-level ACLs beyond what POSIX expresses are not supported.
- **WebDAV** — locks are honoured across protocols; dead properties are stored per-resource.

If you see permission surprises, inspect both the volume ACL and the POSIX bits — both must permit the operation.

## File-level operations across protocols

Most operations do what you'd expect, but some details vary.

| Operation | FUSE | NFS | S3 | WebDAV |
|-----------|------|-----|-----|--------|
| Create file | `open(O_CREAT)` | CREATE | `PutObject` | `PUT` |
| Read range | `pread()` | READ | `GetObject` + `Range:` | `GET` + `Range:` |
| Write range | `pwrite()` | WRITE | Multipart upload (no in-place offset) | `PUT` (no in-place offset) |
| Rename / move | `rename()` | RENAME | `CopyObject` + `DeleteObject` | `MOVE` |
| Delete | `unlink()` | REMOVE | `DeleteObject` | `DELETE` |
| Lock | `flock`/`fcntl` (advisory, local) | NLM v4 (advisory, cross-host) | not applicable | `LOCK`/`UNLOCK` (collection + resource) |
| Extended attributes | `getxattr`/`setxattr` (planned) | not exposed | not exposed | dead properties |
| Access time | controlled by `atime-mode` | same | not updated | not updated |

**In-place writes**: FUSE and NFS support in-place byte-range writes (the storage engine converts these to bounded-chunk rewrites). S3 and WebDAV are object-oriented — updating a byte range means uploading the whole object (or, for S3, a new multipart upload).

**Locking across protocols**: NLM locks from an NFS client are visible to other NFS clients; FUSE `flock` is local to that mount and not shared with NFS clients. WebDAV `LOCK` is visible to other WebDAV clients. Design your application to use one locking domain at a time.

**Atomic rename**: supported within a single volume across all protocols. Cross-volume renames are not atomic — they copy then delete.

## Performance expectations

### What's fast

- **Sequential reads** — the storage engine streams chunks over the TLS data plane. Throughput is typically limited by network or client-side processing, not by the cluster.
- **Sequential writes** via streaming uploads (S3 multipart, WebDAV `PUT`, FUSE/NFS writes) — the cluster never buffers whole files.
- **Directory listings** — metadata is cached client-side via `:pg` invalidation, so repeated `ls` in the same directory is cheap after the first call.
- **Large file storage** — content-addressed chunking deduplicates identical data automatically; uploading the same file twice mostly just touches metadata.

### What's slow

- **Many small files** — every file needs a metadata round-trip. A `cp -r` of a directory with 100K files feels slow compared to the equivalent on local disk. Bundle into tarballs if you're archiving many tiny files.
- **Random writes into a cold volume** — if a chunk isn't in the hot tier, writes trigger a promotion. Warm up the working set first, or ask the operator for a hot-tier volume.
- **Cross-protocol interference** — writing via S3 while a FUSE client has the file mmap'd triggers cache invalidation on the FUSE side.
- **Metadata-heavy workloads** (many `stat` calls, deep directory trees, heavy rename churn) — Ra consensus costs ~milliseconds per metadata write. Apps that `stat` millions of files should batch, cache, or use S3 where appropriate.

### What to avoid

- **`fsync` in tight loops** — each `fsync` forces a quorum write. Batch commits.
- **Many small random writes to a single file** — each becomes a chunk rewrite. Prefer append-only or batched patterns; if the workload is genuinely page-level random, see the [log-structured write absorption design](https://harton.dev/project-neon/neonfs/issues/269) for what's coming.
- **Long-lived HTTP `Connection: close` patterns against S3** — the gateway pools TLS connections. Reuse connections; disable `Connection: close` in your client config.
- **Running `find` over a huge tree for file counts** — use `neonfs volume show` for aggregate stats instead.

### Benchmarking

If you're sizing a workload, benchmark from a client that reflects production conditions (same network path, same client hardware, same concurrency). `fio` with small-block random-read is a reasonable worst case; `dd` with a large `bs` is a reasonable best case. The middle of that range covers most real workloads.

## Getting help

- **`neonfs` output is confusing** — add `--output json` and inspect the structured response.
- **Something isn't working over one protocol but works over another** — that's useful diagnostic signal. Tell your operator which protocols, which paths, and which UIDs/credentials are involved.
- **Permission denied** — check volume ACL (`neonfs acl show <volume>`) and file POSIX mode (`neonfs acl get-file <volume> <path>`). Both must permit the operation.
- **Connection failures** — confirm you're talking to the right hostname/port and your credentials match the protocol. The operator guide's [troubleshooting section](operator-guide.md#troubleshooting) covers the server-side diagnostics.

## Further reading

- [`cli-reference.md`](cli-reference.md) — complete `neonfs` CLI command reference.
- [`operator-guide.md`](operator-guide.md) — installation, cluster operations, server-side troubleshooting.
- [Specification](https://harton.dev/project-neon/neonfs/wiki/Specification) and [Architecture](https://harton.dev/project-neon/neonfs/wiki/Architecture) in the wiki — how the cluster works internally.
