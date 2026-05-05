# NeonFS API Reference

Protocol-level reference for the interfaces NeonFS exposes. Complements [`user-guide.md`](user-guide.md) (which covers "when to pick each" and workflow examples) with exact behaviour, compliance notes, and caveats.

The CLI is documented separately in [`cli-reference.md`](cli-reference.md) — auto-generated from `neonfs --help` output.

## Contents

- [S3](#s3)
- [WebDAV](#webdav)
- [NFSv3 + NLM v4](#nfsv3--nlm-v4)
- [FUSE](#fuse)
- [Docker VolumeDriver](#docker-volumedriver)
- [Cluster RPC (`NeonFS.Core`)](#cluster-rpc-neonfscore)

## S3

Package: [`neonfs_s3`](../neonfs_s3/) (HTTP server), built on `firkin` (the S3 protocol framework). Endpoint: the S3 gateway binds to `NEONFS_S3_BIND:NEONFS_S3_PORT` (default `0.0.0.0:8080`).

### Authentication

- **AWS SigV4**, header or query-string. `region_name=neonfs` (or whatever you configured).
- Credentials issued by an operator via `neonfs s3 create-credential --user <identity>`. Each credential is a `(access_key_id, secret_access_key)` pair tied to one identity.
- `neonfs s3 list-credentials`, `neonfs s3 show-credential`, `neonfs s3 rotate-credential`, `neonfs s3 delete-credential` manage lifecycle.

### Addressing

- **Virtual-hosted-style**: `https://<bucket>.s3.example.com/<key>` — preferred by modern S3 clients.
- **Path-style**: `https://s3.example.com/<bucket>/<key>` — supported for legacy clients.

### Supported operations

Bucket-level:

- `ListBuckets` — buckets map one-to-one with volumes; shows those the authenticated identity has read access to.
- `HeadBucket` — 200 if the volume exists and the caller can read; 404 otherwise.
- `GetBucketLocation` — returns the configured region.

Object-level:

- `PutObject` — streaming, no whole-body buffer on the server.
- `GetObject` — including `Range:` requests.
- `HeadObject` — metadata without body.
- `DeleteObject` — single-key delete.
- `CopyObject` — server-side copy when source and destination are on the same cluster; also used implicitly by WebDAV `COPY`.
- `ListObjectsV2` — forward-paginated listing with `prefix`, `delimiter`, `continuation-token`, `max-keys`.

Multipart:

- `CreateMultipartUpload`
- `UploadPart`
- `UploadPartCopy`
- `CompleteMultipartUpload`
- `AbortMultipartUpload`
- `ListParts`
- `ListMultipartUploads`

### Conditional requests

RFC 7232. On GET/HEAD/PUT/DELETE:

- `If-Match` / `If-None-Match` (ETag based) — returns `412 Precondition Failed` on mismatch (or `304 Not Modified` for GET with `If-None-Match`).
- `If-Modified-Since` / `If-Unmodified-Since` (`Last-Modified` based) — same 412/304 semantics.

### Not supported (or behaviour worth calling out)

- **Bucket creation / deletion** via S3 — volumes are managed via the `neonfs volume` CLI by an operator. `PutBucket` / `DeleteBucket` return 501 Not Implemented.
- **Bucket policies, ACLs, lifecycle rules, CORS** — not exposed; equivalent features live in the volume config (ACL, tiering, etc.).
- **Versioning** — not supported. Every PUT replaces.
- **Object tags** — not supported.
- **Pre-signed URLs** — supported (standard SigV4).
- **SSE headers** — honoured only as an informational marker; actual encryption is volume-level and operator-controlled, not per-object.

### Limits

- Object size: no hard cap; streaming write path has no buffering. Multipart parts have the standard 5 MiB minimum for non-final parts, 5 GiB maximum per part.
- Keys: no NeonFS-specific limit beyond POSIX path limits (255 bytes per path segment, 4096 bytes total per key).

## WebDAV

Package: [`neonfs_webdav`](../neonfs_webdav/), built on `davy`. Endpoint configured per deployment.

### Authentication

HTTP Basic over TLS. Credentials separate from S3 — configured by an operator. Identity maps to a UID/GID for ACL resolution.

### RFC compliance

- **[RFC 4918](https://www.rfc-editor.org/rfc/rfc4918)** (HTTP Extensions for Web Distributed Authoring and Versioning) — compliant for the operations listed below.
- **Dead properties** — arbitrary XML properties persist per-resource; macOS Finder and Windows Explorer rely on this for extended metadata (icons, creation dates, quick-look data).

### Supported methods

| Method | Notes |
|--------|-------|
| `OPTIONS` | Announces `DAV: 1, 2`. |
| `PROPFIND` | Depth `0`, `1`, and `infinity`. `infinity` uses the namespace coordinator (when it lands — #226) for cross-node consistency. |
| `PROPPATCH` | Live and dead properties. |
| `GET` | Including `Range:`. |
| `HEAD` | Metadata only. |
| `PUT` | Streaming. No whole-body buffer on the server. |
| `DELETE` | Recursive for collections. |
| `MKCOL` | Directory create. |
| `COPY` | Collections: recursive. Cross-volume: falls back to copy-then-delete. |
| `MOVE` | Atomic within a single volume. Cross-volume: copy-then-delete. |
| `LOCK` / `UNLOCK` | Depth `0` (resource locks) and `Depth: infinity` (collection locks). |

### Locking

- Exclusive locks only (the spec's `shared` locks are optional; not implemented).
- `Timeout:` honoured; default 1 hour when not specified.
- Lock tokens are UUIDs; `If:` precondition parsing per RFC 4918.

### Dead properties storage

Stored per-resource in a sidecar ETS-backed store per interface node (`NeonFS.WebDAV.DeadPropertyStore`); persisted to the volume via extended attributes where the underlying BlobStore supports them.

### Not supported

- `DAV: 3` (bis-draft features: `PROPFIND` `allprop-include`, etc.).
- CalDAV / CardDAV extensions.
- Shared locks.

## NFSv3 + NLM v4

Package: [`neonfs_nfs`](../neonfs_nfs/). Native-BEAM NFSv3 stack — pure Elixir, no out-of-tree Rust. The cutover from the legacy `nfs3_server` NIF landed under the #113 epic.

### Protocol

- **NFSv3** ([RFC 1813](https://www.rfc-editor.org/rfc/rfc1813)) — full protocol, all RPCs.
- **NLM v4** — advisory byte-range locks. Clients must run `lockd` (standard on Linux).
- **NFSv4** — not exposed.
- **Portmapper / RPCbind** — NeonFS ships its own minimal portmapper alongside the MOUNT protocol service.

### Authentication

- **AUTH_SYS** (`uid`/`gid` over the wire). Standard behaviour — the server trusts what the client says and applies the volume's ACL + per-file POSIX mode.
- Root-squash handled at the volume ACL level: ask the operator to restrict UID 0 if desired.
- **RPCSEC_GSS** not supported.

### Mount options (client-side)

Standard Linux `mount -t nfs <host>:<export> <mountpoint> -o <options>`:

- `vers=3` — force NFSv3 (default on most distros).
- `nolock` — disables NLM. NeonFS's NLM works, but `nolock` is fine if the client doesn't need it.
- `sync` / `async` — NeonFS honours COMMIT per the spec.
- `noatime` — recommended; NeonFS volumes default to `noatime` server-side anyway.

### Export options

Managed via `neonfs nfs export <volume>` and `neonfs nfs unexport <volume>`. Current scope is binary (exported or not); host-based access control lives in the volume ACL, not the export.

### Filehandle stability

Filehandles are stable across server restarts for the lifetime of the file. A file that is unlinked and recreated gets a new handle.

## FUSE

Package: [`neonfs_fuse`](../neonfs_fuse/). Driven by `neonfs fuse mount <volume> <mountpoint>` on the same host as the `neonfs_fuse` daemon.

### Mount options

Mount happens via `fusermount3`. Standard FUSE options apply; NeonFS-specific behaviour:

- Volume's configured `atime-mode` (`noatime` / `relatime`) is honoured.
- POSIX mode bits and ownership are honoured. `chmod`/`chown` go through the volume ACL.
- `statx(2)` returns `btime` (file creation time). `ctime` is the metadata-change time per POSIX.

### Supported operations

| Operation | Notes |
|-----------|-------|
| `open`, `read`, `write`, `close` | Full POSIX. `write` is streamed through the chunker; `read` streams from chunks. |
| `pread`, `pwrite` | Offset-based. |
| `mmap` | Supported for single-reader scenarios. Shared writable mmap across nodes has no coherence guarantee. |
| `create`, `unlink` | Standard. |
| `mkdir`, `rmdir` | Standard. |
| `rename` | Atomic within a single volume. |
| `link` | Hard links supported. |
| `symlink` / `readlink` | Symlinks supported. |
| `truncate` / `ftruncate` | Standard. |
| `chmod`, `chown`, `utimes` | Subject to volume ACL. |
| `flock`, `fcntl(F_SETLK)` | Advisory, local to this mount. Use NLM via NFS for cross-host locking. |
| `getxattr`, `setxattr`, `listxattr`, `removexattr` | Planned — tracked in the native-BEAM FUSE stack (#280). |
| `fallocate` | Not yet supported. |

### Unsupported

- `fsnotify` / `inotify` propagation across nodes — only same-mount events fire.
- Cross-host mmap coherence.
- ACLs beyond POSIX mode + volume-level grants.

## Docker VolumeDriver

Package: [`neonfs_docker`](../neonfs_docker/). HTTP over a Unix socket (`/run/neonfs/docker.sock` by default; dockerd discovers it via `/etc/docker/plugins/neonfs.spec`) speaking the Docker Volume Plugin protocol v1.

### Endpoints (Docker side calls these)

| Method | Path | Purpose |
|--------|------|---------|
| POST | `/Plugin.Activate` | Plugin handshake. Returns `{"Implements": ["VolumeDriver"]}`. |
| POST | `/VolumeDriver.Create` | Create a plugin-named volume. Option `volume=<neonfs-volume>` maps to a NeonFS volume. |
| POST | `/VolumeDriver.Remove` | Remove the plugin record (does not delete the backing NeonFS volume). |
| POST | `/VolumeDriver.Mount` | Mount the volume via FUSE; returns the mountpoint path. |
| POST | `/VolumeDriver.Unmount` | Unmount. Mounts are reference-counted (`MountTracker`) — last unmount actually unmounts. |
| POST | `/VolumeDriver.Get` | Query volume info. |
| POST | `/VolumeDriver.List` | List plugin-registered volumes. |
| POST | `/VolumeDriver.Path` | Return the mountpoint for a given volume name. |
| POST | `/VolumeDriver.Capabilities` | Returns `{"Capabilities": {"Scope": "local"}}`. |
| GET | `/health` | Non-Docker; for operators and load balancers. |

## Cluster RPC (`NeonFS.Core`)

Internal surface used by interface packages. **Not a stable public API** — treat this section as reference material for contributors writing new interface packages, not as a contract.

Interface packages do not import `NeonFS.Core` directly; they route through `NeonFS.Client.Router.call/3`. The facade below is what the other side of that router exposes.

### Volume lifecycle

- `NeonFS.Core.create_volume(name, opts) :: {:ok, volume} | {:error, reason}`
- `NeonFS.Core.update_volume(volume, updates) :: {:ok, volume} | {:error, reason}`
- `NeonFS.Core.delete_volume(volume, opts) :: :ok | {:error, reason}`
- `NeonFS.Core.list_volumes(opts) :: {:ok, [volume]}`
- `NeonFS.Core.get_volume(name) :: {:ok, volume} | {:error, :not_found}`

### File I/O

- `NeonFS.Core.read_file_stream(volume, path, opts) :: Enumerable.t()` — canonical streaming read. Interface nodes use `NeonFS.Client.ChunkReader.read_file_stream/3` which wraps this over the data plane.
- `NeonFS.Core.write_file_streamed(volume, path, stream, opts) :: {:ok, file_meta} | {:error, reason}` — canonical streaming write. Whole-binary `write_file/4` is being removed (#297 / PR #380).
- `NeonFS.Core.write_file_at(volume, path, offset, data, opts) :: {:ok, file_meta} | {:error, reason}` — partial-write path (FUSE / NFS `WRITE`).
- `NeonFS.Core.get_file(volume, path) :: {:ok, file_meta} | {:error, :not_found}`
- `NeonFS.Core.list_dir(volume, path) :: {:ok, map}` — map of name → dirent.
- `NeonFS.Core.delete_file(volume, path) :: :ok | {:error, reason}`
- `NeonFS.Core.rename(volume, src_path, dst_path) :: :ok | {:error, reason}`

### ACL

- `NeonFS.Core.Authorise.check(volume, path, principal, op)` — the gate every interface calls before an I/O op.
- `neonfs acl grant|revoke|show|get-file|set-file` CLI subcommands are thin wrappers around public functions in `NeonFS.CLI.Handler`.

### Events and subscriptions

- `NeonFS.Client.EventBus.subscribe(volume)` — per-volume `:pg`-backed event stream. Events fire on create/update/delete and drive cache invalidation in FUSE/NFS/WebDAV.

### Service registry

- `NeonFS.Client.Registrar` — interface packages register themselves with type (`:s3`, `:webdav`, `:nfs`, `:fuse`, `:docker`, `:cifs`, `:csi`, …) plus metadata.
- `NeonFS.Client.Discovery.list(type)` — find registered services of a given type.
- `NeonFS.Client.CostFunction.pick(nodes)` — pick the best core node for an RPC.

## Further reading

- [`user-guide.md`](user-guide.md) — access-method selection, credentials, workflow-oriented view.
- [`operator-guide.md`](operator-guide.md) — server-side administration, cluster lifecycle, troubleshooting.
- [`cli-reference.md`](cli-reference.md) — full `neonfs` command reference, auto-generated.
- [API Surfaces wiki page](https://harton.dev/project-neon/neonfs/wiki/API-Surfaces) — design-time catalogue of API boundaries.
- Per-package moduledocs in `ex_doc` output (`mix docs` in each subproject).
