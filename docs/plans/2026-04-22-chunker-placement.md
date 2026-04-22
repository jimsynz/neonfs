# Chunker Placement for Cross-Node Streaming Writes

- Date: 2026-04-22
- Scope: `neonfs_client` (possibly new NIF), `neonfs_core` (possibly new data-plane frame), S3/WebDAV write paths
- Issue: [#408](https://harton.dev/project-neon/neonfs/issues/408) — design decision, unblocks #409/#410/#411/#412/#413
- Parent: [#299](https://harton.dev/project-neon/neonfs/issues/299)

## Problem

S3 and WebDAV interface nodes currently drain the incoming HTTP body into a single binary before calling `call_core(:write_file, …)` when the core node is remote — six callsites in `neonfs_s3/lib/neon_fs/s3/backend.ex`, one fallback in `neonfs_webdav/lib/neon_fs/webdav/backend.ex:173`. A multi-GiB upload OOMs the interface node.

The fix is to stream bytes out of the HTTP body and into NeonFS chunks without ever materialising the whole body. That's easy when the interface and core share a BEAM VM (the stream stays in-process and `write_file_streamed/4` does the work). It's hard when they don't — Erlang distribution cannot carry an `Enumerable.t()` across nodes.

Two placements could solve this:

1. **Interface-side chunking.** Run the incremental FastCDC chunker on the interface node. Ship each produced chunk to the appropriate replica node(s) via the existing `Router.data_call(:put_chunk, …)` data plane. Commit the resulting chunk list on core via a new `commit_chunks` RPC.

2. **Core-side streaming.** Keep the chunker on core. Add a new data-plane frame (`:put_chunk_stream` or similar) that lets the interface node push body bytes to a remote chunker process running on core. Core chunks, dedups, commits — everything stays server-side.

This doc picks one and explains why.

## Relevant context

- The `neonfs_blob` Rust NIF crate lives at `neonfs_core/native/neonfs_blob/`. It has incremental chunker entry points `chunker_init` / `chunker_feed` / `chunker_finish` already landed (`lib.rs`, backed by `chunking.rs`).
- The TLS data plane today handles three request shapes in `neonfs_client/lib/neon_fs/transport/handler.ex` — `put_chunk`, `get_chunk`, `has_chunk`. Each is a request/response round-trip, no streaming.
- `NeonFS.Client.Router.data_call/4` is the client-side entry point to the data plane. It selects a target node, borrows a pooled TLS connection from `NimblePool`, sends one framed message, and awaits one framed reply.
- Interface packages depend only on `neonfs_client`. They do **not** currently link any Rust NIF — `neonfs_blob` is a `neonfs_core` dependency.
- PR #298 (closing #207) put `NeonFS.Client.ChunkReader.read_file_stream/3` in `neonfs_client`. It builds a distribution-safe `Stream` locally and calls `Router.data_call(:get_chunk, …)` per chunk. This is the symmetric precedent for the write side.

## Option 1 — Interface-side chunking

Shape:

1. Duplicate or move the chunker code into a new NIF crate accessible to `neonfs_client`. The simplest shape: extract just the chunker (plus its `hash` + `chunking` modules, with dependencies trimmed) into a new crate `neonfs_chunker` under `neonfs_client/native/neonfs_chunker/`. Cargo workspace handles the source sharing; we don't need duplicate code.
2. Add `NeonFS.Client.ChunkWriter.write_file_stream(volume, path, stream, opts)` in `neonfs_client`. Implementation:
   - `chunker_init(opts)` produces a chunker resource.
   - For each element in `stream`: `chunker_feed(ref, bytes)` returns a list of newly-complete chunks; `put_chunk` each one via `Router.data_call` to the chosen replica.
   - `chunker_finish(ref)` returns any trailing partial chunk; `put_chunk` that too.
   - Accumulate `(hash, size)` per chunk.
3. On success, call `NeonFS.Core.commit_chunks(volume, path, chunk_list, opts)` via standard RPC to record the file.

Costs:

- **Chunker NIF duplicated into `neonfs_client`** — small cost (tens of KB compiled, shared Rust source via the Cargo workspace). Worth noting that `neonfs_blob` includes more than the chunker (compression, encryption, erasure, blob store). We only pull the chunker path into `neonfs_chunker` — the rest stays in `neonfs_blob`.
- **FastCDC CPU on interface nodes** — scales with the interface fleet. For Pi-class deployments where interface nodes are dedicated S3 endpoints, this is a feature not a bug; the core node is usually the bottleneck and offloading chunking there is good. For single-omnibus deployments it's a wash (same BEAM).
- **Encryption/compression still happens on core** during `put_chunk` handling. Interface nodes never see plaintext keys — good for multi-tenant deployments where the interface may be less-trusted.

Benefits:

- **Zero new data-plane frames.** `put_chunk` already exists; `commit_chunks` is an ordinary Erlang RPC (not a data-plane frame). Every existing data-plane observability, pooling, retry, and backpressure behaviour carries through.
- **Symmetric with the read path.** PR #298 moved the chunk streaming logic into `neonfs_client`; this parallels it. One place to look for "how does a chunk move between an interface node and a replica".
- **Failure modes are simple.** Each `put_chunk` is an independent request/response; partial failures surface as normal `{:error, _}` tuples and the writer retries or aborts.

Risks:

- **NIF distribution complexity** — every interface package ends up loading `neonfs_chunker`. Build matrices must cover every architecture the interface supports. In practice this is the same matrix `neonfs_core` already covers (amd64 + arm64 via nfpm + Docker), so the incremental release cost is low.
- **Version skew** — the chunker's output is content-addressed, so an interface running a newer chunker produces the same hashes for the same bytes as an older core. We already rely on this invariant — FastCDC parameters are frozen constants (`CDC_MIN_SIZE`, `CDC_AVG_SIZE`, `CDC_MAX_SIZE`). Changing them would be a deduplication-breaking event regardless of placement.
- **Client-side abort tracking** — if the interface crashes mid-stream, chunks are orphaned on replicas. The `PendingWriteLog` pattern in `neonfs_core` solves this server-side; the interface-side equivalent needs to live in `neonfs_client`. Not trivial, but bounded.

## Option 2 — Core-side streaming

Shape:

1. Add a new data-plane frame, roughly `{:put_chunk_stream_start, ref, volume_id, path, opts} → {:ok, ref, stream_id}`, plus `{:put_chunk_stream_frame, stream_id, bytes, final?} → :ok`, plus `{:put_chunk_stream_commit, stream_id} → {:ok, chunk_hashes} | {:error, reason}`.
2. On core, `NeonFS.Transport.Handler` dispatches to a new handler module that spawns a per-stream driver. The driver feeds incoming frames into the existing `write_file_streamed/4` pipeline.
3. Add `NeonFS.Client.ChunkWriter.write_file_stream/4` that opens a stream, sends each body frame, and commits.

Costs:

- **New data-plane protocol surface** — one new frame type, new handler, new driver process lifecycle on core, new client-side state machine. Every new wire shape is a new surface for bugs, version skew, and observability gaps.
- **No existing NimblePool shape for streams.** The current `data_call` is request/response; a streaming write needs persistent connection affinity, in-flight ACK tracking, and backpressure flow control that doesn't currently exist on this transport. Non-trivial to get right.
- **Core bears the FastCDC CPU cost.** On small clusters (where interfaces are co-located with core) this is the same CPU. On Pi-class core nodes serving many interface endpoints, it becomes the bottleneck we were trying to avoid.

Benefits:

- **Chunker stays in one crate.** No duplication, no Cargo workspace restructure.
- **Server-side abort is already solved.** Orphan cleanup via `PendingWriteLog` just works — it's the same code path that already handles local streaming writes.

Risks:

- **Backpressure semantics** — if the stream driver on core is slower than the interface sender, we need either bounded in-flight frames (pull model) or drop-on-overflow (ugly). The existing `{active, 10}` pattern works for request/response; streaming needs more.
- **Connection lifetime coupling** — a mid-stream connection drop leaves the driver process with a half-written pending write. Plumbing the cleanup is more complex than "retry the `put_chunk`".

## Decision

**Option 1 — interface-side chunking.**

Rationale:

1. **It matches the symmetry we already established with `ChunkReader`.** Builds a small, well-defined neonfs_client API surface with one place to reason about "how does a chunk cross the interface↔replica boundary".
2. **It reuses the existing data plane wholesale.** No new frames, no new handler, no new backpressure shape, no new connection lifetime coupling. Every existing test, observability signal, and retry policy on `put_chunk` applies to the new path for free.
3. **CPU offload is the right direction.** Chunking on interface nodes distributes FastCDC CPU across the interface fleet, which is where we want it. Core nodes stay lean — they do replication, quorum, and policy, not per-byte CPU on every upload.
4. **The NIF duplication cost is small and bounded.** We extract a single `neonfs_chunker` crate from `neonfs_blob` into a Cargo workspace member, referenced by both `neonfs_core` and `neonfs_client`. One crate, shared source, no code duplication.

Option 2 wins on abort-tracking simplicity, which is real, but the fix is scoped: an interface-side `PendingWriteLog` ported to `neonfs_client`. That's a well-understood pattern — not a new consensus problem.

## Implications for the rest of #299

With this decision settled, the remaining sub-issues take concrete shape:

- **#409 — `NeonFS.Client.ChunkWriter`** — loads `neonfs_chunker` NIF, drives `put_chunk` via `Router.data_call`, returns chunk list.
- **#410 — `NeonFS.Core.commit_chunks/4`** — accepts pre-chunked hashes, validates via `has_chunk`, writes a `FileIndex` entry with the usual quorum-write semantics. Lock + share-mode enforcement mirrors `write_file_streamed/4`.
- **#411 — S3 migration** — six callsites swap `call_core(:write_file, [_, _, body_binary, _])` for `ChunkWriter.write_file_stream(_, _, stream, _)` + `commit_chunks(_, _, hashes, _)`.
- **#412 — WebDAV migration** — the `audit:bounded cross-node fallback` path swaps to the same shape. The `audit:bounded` annotation disappears.
- **#413 — peer-cluster integration test** — multi-GiB upload, peak-RSS assertion on both interface and core.

An interface-side `PendingWriteLog` is implicit in #409 (the writer has to know what chunks it's written so it can abort them on error). The shape mirrors `neonfs_core/lib/neon_fs/core/pending_write_log.ex` — DETS table under a new `NeonFS.Client.PendingWriteLog` with the same `open_write` / `record_chunk` / `clear` lifecycle.

## Open questions carried forward

1. **Does `neonfs_chunker` need the full FastCDC impl, or can we link against `fastcdc` crate directly?** The current impl is a thin wrapper around `fastcdc` with NeonFS-specific hash/rolling params. If those params remain the constants we already have, a direct `fastcdc` dep in `neonfs_client` might avoid the workspace shuffle entirely. Worth prototyping in #409.

2. **Per-stream NIF resource lifetime.** The chunker is an incremental resource. If the interface process dies mid-stream, the resource's dtor runs and frees native state — good. But we should confirm there's no shared mutable state in the FastCDC roller we haven't noticed. Test in #409.

3. **Abort atomicity under stream interruption.** If the interface crashes after `put_chunk` has succeeded but before `commit_chunks`, the chunks are orphaned. GC reclaims them eventually; for fast recovery we want the interface-side `PendingWriteLog`. Scope: add to #409 or break out as a separate sub-issue. Leaning towards folding it into #409 because it's coupled.

## References

- Parent: [#299](https://harton.dev/project-neon/neonfs/issues/299)
- Read-side precedent: PR #298 (closed #207), `NeonFS.Client.ChunkReader`
- Relevant source: `neonfs_core/native/neonfs_blob/src/{chunking,lib}.rs`, `neonfs_core/lib/neon_fs/core/write_operation.ex` (write_chunk_to_remote), `neonfs_client/lib/neon_fs/transport/handler.ex`, `neonfs_core/lib/neon_fs/core/pending_write_log.ex`
