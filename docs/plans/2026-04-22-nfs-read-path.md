# NFSv3 Read-Path Procedures Implementation Plan

- Date: 2026-04-22
- Scope: `nfs_server/lib/nfs_server/v3/` (new module namespace)
- Issue: [#284](https://harton.dev/project-neon/neonfs/issues/284) — part of [#113](https://harton.dev/project-neon/neonfs/issues/113) (native-beam NFS stack)
- Prereqs: #281 (XDR codec), #282 (ONC RPC framework), #283 (MOUNT protocol) — all closed

## Scope in one line

Implement the read-only NFSv3 procedures (NULL, GETATTR, ACCESS, LOOKUP, READLINK, READ, READDIR, READDIRPLUS, FSSTAT, FSINFO, PATHCONF) as pure-Elixir handlers on top of the existing RPC framework, backed by a new `NFSServer.V3.Backend` behaviour.

## Module layout

Mirrors the existing `NFSServer.Mount.Backend` / `NFSServer.Mount.Handler` / `NFSServer.Mount.Types` split:

```
nfs_server/lib/nfs_server/v3/
├── backend.ex        # behaviour — filesystem callbacks (this issue)
├── handler.ex        # RPC dispatch → backend callbacks → XDR reply
├── types.ex          # RFC 1813 types (fhandle3, fattr3, nfsstat3, sattr3, wcc_data, …)
└── filehandle.ex     # pack / unpack fhandle3 opaque bytes
```

The `handler.ex` plugs into the existing `NFSServer.RPC.Dispatcher` for NFSv3 program number 100003. The MOUNT handler (program 100005) is already wired; adding NFSv3 follows the same pattern.

## Backend behaviour

```elixir
defmodule NFSServer.V3.Backend do
  alias NFSServer.V3.Types
  alias NFSServer.RPC.Auth

  @type ctx :: map()

  @callback lookup(Types.fhandle3(), filename :: String.t(), Auth.credential(), ctx) ::
              {:ok, Types.fhandle3(), Types.post_op_attr()}
              | {:error, Types.nfsstat3()}

  @callback getattr(Types.fhandle3(), Auth.credential(), ctx) ::
              {:ok, Types.fattr3()}
              | {:error, Types.nfsstat3()}

  @callback access(Types.fhandle3(), access_mask :: non_neg_integer(), Auth.credential(), ctx) ::
              {:ok, granted_mask :: non_neg_integer(), Types.post_op_attr()}
              | {:error, Types.nfsstat3()}

  @callback readlink(Types.fhandle3(), Auth.credential(), ctx) ::
              {:ok, target :: String.t(), Types.post_op_attr()}
              | {:error, Types.nfsstat3()}

  @callback read(Types.fhandle3(), offset :: non_neg_integer(), count :: non_neg_integer(),
                 Auth.credential(), ctx) ::
              {:ok, Enumerable.t(), eof? :: boolean(), Types.post_op_attr()}
              | {:error, Types.nfsstat3()}

  @callback readdir(Types.fhandle3(), cookie :: non_neg_integer(), cookieverf :: binary(),
                    count :: non_neg_integer(), Auth.credential(), ctx) ::
              {:ok, cookieverf :: binary(), [Types.entry3()], eof? :: boolean(),
               Types.post_op_attr()}
              | {:error, Types.nfsstat3()}

  @callback readdirplus(Types.fhandle3(), cookie :: non_neg_integer(), cookieverf :: binary(),
                        dircount :: non_neg_integer(), maxcount :: non_neg_integer(),
                        Auth.credential(), ctx) ::
              {:ok, cookieverf :: binary(), [Types.entryplus3()], eof? :: boolean(),
               Types.post_op_attr()}
              | {:error, Types.nfsstat3()}

  @callback fsstat(Types.fhandle3(), Auth.credential(), ctx) ::
              {:ok, Types.fsstat3(), Types.post_op_attr()}
              | {:error, Types.nfsstat3()}

  @callback fsinfo(Types.fhandle3(), Auth.credential(), ctx) ::
              {:ok, Types.fsinfo3(), Types.post_op_attr()}
              | {:error, Types.nfsstat3()}

  @callback pathconf(Types.fhandle3(), Auth.credential(), ctx) ::
              {:ok, Types.pathconf3(), Types.post_op_attr()}
              | {:error, Types.nfsstat3()}
end
```

**`read/5` MUST return an `Enumerable.t()`**, never a materialised binary. The handler calls `Enum.take(stream, byte_count)` (or a more targeted stream-reader helper) but never `Enum.into(<<>>)`. This preserves the no-whole-file-buffering invariant end-to-end — the backend streams, the RPC framework frames in record-marking chunks, the network sees a streamed response.

## Filehandle scheme

64-byte opaque per RFC 1813. Layout:

```
┌──────────────────────────┐
│  magic        (4 bytes)  │  "NFH1"
│  generation   (4 bytes)  │  server generation (boot time, seconds since epoch)
│  volume_id   (16 bytes)  │  NeonFS volume UUID
│  file_id     (16 bytes)  │  NeonFS file UUID
│  reserved    (24 bytes)  │  zeroed, for future use
└──────────────────────────┘
```

Properties:

- **Stateless**: the server never allocates per-client handle state. `FileIndex.get_by_id(volume_id, file_id)` resolves the handle on demand.
- **Survives restart** for the common case — `{volume_id, file_id}` is stable across server restarts.
- **Stale-handle detection**: if `file_id` was deleted, the backend returns `NFS3ERR_STALE`. Generation field lets us deliberately invalidate all handles after a fresh import / rebuild (rare).
- **No collision risk**: UUIDs are effectively unique; no hash-lookup table needed.

`NFSServer.V3.Filehandle.pack(volume_id, file_id, generation) :: binary()` and `unpack(fhandle3) :: {:ok, %{volume_id: _, file_id: _, generation: _}} | {:error, :malformed}`. Centralising this in one module keeps the byte-layout constraint obvious.

## Per-procedure notes

Most procedures are straight backend delegations. These three have non-obvious behaviour that needs calling out:

### READ — streaming semantics

```
handler.read3(req):
  {:ok, handle} = Filehandle.unpack(req.file)
  {:ok, stream, eof?, post_op_attr} = Backend.read(handle, req.offset, req.count, auth, ctx)
  encode_read_response(stream, req.count, eof?, post_op_attr)
```

`encode_read_response` writes the XDR varlen field length, then feeds the stream directly into the record-marking framer via `NFSServer.RPC.RecordMarking.stream_frame/2`. Never collects into a binary. A multi-MiB READ uses bounded RAM on the server.

### READDIR — cookie semantics

NFSv3 pagination is `(cookie, cookieverf)`-based. The client passes:

- `cookie = 0` on the first call, or the last entry's cookie on subsequent calls.
- `cookieverf` = the verifier the server returned on the previous reply, or zeros on the first call.

The server:

1. If `cookieverf != 0` and doesn't match the current directory's verifier → return `NFS3ERR_BAD_COOKIE` (client restarts enumeration).
2. Otherwise, return up to `count` bytes of entries starting after `cookie`, plus the current verifier.

Concrete cookie scheme: use the `file_id`'s high 64 bits as the per-entry cookie. Stable under insertion and deletion as long as `file_id` is stable. `cookieverf` is derived from the directory's own mtime — changes when the directory is mutated, forcing cookie revalidation.

**Include `.` and `..`** as the first two entries (explicit requirement from #108's resolution, referenced in #284's body). Cookies for them are deterministic sentinel values (`0x0000…0001` for `.`, `0x0000…0002` for `..`) so they never collide with real file IDs.

### LOOKUP — case sensitivity, trailing slashes

NeonFS is case-sensitive (POSIX). NFSv3 inherits that. `LOOKUP` on a filename containing a `/` is `NFS3ERR_INVAL`. Empty filename is `NFS3ERR_INVAL`. `.` and `..` are resolved in the handler, not the backend (`.` → same handle, `..` → parent via NeonFS's FileMeta `parent_id`).

## Testing

The existing `nfs_server` tests already cover XDR and RPC framework with unit tests. This slice adds:

1. **Per-procedure unit tests** using a mock backend (`NFSServer.V3.Backend.Mock`) — exercises the handler logic, XDR round-tripping, and RFC 1813 error mapping without standing up a real filesystem.
2. **Filehandle round-trip property test** — `pack(id1, id2, gen) |> unpack == {:ok, %{id1, id2, gen}}` for any valid inputs.
3. **Streaming READ bound test** — produce a 64 MiB pseudo-stream, call the handler, assert peak process memory during the response stays under `4 × 1 MiB` (the write-side bound, since NFSv3 `max_read` is 1 MiB by default).
4. **RFC-compliance fixture tests** — capture the byte layout of each reply for known inputs and compare against frozen fixtures. If RFC behaviour drifts, the fixture diff catches it immediately.

Integration against a real NeonFS backend lands in #286 (cutover).

## Implementation sequence

Two PRs, to keep review surfaces small:

1. **PR A** — `NFSServer.V3.{Types, Filehandle, Backend}` + NULL / GETATTR / ACCESS / LOOKUP. Simplest four procedures, exercises the full stack.
2. **PR B** — READLINK, READ (with streaming), READDIR, READDIRPLUS, FSSTAT, FSINFO, PATHCONF. Everything else.

If reviewer prefers, PR B can split further at READ (streaming adds the most new code).

## Open points

1. **AUTH_SYS credential mapping.** The handler receives `Auth.credential()` from the RPC framework; the backend decides what to do with it. Should access-mode checks happen in the handler (cleaner separation) or the backend (more context)? Leaning backend — NeonFS's own ACL + POSIX-mode resolution lives there. Confirm in PR A.

2. **`post_op_attr` always returned.** RFC 1813 allows the server to reply with a null `post_op_attr` if fetching it is expensive. For NeonFS the attributes are already in the `FileMeta` we had to fetch to do the operation, so it's free. Always return it; clients use it for attribute-cache invalidation.

3. **Large directory listings.** A directory with 1M entries makes READDIRPLUS response size a problem even with the client's `dircount`/`maxcount`. The cookie-based pagination covers this, but the backend must walk only the requested window, not the whole dir. Make sure the backend's `list_children` supports offset+limit.

## References

- Issue: [#284](https://harton.dev/project-neon/neonfs/issues/284)
- Parent: [#113](https://harton.dev/project-neon/neonfs/issues/113)
- Existing modules: `nfs_server/lib/nfs_server/mount/{backend,handler,types}.ex` — template for the V3 module split
- RFC 1813 — https://www.rfc-editor.org/rfc/rfc1813
