# Samba VFS Scaffold (`neonfs_cifs`) Implementation Plan

- Date: 2026-04-22
- Scope: new package `neonfs_cifs/` — first slice of [#116](https://harton.dev/project-neon/neonfs/issues/116)
- Issue: [#383](https://harton.dev/project-neon/neonfs/issues/383)

The issue body already pins package layout, target VFS operations, and out-of-scope boundaries. This plan focuses on the design decisions the issue body doesn't settle.

## Package shape

Copy from `neonfs_docker` — the most recently landed interface package and the pattern documented in the [Codebase Patterns](https://harton.dev/project-neon/neonfs/wiki/Codebase-Patterns) wiki under "Package shape for new interface packages". Deviations from that template are called out below.

```
neonfs_cifs/
├── mix.exs                                 # deps: neonfs_client only
├── config/config.exs
├── lib/neon_fs/cifs/
│   ├── application.ex                      # supervisor → HealthCheck.register → notify_ready
│   ├── supervisor.ex                       # HandleStore → Registrar(type: :cifs) → Listener
│   ├── health_check.ex
│   ├── listener.ex                         # accept() loop on the Unix socket
│   ├── connection.ex                       # one GenServer per accepted smbd worker
│   ├── frame.ex                            # ETF framing codec
│   ├── dispatcher.ex                       # VFS opcode → handler fan-out
│   ├── handlers/                           # one module per VFS op
│   │   ├── stat.ex
│   │   ├── read.ex
│   │   ├── write.ex
│   │   ├── readdir.ex
│   │   └── …
│   └── handle_store.ex                     # opaque 64-bit handle ↔ {file_id, offset, mode, …}
├── rel/{env.sh.eex, vm.args.eex}
└── test/
```

## Wire protocol

### Framing

4-byte big-endian length prefix + ETF payload. Length excludes its own 4 bytes. Max frame: 4 MiB (a sane ceiling; individual VFS ops never ship more than chunk-max bytes per roundtrip).

Bandit's `thousand_island` acceptor supports `{packet, 4}` directly via `transport_options`. Same shape `neonfs_docker` uses with its Unix socket.

### Message shape

```
Request:  {ref :: integer, op :: atom, args :: tuple}
Response: {ref :: integer, :ok, result :: term} | {ref :: integer, :error, reason :: term}
```

`ref` is a monotonically increasing integer allocated by the C shim; the Elixir side echoes it back so multiple outstanding requests can be correlated. Simpler than a session / stream ID and matches how most of the BEAM's own protocols frame request/response.

Errors map to POSIX errno atoms — `:enoent`, `:eacces`, `:eio`, etc. The C shim translates these into the integer errno values libc expects. Using atoms on the wire keeps the Elixir side readable; the translation table lives in the C shim (next slice, not this one).

## Handle allocation

VFS file/dir handles are opaque to Samba — the shim passes them back on subsequent ops. On the NeonFS side there's no POSIX fd to hand out. A simple approach:

- A `NeonFS.CIFS.HandleStore` GenServer owns a `:counters` counter and an `:ets` table.
- `open_fn` / `fdopendir_fn` allocate a new 64-bit token (`:counters.add/2`) and record `%{file_id, volume_id, mode, offset, opened_at}` keyed by token.
- `pread_fn` / `pwrite_fn` / `readdir_fn` etc. fetch their state by token.
- `close_fn` / `closedir_fn` delete the token.
- Connection teardown drops every token the connection opened (so a crashing smbd worker doesn't leak state).

Counters are monotonic per node lifetime — no reuse, no ABA risk. 64 bits are enough for centuries at any realistic op rate.

## Reads

`pread_fn` takes `(handle, offset, length)`. Back it by `NeonFS.Client.ChunkReader.read_file_stream/3` with `:offset` and `:length` set. Consume the stream directly into the response payload; ETF encodes binaries in place so there's no intermediate iolist to worry about — but **do not** materialise the whole stream: `Enum.reduce_while/3` into a bounded iolist (or just take the first chunk if the request fits), aborting as soon as we've satisfied `length`.

This keeps the `scripts/check_buffering.sh` guard happy — the read path never holds more than one chunk worth of bytes.

## Writes

`pwrite_fn` takes `(handle, offset, data)`. Two cases:

- **Co-located** (handler on the same BEAM as core): route through `NeonFS.Core.write_file_at/5` (the existing partial-write path). Single `:erpc.call` boundary.
- **Non-co-located** (handler on an interface node with remote core): use whatever `NeonFS.Client.ChunkWriter.write_file_stream/4` ships under #299 + #408's decision. Until #409 lands, fall back to `call_core(:write_file_at, …)` with an `audit:bounded` annotation referencing #299 — same shape S3 / WebDAV use today.

For #383's scope, we can ship with the remote fallback behind the annotation; #299's sub-issues handle the optimisation later. The scaffold doesn't need the streaming write path in place first.

## Supervision and lifecycle

`Supervisor.init/1` starts children in order, `:one_for_one`:

1. `NeonFS.CIFS.HandleStore` — creates ETS, crashing loses all handles (acceptable; sessions can re-open).
2. `NeonFS.Client.Registrar` with `type: :cifs`, `metadata: %{socket_path: path}`.
3. `NeonFS.CIFS.Listener` — accepts on the socket, spawns a `NeonFS.CIFS.Connection` GenServer per accepted worker.
4. A `Task.Supervisor` for detached work (e.g. directory iteration that wants backpressure beyond the frame loop).

`Application.start/2` follows the standard three-step: `Supervisor.start_link` → `HealthCheck.register_checks` → `NeonFS.Systemd.notify_ready` (guarded by `unless Application.spec(:neonfs_omnibus)`).

## Testing

Two layers for this slice:

1. **ETF frame round-trip**: per `frame.ex` — property tests for arbitrary-term encode/decode.
2. **Per-handler unit tests** (`test/neon_fs/cifs/handlers/<name>_test.exs`): mock out `NeonFS.Client.Router` / `ChunkReader` / `HandleStore` and assert each handler translates VFS op args to client calls correctly and maps client errors to POSIX atoms correctly.

Both run in-process — no Samba, no C shim, no real kernel. That's `neonfs_integration` / `vfs_neonfs.so` territory, tracked in later sub-issues.

## Config surface

`config/runtime.exs`:

| Key | Default | Purpose |
|-----|---------|---------|
| `NEONFS_CIFS_SOCKET_PATH` | `/run/neonfs/cifs.sock` | Unix socket the smbd shim connects to. |
| `NEONFS_CIFS_MAX_CONNECTIONS` | `256` | Soft cap; refuses new accepts when full. |
| Cookie / core-node bootstrap | (standard) | Same env as every other interface. |

## Out of scope — hard lines

- `vfs_neonfs.so` C shim (#384).
- Packaging (#385).
- End-to-end test with real smbd (#386).
- Second-pass ops: xattrs, locks, async pread/pwrite.
- SMB3 directory leases (#307 — depends on the namespace coordinator #226).

## Open points

1. **Which VFS call numbers to target.** Samba's `vfs_handle_struct` has ~100 operations. The issue lists 20 "Must implement". Some of those have multiple variants (`openat` vs `open`; `stat` vs `lstat` vs `fstat`). The shim chooses one per op; this plan follows the issue's naming. If the C shim discovers a variant is wrong during #384, revisit.
2. **Time representation.** Samba wants `struct timespec` for `stat` results; NeonFS stores DateTimes internally. The handler does the conversion. Use seconds+nanoseconds tuples on the wire, translated to `struct timespec` by the C shim. Matches how other BEAM↔C bridges (e.g. `:file` module) do it.
3. **Interleaving async ops on one connection.** The first cut uses a single GenServer per connection — requests are handled sequentially. That's fine for Samba's process-per-connection model but caps throughput if a single worker is doing many parallel reads. Ship sequential; measure; parallelise inside the connection via `Task.async_stream` if the workload demands it.

## References

- Issue: [#383](https://harton.dev/project-neon/neonfs/issues/383)
- Parent: [#116](https://harton.dev/project-neon/neonfs/issues/116)
- Template: `neonfs_docker/` (newest interface package)
- Wiki pattern: [Codebase Patterns — Package shape for new interface packages](https://harton.dev/project-neon/neonfs/wiki/Codebase-Patterns)
- Related: [#299](https://harton.dev/project-neon/neonfs/issues/299) (cross-node streaming writes, for the remote-core write path)
