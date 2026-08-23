# NeonFS Block

Serves NeonFS volumes as Linux block devices, over the Network Block Device
protocol or — on a host whose kernel has `ublk_drv` — directly through
`io_uring`.

A device is one sized file in a NeonFS volume, written through
`NeonFS.Core.BlockBacking` with a forced fixed chunk size so a guest write
lands on a predictable chunk boundary. Reads pull chunks over the TLS data
plane; a guest flush or FUA maps onto the volume's durability barrier and is
never acknowledged early.

## Frontends

Both frontends answer the same `NeonFS.Block.Frontend` callbacks against the
same IO core, so the device's behaviour does not depend on how a guest reached
it. What differs is the transport and who initiates.

| | NBD | ublk |
| --- | --- | --- |
| Reached by | a client dialling the listener | this node attaching the device |
| Per-device process | `NeonFS.Block.ConnectionHandler` | `NeonFS.Block.Ublk.Target` |
| Transport | TCP | `io_uring`, via a helper process on Unix sockets |
| Needs | nothing on the host | `ublk_drv`, i.e. `/dev/ublk-control` |

The ublk helper (`native/neonfs_ublk`) is a small Rust binary: it owns the
`io_uring` and ublk control ioctls and forwards every IO to the BEAM over a
socket per queue, with a four-byte length in front of each frame in either
direction. It carries no policy at all, which is the point — the half
that needs a kernel feature the CI containers do not have is the half with
nothing in it to get wrong, and everything decided about an IO is decided on
the BEAM side where it is testable everywhere.

It is compiled by `neonfs_block`'s own `mix compile` (see
`Mix.Tasks.Compile.NeonfsUblk`) even on hosts that cannot run it. Both halves
of `NeonFS.Block.Ublk.Protocol` are hand-rolled against one written layout, so
compiling them together is what stops a change to one side reaching a host
with ublk before anything notices.

`NeonFS.Block.Ublk.Supervisor.attach/2` takes the device; a node without the
driver refuses the attach rather than failing later, and does not advertise the
`:ublk` capability at all.

## Encryption

Block volumes are **not** encrypted by NeonFS. `volume create --type block`
refuses a volume with encryption configured, and the target reads and writes
whatever bytes the guest gives it.

Encrypt in the guest instead — LUKS/dm-crypt on the mapped device — so
plaintext never reaches the target process or an attach node's page cache. See
[Block Volume Encryption](https://harton.dev/project-neon/neonfs/wiki/Block-Volume-Encryption)
for the setup, the discard-passthrough caveat, and what compression and dedup
cost you.

## Metrics

Telemetry on the IO path doubles as operational metrics and as test
synchronisation:

| Event | Measurements | Metadata |
| --- | --- | --- |
| `[:neonfs, :block, :command]` | `bytes`, `duration`, `chunk_bytes` (write and zero-fill), `chunks_replaced` (zero-fill) | `export`, `command` (`:read`/`:write`/`:flush`/`:write_zeroes`), `status` |
| `[:neonfs, :block, :attached]` | `holders` | `export` |
| `[:neonfs, :block, :detached]` | — | `export` |

`NeonFS.Block.Telemetry` maps those onto Prometheus metrics, served at
`GET /metrics` on port 9573 when metrics are enabled:

```
NEONFS_BLOCK_METRICS=true
NEONFS_BLOCK_METRICS_PORT=9573
NEONFS_BLOCK_METRICS_BIND=0.0.0.0
```

Flush latency is the one to alert on. A flush is a durability barrier that
returns only once the write has reached the volume's `min_copies`, so a guest
filesystem's journal commits at exactly the rate flush returns — a slow flush
is a slow guest, whatever the read and write rates say.

## Protocol notes

- Fixed-newstyle handshake only. The oldstyle handshake is not implemented.
- Structured replies are refused (`NBD_REP_ERR_UNSUP`); simple replies carry
  every response. This server has no need to split a read or to report a hole
  separately, since an unwritten region reads as zeroes either way.
- 4Kn geometry: 4 KiB logical and physical blocks. Sub-block writes are
  absorbed by the guest's page cache.
