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
`Mix.Tasks.Compile.NeonfsUblk`) even on hosts that cannot run it, so building
this package needs **`libclang-dev`** — `libublk-rs-sys` generates the ublk
bindings with bindgen, which loads `libclang` at build time. Both halves
of `NeonFS.Block.Ublk.Protocol` are hand-rolled against one written layout, so
compiling them together is what stops a change to one side reaching a host
with ublk before anything notices.

### Choosing one

`NEONFS_BLOCK_FRONTEND` is `auto` (the default), `ublk` or `nbd`, and
`NeonFS.Block.select/1` resolves it against what this node can actually do.
`auto` prefers ublk and falls back; **forcing does not fall back** — it fails
naming the check that failed, because a silent fallback is how a comparison of
the two ends up measuring one of them twice.

Availability is two checks, not one, and either can fail alone: the kernel
driver (`/dev/ublk-control`) and the helper binary. A host with the driver and
a release assembled without its native binary is a real state, and being told
only "ublk unavailable" sends an operator to `modprobe` for a problem
`modprobe` cannot fix. `NeonFS.Block.frontends/0` is what the service
registration advertises, so what a node offers and what it will serve cannot
disagree.

The probe is cached per node — not for speed, it is two `File.exists?` calls,
but so that every attachment on a node agrees about what that node can do.
`NeonFS.Block.Ublk.Capability.refresh/0` is for an operator who has just
loaded the module.

### ublk is local; NBD is not

The device node appears on the kernel of the host running the target, so a
caller on another host cannot use ublk however capable this node is. That is
why the CSI driver's own selection asks a different question — whether a block
target is on *its* host — and why the shipped Kubernetes chart, whose node
DaemonSet runs no block target, always resolves to NBD.

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
