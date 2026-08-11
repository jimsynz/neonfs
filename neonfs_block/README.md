# NeonFS Block

Serves NeonFS volumes as Linux block devices over the Network Block Device
protocol.

A device is one sized file in a NeonFS volume, written through
`NeonFS.Core.BlockBacking` with a forced fixed chunk size so a guest write
lands on a predictable chunk boundary. Reads pull chunks over the TLS data
plane; a guest flush or FUA maps onto the volume's durability barrier and is
never acknowledged early.

## Encryption

Block volumes are **not** encrypted by NeonFS. `volume create --type block`
refuses a volume with encryption configured, and the target reads and writes
whatever bytes the guest gives it.

Encrypt in the guest instead — LUKS/dm-crypt on the mapped device — so
plaintext never reaches the target process or an attach node's page cache. See
[Block Volume Encryption](https://harton.dev/project-neon/neonfs/wiki/Block-Volume-Encryption)
for the setup, the discard-passthrough caveat, and what compression and dedup
cost you.

## Protocol notes

- Fixed-newstyle handshake only. The oldstyle handshake is not implemented.
- Structured replies are refused (`NBD_REP_ERR_UNSUP`); simple replies carry
  every response. This server has no need to split a read or to report a hole
  separately, since an unwritten region reads as zeroes either way.
- 4Kn geometry: 4 KiB logical and physical blocks. Sub-block writes are
  absorbed by the guest's page cache.
