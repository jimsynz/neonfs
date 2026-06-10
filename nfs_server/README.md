# NFSServer

A standalone Elixir library for building NFSv3 file servers natively
on the BEAM — no kernel NFS server, no NIFs, no out-of-tree Rust. If
you can implement a behaviour, you can serve NFS from an Elixir
application.

The whole stack is here, from the wire up:

- `NFSServer.XDR` — codec for every ONC RPC and NFSv3 data structure
- `NFSServer.RPC` — the ONC RPC framework (record marking, programs,
  TCP transport)
- `NFSServer.Mount` — the MOUNT protocol, against a
  `NFSServer.Mount.Backend` behaviour
- `NFSServer.NFSv3` — the NFSv3 procedures, against a
  `NFSServer.NFSv3.Backend` behaviour

Implement the two backend behaviours over your storage and any NFS
client built into Linux, macOS, or Windows can mount it.

The library is deliberately NeonFS-agnostic — it follows the same
split-of-concerns pattern as [`firkin`](https://hex.pm/packages/firkin)
(S3) and [`davy`](https://hex.pm/packages/davy) (WebDAV): protocol
library here, storage-specific backend elsewhere. The NeonFS backend
lives in [`neonfs_nfs`](../neonfs_nfs/), which is also the reference
implementation to copy from.

## Building and testing

```bash
mix deps.get
mix compile
mix test
```

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
