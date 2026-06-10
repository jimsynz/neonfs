# neonfs_blob

The Rust data path of NeonFS, loaded into `neonfs_core` as a Rustler
NIF (`NeonFS.Core.Blob.Native`). Everything CPU- or I/O-heavy that
touches chunk bytes happens in this crate, behind a single NIF
boundary crossing per chunk:

- `chunking` — FastCDC content-defined chunking (plus fixed-size
  strategies) with an incremental chunker for streaming writes
- `hash` — SHA-256 content addressing
- `compression` — Zstandard
- `encryption` — AES-256-GCM, including envelope encryption parameters
- `erasure` — Reed–Solomon encoding/decoding for erasure-coded volumes
- `store` — the on-disk `BlobStore`: sharded content-addressed layout,
  tiered paths (hot/warm/cold), and the `WriteOptions`/`ReadOptions`
  pipeline that applies compression and encryption in one pass
- `index_tree` — persistent index structures backed by the blob store

The Elixir side treats chunks as opaque binaries; policy (which volume
gets which compression, encryption, durability) lives in
`neonfs_core`, while this crate executes it.

## Developing

```bash
cargo test
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

The crate is compiled automatically by `mix compile` in `neonfs_core`.
Rustler wraps return values: `Result<(), E>` success arrives in Elixir
as `{:ok, {}}`, not `:ok`.
