# Task 0083: BlobStore Metadata Namespace

## Status
Complete

## Phase
5 - Metadata Tiering

## Description
Add a metadata storage namespace to the BlobStore, parallel to the existing `blobs/` directory. Metadata records are stored using the same atomic write + fsync pattern as data chunks, giving metadata identical durability guarantees. The directory structure is `{base_dir}/meta/{segment_id}/{prefix}/{key_hash}`. Keys are deterministic (derived from record type + primary key, e.g., `SHA256("chunk:" ++ chunk_hash)`), not content-addressed. Also create a MetadataStore Elixir module that wraps BlobStore metadata calls, manages per-segment ETS caches, and handles serialisation and HLC timestamps.

## Acceptance Criteria
- [ ] New Rust `metadata_path` function in `path.rs`: `metadata_path(base_dir, segment_id, key_hash, prefix_depth)` → `{base_dir}/meta/{segment_id}/{prefix_1}/{prefix_2}/{key_hash}`
- [ ] New Rust methods in `store.rs`:
  - `write_metadata(segment_id, key_hash, data)` — atomic write (temp file + fsync + rename), same pattern as chunk writes
  - `read_metadata(segment_id, key_hash)` — read raw bytes
  - `delete_metadata(segment_id, key_hash)` — remove file
  - `list_metadata_segment(segment_id)` — directory walk, returns list of key hashes
- [ ] New NIF exports in `lib.rs`:
  - `metadata_write(store, segment_id, key_hash, data)` → `{:ok, {}}`
  - `metadata_read(store, segment_id, key_hash)` → `{:ok, binary}` or `{:error, :not_found}`
  - `metadata_delete(store, segment_id, key_hash)` → `{:ok, {}}`
  - `metadata_list_segment(store, segment_id)` → `{:ok, [binary]}`
- [ ] NIF declarations added to `native.ex`
- [ ] BlobStore GenServer extended with metadata API functions (GenServer calls wrapping NIFs)
- [ ] New `NeonFS.Core.MetadataStore` module:
  - Wraps BlobStore metadata calls
  - Manages per-segment ETS caches (read-through cache with configurable size)
  - Handles serialisation: `:erlang.term_to_binary/1` for writes, `:erlang.binary_to_term/1` for reads
  - Attaches HLC timestamps to all records on write
  - Supports tombstone markers for deletes (needed for quorum replication)
  - DynamicSupervisor children per segment (one process per segment this node is responsible for)
- [ ] `MetadataStore.write/3` — `(segment_id, key, value)` → serialises value with HLC timestamp, writes via BlobStore, updates ETS cache
- [ ] `MetadataStore.read/2` — `(segment_id, key)` → reads from ETS cache or BlobStore, deserialises
- [ ] `MetadataStore.delete/2` — `(segment_id, key)` → writes tombstone marker, updates cache
- [ ] `MetadataStore.list_segment/1` — `(segment_id)` → returns all live keys in segment (excludes tombstones)
- [ ] `MetadataStore.load_segment/1` — loads all records from BlobStore directory walk into ETS (for startup/rebalancing)
- [ ] Metadata prefix_depth matches chunk prefix_depth (default 2)
- [ ] Metadata stored on hot-tier drives: MetadataStore selects a hot-tier drive for the `meta/` namespace via DriveRegistry; falls back to warmest available tier if no hot-tier drive exists
- [ ] Drive selection is per-node (independent of the consistent hashing ring — the ring determines which nodes, each node picks its fastest drive)
- [ ] Rust unit tests: write/read/delete metadata round-trip, list_metadata_segment
- [ ] Rust unit tests: metadata path generation
- [ ] Elixir unit tests: NIF round-trip through NIF boundary
- [ ] Elixir unit tests: MetadataStore write/read/delete/list with serialisation
- [ ] Elixir unit tests: tombstone handling (deleted keys excluded from list)
- [ ] Elixir unit tests: ETS cache hit/miss behaviour

## Testing Strategy
- Cargo test for Rust metadata path generation and store operations
- Cargo test for atomic write pattern (crash simulation — temp file left behind)
- ExUnit tests calling metadata NIFs directly (round-trip binary data)
- ExUnit tests for MetadataStore: write Elixir terms, read back, verify equal
- ExUnit tests for MetadataStore tombstones: write, delete, verify excluded from list but tombstone record exists
- ExUnit tests for segment loading: write several records, call load_segment, verify all in ETS
- Verify existing blob store tests pass unchanged

## Dependencies
- task_0080 (HLC — timestamps on metadata records)
- task_0082 (MSM v5 — segment assignments determine which segments to open)

## Files to Create/Modify
- `neonfs_core/native/neonfs_blob/src/path.rs` (modify — add `metadata_path` function)
- `neonfs_core/native/neonfs_blob/src/store.rs` (modify — add metadata write/read/delete/list methods)
- `neonfs_core/native/neonfs_blob/src/lib.rs` (modify — add metadata NIF exports)
- `neonfs_core/lib/neon_fs/core/blob/native.ex` (modify — add metadata NIF declarations)
- `neonfs_core/lib/neon_fs/core/blob_store.ex` (modify — add metadata API functions)
- `neonfs_core/lib/neon_fs/core/metadata_store.ex` (new — Elixir wrapper with ETS caching, serialisation, HLC timestamps)
- `neonfs_core/test/neon_fs/core/metadata_store_test.exs` (new)
- `neonfs_core/test/neon_fs/core/blob/native_metadata_test.exs` (new — NIF-level tests)

## Reference
- spec/metadata.md — Tier 2: Chunk Metadata, Tier 3: Filesystem Metadata
- spec/architecture.md — NIF boundaries, BlobStore directory layout
- Existing pattern: `neonfs_core/native/neonfs_blob/src/path.rs` — `chunk_path()` function
- Existing pattern: `neonfs_core/native/neonfs_blob/src/store.rs` — `write_chunk_with_options` (atomic write + fsync)

## Notes
The key design insight is that metadata and data chunks share the same durability model: atomic write to temp file, fsync, rename into place. This means metadata has identical crash-safety to data. The `meta/` namespace is completely separate from `blobs/` — no risk of key collision. Keys are deterministic (not content-addressed): `SHA256("chunk:" ++ chunk_hash)` for chunk metadata, `SHA256("file:" ++ file_id)` for file metadata, `SHA256("dir:" ++ volume_id ++ ":" ++ parent_path)` for directory entries. Tombstones are needed because quorum deletes must propagate: a delete on one replica writes a tombstone that other replicas can observe during anti-entropy. Tombstones should include an HLC timestamp so they can be ordered against writes. Tombstone GC (removing tombstones after all replicas have converged) is deferred to anti-entropy (task 0090).

**Metadata drive placement:** Metadata is small but latency-sensitive — every file operation starts with a metadata lookup. The MetadataStore should select a hot-tier drive for its `meta/` directory via DriveRegistry, following the same pattern as Ceph (metadata pool on SSD/NVMe). If no hot-tier drive is configured, use the warmest available. This is independent of the consistent hashing ring: the ring determines which *nodes* replicate each segment, and each node stores its replica on its fastest available drive. The BlobStore NIF already accepts a base directory per store handle — MetadataStore creates its handle pointing at the selected hot-tier drive's path.
