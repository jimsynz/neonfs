# Task 0058: Stripe Struct and Volume Durability Extension

## Status
Complete

## Phase
4 - Erasure Coding

## Description
Create the `NeonFS.Core.Stripe` struct as a shared type in neonfs_client (alongside `Volume` and `FileMeta`) and extend the Volume durability configuration to support erasure coding in addition to replication. The Stripe struct represents a group of data chunks plus their computed parity chunks, tracking the encoding configuration, chunk ordering, and partial-stripe metadata.

## Acceptance Criteria
- [x] New `NeonFS.Core.Stripe` module in neonfs_client with struct fields: `id`, `volume_id`, `config` (`%{data_chunks: pos_integer(), parity_chunks: pos_integer(), chunk_size: pos_integer()}`), `chunks` (ordered list of hashes — data indices first, then parity), `partial` (boolean, default false), `data_bytes` (non_neg_integer — actual data before padding), `padded_bytes` (non_neg_integer — zero-fill padding)
- [x] `Stripe.new/1` constructor with defaults and validation
- [x] `Stripe.validate/1` returns `:ok` or `{:error, reason}` — validates data_chunks >= 1, parity_chunks >= 1, chunk_size > 0, chunks list length equals `data_chunks + parity_chunks` (when set), data_bytes >= 0, padded_bytes >= 0
- [x] `Stripe.total_chunks/1` returns `config.data_chunks + config.parity_chunks`
- [x] `Stripe.data_chunk_hashes/1` returns first `config.data_chunks` entries from chunks list
- [x] `Stripe.parity_chunk_hashes/1` returns last `config.parity_chunks` entries from chunks list
- [x] `Volume.durability_config` type extended: `%{type: :replicate, factor: pos_integer(), min_copies: pos_integer()} | %{type: :erasure, data_chunks: pos_integer(), parity_chunks: pos_integer()}`
- [x] `Volume.validate/1` updated to validate erasure durability configs (data_chunks >= 1, parity_chunks >= 1)
- [x] Default durability remains `%{type: :replicate, factor: 3, min_copies: 2}` (no change)
- [x] `Volume.erasure?/1` helper returns true if `volume.durability.type == :erasure`
- [x] Unit tests for Stripe struct creation, validation, helper functions
- [x] Unit tests for Volume with erasure durability config (valid and invalid)
- [x] All existing Volume tests still pass

## Testing Strategy
- Unit tests for `Stripe.new/1` with valid params
- Unit tests for `Stripe.validate/1` — valid configs, invalid data_chunks (0, negative), invalid chunk_size, mismatched chunks list length
- Unit tests for `Stripe.data_chunk_hashes/1` and `Stripe.parity_chunk_hashes/1`
- Unit tests for `Volume.new/1` with `%{type: :erasure, data_chunks: 10, parity_chunks: 4}` durability
- Unit tests for `Volume.validate/1` rejecting invalid erasure configs
- Run full `mix test` in neonfs_client to verify no regressions

## Dependencies
- None (can run in parallel with task_0057)

## Files to Create/Modify
- `neonfs_client/lib/neon_fs/core/stripe.ex` (new — Stripe struct, validation, helpers)
- `neonfs_client/lib/neon_fs/core/volume.ex` (modify — extend durability_config type, validation, add `erasure?/1`)
- `neonfs_client/test/neon_fs/core/stripe_test.exs` (new — Stripe unit tests)
- `neonfs_client/test/neon_fs/core/volume_test.exs` (modify — add erasure durability tests)

## Reference
- spec/data-model.md — Stripe definition, chunk ordering
- spec/replication.md — Partial stripe handling, stripe metadata
- spec/implementation.md — Phase 4 deliverables

## Notes
The Stripe struct does not include a `state` field (:healthy/:degraded/:critical) because state is derived at query time by checking chunk availability — it is not stored. The `chunks` list ordering is significant: indices 0 through `data_chunks - 1` are data chunks, indices `data_chunks` through `data_chunks + parity_chunks - 1` are parity chunks. This ordering matches what Reed-Solomon encode/decode expects. The `partial` flag and `data_bytes`/`padded_bytes` fields handle the case where a file's final stripe doesn't fill all data chunk slots — these are padded with zeros for consistent erasure coding but reads must respect the `data_bytes` boundary to avoid returning padding.
