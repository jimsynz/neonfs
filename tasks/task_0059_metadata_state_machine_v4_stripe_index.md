# Task 0059: MetadataStateMachine v4 and StripeIndex

## Status
Complete

## Phase
4 - Erasure Coding

## Description
Bump the MetadataStateMachine from version 3 to version 4, adding a `stripes` map to the Ra-backed state and CRUD commands for stripe metadata. Create a `StripeIndex` GenServer that mirrors the `ChunkIndex` pattern: an ETS cache backed by Ra writes, with startup restoration from Ra state. This provides fast local lookups for stripe metadata while maintaining cluster-wide consistency via Ra consensus.

## Acceptance Criteria
- [ ] MetadataStateMachine `version/0` returns 4
- [ ] State map includes `stripes: %{stripe_id => stripe_data}` alongside existing `chunks`, `files`, `volumes`, `services`
- [ ] Migration handler for `{:machine_version, 3, 4}` adds empty `stripes` map to state
- [ ] New command `{:put_stripe, stripe_data}` — stores stripe metadata, returns `{:ok, stripe_id}`
- [ ] New command `{:update_stripe, stripe_id, updates}` — merges updates into existing stripe, returns `:ok` or `{:error, :not_found}`
- [ ] New command `{:delete_stripe, stripe_id}` — removes stripe from state, returns `:ok`
- [ ] `StripeIndex` GenServer with ETS table (`:public`, `read_concurrency: true`)
- [ ] `StripeIndex.put/1` — stores stripe in ETS and submits Ra command
- [ ] `StripeIndex.get/1` — reads from local ETS cache
- [ ] `StripeIndex.delete/1` — removes from ETS and submits Ra command
- [ ] `StripeIndex.list_by_volume/1` — returns all stripes for a given volume_id
- [ ] `StripeIndex` restores state from Ra on startup (like ChunkIndex pattern)
- [ ] `StripeIndex` added to core supervision tree
- [ ] Persistence module updated to snapshot StripeIndex ETS table (like ChunkIndex/FileIndex)
- [ ] Unit tests for new MSM commands (put, update, delete stripe)
- [ ] Unit tests for MSM migration from v3 to v4
- [ ] Unit tests for StripeIndex CRUD and list_by_volume
- [ ] All existing MetadataStateMachine tests still pass

## Testing Strategy
- Unit tests for MSM `{:put_stripe, data}` command — verify stripe stored in state
- Unit tests for MSM `{:update_stripe, id, updates}` — verify merge, verify not_found error
- Unit tests for MSM `{:delete_stripe, id}` — verify removal
- Unit tests for MSM migration: create v3 state, apply migration, verify stripes map exists
- Unit tests for StripeIndex: put/get/delete/list_by_volume
- Integration: verify StripeIndex survives process restart (restores from Ra)
- Run full `mix test` in neonfs_core

## Dependencies
- task_0058 (needs Stripe struct definition)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/metadata_state_machine.ex` (modify — version bump, stripes state, new commands, migration)
- `neonfs_core/lib/neon_fs/core/stripe_index.ex` (new — GenServer with ETS cache)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add StripeIndex to supervision tree)
- `neonfs_core/lib/neon_fs/core/persistence.ex` (modify — add StripeIndex ETS snapshot)
- `neonfs_core/test/neon_fs/core/metadata_state_machine_test.exs` (modify — add stripe command tests, migration test)
- `neonfs_core/test/neon_fs/core/stripe_index_test.exs` (new)

## Reference
- spec/metadata.md — Distributed metadata architecture, quorum segments
- spec/replication.md — Stripe metadata requirements
- Existing pattern: `neonfs_core/lib/neon_fs/core/chunk_index.ex` (follow same GenServer + ETS + Ra pattern)

## Notes
The StripeIndex follows the same architecture as ChunkIndex and FileIndex: local ETS for fast reads, Ra commands for durable writes. The stripe data stored in Ra should be the full Stripe struct fields (serialised as a map). Cross-node stripe lookups go through the Router/RPC mechanism just like chunk lookups. The MSM migration from v3 to v4 is straightforward — just add an empty `stripes` map to the existing state. Existing volumes with replicated durability don't use stripes, so the map starts empty.
