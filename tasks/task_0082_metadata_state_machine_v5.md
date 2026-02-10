# Task 0082: MetadataStateMachine v5 (Segment Assignments + Intents)

## Status
Complete

## Phase
5 - Metadata Tiering

## Description
Bump MetadataStateMachine from v4 to v5 with a clean state shape for the tiered metadata architecture. This is a breaking change — no migration from v4 is needed since there are no production deployments. The v5 state removes `chunks`, `files`, and `stripes` maps (which move to quorum-based BlobStore storage) and adds `segment_assignments` and intent tracking. Ra now only stores Tier 1 data: services, volumes, segment assignments, and active write intents.

## Acceptance Criteria
- [ ] MetadataStateMachine `version/0` returns 5
- [ ] Clean v5 state shape: `%{services, volumes, data, segment_assignments, intents, active_intents_by_conflict_key}` — no `chunks`, `files`, or `stripes` maps
- [ ] `segment_assignments` map: `%{segment_id => %{replica_set: [node_ids], version: N}}`
- [ ] `intents` map: `%{intent_id => %Intent{}}` where Intent has id, operation, conflict_key, params, state, started_at, expires_at
- [ ] `active_intents_by_conflict_key` map: `%{conflict_key => intent_id}` for fast conflict detection
- [ ] New Ra commands for segment management:
  - `{:assign_segment, segment_id, replica_set}` — assign a segment to a replica set
  - `{:bulk_update_assignments, %{segment_id => replica_set}}` — batch assignment update (for rebalancing)
- [ ] New Ra commands for intent management:
  - `{:try_acquire_intent, intent}` — atomic check-and-set: acquire if no active intent with same conflict_key, or if existing intent has expired
  - `{:complete_intent, intent_id}` — mark intent completed, release conflict_key
  - `{:fail_intent, intent_id, reason}` — mark intent failed, release conflict_key
  - `{:extend_intent, intent_id, additional_seconds}` — extend TTL for long-running operations
  - `{:cleanup_expired_intents}` — remove all intents past their TTL (periodic housekeeping)
- [ ] All chunk/file/stripe Ra commands removed (`:put_chunk`, `:delete_chunk`, `:put_file`, `:delete_file`, `:put_stripe`, `:delete_stripe`, etc.)
- [ ] Query functions: `get_segment_assignments/0`, `get_intent/1`, `list_active_intents/0`
- [ ] `try_acquire_intent` returns `{:ok, :acquired}` or `{:ok, :conflict, existing_intent}` (with expired intent takeover)
- [ ] Existing `services` and `volumes` commands continue to work unchanged
- [ ] `data` map preserved for volume-level metadata
- [ ] Persistence module updated to snapshot new state shape
- [ ] Unit tests for each new command type
- [ ] Unit tests for intent conflict detection and expiry takeover
- [ ] Unit tests verifying chunk/file/stripe commands are removed

## Testing Strategy
- ExUnit tests for segment assignment commands (assign, bulk update, query)
- ExUnit tests for intent lifecycle: acquire → extend → complete
- ExUnit tests for intent conflict detection: two intents with same conflict_key
- ExUnit tests for expired intent takeover: create intent, advance time past TTL, acquire new intent with same conflict_key
- ExUnit tests for cleanup_expired_intents: create several expired intents, run cleanup, verify removed
- ExUnit tests verifying old chunk/file/stripe commands no longer exist
- All existing service and volume tests pass without modification

## Dependencies
- task_0081 (Consistent hashing ring — segment concept needed for assignment commands)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/metadata_state_machine.ex` (modify — version 5, new state shape, new commands, remove old commands)
- `neonfs_core/lib/neon_fs/core/intent.ex` (new — Intent struct)
- `neonfs_core/lib/neon_fs/core/persistence.ex` (modify — snapshot new state shape, remove metadata DETS for chunks/files/stripes)
- `neonfs_core/test/neon_fs/core/metadata_state_machine_test.exs` (modify — replace old tests with v5 tests)

## Reference
- spec/metadata.md — Tier 1: Cluster State (Ra)
- spec/metadata.md — Intent Log: Transaction Safety and Write Coordination
- spec/metadata.md — Exclusive Intent Acquisition (Ra state machine apply/3 example)
- Existing pattern: MetadataStateMachine v3→v4 (task_0059)

## Notes
This is a clean cutover, not a migration. Since there are no production deployments, we can replace the state shape entirely rather than writing migration code. The `data` map in the state is kept for volume-level metadata that doesn't need per-file granularity. The intent conflict detection is the critical piece for write coordination — it must be atomic within Ra's `apply/3` callback. The `active_intents_by_conflict_key` index exists for O(1) conflict detection; without it, every `try_acquire_intent` would need to scan all intents. Intent TTL defaults to 300 seconds; the `extend_intent` command allows long-running operations to keep their lease alive.
