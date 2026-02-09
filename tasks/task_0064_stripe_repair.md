# Task 0064: Stripe Repair

## Status
Complete

## Phase
4 - Erasure Coding

## Description
Implement a background stripe repair process that detects degraded stripes (missing chunks) and reconstructs them using Reed-Solomon decoding. Repair reads K available chunks (any mix of data + parity), reconstructs the missing chunks via NIF, and stores the reconstructed chunks on available nodes. Repair tasks are submitted via the existing BackgroundWorker infrastructure with priority-based scheduling. This is the erasure coding equivalent of re-replication for replicated volumes.

## Acceptance Criteria
- [ ] New `NeonFS.Core.StripeRepair` module
- [ ] `scan_stripes/0` — iterates all stripes via StripeIndex, calculates state for each, returns list of `{stripe_id, state, missing_count}` for non-healthy stripes
- [ ] `repair_stripe/1` — given a stripe_id: fetch K available chunks, call `Native.erasure_decode/4` to reconstruct all data shards, identify which specific chunks are missing, store reconstructed chunks on available nodes, update ChunkMeta locations
- [ ] Repair priority ordering: `:critical` stripes before `:degraded`, then by volume `repair_priority` (`:critical` > `:high` > `:normal` > `:low`), then by most recently accessed stripes first
- [ ] Repair submitted via `BackgroundWorker.submit/2` with appropriate priority (`:high` for critical stripes, `:normal` for degraded)
- [ ] Periodic scan: configurable interval (default 5 minutes), submits repair tasks for all non-healthy stripes
- [ ] Only reconstructs and stores the *missing* chunks, not all chunks (optimisation: don't re-store chunks that already exist)
- [ ] Reconstructed chunks placed on nodes that don't already hold stripe chunks (maximise failure domain separation)
- [ ] Handles concurrent repair attempts: if a stripe is already being repaired (e.g. by another node), detect via lock or skip gracefully
- [ ] Telemetry events: repair_scan (stripes_scanned, degraded_found, critical_found), repair_start, repair_success, repair_failure
- [ ] Repair skips healthy stripes (no-op)
- [ ] Repair handles the case where stripe becomes healthy between scan and repair (chunks restored by other means)
- [ ] Unit tests for scan_stripes with mixed healthy/degraded/critical stripes
- [ ] Unit tests for repair_stripe — reconstruct missing chunks, verify stored correctly
- [ ] Unit tests for priority ordering
- [ ] All existing tests still pass

## Testing Strategy
- Unit test: create stripe, delete 1 data chunk → `scan_stripes` reports degraded, `repair_stripe` reconstructs it
- Unit test: create stripe, delete `parity_chunks` data chunks → repair still succeeds (K chunks available)
- Unit test: create stripe, delete `parity_chunks + 1` chunks → repair reports `{:error, :insufficient_chunks}`
- Unit test: repair places reconstructed chunk on different node than existing chunks
- Unit test: healthy stripe → repair is a no-op
- Unit test: priority ordering — critical stripes sorted before degraded
- Unit test: concurrent repair — second repair attempt gracefully skipped
- Integration: delete chunks in multi-node cluster, trigger repair, verify stripe returns to healthy
- Run full `mix test` in neonfs_core

## Dependencies
- task_0057 (Reed-Solomon NIF for decode/reconstruct)
- task_0059 (StripeIndex for stripe metadata lookups)
- task_0061 (read path for fetching available chunks — reuse chunk fetching logic)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/stripe_repair.ex` (new — scan, repair, scheduling)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add StripeRepair to supervision tree if it runs as a periodic GenServer)
- `neonfs_core/test/neon_fs/core/stripe_repair_test.exs` (new)

## Reference
- spec/replication.md — Stripe repair, repair prioritisation
- spec/node-management.md — Repair prioritisation, cost functions
- Existing pattern: `neonfs_core/lib/neon_fs/core/background_worker.ex` (submit repair tasks)
- Existing pattern: `neonfs_core/lib/neon_fs/core/tier_migration.ex` (lock-based concurrent operation protection)

## Notes
Stripe repair is fundamentally more expensive than re-replication: it requires reading K chunks (potentially from multiple remote nodes) and running Reed-Solomon decode to reconstruct missing shards. This makes caching reconstructed data (via ChunkCache, when volume config enables `reconstructed_stripes`) particularly valuable — a degraded read that triggers reconstruction can also feed the repair process. The repair process should use the same ETS lock table pattern from TierMigration to prevent concurrent repairs of the same stripe. Consider using the stripe_id as the lock key. The periodic scan interval should be configurable but defaults to 5 minutes — frequent enough to catch degradation quickly but not so frequent as to waste CPU scanning healthy stripes.
