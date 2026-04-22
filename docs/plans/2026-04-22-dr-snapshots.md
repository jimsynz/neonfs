# DR Snapshots `DRSnapshot` Module Implementation Plan

- Date: 2026-04-22
- Scope: `neonfs_core/lib/neon_fs/core/dr_snapshot/` (new module namespace)
- Issue: [#322](https://harton.dev/project-neon/neonfs/issues/322) — first slice of [#247](https://harton.dev/project-neon/neonfs/issues/247)

## Scope reminder

Build `NeonFS.Core.DRSnapshot.create/1`. Captures every Ra-backed metadata index at a point in time, writes the result under `/dr/<timestamp>/` in the `_system` volume. **No scheduler, no retention, no CLI, no restore** — those are #323 and #324.

## Point-in-time consistency — the real design decision

The issue body assumes "every Ra-backed module supports `snapshot_at(hlc)` or similar". That machinery does not currently exist. `NeonFS.Core.FileIndex.list_all/0` and friends return the *current* state as of the call, not "as of HLC T". Before writing `DRSnapshot` we have to pick a consistency strategy.

Three options:

1. **Add `snapshot_at(hlc)` to each Ra-backed module.** Every module with MetaState gains a way to rewind its ETS mirror to an HLC. This is the textbook answer — true point-in-time, concurrent writes don't block. It also multiplies the work across five or six modules (FileIndex, ChunkIndex, StripeIndex, VolumeRegistry, DriveRegistry, ServiceRegistry), each of which needs per-record HLC columns and a history buffer. Significant new infrastructure.

2. **Pause writes for the duration of the snapshot.** Acquire a cluster-wide write lock, read every index's current state, release. Simple; the snapshot is authoritative. Downside: writes stall for the snapshot duration — minutes on a large cluster. Unacceptable for anything but maintenance windows.

3. **Consistent-enough snapshot via `:ra.consistent_query`.** Ra's `consistent_query` waits for the leader's local state to reach its latest committed index, then reads. Issue one `consistent_query` per module in quick succession. HLC drift between modules is bounded by the time between the calls (milliseconds, not minutes). The snapshot is not perfectly consistent across modules under concurrent writes, but any observer sees a state that *could have existed* — the cluster never rolls back, and every captured record's HLC is within the snapshot window.

### Decision

**Option 3 for the first slice. Option 1 deferred to a follow-up if operators need strict cross-module consistency.**

Rationale:

- **Option 3 ships.** Options 1 and 2 are substantially more work; option 1 especially adds a new piece of machinery to every Ra-backed module. #322 says "first slice" — the point is to have something working, not to solve DR perfectly on the first PR.
- **The consistency gap is bounded and named.** The manifest records the timestamp of each per-index `consistent_query`. Restore (future #TBD) sees the window and can refuse if drift exceeds a configurable threshold.
- **Option 2 is wrong for production.** Pausing writes for minutes is not a DR strategy; it's an outage.
- **Option 1 is a proper feature later.** If #323/#324 ship and operators actually use DR and the drift bites someone, add `snapshot_at(hlc)` per-module. This plan doesn't close off that path — the `DRSnapshot.create/1` signature is the same whether it's backed by option 1 or option 3.

## Module surface

```elixir
defmodule NeonFS.Core.DRSnapshot do
  @type result :: %{
          id: String.t(),                    # snapshot timestamp, e.g. "2026-04-22-203045"
          path: String.t(),                  # "/dr/2026-04-22-203045/"
          manifest: map(),                   # parsed manifest.json contents
          indices: [atom()],                 # [:file_index, :chunk_index, ...]
          window: {DateTime.t(), DateTime.t()}  # start and end of the capture window
        }

  @spec create(keyword()) :: {:ok, result} | {:error, term()}
  def create(opts \\ []) do
    # opts:
    #   volume — default "_system"
    #   prefix — default "/dr"
    #   indices — default [:file_index, :chunk_index, :stripe_index, :volume_registry,
    #                      :drive_registry, :service_registry, :key_manager, :ca_state]
    #   hlc_drift_alert_ms — default 5_000; log warning if window exceeds
  end
end
```

## Per-index capture

Each index has a `capture_for_dr/1` module callback (or a behaviour `NeonFS.Core.DRSnapshotSource`). The callback is the module's authoritative "dump yourself" entry — it uses `:ra.consistent_query` internally.

```elixir
@callback capture_for_dr(capture_ctx) :: {:ok, Enumerable.t(), metadata :: map()}
```

Returns an enumerable so `DRSnapshot.create/1` can stream it into `NeonFS.Core.write_file_streamed/4` — **no whole-index buffering** (the CLAUDE.md rule). Metadata is appended to the snapshot's manifest.

Each module's behaviour impl is a thin wrapper around its existing `list_all/0`-style function; the only new infrastructure is the one-line callback.

## Snapshot layout

```
/dr/2026-04-22-203045/
├── manifest.json                # envelope — see below
├── file_index.snapshot          # newline-delimited JSON records
├── chunk_index.snapshot
├── stripe_index.snapshot
├── volume_registry.snapshot
├── drive_registry.snapshot
├── service_registry.snapshot
├── key_manager.snapshot
└── ca_state.snapshot
```

`manifest.json` shape:

```json
{
  "id": "2026-04-22-203045",
  "created_at": "2026-04-22T20:30:45.123Z",
  "cluster_id": "clust_abc123",
  "cluster_name": "production",
  "neonfs_version": "0.1.11",
  "window": {
    "start_hlc": "01HZ...",
    "end_hlc": "01HZ...",
    "drift_ms": 42
  },
  "indices": [
    {"name": "file_index", "path": "file_index.snapshot",
     "record_count": 1234, "sha256": "..."},
    ...
  ]
}
```

SHA-256 per-file lets the restore path detect corruption before applying.

## Streaming writes

`DRSnapshot.create/1` opens `manifest.json` last because its content depends on per-index metadata. Per-index streams go through `NeonFS.Core.write_file_streamed/4`. No intermediate buffer — the enumerable from `capture_for_dr/1` feeds straight into the chunker.

Each record is encoded as `:json.encode/1` + newline — line-delimited JSON (JSONL). Restore can parse lazily without holding the whole file.

## Telemetry

- `[:neonfs, :dr_snapshot, :start]` — `%{snapshot_id, indices}`.
- `[:neonfs, :dr_snapshot, :index, :captured]` — per index: `%{name, record_count, duration_ms}`.
- `[:neonfs, :dr_snapshot, :complete]` — `%{snapshot_id, duration_ms, size_bytes, drift_ms}`.
- `[:neonfs, :dr_snapshot, :drift_warning]` — emitted when drift exceeds `hlc_drift_alert_ms`.

These feed the #323 scheduler's visibility into how long snapshots take in practice.

## Config surface

None initially. Defaults live in `create/1`'s opts. The #323 scheduler (next sub-issue) will pull config from `volume`-level snapshot policy.

## Testing

1. **Unit tests with in-memory fakes** for each `capture_for_dr/1` callback — assert the enumerable round-trips through the snapshot writer and lands correctly.
2. **Integration test** (`neonfs_integration/test/integration/dr_snapshot_test.exs`):
    - 3-node cluster with some volumes / files / chunks pre-populated.
    - `DRSnapshot.create/1`.
    - Assert manifest contents, per-index SHA-256s match the written files, record counts are correct.
    - `@moduletag cluster_mode: :shared` — snapshot is read-only, no state mutation between assertions.
3. **Concurrent-write test**:
    - Kick off a snapshot while a background process writes to FileIndex.
    - Assert the snapshot's captured HLC window is recorded in the manifest.
    - Assert the snapshot's FileIndex records are either "definitely pre-write" or "definitely post-write" — no partial record.
4. **Memory-bound test**: populate FileIndex with many records (~10k), snapshot, assert peak RSS during snapshot is bounded (streaming write works).

## Implementation sequence

Single PR, staged:

1. Behaviour definition (`NeonFS.Core.DRSnapshotSource`).
2. Per-index `capture_for_dr/1` implementations — one commit per index, simple.
3. `DRSnapshot.create/1` — the orchestrator.
4. Tests.

## Out of scope — hard

- Scheduler / retention policy (#323).
- CLI `neonfs dr snapshot create | list | show` (#324).
- Restore path (separate future issue).
- Per-volume snapshot policy (volume config change — separate).
- Any guarantee of cross-module consistency tighter than `consistent_query`-per-module — tracked as a follow-up if needed.

## Open points

1. **HLC drift budget.** 5 seconds is the default `hlc_drift_alert_ms`. Too permissive? Measure on a real cluster during the integration test and tune. Either way it's a warning, not an error — restore decides whether to use the snapshot.
2. **Ordering of captures.** Capture registries (volume, drive, service) before indexes (file, chunk, stripe)? If so, indexes reference registry records that exist in the same snapshot — restore has fewer dangling-reference cases. Do this.
3. **Encryption of the snapshot itself.** `_system` volume is stored like any other — honours the volume's own encryption policy. If the deployment encrypts `_system`, the snapshot is encrypted. Worth mentioning in #323's retention policy design — the snapshot carries the cluster's own keys (`key_manager.snapshot`), so a snapshot restored to a fresh cluster needs access to the original CA's private material to unlock volumes. Out of scope here.

## References

- Issue: [#322](https://harton.dev/project-neon/neonfs/issues/322)
- Parent: [#247](https://harton.dev/project-neon/neonfs/issues/247)
- Sibling sub-issues: #323 (scheduler), #324 (CLI)
- CLAUDE.md — "No Whole-File Buffering" rule, applied to streaming index capture
- Related: `NeonFS.Core.FileIndex`, `ChunkIndex`, `StripeIndex`, `VolumeRegistry`, `DriveRegistry`, `ServiceRegistry`, `KeyManager`, `CertificateAuthority`
