# Capacity History Store Decision and Implementation Plan

- Date: 2026-04-22
- Scope: `neonfs_core/lib/neon_fs/core/storage_history.ex` (new) + sampler + ETS mirror
- Issue: [#320](https://harton.dev/project-neon/neonfs/issues/320) — first slice of [#246](https://harton.dev/project-neon/neonfs/issues/246) (Capacity planning)

## The decision the issue asks for

#320 presents two options:

1. **Internal ring-buffer store** — a `NeonFS.Core.StorageHistory` GenServer samples `StorageMetrics.cluster_capacity/0` on a timer, persists to a Ra-replicated ETS-backed store, serves queries locally.
2. **Defer to Prometheus** — the scraper already exists (`metrics_plug.ex`); the forecaster queries the Prometheus HTTP API.

## Decision

**Option 1 — internal ring-buffer store.**

Rationale (aligned with existing NeonFS design principles):

- **Self-contained deployments are a first-class target.** Raspberry-Pi homelab clusters don't run Prometheus; single-node omnibus deployments don't need to. Making capacity forecasting require an external service breaks the "install one Debian package, it works" story.
- **The data lives on a timescale Ra handles well.** 5-min samples × 90 days × (drives + tiers + volumes) is a few tens of KB per drive. Even a 100-drive cluster sits under ~5 MB of history — well within Ra snapshot budgets. This is not "time-series at scale"; it's a small rolling window of per-drive scalars.
- **Prometheus retention is wrong for forecasting anyway.** Prometheus defaults to 15-day retention. Tuning it per deployment to hold 90 days means now the operator has two things to configure instead of zero.
- **The forecaster wants the same process's data, not an HTTP round-trip.** `query_history/3` is on the hot path of `neonfs cluster forecast` calls; going through Prometheus adds latency and a failure mode NeonFS can't recover from.
- **Prometheus stays useful for external dashboards.** The existing `metrics_plug.ex` exposes the same gauges it always has. Option 1 doesn't replace Prometheus; it just stops depending on it for a feature NeonFS owns.

Option 2 is a valid deployment choice for clusters that already run Prometheus and want one authoritative source. It's not wrong; it's just not the default. The `query_history/3` API this plan defines is trivial to satisfy from a Prometheus backend later if someone wants it — the public API doesn't leak the storage choice.

## Module surface

```elixir
defmodule NeonFS.Core.StorageHistory do
  @moduledoc "Rolling-window history of storage capacity samples."

  @type entity :: {:drive, String.t()} | {:tier, :hot | :warm | :cold} | {:volume, String.t()}
  @type sample :: %{timestamp: DateTime.t(), used_bytes: non_neg_integer(),
                    capacity_bytes: non_neg_integer()}

  @spec query_history(entity, DateTime.t(), DateTime.t()) :: [sample]
  @spec record_sample(entity, sample) :: :ok                  # internal — called by sampler
end
```

Sampling is driven by `NeonFS.Core.StorageHistory.Sampler`, a `Process.send_after/3`-scheduled GenServer that runs `StorageMetrics.cluster_capacity/0`, fans out per-entity, and writes via the Ra command.

## State and retention

Ra state-machine struct:

```elixir
%NeonFS.Core.StorageHistory.State{
  samples: %{entity => [sample]},   # ordered newest-first, capped per tier
  last_downsample: %{entity => DateTime.t()}
}
```

Retention tiers (downsample in place — no separate archive):

| Age bucket | Sample resolution |
|------------|-------------------|
| 0 – 7 days | 5 min (native rate) |
| 7 – 30 days | 1 hour |
| 30 – 90 days | 6 hours |
| > 90 days | dropped |

Downsampling runs on the same timer as the sampler, once an hour, operating on each entity's sample list: walk from oldest, collapse any windows that have aged into the next bucket. Linear in the sample count; at 90 days × 1-hour resolution worst case, ~2000 samples per entity.

## ETS mirror for reads

Same pattern as `AclManager` / the proposed `NamespaceCoordinator`. `query_history/3` never round-trips through Ra — it reads from a node-local ETS table that the GenServer keeps in sync with committed Ra state.

`:persistent_term` for the retention config (read-heavy from non-GenServer functions).

## Telemetry

- `[:neonfs, :storage_history, :sample]` — emitted on every sample, metadata `%{entity, used_bytes, capacity_bytes}`. This is the event the #246 forecaster hooks into *and* what `:telemetry_metrics_prometheus_core` hooks for the existing dashboard.
- `[:neonfs, :storage_history, :downsample]` — emitted when downsampling runs, measurement `%{samples_removed: N}`.
- `[:neonfs, :storage_history, :query]` — emitted on every query with the entity and the window size. Useful for operator debugging of slow queries.

## Config surface

Under `storage_history` in volume-free app env:

| Key | Default | Purpose |
|-----|---------|---------|
| `enabled` | `true` | Opt-out only; most deployments want this on. |
| `sample_interval_ms` | `300_000` (5 min) | How often the sampler fires. |
| `downsample_interval_ms` | `3_600_000` (1 h) | How often downsampling runs. |
| `retention_days` | `90` | Maximum age before drop. |

All live-adjustable via `Application.put_env/3`; the sampler re-reads on each timer tick.

## Testing

1. **Unit tests** (state machine in isolation):
   - Recording a sample appends to the entity's list.
   - Downsampling collapses samples per the bucket rules.
   - `query_history/3` returns only samples within the window.
   - Retention drop: samples older than the max age vanish.
2. **Property test** (StreamData): for a random insertion sequence and query window, reported samples match a reference implementation.
3. **Integration test** in `neonfs_integration`:
   - 3-node cluster, inject fake drive usage via `StorageMetrics` test helpers.
   - Force the sampler via a telemetry-observable manual tick.
   - Query from a different node, verify the data is visible (Ra replication worked).

Follow the `:telemetry_test.attach_event_handlers` pattern from `CLAUDE.md` — no `Process.sleep`.

## Implementation sequence

Single PR, staged:

1. Pure state-machine module + unit tests.
2. Ra wiring + ETS mirror.
3. Sampler GenServer + public API.
4. Supervision tree integration.
5. Integration test.

## Out of scope

- **Forecasting / regression** — that's the next sub-issue in #246 (#321 in the parent breakdown).
- **CLI `neonfs capacity history`** — same.
- **Prometheus backend alternative** — not ruled out forever, but this PR doesn't build it.

## Open points

1. **Per-tier and per-volume rollup vs per-drive storage.** The sampler could store only per-drive samples and compute tier/volume sums at query time, OR store all three and burn ~3× space. Store all three — ~3× a tiny number is still a tiny number, and query-time rollup means queries scan N drives per call. Decide finally in the implementation PR after sizing the volumes involved.
2. **Sample alignment.** Should samples land on clock-aligned boundaries (12:00, 12:05, ...) or wherever the timer happens to fire? Aligned makes dashboard lines cleaner; unaligned is simpler. Start unaligned; align only if dashboards demand it.
3. **Cross-node sample deduplication.** Every core node runs its own sampler. Do they coordinate or just each emit independently? Currently assumes coordination: the Ra machine accepts the first sample per `(entity, bucket_start)` and ignores duplicates from other nodes. Nail the `bucket_start` in the implementation PR.

## References

- Issue: [#320](https://harton.dev/project-neon/neonfs/issues/320)
- Parent: [#246](https://harton.dev/project-neon/neonfs/issues/246)
- Template: `NeonFS.Core.AclManager` — same Ra + ETS-mirror pattern
- Source: `NeonFS.Core.StorageMetrics.cluster_capacity/0` provides the values being sampled
