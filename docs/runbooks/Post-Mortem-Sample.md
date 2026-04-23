# Post-mortem: drive evacuation stalled during node-level reboot

> **Sample post-mortem.** Illustrative fictional incident showing the shape of a filled-in post-mortem using [Post-Mortem-Template.md](Post-Mortem-Template.md). Treat this as a worked example, not a real event; names, timestamps, and specifics are invented.

- **Date of incident:** 2026-03-14
- **Author:** Avery Tan
- **Status:** closed
- **Severity:** SEV-3

## Summary

During a planned drive-failure response on `core-04`, an in-progress drive evacuation stalled when the host was rebooted to complete a kernel-security update. The evacuation job resumed automatically on service restart, but for 2 hours 11 minutes approximately 18% of chunks on the draining drive were under-replicated. No volumes lost read availability; a small number of writes against the affected volumes reported degraded latency (95th percentile up from 42ms to 380ms). The incident resolved when the evacuation job completed and replication reconciled.

## Timeline

| Time (UTC) | Local NZDT | Event |
|---|---|---|
| 2026-03-13T22:14:02Z | 11:14 (+1 day) | SMART warning fires on `core-04:nvme0`. Prometheus alert delivered to on-call. |
| 2026-03-13T22:17:48Z | 11:17 | On-call engineer (Avery) acknowledges. [Drive-Failure](Drive-Failure.md) runbook opened. |
| 2026-03-13T22:23:11Z | 11:23 | `neonfs drive evacuate nvme0 --node core-04` started. Job ID `job-20260313-221`. Estimated ~3h at observed rate. |
| 2026-03-13T22:51:07Z | 11:51 | Scheduled kernel-security update on `core-04` executes automatically. Host reboots. |
| 2026-03-13T22:51:45Z | 11:51 | `core-04` down per `neonfs node list`. Evacuation job pauses per its design (source drive no longer reachable). |
| 2026-03-13T22:52:09Z | 11:52 | Cluster status degrades. Replication factor of affected-volume chunks drops; `under_replicated_chunks` jumps to 18% of drive-resident set. |
| 2026-03-13T22:54:33Z | 11:54 | Latency alert fires on `volume-orders` 95th-percentile write latency. |
| 2026-03-13T22:57:10Z | 11:57 | `core-04` returns; `systemctl status neonfs-core` reports active (running). |
| 2026-03-13T22:57:52Z | 11:57 | Evacuation job resumes automatically. Progress advances. |
| 2026-03-13T23:46:04Z | 12:46 | Evacuation job reports `bytes_remaining: 0`. Chunks fully replicated to other drives. |
| 2026-03-14T01:05:22Z | 14:05 | Latency 95th-percentile back within target (42ms). `under_replicated_chunks` = 0. |
| 2026-03-14T01:05:55Z | 14:05 | `neonfs drive remove nvme0` completes cleanly. Hardware retired. |

## Impact

- **What users / workloads saw.** `volume-orders` and `volume-events` writes experienced elevated 95th-percentile latency (42ms → 380ms) for 2h11m. No read failures, no write failures — writes retried at the client and eventually succeeded via the surviving replicas.
- **Duration of impact.** 2026-03-13T22:54:33Z to 2026-03-14T01:05:22Z (UTC), 2h10m49s.
- **Data loss.** None. Every chunk retained at least one replica on a reachable drive throughout.
- **SLO / SLA consequences.** `volume-orders` 99th-percentile latency target of 500ms was breached briefly (peak 647ms). No contractual threshold hit; internal error budget consumed ~1.4% for the day.

## Root cause

Proximate cause: the kernel-security update schedule on `core-04` was configured with automatic reboot enabled, and it fired during an in-progress drive evacuation. The evacuation job design handles node-reboot cleanly (it pauses and resumes), but the pause-resume gap left chunks under-replicated for the duration of the reboot.

Link: `/etc/apt/apt.conf.d/50unattended-upgrades` on `core-04` has `Unattended-Upgrade::Automatic-Reboot "true"` and `Automatic-Reboot-Time "22:30"`. Evacuation was started at 22:23 without awareness of the 22:30 reboot window.

## Contributing factors

- **No operator-facing "automatic-reboot scheduled soon" signal.** The runbook (step 2, graceful evacuation) does not prompt the operator to check the host's scheduled-reboot calendar before starting a long-running drive evacuation. The operator had the information available via `systemctl list-timers` but did not think to check.
- **The drive-failure alert fires independently of the host's maintenance window.** It is correct for the alert to fire on SMART regardless of maintenance, but the runbook could recommend deferring the evacuation start until after a known reboot window.
- **No per-volume alert that `under_replicated_chunks > 0` persisted for > 30 minutes.** Such an alert would have escalated earlier than the latency-derived alert eventually did.
- **Prior near-miss**: a similar pattern was noted in a 2025-11 incident but not written up — it was resolved inside its own window and forgotten.

## What went well

- SMART alerting fired at the first warning, not the first error — the evacuation could start proactively rather than reactively.
- Evacuation resumed automatically after the unplanned reboot, exactly as the Drive-Failure runbook promises. No manual intervention was needed to restart it.
- Client-side retries masked the latency spike enough that no user-visible errors were reported.
- The operator followed the Drive-Failure runbook end-to-end and captured the correct context — the post-mortem draft took < 30 min to write.

## What did not go well

- The 22:30 automatic-reboot window was a known host-level policy but was not surfaced anywhere an evacuation operator would see.
- The latency alert was the first signal of impact; the directly-causal signal (under-replication after node drop) did not have a dedicated alert.
- The 2025-11 near-miss was not written up, so this incident rediscovered the same root cause shape.

## Action items

| # | Action | Owner | Due | Done-when |
|---|---|---|---|---|
| 1 | Add "check host maintenance windows" to Drive-Failure runbook step 3 (graceful evacuation). | Avery | 2026-03-21 | Runbook PR merged, referencing this post-mortem. |
| 2 | Add Prometheus alert: `under_replicated_chunks{scope="drive"} > 0 for 30m`. | Priya | 2026-03-28 | Alert rule in `packaging/prometheus/` merged. |
| 3 | Disable `Unattended-Upgrade::Automatic-Reboot` on all core nodes; schedule reboots via change-management instead. | Kenji (Ops) | 2026-03-21 | Ansible role updated, verified on `core-01` through `core-07`. |
| 4 | CLI: `neonfs drive evacuate` should log the host's next scheduled reboot (from `systemctl list-timers`) to the job metadata. | Avery | 2026-04-11 | PR merged to `neonfs-cli` and `neonfs_core`. |
| 5 | Retroactive write-up of the 2025-11 near-miss so it is searchable. | Avery | 2026-03-28 | File under `docs/runbooks/post-mortems/` (or equivalent). |

## Related runbooks

- [Drive failure](Drive-Failure.md) — run. Worked end-to-end. Action item 1 adds the maintenance-window check.
- [Node down](Node-Down.md) — briefly consulted when the automatic reboot was first noticed. Resolution was inside its own window so Node-Down procedures did not need to execute.
- [Capacity pressure](Capacity-Pressure.md) — not applicable; replication recovery had capacity headroom throughout.

## Lessons / themes

Drive evacuations are long-running jobs and interact with other scheduled operations on the host. Our tooling treats them as independent events, and operators discover the coupling reactively (as they did here). The action items above address this specific incident, but the deeper theme is that any long-running job should expose its expected duration and check it against other scheduled operations on the same host before starting — a durable capability we should consider for the broader job framework.

This is the second instance of automatic maintenance windows interfering with cluster-internal background jobs (action item 5 captures the first). If we see a third, this should escalate from a per-incident fix to a change-management policy shift.
