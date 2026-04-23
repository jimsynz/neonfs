# Post-mortem template

Fill this out after any runbook-triggering incident — node down, drive failure, quorum loss, CA expiry, capacity pressure, upgrade regression, key rotation under compromise, or any other "we had to do something unplanned" event.

A **sample filled-in post-mortem** using this template: [Post-Mortem-Sample.md](Post-Mortem-Sample.md). Read it alongside this skeleton — the shape is easier to copy than to invent.

**Principles.** Blameless. People acted on the information they had in front of them at the time. The goal is a durable record that shapes the next runbook, the next alert, the next CLI safety-check — not to assign fault. If a section below feels sharp, re-phrase it to address the system rather than any individual.

---

# Post-mortem: `<concise incident label>`

- **Date of incident:** `YYYY-MM-DD`
- **Author:** `<your name>`
- **Status:** draft / reviewed / closed
- **Severity:** `SEV-<N>` (using your org's severity scale)

## Summary

One paragraph. Plain language, no jargon the whole audience won't recognise. What happened, who / what was affected, for how long, and how it ended. A sibling team reading this paragraph alone should come away with an accurate mental model.

## Timeline

Every timestamp includes the timezone. Prefer UTC for the canonical log; a local-time column is fine as a convenience.

| Time (UTC) | Local | Event |
|---|---|---|
| YYYY-MM-DDTHH:MM:SSZ | HH:MM | Monitoring alert fires on `<metric>`. |
| YYYY-MM-DDTHH:MM:SSZ | HH:MM | On-call engineer acknowledges. |
| YYYY-MM-DDTHH:MM:SSZ | HH:MM | Initial diagnosis: `<runbook-name>`. |
| YYYY-MM-DDTHH:MM:SSZ | HH:MM | Step 1 of runbook executed: `<what>`. Outcome: `<what>`. |
| YYYY-MM-DDTHH:MM:SSZ | HH:MM | … |
| YYYY-MM-DDTHH:MM:SSZ | HH:MM | Mitigation confirmed (cluster status green, alerts cleared). |
| YYYY-MM-DDTHH:MM:SSZ | HH:MM | Full resolution (all jobs settled, verification checklist complete). |

Keep the timeline tight. Events that didn't change the state of the incident (ACK'd alerts, routine status checks) can be elided. Events that *did* change state — decisions, actions, observations that shaped the next action — stay in.

## Impact

- **What users / workloads saw.** Which volumes were affected, which protocols, which client populations.
- **Duration of impact.** Start and end in UTC; state the clock that defines "impact" (e.g. "write availability on volume-X between first failed write and first successful write post-recovery").
- **Data loss.** Yes / no / unknown-pending-reconciliation. If yes, what was lost and how was it quantified.
- **SLO / SLA consequences.** What error budget was consumed, what contract terms apply, what notifications are required (compliance, customer-facing).

## Root cause

The technical explanation, scoped to the single defect or decision that started the cascade. Link to the code path, config, or config change. If there are multiple causes — say, a defect AND a latent alerting gap — pick the proximate root cause here and put the rest in [Contributing factors](#contributing-factors).

Root-cause analysis that stops at "operator error" is usually incomplete. The richer question is: what information was the operator acting on, and why did that information not reflect reality?

## Contributing factors

Things that made the incident worse or harder to resolve than it had to be. Non-exhaustive:

- Monitoring / alerting that fired too late or not at all.
- A runbook that was missing, out of date, or unclear at the point it was needed.
- Tooling that required operator-applied workarounds instead of offering a guarded path.
- Team / rota factors — who was on-call, who else was needed, how fast they joined.
- Previous near-misses that should have flagged this class of failure sooner.

## What went well

Detections that fired correctly. Runbook steps that worked first try. Fast decisions that turned out right. This section is not just for morale — validated behaviour is worth recording so the next incident knows to keep doing those things.

## What did not go well

The counterpart to "what went well". Where did the runbook fall short? Where did tooling gaps require improvisation? Where did communication break down? Describe the system-level gap, not the individual.

## Action items

Concrete, assigned, dated. Each action item names an owner, a due date, and — critically — the artefact that proves it is done. Vague action items die in backlogs.

| # | Action | Owner | Due | Done-when |
|---|---|---|---|---|
| 1 | `<what change>` | `<name>` | YYYY-MM-DD | `<PR number, runbook update, alert rule committed, …>` |
| 2 | `<what change>` | `<name>` | YYYY-MM-DD | `<…>` |

At least one action item should address the monitoring gap that let the incident progress as far as it did. If monitoring was perfect, say so — "no action item: detection fired at T+0 and caught the first real symptom" — but that is rare.

## Related runbooks

Which runbooks were run during this incident? Which should have been? Which are missing?

- [Node down](Node-Down.md) — run / should have been run / not applicable.
- [Drive failure](Drive-Failure.md) — …
- [Quorum loss](Quorum-Loss.md) — …
- [CA rotation](CA-Rotation.md) — …
- [Capacity pressure](Capacity-Pressure.md) — …
- [Cluster upgrade](Cluster-Upgrade.md) — …
- [Key rotation](Key-Rotation.md) — …

If a runbook fell short or the incident exposed a runbook gap, capture that as an action item above and reference the issue number.

## Lessons / themes

One or two paragraphs above-the-waterline framing. What does this incident say about our system beyond the proximate technical cause? Recurring themes over multiple post-mortems are the strongest signal that a class of work needs prioritising.

---

## Template usage notes

- Copy this file to `docs/runbooks/post-mortems/YYYY-MM-DD-<slug>.md` (directory forthcoming). Keep the incident date in the filename so chronological listing works out of the box.
- Commit the post-mortem in a git branch named `post-mortem/YYYY-MM-DD-<slug>` and open a PR — reviewers comment on timeline accuracy and the action items before it lands.
- Once landed, the action items are the contract. Track them to close as you would any other engineering work.
- Link the post-mortem from the issue that tracked the incident if there was one.
