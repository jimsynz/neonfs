# NeonFS Operational Runbooks

Incident-shaped procedures for operators. Each runbook carries the same sections so you can skim for the one you need under pressure:

- **Symptom** — what you observe (log line, alert, CLI output).
- **Precondition** — assumed cluster state and version.
- **Procedure** — ordered steps with exact commands.
- **Verification** — commands whose output proves the incident is resolved.
- **Escalation path** — when to stop and raise for help, and the context to capture first.

Runbooks reference the operator guide, CLI reference, and spec pages rather than restating them.

## Incident response

- [Node down](Node-Down.md) — diagnosing a stopped core or interface node, short-term recovery, decommission decision.
- [Drive failure](Drive-Failure.md) — SMART / I/O-error diagnosis, evacuation, physical replacement, replication-based recovery when evacuation is not possible.
- [Quorum loss](Quorum-Loss.md) — Ra quorum broken, no leader, metadata writes hang. Partition-vs-failure assessment, wait-it-out vs forced-reset paths, explicit data-loss call-outs.
- [CA rotation](CA-Rotation.md) — cluster CA or node-cert expiry. Planned rotation ahead of time, and emergency rotation when expiry has already broken distribution.
- [Capacity pressure](Capacity-Pressure.md) — drive, tier, or cluster running out of room. Immediate relief (GC, tier eviction, caching changes), permanent capacity growth, and the decisions not to make under pressure.

## Scheduled procedures

- [Cluster upgrade](Cluster-Upgrade.md) — rolling core + interface upgrade, mid-upgrade incident handling, and the rollback decision tree (safe vs unsafe against a state-machine version bump).
- [Key rotation](Key-Rotation.md) — scheduled-cadence and suspected-compromise key rotation for encrypted volumes, with backup-lifecycle guidance for ciphertext encrypted under the old key.

## Recovery procedures

- [DR snapshot restore](DR-Snapshot-Restore.md) — rolling cluster metadata back to a known-good DR snapshot after a catastrophic metadata event. Cluster-wide and partial-restore paths, with the engineering-applied step explicitly called out where first-class tooling is missing.
- [Disaster recovery (reconstruct-from-disk)](Disaster-Recovery.md) — rebuild the bootstrap-layer Ra state from on-disk volume data when Ra logs are unrecoverable. Last-resort path when no snapshot is usable; preserves drives + volume registry, not encryption keys / ACLs / IAM state.

## When nothing fits

If the symptom you see does not match any runbook here:

1. Capture the output of `neonfs cluster status` and `neonfs node status` for every node you can reach.
2. Capture the last ~200 lines of `journalctl -u neonfs-core` (and the interface service if applicable) from each affected node.
3. Note the timestamps of the first and last observed symptom.
4. File an incident issue against [project-neon/neonfs](https://harton.dev/project-neon/neonfs/issues) with the above, plus the current NeonFS version from `/etc/neonfs/neonfs.conf` or the package install record.

## Post-incident

- [Post-mortem template](Post-Mortem-Template.md) — blameless template every runbook references in its Escalation / follow-up section. Copy, fill in, land as a PR.
- [Post-mortem sample](Post-Mortem-Sample.md) — filled-in worked example using the template (fictional drive-evacuation-during-reboot scenario).

Every runbook ends with an escalation path that links the post-mortem template. When a runbook has been fired, write it up. Over time the set of runbooks + post-mortems becomes the actual playbook — the wiki or this directory is just where it lives between incidents.
