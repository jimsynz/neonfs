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

## Scheduled procedures

_(Forthcoming — tracked in [#253](https://harton.dev/project-neon/neonfs/issues/253): cluster upgrade, certificate rotation, capacity pressure response, key rotation, DR snapshot restore, post-mortem template.)_

## When nothing fits

If the symptom you see does not match any runbook here:

1. Capture the output of `neonfs cluster status` and `neonfs node status` for every node you can reach.
2. Capture the last ~200 lines of `journalctl -u neonfs-core` (and the interface service if applicable) from each affected node.
3. Note the timestamps of the first and last observed symptom.
4. File an incident issue against [project-neon/neonfs](https://harton.dev/project-neon/neonfs/issues) with the above, plus the current NeonFS version from `/etc/neonfs/neonfs.conf` or the package install record.

## Post-incident

Every runbook ends with an escalation path that links the post-mortem template. When a runbook has been fired, write it up. Over time the set of runbooks + post-mortems becomes the actual playbook — the wiki or this directory is just where it lives between incidents.
