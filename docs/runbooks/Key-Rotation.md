# Runbook: Key rotation (scheduled and emergency)

Rotate the encryption key on an encrypted volume. Two scenarios:

- **Scheduled rotation** — part of a security hygiene cadence. Low risk; the online rotation CLI handles the mechanics. This runbook frames it with pre-flight, verification, and the backup-interaction call-out.
- **Emergency rotation** — a key is suspected compromised (credential leak, repository exfiltration, ex-operator off-boarding). Higher urgency; faster cutover; old key becomes untrusted on a schedule you choose.

Both paths share the same underlying CLI. The difference is in cadence, operator paranoia level, and what you do with existing backups / snapshots encrypted under the old key.

## Symptom (trigger)

### Scheduled rotation

- Security calendar reminder fires ("annual key rotation for production-vol").
- Compliance policy requires a key-rotation audit entry before the next review.
- Onboarding a new operator cohort and the rotation cadence ends in `_cohort`.

### Emergency rotation

- Suspected or confirmed key-material compromise — credential leak in logs, repository leak, missing backup tape, ex-operator off-boarding where the key was in their custody.
- Intrusion-detection alert implicating a host that handled key material.
- Compliance event requiring attested rotation (breach-disclosure clock starts running — the rotation must be completed and auditable within a window).

Emergency rotation is **not** driven by a failure of the volume itself. The volume is healthy; the concern is that an adversary may have a copy of the current key.

## Precondition

- Cluster has quorum and the volume is in a healthy state (`neonfs volume show <name>` reports no in-progress migrations or rotations).
- Current key material is backed up off-cluster at a known location. Rotation replaces the current key; losing the CURRENT key during rotation mid-flight would be recoverable only from the backup.
- You have audit-log write access — rotation events are captured there for compliance.
- For emergency rotation: you have identified the time window when the suspected compromise occurred and the set of backups / DR snapshots taken under the compromised key. You cannot un-compromise a backup that was exfiltrated before rotation; the goal is to limit what future access yields.

## Procedure

### Scheduled rotation

#### 1. Pre-flight

```bash
neonfs volume show <volume-name>
# Confirm: encrypted=true, rotation not already in progress, no stuck jobs.

neonfs volume rotation-status <volume-name>
# Confirm: no rotation currently running. If one is, wait or investigate.
```

If an earlier rotation is stuck (progress not advancing, not completing), do not start a new one — see [Escalation](#escalation-path).

#### 2. Start the rotation

```bash
neonfs volume rotate-key <volume-name>
```

Rotation is online. The volume stays readable and writable throughout — existing clients keep working. Under the hood, each chunk is re-encrypted with the new key as a background job; the new key is the active one for all new writes immediately.

#### 3. Monitor progress

```bash
neonfs volume rotation-status <volume-name>
```

Output reports how many chunks have been migrated to the new key and the rate of progress. A large volume can take hours — the job is backgrounded and will resume on node restart, so there is no action needed except to monitor.

#### 4. Verify completion

```bash
neonfs volume rotation-status <volume-name>
# Status reports "complete" or equivalent. No chunks pending re-encryption.

neonfs volume show <volume-name>
# Current key version has advanced from its pre-rotation value.
```

A quick smoke-test read and write against the volume confirm client-level continuity.

#### 5. Audit-log entry

Rotation commits an audit-log entry automatically. Verify it is present:

```bash
neonfs audit list --volume <volume-name> --event key_rotation --limit 5
```

The most recent entry has the rotation timestamp, the operator identity, the old and new key versions (not the keys themselves), and `status=complete`.

### Emergency rotation

The procedural steps mirror the scheduled path (steps 1–5 above), but the operational shape differs.

#### 6. Declare and scope

Before any CLI action, answer:

- **Which volumes used this key?** The compromise scope determines how many volumes need rotation. If the cluster shares keys across volumes (unusual), treat every touched volume as in-scope.
- **When did the compromise window start?** This determines which backups / DR snapshots might also be compromised. Rotation cannot un-compromise those, but it can bound future exposure.
- **Who needs to know?** Incident response typically requires compliance / legal notification on a clock. Start that notification in parallel with technical rotation; do not serialise.

#### 7. Run rotation in urgent mode

```bash
neonfs volume rotate-key <volume-name>
```

The CLI itself has no separate "emergency" flag; the urgency is procedural, not technical. For each in-scope volume, kick off the rotation. If there are multiple volumes, run them in parallel — they do not contend for the same Ra resources beyond normal cluster headroom.

Stay on top of `rotation-status` — an emergency rotation that stalls is worth escalating to engineering immediately rather than waiting.

#### 8. Handle backups and DR snapshots under the old key

Backups and DR snapshots taken before rotation are still encrypted with the old key. After rotation:

- **If the old key is known compromised**: treat those backups as compromised too. Depending on what they contain and your retention policy, re-encrypt them under the new key (the backup tool may support this) or destroy them. Coordinate with whoever owns the backup lifecycle — this is rarely the same operator who ran the rotation.
- **If the old key is suspected but not confirmed compromised**: tag the backup range in your backup catalogue. A future compliance query needs to be able to answer "which backups were encrypted under the suspect key?".
- **Forward-going**: the next backup taken after rotation is encrypted with the new key. Check the first post-rotation backup succeeds end-to-end before concluding the rotation is done.

**Do not** rely on "we rotated, the key is gone" as a mitigation for already-exfiltrated backups. The attacker who got the old key can decrypt any ciphertext they previously intercepted or can still access.

#### 9. Invalidate the old key

The rotation process advances the "active" key version. The old key is retained for a grace window to keep reading pre-rotation chunks that have not yet migrated. Once rotation completes (`rotation-status` reports `complete`), the old key is no longer in active use.

For emergency rotation, shorten the retention of old-key material off-cluster — destroy the backup you took in the [Precondition](#precondition) step once you are confident the rotation is complete. Keeping a compromised key accessible is the shape of risk rotation was meant to eliminate.

#### 10. Audit and post-incident

Same as scheduled rotation (step 5), but with additional scope notes:

```bash
neonfs audit list --volume <volume-name> --event key_rotation
neonfs audit list --event key_rotation --since <compromise-window-start>
```

The post-incident write-up (using [Post-Mortem-Template.md](Post-Mortem-Template.md) _(forthcoming)_) captures the compromise-window start, the rotation-completion time, the list of in-scope volumes, the backup-lifecycle outcome, and the alerting or process gap that let the compromise go undetected.

## Verification

A rotation (scheduled or emergency) is resolved when:

- `neonfs volume rotation-status <name>` reports complete with 0 pending chunks.
- `neonfs volume show <name>` reports the new `current_key_version`, advanced from the pre-rotation value.
- An audit-log entry with `event=key_rotation`, `status=complete`, and the correct operator identity is present.
- A smoke-test write and a smoke-test read against the volume succeed.
- For emergency rotation specifically: the first post-rotation backup has succeeded, and the old-key handling (re-encrypt, destroy, or catalogue-tag) is documented in the post-mortem.

## Escalation path

Escalate to engineering if any of the following hold. Capture context before stopping:

- `rotation-status` shows no progress for > 10 minutes and no errors in the journal. This suggests the background job is stuck, not rate-limited.
- `rotate-key` returns `{:error, :rotation_in_progress}` on every attempt despite `rotation-status` reporting nothing in flight. State inconsistency — do not try to force-start another rotation.
- You find chunks that fail decryption after rotation completes (`{:error, :decryption_failed}` on reads of older files). This indicates the old-to-new migration lost chunks, which is data loss territory.
- An emergency rotation needs to hit multiple cluster-internal keys that do not have a `volume rotate-key` path (e.g. cluster CA — that belongs to [CA-Rotation](CA-Rotation.md); Erlang cookie — separate procedure not yet in this runbook set).
- Compliance window is tight and the rotation cannot complete within it — may need to take the volume offline to accelerate, which is a change-management decision, not a runbook one.

Capture before escalating:

- `neonfs volume show <volume-name>` and `neonfs volume rotation-status <volume-name>` output (JSON).
- `neonfs audit list --volume <volume-name> --since <compromise-window-start>` output.
- Last 200 lines of `journalctl -u neonfs-core` from every core node.
- Timeline: compromise-suspected window, rotation-start time, first symptom, first operator action.

## Known limitations

- Rotation is per-volume. There is no cluster-wide "rotate every volume" command; operators script it as a loop over `volume list | filter encrypted=true`. Tracked as a future enhancement.
- `rotate-key` has no `--urgent` flag that would deprioritise other background jobs. On a busy cluster, an emergency rotation competes with scrub / GC / rebalance for the same worker pool. Operators cancel those jobs manually if they need to clear the runway.
- No facility to rotate a specific key version'\''s grace period — once rotation is running, the old key is retained until rotation completes. If you need the old key invalidated faster than rotation can re-encrypt every chunk (e.g. for a regulatory breach-disclosure clock), escalate.
- There is no built-in notification to downstream backup systems that the volume'\''s key has changed. Backup-lifecycle integration (re-encrypting old backups, tagging the compromise window) is operator-driven today.
- Erlang cookie rotation and CA rotation are separate procedures with their own runbook ([CA-Rotation](CA-Rotation.md)) / runbook gap (Erlang cookie rotation has no runbook yet).

## References

- [Operator guide §Key rotation](../operator-guide.md#key-rotation) — CLI mechanics.
- [Operator guide §Volume management](../operator-guide.md#volume-management) — encryption configuration.
- [CLI reference §neonfs volume](../cli-reference.md#neonfs-volume) (`rotate-key`, `rotation-status`, `show`).
- [CA-Rotation runbook](CA-Rotation.md) — when the compromise scope includes the cluster CA, not just a volume key.
- [Capacity-Pressure runbook](Capacity-Pressure.md) — rotation throughput competes with GC / scrub / rebalance; if the cluster is under capacity pressure, resolve that first.
