# Task 0077: Audit Logging

## Status
Not Started

## Phase
6 - Security

## Description
Implement operational audit logging for security-relevant events. Audit events are stored in a bounded, Ra-backed log with configurable retention. Events cover node join/leave, volume lifecycle, ACL changes, encryption operations, key rotation, and administrative actions. All events record the acting UID (not username). The audit log is queryable via CLI. As noted in the spec, these are operational logs — not tamper-proof forensic records.

## Acceptance Criteria
- [ ] New `NeonFS.Core.AuditLog` GenServer with ETS cache for recent events and Ra for durable storage
- [ ] `AuditEvent` struct: `id`, `timestamp`, `event_type`, `actor_uid` (integer), `actor_node` (atom), `resource` (volume_id, path, etc.), `details` (map), `outcome` (`:success` or `:denied`)
- [ ] Event types: `:node_joined`, `:node_left`, `:volume_created`, `:volume_deleted`, `:volume_acl_changed`, `:file_acl_changed`, `:encryption_enabled`, `:key_rotated`, `:rotation_completed`, `:authorisation_denied`, `:admin_action`
- [ ] `AuditLog.log/1` records an event (async via `GenServer.cast/2` — must never slow down operations)
- [ ] `AuditLog.query/1` returns events matching criteria: by event_type, actor_uid, resource, time range
- [ ] `AuditLog.recent/1` returns the N most recent events (from ETS cache)
- [ ] Bounded storage: configurable max events (default 100,000), oldest events pruned when limit reached
- [ ] Retention: configurable time-based retention (default 90 days), expired events pruned periodically
- [ ] Audit events emitted from: ACLManager (ACL changes), KeyRotation (rotation events), Cluster.Join (node join), CLI.Handler (admin actions), Authorise (denied access)
- [ ] Added to core supervision tree
- [ ] Persistence module updated to snapshot audit ETS
- [ ] CLI handler: `handle_audit_list/1` with optional filters (type, actor_uid, time range, limit)
- [ ] Telemetry events: `[:neonfs, :audit, :logged]` for monitoring audit log health
- [ ] Unit tests for event creation and storage
- [ ] Unit tests for query filtering
- [ ] Unit tests for retention and pruning

## Testing Strategy
- ExUnit tests for AuditLog.log/1 (create events, verify stored)
- ExUnit tests for query filtering (by type, actor_uid, resource, time range)
- ExUnit tests for bounded storage (exceed limit, verify oldest pruned)
- ExUnit tests for retention pruning (create old events, trigger prune, verify removed)
- ExUnit tests verifying audit events are emitted from security operations
- Verify AuditLog.log/1 is non-blocking (cast, not call)

## Dependencies
- task_0070 (MetadataStateMachine v6 — Ra storage for audit events)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/audit_log.ex` (new — GenServer with ETS + Ra)
- `neonfs_client/lib/neon_fs/core/audit_event.ex` (new — AuditEvent struct)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add AuditLog to supervision tree)
- `neonfs_core/lib/neon_fs/core/persistence.ex` (modify — snapshot audit ETS)
- `neonfs_core/lib/neon_fs/core/cli/handler.ex` (modify — add audit list handler, emit audit events)
- `neonfs_core/lib/neon_fs/core/acl_manager.ex` (modify — emit audit events)
- `neonfs_core/lib/neon_fs/core/key_rotation.ex` (modify — emit audit events)
- `neonfs_core/test/neon_fs/core/audit_log_test.exs` (new)

## Reference
- spec/operations.md — Audit Logging section
- spec/security.md — Threat Model (what events are security-relevant)

## Notes
The audit log uses `GenServer.cast/2` (async) for recording — audit logging must never block or slow operations. If the audit GenServer is overloaded, events may be dropped — acceptable for operational logs. Consider whether audit events should use a separate Ra state machine (to avoid bloating the main metadata state) or a bounded ring buffer. A separate state machine or even just local ETS with periodic persistence may be cleaner than putting every event through Ra consensus. For tamper-resistant audit trails, the spec recommends shipping logs to an external system (syslog, SIEM) — that is a deployment concern.
