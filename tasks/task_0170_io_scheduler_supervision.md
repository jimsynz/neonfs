# Task 0170: I/O Scheduler Supervision Tree

## Status
Complete

## Phase
Gap Analysis — H-1 (6/8)

## Description
Wire all I/O scheduler components into a supervision tree under the
`NeonFS.Core` application. The supervisor manages the producer, priority
adjuster, and a `DynamicSupervisor` for drive workers. Drive workers are
started dynamically as drives are discovered by `DriveRegistry`.

## Acceptance Criteria
- [ ] `NeonFS.IO.Supervisor` defined as a `Supervisor` with `:one_for_one` strategy
- [ ] Children: `NeonFS.IO.Producer`, `NeonFS.IO.PriorityAdjuster`, `NeonFS.IO.WorkerSupervisor` (DynamicSupervisor)
- [ ] `NeonFS.IO.WorkerSupervisor` starts drive workers dynamically via `start_child/1`
- [ ] `NeonFS.IO.Scheduler.start_worker/1` registers a new drive worker for a given drive
- [ ] `NeonFS.IO.Scheduler.stop_worker/1` terminates a drive worker when a drive is removed
- [ ] Workers registered in `Registry` by drive_id for lookup
- [ ] On startup, queries `DriveRegistry` for all known drives and starts workers
- [ ] Subscribes to drive discovery events (telemetry or PubSub) to add/remove workers dynamically
- [ ] `NeonFS.IO.Supervisor` added as a child of the core application supervisor
- [ ] Supervisor restart strategy: producer crash restarts all workers (operations re-submitted by callers)
- [ ] Worker crash isolated to that drive only (other drives unaffected)
- [ ] Unit test: supervision tree starts with expected children
- [ ] Unit test: adding a drive starts a new worker
- [ ] Unit test: removing a drive stops its worker
- [ ] Unit test: producer crash doesn't kill the priority adjuster
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_core/test/neon_fs/io/supervisor_test.exs`:
  - Start the full I/O supervision tree in test
  - Verify all children are running
  - Simulate drive add/remove, verify worker lifecycle
  - Kill the producer, verify it restarts and workers re-subscribe

## Dependencies
- Task 0168 (Drive worker — the component being supervised)
- Task 0169 (Priority adjuster — supervised alongside producer)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/io/supervisor.ex` (create — supervision tree)
- `neonfs_core/lib/neon_fs/io/worker_supervisor.ex` (create — DynamicSupervisor for drive workers)
- `neonfs_core/lib/neon_fs/io/scheduler.ex` (modify — add start_worker/stop_worker, query DriveRegistry on init)
- `neonfs_core/lib/neon_fs/core/application.ex` (modify — add IO.Supervisor to children)
- `neonfs_core/test/neon_fs/io/supervisor_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-1
- `spec/architecture.md` lines 501–510 (I/O supervisor placement)
- Existing: `neonfs_core/lib/neon_fs/core/application.ex` (supervision tree)
- Existing: `neonfs_core/lib/neon_fs/core/drive_registry.ex`

## Notes
The supervision tree structure:

```
NeonFS.IO.Supervisor (:one_for_one)
├── NeonFS.IO.Producer (GenStage producer)
├── NeonFS.IO.PriorityAdjuster (GenServer)
└── NeonFS.IO.WorkerSupervisor (DynamicSupervisor)
    ├── DriveWorker (drive_a)
    ├── DriveWorker (drive_b)
    └── ...
```

Using `:one_for_one` means a worker crash only restarts that worker. If the
producer crashes, workers lose their subscription — they should detect this
and re-subscribe (GenStage handles this via the `:subscribe_to` option in
`init/1`).

The I/O supervisor should be placed after `DriveRegistry` in the application
startup order, since it queries drives on init.
