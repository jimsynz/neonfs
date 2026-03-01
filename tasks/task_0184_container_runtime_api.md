# Task 0184: Container Runtime Detection and Basic API

## Status
Blocked — premature; deferring chaos testing to a later stage

## Phase
Gap Analysis — H-4 (1/7)

## Description
Create a container runtime abstraction layer that auto-detects whether
Docker or Podman is available and provides a uniform API for container
lifecycle operations. This is the foundation for the chaos testing
framework.

## Acceptance Criteria
- [ ] `NeonFS.Integration.Container` module created with runtime detection
- [ ] `detect_runtime/0` checks for Docker, then Podman, returns `{:ok, :docker | :podman}` or `{:error, :no_runtime}`
- [ ] Detection uses `System.find_executable/1` to locate `docker` or `podman` binaries
- [ ] `run/2` starts a container: accepts image name and options (name, env, ports, network, volumes)
- [ ] `stop/1` stops a running container by name or ID
- [ ] `kill/1` forcefully kills a container (SIGKILL)
- [ ] `pause/1` pauses a container (freezes all processes — true partition simulation)
- [ ] `unpause/1` resumes a paused container
- [ ] `rm/1` removes a stopped container
- [ ] `exec/2` executes a command inside a running container
- [ ] `inspect/1` returns container status (running, paused, exited, etc.)
- [ ] `logs/1` returns container stdout/stderr
- [ ] All functions return `{:ok, result}` or `{:error, reason}`
- [ ] Commands executed via `System.cmd/3` with appropriate timeouts
- [ ] Unit test: runtime detection finds at least one runtime (or skips)
- [ ] Unit test: `run/2` then `stop/1` then `rm/1` lifecycle works
- [ ] Unit test: `pause/1` then `unpause/1` works
- [ ] Tests tagged `@tag :container` for selective execution
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_integration/test/neonfs/integration/container_test.exs`:
  - Skip tests if no container runtime is available
  - Use a lightweight image (`alpine:latest` or `busybox`) for lifecycle tests
  - Verify each lifecycle operation works end-to-end
  - Clean up containers in `on_exit` callback

## Dependencies
- None

## Files to Create/Modify
- `neonfs_integration/lib/neonfs/integration/container.ex` (create — runtime abstraction)
- `neonfs_integration/test/neonfs/integration/container_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — H-4
- `spec/testing.md` lines 608–680 (container-based testing)
- Docker CLI reference (run, stop, kill, pause, unpause, exec, inspect)
- Podman CLI reference (compatible with Docker CLI)

## Notes
Docker and Podman have nearly identical CLI interfaces for the operations
we need. The main difference is rootless Podman may have different network
namespace behaviour, but for container pause/unpause and basic lifecycle,
they are interchangeable.

The `run/2` options should support:

```elixir
Container.run("neonfs-core:latest", [
  name: "neonfs-core-1",
  env: %{"RELEASE_COOKIE" => "test", "RELEASE_NODE" => "core1@172.18.0.2"},
  network: "neonfs-test",
  ports: [{4369, 4369}, {9100, 9100}],
  detach: true
])
```

Container cleanup is critical in tests. Always use `on_exit` callbacks to
stop and remove containers, even if the test fails.

For CI, ensure the container runtime is available in the CI environment.
The existing `bake.hcl` builds NeonFS container images that can be used
for chaos tests.
