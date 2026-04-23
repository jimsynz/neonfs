# Docker VolumeDriver Real-FUSE Integration Test Plan

- Date: 2026-04-22
- Scope: `neonfs_integration/test/integration/` OR `neonfs_fuse/native/neonfs_fuse/tests/` (placement is the open question this doc settles)
- Issue: [#387](https://harton.dev/project-neon/neonfs/issues/387) — part of [#243](https://harton.dev/project-neon/neonfs/issues/243) (Docker VolumeDriver)
- Prereq: #310 (Mount/Unmount plumbing) — closed

## Why this exists

The Docker VolumeDriver plugin's `Mount` / `Unmount` endpoints set up real FUSE mounts. Existing `neonfs_docker` tests stub the mount via an injected `mount_fn` (see `NeonFS.Docker.MountTracker`'s `default_mount_fn`), so they never exercise real FUSE. An end-to-end confidence check needs a real `/dev/fuse`.

Real FUSE mounts from an ExUnit process are blocked by the BEAM's `SIGCHLD = SIG_IGN` (documented in `neonfs_integration/test/test_helper.exs` and the parent epic #107). The existing Rust integration tests at `neonfs_fuse/native/neonfs_fuse/tests/mount_integration.rs` mount successfully because they run as normal OS processes, not under BEAM.

This plan settles where the test lives and what it drives.

## Options (from the issue body)

1. **Drive the plugin HTTP endpoints from a Rust integration test alongside the existing FUSE mount tests.**
2. **Spin up the plugin via `System.cmd/3` in an Elixir test but trigger the mount from an external Rust binary.**
3. **Exercise against real Docker in a privileged CI container.**

## Decision

**Option 1 — Rust integration test alongside the existing FUSE mount tests.**

Rationale:

- **No new CI infrastructure.** Option 3 needs Docker-in-Docker (or a privileged container running a real Docker daemon) in the CI runner. That's a separate infrastructure change (and pre-blocks #312). Option 1 runs in the same Rust test harness that's already green.
- **Isolates the FUSE-only concern.** #387's scope is "does `POST /VolumeDriver.Mount` set up a real FUSE mount and does reading/writing through it round-trip?" — that's a two-request test with HTTP + filesystem operations. Option 3 also sets up a `docker run` container, which exercises #312's scope, not #387's. #387 and #312 should stay separate so regression signals stay scoped.
- **Fewer moving parts.** Option 2 straddles BEAM and non-BEAM in one test — spawn plugin from Elixir, drive from Rust. The handshake for "plugin is ready" becomes non-trivial (wait for the Unix socket to appear, poll, time out on failure). Option 1 keeps everything in one process space.
- **Symmetric with what already works.** `neonfs_fuse/native/neonfs_fuse/tests/mount_integration.rs` already mounts FUSE from Rust. Adding a sibling test that also spawns the plugin (via `std::process::Command`) and drives its HTTP endpoint uses the same muscles.

Option 3 becomes the right choice when #312 lands — that issue's scope genuinely requires a real Docker daemon. Don't pre-optimise by trying to share infrastructure before the second caller exists.

## Shape

New test file: `neonfs_fuse/native/neonfs_fuse/tests/docker_plugin_integration.rs`.

Per-test layout:

1. **Start a minimal NeonFS cluster** — spawn `neonfs_core` release (or `neonfs_omnibus` for simplicity) via `std::process::Command`, wait for readiness via the health endpoint. Capture stdout/stderr for test output.
2. **Start the `neonfs_docker` plugin** — `System.cmd`-style spawn (from Rust), pointed at a temp `socket_path` and the local core node. Wait for the Unix socket to appear (poll with timeout).
3. **`POST /VolumeDriver.Create`** — create a plugin volume named `it-test-vol` with option `volume=<neonfs-volume-name>`.
4. **`POST /VolumeDriver.Mount`** — mount the volume, parse the returned `Mountpoint`.
5. **Verify the FUSE mount is live** — write a file through the mountpoint, read it back through `NeonFS.Core` (via a CLI call or a second mount), assert byte-identical.
6. **`POST /VolumeDriver.Unmount`** — teardown.
7. **`POST /VolumeDriver.Remove`** — cleanup.
8. Assert the mountpoint directory is no longer mounted.

The HTTP client is `reqwest` with Unix-socket transport (available via `reqwest::Client::builder().unix_socket(path)`). `hyper-util`'s `UnixConnector` is the low-level alternative if `reqwest` doesn't plug in clean.

## Test prerequisites

- `/dev/fuse` on the host. Guarded with a CI tag — same pattern as `mount_integration.rs` today.
- `fusermount3` on PATH.
- The `neonfs_omnibus` or `neonfs_core` release binary available — CI needs a build step that compiles the release before running this test. Easiest: run this test in the `neonfs_integration` CI job (which already builds releases) instead of the Rust test matrix.

Alternative if building a release in the test is too heavy: fake a cluster by running `iex --no-halt` against `neonfs_core/` (compile-only, no release) from the Rust test. Measure and decide in the implementation PR — the compile-cached release is likely fast enough.

## Teardown robustness

A panicking test that leaves a FUSE mount in place will break subsequent tests. Wrap the whole test body in:

```rust
let mount_point = ...;
let result = std::panic::catch_unwind(|| {
    // test body
});

// Always try to unmount, even on panic.
let _ = std::process::Command::new("fusermount3")
    .args(["-u", mount_point.to_str().unwrap()])
    .status();

result.unwrap();  // re-raise the panic if there was one.
```

Same shape as the existing `mount_integration.rs` uses for cleanup on panic.

## CI placement

`.forgejo/workflows/ci.yml` already has a `neonfs_fuse` job that runs with FUSE available. Add the new test to the same job's Rust test matrix — no new CI surface. If CI needs to build a `neonfs_omnibus` release first, the job gains one extra step but stays within the same runner.

## Out of scope (hard)

- Driving the plugin through a real `docker run` — that is #312.
- Testing volume policy (replication, tiering, encryption) — those have their own test paths.
- Multi-node cluster behaviour. One-node cluster is enough to exercise the plugin's Mount/Unmount plumbing.

## Done when

- Test spins up `neonfs_docker`, a NeonFS cluster, and a real FUSE mount driven via a real `POST /VolumeDriver.Mount` call.
- Reads/writes through the returned mountpoint round-trip through NeonFS core.
- `Unmount` cleanly tears the mount down (confirmed via `/proc/self/mountinfo`).
- Test is green in CI on the `neonfs_fuse` runner.

## References

- Issue: [#387](https://harton.dev/project-neon/neonfs/issues/387)
- Sibling issue: [#312](https://harton.dev/project-neon/neonfs/issues/312) — full `docker run` E2E; will want a different infrastructure shape.
- Template: `neonfs_fuse/native/neonfs_fuse/tests/mount_integration.rs`
- Plugin source: `neonfs_docker/lib/neon_fs/docker/{supervisor,plug}.ex`
