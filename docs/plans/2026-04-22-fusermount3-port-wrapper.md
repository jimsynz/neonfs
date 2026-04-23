# `fusermount3` Port Wrapper Implementation Plan

- Date: 2026-04-22
- Scope: `fuse_server/native/fuse_server/` (add NIF entry points), `fuse_server/lib/` (new `Fusermount` module), tests
- Issue: [#275](https://harton.dev/project-neon/neonfs/issues/275) — part of [#107](https://harton.dev/project-neon/neonfs/issues/107) (native-beam FUSE stack)
- Prereq: #274 (`/dev/fuse` NIF with `enif_select`) — closed

## Why this exists

The current FUSE implementation (via `fuser` 0.17 NIF) calls `fusermount3` through `std::process::Command::spawn()` inside the BEAM. Because the BEAM sets `SIGCHLD` to `SIG_IGN`, `fusermount3`'s internal `waitpid` fails with `ECHILD` and the crate's error recovery path panics — crashing the `MountManager` GenServer. The parent epic (#107) documents the failure mode in detail.

The fix, split out into #275 so it can land and be tested independently, is to spawn `fusermount3` through an Erlang `Port` (which uses `erl_child_setup` and reaps children correctly) and receive the `/dev/fuse` fd that `fusermount3` sends back via `SCM_RIGHTS` on a Unix domain socket.

## Design

### Responsibility split between Port and NIF

- **`Port.open/2`** handles the child-process lifecycle: fork + exec `fusermount3`, wait for exit, collect stderr. This is the specific BEAM capability the current `fuser` crate can't use — it's why we're rewriting.
- **New NIF entry points** handle the two operations that a Port can't express: allocating a Unix socketpair up front, and reading the `SCM_RIGHTS` control message after `fusermount3` writes it. The received fd becomes a `FuseFd` resource — the same resource type `fuse_server`'s #274 NIF already exposes.

Keeping the Port on Elixir side means the existing `MountManager` supervision tree owns the port exit signal flow without special cases. Keeping the socketpair and `SCM_RIGHTS` receive in the NIF keeps the unsafe syscall glue localised.

### NIF surface

Two new entries in `fuse_server/native/fuse_server/src/lib.rs`, alongside the existing `open_dev_fuse`, `pipe_pair`, `select_read`, `read_frame`, `write_frame`:

```rust
/// Allocate a Unix-domain SOCK_DGRAM socketpair. Returns the two fds as
/// (parent_fd, child_fd) integers. The caller is responsible for closing
/// parent_fd when done and passing child_fd to the fusermount3 child
/// process via its ` _FUSE_COMMFD` env var (= the numeric fd).
#[rustler::nif]
pub fn commfd_pair() -> NifResult<(i32, i32)>;

/// Read the SCM_RIGHTS message from `parent_fd` and return a FuseFd
/// resource holding the received /dev/fuse fd. Blocks until fusermount3
/// writes or closes the socket. Errors:
/// - `:no_message`        — socket closed before fusermount3 wrote
/// - `:malformed_message` — control data wasn't a SCM_RIGHTS with one fd
/// - `{:errno, atom}`     — underlying recvmsg error
#[rustler::nif]
pub fn recv_fuse_fd(parent_fd: i32) -> NifResult<ResourceArc<FuseFd>>;
```

The resource type is the existing `FuseFd` from #274. No new resource; the destructor + stop-callback glue is already in place.

### Elixir module surface

New `fuse_server/lib/fuse_server/fusermount.ex`:

```elixir
defmodule FuseServer.Fusermount do
  @moduledoc """
  Spawns `fusermount3` to mount a FUSE filesystem, receiving the
  `/dev/fuse` fd via SCM_RIGHTS. Unmount shells out to `fusermount3 -u`.

  The BEAM sets SIGCHLD = SIG_IGN, which breaks fuser's
  `Command::spawn`-based mount (ECHILD from internal waitpid). This
  module routes the spawn through Port.open/2 so `erl_child_setup`
  handles reaping correctly.
  """

  @type mount_opts :: [fsname: String.t(), subtype: String.t(), ...]

  @spec mount(Path.t(), mount_opts) :: {:ok, FuseServer.Native.fuse_fd()} | {:error, term()}
  def mount(mount_point, opts \\ []) do
    # 1. commfd_pair/0 → {parent_fd, child_fd}
    # 2. Port.open({:spawn_executable, fusermount3_path}, [
    #      args: ["-o", opts_string, "--", mount_point],
    #      env: [{~c"_FUSE_COMMFD", :erlang.integer_to_list(child_fd) ++ [0]}],
    #      exit_status: true,
    #      :stderr_to_stdout
    #    ])
    # 3. recv_fuse_fd(parent_fd) → {:ok, fuse_fd_resource}
    # 4. Await {port, {:exit_status, 0}}; on non-zero, collect stderr and return {:error, {:fusermount, stderr}}
    # 5. close_fd(parent_fd)
    # 6. return {:ok, fuse_fd_resource}
  end

  @spec unmount(Path.t()) :: :ok | {:error, term()}
  def unmount(mount_point) do
    # Port.open({:spawn_executable, fusermount3_path}, args: ["-u", mount_point], exit_status: true)
    # receive {port, {:exit_status, 0}} -> :ok; non-zero -> {:error, {:fusermount, stderr}}
  end
end
```

The `fusermount3_path` defaults to `"/usr/bin/fusermount3"` but is configurable via `Application.get_env/3` so container images with a different layout can override. Found once at module compile time or first-use, not on every mount.

### Error shape

Every error tuple uses `{:error, {:fusermount, reason}}` so callers can pattern-match on the outer tag. `reason` is:

- `:enoent` — `fusermount3` binary not found at configured path.
- `:timeout` — port exited without writing the fd within 10 s (generous default).
- `{:exit, status, stderr}` — non-zero exit; `stderr` captured from `:stderr_to_stdout`.
- `:malformed_message` / `:no_message` — bubbled up from `recv_fuse_fd/1`.
- `{:errno, atom}` — bubbled up from `recv_fuse_fd/1` for low-level socket errors.

### Cleanup semantics

- **Successful mount**: caller owns the `FuseFd` resource. When it's dropped (Elixir GC or explicit release), the NIF destructor closes `/dev/fuse` — same as the #274 path.
- **Failed mount**: `recv_fuse_fd/1` failure leaves nothing for the caller to clean up. The socketpair's `parent_fd` must always be closed (add a new `close_fd/1` NIF for this, or bundle close into `recv_fuse_fd/1`'s error path — simpler to bundle).
- **Port exits unexpectedly**: the mount caller should `:erlang.port_close/1` its port reference if `Port.open/2` returned but `recv_fuse_fd/1` hung. Timeout guards against stuck `fusermount3`.

### Options-string rendering

`fusermount3 -o <opts>` takes a comma-separated list. The Elixir side renders the `mount_opts` list to the same canonical form the existing FUSE tests expect (`fsname=<v>,subtype=<v>,<bool_flag>`). Any shell-unsafe character in an option value raises `ArgumentError` at encode time — no shell injection even though Port doesn't go through a shell (belt and braces).

## Test plan

Two kinds of tests:

1. **Unit-ish / NIF-ish** (`fuse_server/test/fuse_server/fusermount_test.exs`):
   - Use `commfd_pair/0` + `recv_fuse_fd/1` with a fake sender (helper that does `sendmsg(SCM_RIGHTS, [fd])` from another process — can be a small test-only Rust helper or a spawned `socat`-ish shim).
   - Covers: happy path, `:no_message` (sender closes socket without sending), `:malformed_message` (sender sends a plain message), `{:errno, _}` (pass a closed fd to `recv_fuse_fd`).
   - No real FUSE mount. Runs anywhere.

2. **Integration** (`fuse_server/test/integration/fusermount_integration_test.exs`, tagged `:fuse`):
   - Requires `/dev/fuse` and `fusermount3` on the test host (guard with `@moduletag :fuse`; CI jobs without FUSE skip the tag).
   - Mounts to a temp dir, confirms the returned `FuseFd` resource is usable (read one frame — a FUSE `INIT` request from the kernel), unmounts.
   - Uses `System.tmp_dir!/0` + `System.cmd("fusermount3", ["-u", ...])` in `on_exit` to clean up if a test panics.

CI already runs `:fuse`-tagged tests on the `neonfs_fuse` runner per `.forgejo/workflows/ci.yml`. Add `fuse_server` to that runner's package list (or extend the existing `neonfs_fuse` job — it already has FUSE available).

## Implementation breakdown

The doc lives first so the implementer can branch and pick up without re-design:

1. NIF additions (`commfd_pair/0`, `recv_fuse_fd/1`, `close_fd/1` bundled where appropriate) — ~100 lines Rust + tests.
2. Elixir `FuseServer.Fusermount` module — ~80 lines + moduledoc.
3. Test files — unit + integration.
4. `.check.exs` / `mix.exs` — ensure new test-only deps (if any; current plan uses no new crates).
5. PR title: `feat(fuse_server): port-based fusermount3 wrapper with SCM_RIGHTS fd receive`.

## Open points carried forward

1. **Fallback to `mount(2)` syscall.** The issue body flags this as "can be a follow-up — fusermount3 is the primary path". Defer. Only needed if deployments want to drop the `fusermount3` dependency entirely (unusual on Debian).

2. **Finding `fusermount3`.** `libfuse3` installs it at `/usr/bin/fusermount3` on Debian. For non-Debian distros the path may differ; honour `$PATH` by using `System.find_executable/1` at module load time, fall back to `/usr/bin/fusermount3`, error at mount time if neither resolves.

3. **Timeout default.** 10 s is a guess. `fusermount3` typically returns in tens of milliseconds but has to touch `/proc` and the kernel's FUSE driver; 10 s buys room for cold caches. Make it configurable via an opt to `mount/2` once there's a workload that exercises the edge.

## References

- Issue: [#275](https://harton.dev/project-neon/neonfs/issues/275)
- Parent epic: [#107](https://harton.dev/project-neon/neonfs/issues/107)
- Prereq (closed): [#274](https://harton.dev/project-neon/neonfs/issues/274) — `/dev/fuse` NIF with `enif_select`
- Source: `fuse_server/native/fuse_server/src/lib.rs` (existing NIF scaffolding, resource type pattern)
- Protocol reference: `fuser-0.17.0/src/mnt/fuse_pure.rs::receive_fusermount_message` — `SCM_RIGHTS` envelope layout
