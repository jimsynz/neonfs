# CLAUDE.md

This file provides guidance to coding agents when working with code in this repository.

## Project Overview

NeonFS is a BEAM-orchestrated distributed filesystem combining Elixir's coordination strengths with Rust's performance for storage operations. The project follows a strict separation of concerns: Elixir handles coordination, policy, and APIs; Rust handles I/O, chunking, and cryptography via Rustler NIFs.

**Current Status:** Phase 2 (Distributed) complete - multi-node Ra cluster with replication, node failure recovery, and full acceptance tests passing.

## Build Commands

### Elixir (from repository root or individual packages)
```bash
mix check --no-retry           # Run all checks in all subprojects (from root)
mix compile                    # Compile Elixir + Rustler NIFs
mix test                       # Run ExUnit tests
mix test path/to/test.exs      # Run specific test file
mix test path/to/test.exs:42   # Run test at specific line
mix format                     # Format code
mix format --check-formatted   # Check formatting
mix credo --strict             # Code style checker
mix dialyzer                   # Static type analysis
```

### Pre-Commit Checks

After making changes, always run these before committing:
```bash
mix format
mix credo --strict
mix doctor
```
These checks frequently catch issues (missing struct specs, Credo warnings) that require follow-up fixes.

### Rust (from native/ crates)
```bash
cargo test                                        # Run tests
cargo clippy --all-targets -- -D warnings         # Linting
cargo fmt --check                                 # Format check
```

## Git Commit Signing

The DevPod environment configures `devpod-ssh-signature` as the GPG signing program for SSH-based commit signing. This tool does not support the `-U` flag that newer versions of git pass for the signing buffer file, causing `fatal: failed to write commit object` errors.

**Workaround:** Use `-c commit.gpgsign=false` when committing:
```bash
git -c commit.gpgsign=false commit -m "commit message"
```

## Architecture

```
neonfs_client/        # Shared types & service discovery (pure Elixir library)
├── lib/neon_fs/client/
│   ├── connection.ex     # Bootstrap node connectivity
│   ├── cost_function.ex  # Latency/load-based node selection
│   ├── discovery.ex      # Service discovery cache (ETS)
│   └── router.ex         # RPC routing with failover
└── lib/neon_fs/core/
    ├── file_meta.ex      # Shared file metadata type
    └── volume.ex         # Shared volume type

neonfs_core/          # Elixir control plane (depends on neonfs_client)
├── lib/neon_fs/core/
│   ├── application.ex    # Supervision tree root
│   ├── service_registry.ex  # Ra-backed service registry
│   └── core.ex           # Main module
├── native/               # Rustler NIFs (via mix rustler.new)
└── test/

neonfs_fuse/          # FUSE filesystem package (depends on neonfs_client only)
├── lib/neon_fs/fuse/
│   ├── application.ex
│   └── fuse.ex
├── native/
└── test/

neonfs_nfs/           # NFSv3 server package (depends on neonfs_client only)
├── lib/neon_fs/nfs/
│   ├── handler.ex       # NFS operation → core RPC translation
│   ├── export_manager.ex # Volume export lifecycle
│   └── metadata_cache.ex # ETS-backed metadata cache
├── native/
│   └── neonfs_nfs/      # Rust NIF (nfs3_server + nfs3_client)
└── test/

neonfs_integration/   # Peer-based integration tests
├── lib/neonfs/integration/
│   └── peer_cluster.ex   # Spawns real peer nodes for testing
└── test/integration/     # Multi-node integration tests

```

Architecture and design documentation lives in the [wiki](https://harton.dev/project-neon/neonfs/wiki) — start with [Specification](https://harton.dev/project-neon/neonfs/wiki/Specification).

### Dependency Graph

```
neonfs_client  ← neonfs_core  (shared types, service registry)
neonfs_client  ← neonfs_fuse  (service discovery, RPC routing)
neonfs_client  ← neonfs_nfs   (service discovery, RPC routing)
neonfs_core    ← neonfs_integration (all packages for integration tests)
neonfs_fuse    ← neonfs_integration
```

neonfs_fuse and neonfs_nfs have **no dependency** on neonfs_core. All communication with core nodes happens via Erlang distribution, routed through the `NeonFS.Client.Router` module.

### Key Design Principles
- All data flows through Elixir for single code path and consistency
- Content-addressed storage: immutable SHA-256 identified chunks
- Per-volume supervision trees for isolation
- Async Rust NIFs for backpressure via BEAM scheduler

## No Whole-File Buffering (CRITICAL)

**Never load an entire file's contents into memory.** This is a recurring bug in this codebase and every instance is a correctness defect, not a performance nit.

A single volume can hold files much larger than available RAM. Buffering a whole file — as a binary, iolist, `Vec<u8>`, or any other "one value holding all the bytes" — will OOM the node under realistic workloads. It has already happened multiple times.

### The rule

When reading or writing file contents, process data as a stream of chunks with a bounded working set. The working set may be a single chunk, a small sliding window, or a fixed-size buffer — it must NOT scale with file size.

This applies everywhere: core read/write paths, interface packages (FUSE, NFS, S3, WebDAV, Docker, CSI), content-type detection, checksums, compression, encryption, backup/restore, copy/move, and any new feature that touches file bytes.

### Concrete guidance

Reads:

- Use `NeonFS.Core.read_file_stream/3` — returns a `Stream` that pulls chunks lazily. This is the canonical API.
- For byte-range reads, pass `:offset` and `:length` and consume the stream; don't `Enum.into(<<>>)` the whole thing.
- On the interface side, use `NeonFS.Client.ChunkReader` for data-plane reads. If a callsite calls `read_file/2,3` and buffers the result, that's a bug — convert it to a stream.

Writes:

- Accept an `Enumerable` / `Stream` input, not a binary blob (streaming write API tracked in #195).
- If you must stage a partial chunk to align to the volume's chunk boundary, the staging buffer is bounded by **chunk size** — never by file size.
- Multipart / chunked HTTP uploads (S3 multipart, WebDAV PUT) must feed chunks through as they arrive. Collecting all parts before a single write is a violation.

### Prohibited patterns

```elixir
# WRONG — File.read/1 loads the entire file into memory.
{:ok, data} = File.read(path)
write_file(volume, dest, data)

# WRONG — Stream collapsed into a single binary.
stream
|> Enum.into(<<>>, fn chunk -> chunk end)
|> then(&write_file(volume, dest, &1))

# WRONG — Plug conn body read to completion before forwarding.
{:ok, body, conn} = Plug.Conn.read_body(conn, length: :infinity)
```

```rust
// WRONG — reads the whole file into a Vec<u8>.
let data = std::fs::read(path)?;

// WRONG — read_to_end on an untrusted-size stream.
let mut buf = Vec::new();
reader.read_to_end(&mut buf).await?;
```

### Required patterns

```elixir
# Right — pull chunks lazily, send each one downstream.
NeonFS.Core.read_file_stream(volume, path)
|> Stream.each(&handler.send_chunk/1)
|> Stream.run()
```

```rust
// Right — bounded 64 KiB buffer, copy in a loop.
let mut buf = [0u8; 64 * 1024];
loop {
    let n = reader.read(&mut buf).await?;
    if n == 0 { break; }
    writer.write_all(&buf[..n]).await?;
}
```

### If you think you need to violate this

Don't. If you believe a case genuinely requires whole-file buffering, stop and ask — there is almost always a streaming alternative, and the correct answer is to push streaming further up the call chain rather than buffer here.

## Work Tracking

Work items are tracked as [repository issues on Forgejo](https://harton.dev/project-neon/neonfs/issues). Pick one, work it, close it.

Historical context:
- [Progress Archive wiki page](https://harton.dev/project-neon/neonfs/wiki/Progress-Archive) — chronological log through 2026-04-19
- [Codebase Patterns wiki page](https://harton.dev/project-neon/neonfs/wiki/Codebase-Patterns) — living reference of patterns and gotchas; update as you learn

Release notes live in [`CHANGELOG.md`](CHANGELOG.md), generated from conventional commits.

## Testing

**CRITICAL: Never bypass or exclude tests.** Skipped tests create a false sense of confidence. If a test requires specific environment setup (FUSE support, privileges, etc.), the CI environment must be configured correctly - not the tests excluded. A failing build due to missing infrastructure is preferable to silently skipped tests.

**Never use the `--no-start` flag when running tests.** The application must be started for tests to work correctly.

**For integration tests in neonfs_integration**, ensure dependencies are fetched separately (`mix deps.get` in the subproject directory) before running tests.

**Testing layers (bottom to top):**
1. Static analysis: Dialyzer, Clippy, Credo
2. Unit/property tests: ExUnit + StreamData (Elixir), cargo test + proptest (Rust)
3. NIF boundary tests: Elixir calling Rust NIFs
4. Integration tests: Peer-based multi-node clusters (neonfs_integration/)

**Running tests:**
```bash
mix test                       # All Elixir tests
cargo test                     # Rust tests
```

**Test suite performance:** The full check suite (`mix check --no-retry`) takes several minutes, and the integration tests (`neonfs_integration`) alone can take 6+ minutes. **Save test output to a file and grep it** rather than re-running the suite each time you need to inspect results:
```bash
mix check --no-retry 2>&1 | tee /tmp/neonfs_check.txt
grep -E 'failure|FAILED|✕' /tmp/neonfs_check.txt
```
Run individual test files first to iterate quickly before running the full suite.

## BEAM Memory in Containers

Docker 25+ containers inherit the kernel's `nr_open` as `RLIMIT_NOFILE`
(~1e9 on modern kernels). OTP sizes its port table from that, capped at
`2^27-1` entries, which pre-allocates ~1.6 GB **per BEAM VM** before any
code runs. Multiplied by peer-cluster integration tests, this will OOM a
laptop quickly.

We pin the port table to a sensible size in two places:
- `.devcontainer/devcontainer.json` via `containerEnv.ERL_ZFLAGS`
- `.forgejo/workflows/ci.yml` via the top-level `env.ERL_ZFLAGS`

Both set `ERL_ZFLAGS="+Q 65536"`, which propagates to every BEAM
including peer VMs spawned by `:peer.start_link`. Releases already cap
ports via `rel/vm.args.eex` (`ERL_MAX_PORTS 4096`), so production was
unaffected. If you see baseline BEAM RSS above ~100 MB with no code
loaded, check `erlang:system_info(port_limit)` and `ulimit -n`.

## Version Requirements

From `.tool-versions`:
- Elixir 1.19.5 (OTP 28)
- Erlang 28.3.1
- Rust 1.93.0

## Key Specification Documents

Always consult these before implementing (all live in the [wiki](https://harton.dev/project-neon/neonfs/wiki)):
- [Specification](https://harton.dev/project-neon/neonfs/wiki/Specification) — start here for overview
- [Architecture](https://harton.dev/project-neon/neonfs/wiki/Architecture) — system design, NIF boundaries
- [Implementation](https://harton.dev/project-neon/neonfs/wiki/Implementation) — phase roadmap, dependency tables
- [Testing](https://harton.dev/project-neon/neonfs/wiki/Testing) — test examples and patterns
- [Service Discovery](https://harton.dev/project-neon/neonfs/wiki/Service-Discovery) — node discovery and cluster formation

## Module Naming

- Top-level: `NeonFS.Client.*`, `NeonFS.Core.*`, `NeonFS.FUSE.*`, `NeonFS.NFS.*`, and `NeonFS.Integration.*`
- File paths use underscore: `NeonFS.Core` → `lib/neon_fs/core.ex`
- Type specs required on all public Elixir functions (for Dialyzer)

## Forgejo

This repository is hosted on a Forgejo instance at `harton.dev`. Use the `fj` CLI (not `gh`) for pull requests, issues, and other forge operations:
```bash
fj pr create --base main "PR title"    # Create a pull request
fj pr search --state open              # List open pull requests
fj issue search --state open           # List open issues
fj issue view 123                      # View issue details
fj pr view 123                         # View PR details
```

### API Access

For API operations that `fj` doesn't support well (CI status, issue comments, PR creation with body), use the Forgejo REST API with the `fj` CLI's stored token:

```bash
FJ_TOKEN=$(jq -r '.hosts["harton.dev"].token' ~/.local/share/forgejo-cli/keys.json)
```

The git credential token (`git credential fill`) has limited scopes and cannot read/write issues or comments. Always use the `fj` CLI token for API calls.

**Add a comment to an issue:**
```bash
curl -s -X POST \
  -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/issues/123/comments" \
  -d '{"body": "Comment text here"}'
```

**Create a pull request with body:**
```bash
curl -s -X POST \
  -H "Authorization: token $FJ_TOKEN" \
  -H "Content-Type: application/json" \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/pulls" \
  -d '{"title": "PR title", "body": "PR body", "head": "branch-name", "base": "main"}'
```

### Retrieving CI Status and Logs

The `fj pr status` command is broken on this Forgejo version. Use the API with `FJ_TOKEN` (see above).

**Check CI status for a commit:**
```bash
COMMIT=$(git rev-parse HEAD)
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/commits/$COMMIT/statuses" \
  | jq '.[] | {context, status, description}'
```

**Show only failures:**
```bash
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/commits/$COMMIT/statuses" \
  | jq '.[] | select(.status == "failure") | {context, description}'
```

**List workflow runs for the current branch (with job-level detail):**
```bash
SHA=$(git rev-parse HEAD)
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/actions/tasks?limit=20" \
  | jq ".workflow_runs[] | select(.head_sha == \"$SHA\") | {id, name, status, event}"
```

**Compare against main to identify pre-existing failures:**
```bash
MAIN_SHA=$(git rev-parse origin/main)
curl -s -H "Authorization: token $FJ_TOKEN" \
  "https://harton.dev/api/v1/repos/project-neon/neonfs/commits/$MAIN_SHA/statuses" \
  | jq '.[] | select(.status == "failure") | {context, description}'
```

**Note:** Forgejo does not expose job logs via the API. If you need to see the actual log output of a failing CI job, reproduce the failure locally by running the same commands from the workflow file (`.forgejo/workflows/ci.yml`). Each CI job runs `mix check` or `cargo test`/`cargo clippy` in the relevant package directory.

## Container Building

Build containers for local testing (single-arch, loaded locally):
```bash
PLATFORMS='linux/amd64' docker buildx bake -f bake.hcl --load core fuse nfs cli
```

The `--load` flag is required to load images into the local Docker daemon. Without it, images are only pushed to the registry. Multi-platform builds don't support `--load`, so override PLATFORMS for local testing.

## Multi-Node Architecture

neonfs_core, neonfs_fuse, and neonfs_nfs run as separate Erlang nodes communicating via distribution:
- Core node: `neonfs_core@neonfs-core` (storage, metadata, CLI handler, service registry)
- FUSE node: `neonfs_fuse@neonfs-fuse` (FUSE mount operations, routes to core via neonfs_client)
- NFS node: `neonfs_nfs@neonfs-nfs` (NFSv3 server, routes to core via neonfs_client)
- CLI connects to core node, core makes RPC calls to FUSE node for mount operations
- Ensure matching `RELEASE_COOKIE` across all nodes

### Service Discovery

Non-core nodes (FUSE, NFS, S3, Docker, etc.) use `neonfs_client` to discover and communicate with core nodes:
- `NeonFS.Client.Connection` — connects to bootstrap nodes via `Node.connect/1`
- `NeonFS.Client.Discovery` — queries `NeonFS.Core.ServiceRegistry` on core nodes, caches in local ETS
- `NeonFS.Client.CostFunction` — measures latency and load to select optimal core node
- `NeonFS.Client.Router` — routes RPC calls with automatic failover

Non-core nodes join the cluster using the same invite token mechanism but skip Ra membership. They register as services in `NeonFS.Core.ServiceRegistry`, which is backed by Ra consensus and replicated across core nodes.

## GenServer Persistence Patterns

For GenServers that own ETS tables and need persistence on shutdown:
1. Add `Process.flag(:trap_exit, true)` in `init/1` - without this, `terminate/2` is not called
2. Supervisor shuts down children in REVERSE start order
3. Each GenServer should persist its own ETS tables in `terminate/2` while they still exist
4. Don't rely on a central Persistence GenServer to snapshot tables owned by other processes

## Test Synchronisation

**Never use `Process.sleep` for test synchronisation.** Sleeps are timing-dependent, cause flakiness, and slow down the suite. Use event-driven synchronisation instead:

**Telemetry events + `:telemetry_test`** — for waiting on async operations (GenServer cycles, background tasks):
```elixir
# In source code — emit telemetry at the point of interest:
:telemetry.execute([:neonfs, :component, :action], %{}, %{key: value})

# In tests — subscribe and assert_receive:
ref = :telemetry_test.attach_event_handlers(self(), [
  [:neonfs, :component, :action]
])
# ... trigger the action ...
assert_receive {[:neonfs, :component, :action], ^ref, %{}, %{key: _}}, 1_000
```

**`:sys.get_state/1`** — for synchronising with a GenServer's mailbox after sending it a message:
```elixir
send(genserver, :some_message)
:sys.get_state(genserver)  # blocks until handle_info returns
# now safe to assert on side effects
```

**Ready signals** — for waiting on spawned processes to complete setup:
```elixir
parent = self()
spawn(fn ->
  do_setup()
  send(parent, :child_ready)
  # ...
end)
assert_receive :child_ready, 1_000
```

Telemetry events serve double duty: they enable deterministic tests AND provide operational observability (metrics, alerting, dashboards). When adding new async behaviour, always consider adding telemetry — it's useful beyond just testing.

## Phase 5 Metadata Migration

Phase 5 migrates metadata indexes (ChunkIndex, FileIndex, StripeIndex) from Ra-backed storage to leaderless quorum-replicated BlobStore via QuorumCoordinator. **There is no need for backward compatibility with Ra in the migrated modules.** When migrating an index module, remove all Ra fallback code paths entirely — the module should require `quorum_opts` and use QuorumCoordinator exclusively. Do not add dual-mode (Ra + quorum) support.

## Rustler NIF Return Values

Rustler wraps Rust `Result<T, E>` types:
- `Result<(), E>` success → `{:ok, {}}` (not `:ok`)
- `Result<T, E>` success → `{:ok, value}`
- `Result<T, E>` error → `{:error, reason}`

Handle the `{:ok, {}}` case explicitly when expecting simple `:ok`.

## Phase Completion Requirements

**A phase is NOT complete until all components are fully integrated and tested together.**

Before declaring any implementation phase complete:

1. **Run the full test suite**: `mix check --no-retry` from the repository root (runs checks in all subprojects)
2. **All integration tests must pass** - the neonfs_integration package spawns real peer nodes to test multi-node scenarios
3. **Verify inter-service communication works**:
   - CLI → Core (via Erlang distribution)
   - Core → FUSE (via RPC/distribution)
   - FUSE → Core (via neonfs_client Router/Discovery)
4. **Test failure scenarios**: node restart, node failure, recovery

Unit tests passing is necessary but NOT sufficient. Integration between:
- neonfs_core and neonfs_fuse
- CLI and daemon communication
- Multi-node Ra cluster coordination

must all work via the peer-based integration tests before moving to the next phase.

**Common integration issues to check:**
- Erlang nodes not connected (need explicit `Node.connect/1` or matching cookies)
- Service discovery failing (check `NeonFS.Client.Discovery.get_core_nodes/0` or `Node.list()`)
- RPC calls returning `{:badrpc, _}` or `{:error, :all_nodes_unreachable}` (nodes not reachable)
- Client infrastructure not ready (Connection, Discovery, CostFunction need time to probe after startup)
