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

spec/                 # Specification documents (start here)
├── specification.md     # Entry point & overview
├── architecture.md      # System design, supervision trees
├── implementation.md    # Phases 1-7, dependencies
└── testing.md           # Testing strategy

tasks/                # Implementation task specifications
├── README.md         # Task overview & dependency graph
└── task_NNNN_*.md    # Individual tasks with acceptance criteria
```

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

## Task System

Implementation follows 37 task specifications in `/workspace/tasks/`. Each task has:
- Status, phase, description
- Acceptance criteria checkboxes
- Testing strategy
- Dependencies on other tasks

**Parallel work streams (Phase 1):**
- Tasks 0001-0009: neonfs_blob Rust crate (independent)
- Tasks 0010-0013: neonfs_fuse Rust crate (independent)
- Tasks 0014-0019: Elixir metadata & read/write (depends on blob scaffolding)
- Tasks 0022-0025: CLI (can develop alongside core)

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

## Version Requirements

From `.tool-versions`:
- Elixir 1.19.5 (OTP 28)
- Erlang 28.3.1
- Rust 1.93.0

## Key Specification Documents

Always consult these before implementing:
- `spec/specification.md` - Start here for overview
- `spec/architecture.md` - System design, NIF boundaries
- `spec/implementation.md` - Phase roadmap, dependency tables
- `spec/testing.md` - Test examples and patterns
- `spec/service_discovery.md` - Node discovery and cluster formation
- `tasks/README.md` - Task dependency graph

## Module Naming

- Top-level: `NeonFS.Client.*`, `NeonFS.Core.*`, `NeonFS.FUSE.*`, `NeonFS.NFS.*`, and `NeonFS.Integration.*`
- File paths use underscore: `NeonFS.Core` → `lib/neon_fs/core.ex`
- Type specs required on all public Elixir functions (for Dialyzer)

## Forgejo

This repository is hosted on a Forgejo instance at `harton.dev`. Use the `fj` CLI (not `gh`) for pull requests, issues, and other forge operations:
```bash
fj pr create --base main "PR title"    # Create a pull request
fj pr list                              # List pull requests
fj issue list                           # List issues
```

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
