# NeonFS Developer Guide

This guide is for contributors working on NeonFS itself. It complements the operator and user guides with the information you need to make changes to the codebase.

This is a living document — keep it aligned with [`CLAUDE.md`](../CLAUDE.md) and [`AGENTS.md`](../AGENTS.md) at the repo root (those are the authoritative agent-facing references; this guide is the human-facing overview).

For day-to-day pattern lookups, the [Codebase Patterns](https://harton.dev/project-neon/neonfs/wiki/Codebase-Patterns) wiki page is where we record gotchas as we find them.

## Repository layout

NeonFS is a multi-package umbrella project. Each package is a separate Mix project under the repo root, plus one Rust CLI crate. There is no parent Mix umbrella — the top-level `mix.exs` uses `ex_check` to run per-package checks from the root.

```
neonfs/
├── neonfs_client/          # Shared types, service discovery, transport pool, ChunkReader
├── neonfs_core/            # Storage engine, metadata, Ra consensus, cluster CA, policy
├── neonfs_fuse/            # FUSE filesystem daemon
├── neonfs_nfs/             # NFSv3 + NLM v4 server
├── neonfs_s3/              # S3-compatible HTTP gateway
├── neonfs_webdav/          # WebDAV server
├── neonfs_docker/          # Docker VolumeDriver plugin
├── neonfs_omnibus/         # All-in-one bundle (core + interfaces)
├── neonfs_integration/     # Peer-based multi-node integration tests
├── neonfs-cli/             # Rust CLI (`neonfs` binary)
├── fuse_server/            # Standalone FUSE server process (native BEAM FUSE stack)
├── nfs_server/             # Standalone NFS server process (native BEAM NFS stack)
├── packaging/              # nfpm configs, systemd units, postinst scripts
├── containers/             # Dockerfiles and bake.hcl
└── docs/                   # Guides (this file, user/operator, deployment, orchestration, etc.)
```

### Dependency graph

```
neonfs_client  ← neonfs_core
neonfs_client  ← neonfs_fuse
neonfs_client  ← neonfs_nfs
neonfs_client  ← neonfs_s3
neonfs_client  ← neonfs_webdav
neonfs_client  ← neonfs_docker
neonfs_core, neonfs_fuse, neonfs_nfs, neonfs_s3, neonfs_webdav, neonfs_docker  ← neonfs_omnibus
all of the above                                                               ← neonfs_integration
```

Interface packages depend **only** on `neonfs_client`, not on `neonfs_core`. All communication with core flows through `NeonFS.Client.Router` over Erlang distribution (metadata) and a dedicated TLS data plane (chunks). This is a hard rule — breaking it means an interface package can't deploy on a node separate from core.

### Control plane vs data plane

Two parallel transports carry traffic:

- **Erlang distribution** — the BEAM cluster's built-in distribution channel. Used for metadata RPCs, service discovery, `:pg` event notifications, and Ra consensus. Framed, TLS-wrapped in production (via Erlang's distribution TLS).
- **TLS data plane** — a dedicated mTLS channel with `{packet, 4}` framing, NimblePool connection pooling, and `{active, 10}` backpressure. Used for bulk chunk traffic (reads, writes, replication). Never carries metadata.

Keeping these separate means large chunk transfers don't starve metadata RPCs or block Ra consensus messages.

## Key subsystems

### BlobStore (`neonfs_core/lib/neon_fs/core/blob_store.ex` + `native/neonfs_blob/`)

Content-addressed chunk storage. Chunks are immutable, identified by SHA-256 of the plaintext (after compression, before encryption). Tiered across `hot`/`warm`/`cold` drives per volume policy. The `neonfs_blob` Rust NIF does the heavy lifting — FastCDC chunking, SHA-256, Zstandard compression, AES-256-GCM encryption.

Key gotchas:

- Chunk paths include a codec suffix (compression + encryption nonce fingerprint) — different codec variants coexist. Always resolve paths via `BlobStore.codec_opts_for_chunk/1` rather than hand-constructing them.
- `verify: true` + `decompress: true` checks the decompressed hash (which is what the outer address refers to). Don't add a redundant manual SHA-256.
- BlobStore tiers are strings (`"hot"`, `"warm"`, `"cold"`) when crossing the NIF boundary; metadata is atoms. Use `Atom.to_string/1` when calling BlobStore.

### Ra (`neonfs_core/lib/neon_fs/core/ra_server.ex`)

Raft-backed state machine for cluster membership, service registry, job tracker, drive configuration, policies, and scheduler state. Single state machine per cluster; all core nodes participate.

Testing with Ra:

- Ra requires a named node — `:nonode@nohost` fails with `:system_not_started`. Integration tests always run under `peer:start_link`.
- Ra UIDs must be sanitised — strip `@` and `.` from node names.
- State persists between tests even with `start_supervised!` — use `System.unique_integer` in IDs.

### Quorum metadata (`neonfs_core/lib/neon_fs/core/quorum_coordinator.ex` and `*_index.ex`)

Leaderless quorum-replicated indexes for chunk/file/stripe metadata. Configurable N/R/W; default N=3, R=2, W=2. Migrated off Ra during Phase 5 (see `CLAUDE.md` for migration notes — do not add Ra fallback paths to migrated modules).

Backed by `persistent_term` reads from public non-GenServer functions (read-heavy). Erase in `terminate/2`.

### Distributed Lock Manager (DLM)

Quorum-backed distributed locks, shared across all interface protocols. Named locks (POSIX paths, WebDAV resource URIs, NFS NLM). See `NeonFS.Core.LockManager`.

### ChunkReader (`neonfs_client/lib/neon_fs/client/chunk_reader.ex`)

Client-side streaming chunk fetch. Builds a distribution-safe `Stream` locally and pulls each chunk via the TLS data plane. Interface packages use `read_file_stream/3` — never `read_file/2` with whole-file buffering (the buffering-check CI guard will reject it).

### Interface packages

Each shipping interface (`neonfs_fuse`, `neonfs_nfs`, `neonfs_s3`, `neonfs_webdav`, `neonfs_docker`) follows the same shape:

```
neonfs_<name>/
├── lib/neon_fs/<name>/
│   ├── application.ex       # start_link + HealthCheck register + systemd notify
│   ├── supervisor.ex        # VolumeStore/MountTracker → Registrar → listener
│   ├── health_check.ex      # package-specific health checks
│   ├── plug.ex or handler.ex # protocol entry point
│   └── <protocol-specific>
├── native/                  # Rustler NIF crates (if needed)
├── rel/{vm.args.eex,env.sh.eex}  # release config
└── test/
```

Copy from `neonfs_docker` (the newest addition) when scaffolding a new interface package. See [Adding a new interface package](#adding-a-new-interface-package).

## Rustler NIF conventions

NIFs live under `<package>/native/<crate_name>/`. Create new crates with `mix rustler.new`.

### Crate layout

- `src/lib.rs` — `#[rustler::nif]` functions are auto-discovered as of Rustler 0.37+ (no explicit list in `rustler::init!`).
- Use `use Rustler, otp_app: :neonfs_<package>, crate: :<crate_name>` in the Elixir wrapper module. No `mix.exs` compiler modification needed.
- For resources (native objects Elixir holds onto), use `#[rustler::resource_impl]` on `impl Resource for T {}`.

### Binary handling

- To return an Erlang binary from a NIF: use `NewBinary::new(env, len)`, copy bytes in, then `.into()` to get `Binary<'a>`. `Vec<u8>` returns as an Erlang list (probably not what you want).
- For inputs: `Binary<'a>` holds a borrowed slice; use `.as_slice()` to read.

### Error encoding

Rustler maps Rust `Result<T, E>` to Elixir tuples:

- `Result<(), E>` success → `{:ok, {}}` (not bare `:ok`). Handle this explicitly when calling from Elixir.
- `Result<T: Encoder, E: Encoder>` → `{:ok, T} | {:error, E}`. So `Result<Atom, Atom>` returns `{:ok, :ok} | {:error, :reason}` — rarely what you want.
- For idiomatic `:ok | {:error, reason}` side-effect NIFs, use `NifResult<Atom>` and return `Err(rustler::Error::Term(Box::new(atom)))` for errors.

### Integrating encryption / compression

Encryption is part of the BlobStore pipeline (`WriteOptions` / `ReadOptions`), **not** a separate NIF. One NIF boundary crossing per chunk. Don't add parallel NIFs for encryption — extend the existing pipeline.

### Common Clippy gotcha

Clippy's `too_many_arguments` fires at 8+ params. Accept it with `#[allow(clippy::too_many_arguments)]` when the function genuinely needs many options (typically `WriteOptions`/`ReadOptions` constructors).

## Testing strategy

NeonFS runs tests at four layers, from cheapest to most expensive:

1. **Static analysis** — Dialyzer (Elixir), Clippy (Rust), Credo (Elixir style).
2. **Unit / property tests** — ExUnit + StreamData (Elixir), `cargo test` + `proptest` (Rust).
3. **NIF boundary tests** — Elixir calling Rust NIFs, verifying Rustler encoding round-trips.
4. **Integration tests** — `neonfs_integration/` spawns real BEAM peers via `:peer.start_link`, exercising multi-node scenarios.

Run everything from the repo root:

```bash
mix check --no-retry       # all checks across all subprojects
```

Run just one package's tests:

```bash
cd neonfs_core && mix test
cd neonfs_core && mix test test/neon_fs/core/some_test.exs:42
```

For Rust crates:

```bash
cd neonfs_core/native/neonfs_blob && cargo test
cargo clippy --all-targets --all-features -- -D warnings
```

### Test synchronisation — no `Process.sleep`

Sleep-based test synchronisation is banned (it makes tests flaky and slow). Use one of:

- **Telemetry events** — emit at the point of interest; subscribe with `:telemetry_test.attach_event_handlers/2`; `assert_receive`.
- **`:sys.get_state/1`** — sends a synchronous message and waits for the GenServer's mailbox to drain.
- **Ready signals** — spawned processes `send(parent, :ready)` once set-up is complete; parent `assert_receive`.

Telemetry events serve double duty — they enable deterministic tests AND feed operational dashboards. Prefer emitting telemetry for async boundaries.

### Integration tests

`neonfs_integration` spawns multi-node clusters using `:peer.start_link`, including partition tests that flip cookies to prevent auto-reconnect. Read `neonfs_integration/lib/neonfs/integration/peer_cluster.ex` for the helpers.

Integration runs are expensive (6+ minutes end-to-end) — run individual files first when iterating, then the full suite before pushing.

### Flaky test / partition simulation

`Node.disconnect/1` alone doesn't hold — Ra and `:pg` auto-reconnect. To simulate a partition:

```elixir
:erlang.set_cookie(target, :partition_block)   # on both sides
Node.disconnect(target)
# ... test partition behaviour ...
:erlang.set_cookie(target, cluster.cookie)
Node.connect(target)
```

## Contributing workflow

### Branch naming

`<type>/<issue-number>-<short-slug>`, where `<type>` matches the conventional-commit type:

```
feat/123-s3-multipart-retries
fix/234-nfs-atime-regression
docs/345-runbook-partition-recovery
improvement/456-volume-scrub-parallelism
chore/567-bump-erlang
refactor/678-extract-write-op
test/789-additional-partition-coverage
```

Match the type to what you're actually doing — `feat` is for genuinely new user-facing features; most enhancements to existing features use `improvement`.

### Commit messages — conventional commits

**Every commit message and every PR title must follow [Conventional Commits](https://www.conventionalcommits.org/).** `git_ops` parses them to generate `CHANGELOG.md` and bump versions — non-conforming commits break the release pipeline.

Format:

```
<type>(<optional scope>): <lowercase description>
```

Types: `feat`, `improvement`, `fix`, `docs`, `chore`, `test`, `refactor`. Breaking changes append `!` before the colon.

Examples:

```
improvement(core): add distributed lock manager
fix(webdav): preserve dead properties across COPY
improvement(s3,client): virtual-hosted-style routing
chore(deps): update rust crate clap to v4.6.1
docs: point AGENTS.md to wiki
fix(deps)!: update Erlang to v28.3.1 (breaks OTP 27 compatibility)
```

Rules:

- Lowercase description. Proper nouns and code identifiers keep their casing.
- Backtick code identifiers: `` fix(fuse): handle `ENOSPC` in write path ``.
- `git_ops` adds the `(#N)` PR suffix automatically on squash-merge — don't type it yourself.
- No `Co-Authored-By` or `Generated by` footers.
- Don't `git commit --amend` unless explicitly asked — create a new commit instead.

### Pull requests

PR titles follow the same conventional-commit format (they become the squash-merge commit subject).

PR body should include:

```
Closes #<issue>
Follow-ups: #<M>, #<O>     # optional, for follow-up work you captured

<short summary of what changed and why>
```

Keep PRs focused — one issue, one PR. If mid-implementation you discover adjacent work, open a follow-up issue rather than widening scope.

### CI requirements

Before pushing, run from the repo root:

```bash
mix check --no-retry
```

This runs `mix format --check-formatted`, Credo, Dialyzer, Doctor, ExUnit across every subproject, plus Clippy and `cargo test` for Rust crates.

CI (Forgejo Actions, `.forgejo/workflows/ci.yml`) re-runs the same checks per-package. A PR can't merge until all contexts are green — except `neonfs_integration`, which takes 6+ minutes and is the usual bottleneck.

### Code review expectations

- Review in one pass where possible. NeonFS is small enough that context-switching between many open PRs hurts more than it helps.
- Approve or request changes concretely — pointing at a line is better than a general concern.
- Maintainer handles squash-merges. The bot adds the PR number suffix to the commit subject automatically.

## Adding a new interface package

Copy the shape from the most recently landed interface package (currently `neonfs_docker`). The minimum deliverable:

1. **Scaffold**: `lib/neon_fs/<name>/{application.ex, supervisor.ex, health_check.ex, plug.ex | handler.ex}`.
2. **Supervisor**: service-local state (VolumeStore / MountTracker / cache) → `NeonFS.Client.Registrar` (type `:<name>`) → protocol listener. Strategy `:one_for_one`.
3. **Application**: `start_link()` first, then `HealthCheck.register_checks()`, then (if not omnibus) `NeonFS.Systemd.notify_ready()`. The `unless Application.spec(:neonfs_omnibus)` guard is critical — omnibus coordinates sd_notify itself.
4. **Release**: `rel/vm.args.eex` + `rel/env.sh.eex` following the `neonfs_nfs` template (custom EPMD, cookie from `/var/lib/neonfs/.erlang.cookie`, optional TLS-distribution config).
5. **Container**: `containers/Containerfile.<name>` (three-stage Debian trixie, mirror `Containerfile.nfs`), add `contexts = { client, src, base, cli }` entry in `bake.hcl`.
6. **Debian packaging**: `packaging/nfpm/neonfs-<name>.yaml` + `packaging/systemd/neonfs-<name>.{service,-daemon}`. Append the new name to every relevant list: `build-debs.sh` build loop + nfpm package loop, `post-install.sh`, `pre-remove.sh`, `release.yml` amd64 + arm64 matrix.
7. **Omnibus integration**: add as optional dep in `neonfs_omnibus/mix.exs`, start via `neonfs_omnibus/lib/neon_fs/omnibus/application.ex`.
8. **Docs**: add to `README.md` packages table, add operator fragment to `docs/operator-guide.md`, document the protocol in `docs/user-guide.md`.

See `neonfs_docker` (issues #243, #310, #311) for a worked example from scaffold to release pipeline.

### Cross-node calls from interface packages

Interface packages must not import `neonfs_core` directly. Two established routes:

- **Core RPCs** — `NeonFS.Client.Router.call(NeonFS.Core, :function, [args])`. Handles failover and node selection.
- **Specific peer node** — `GenServer.call({Target.Module, node_atom}, msg)`. Use when the target isn't a core node (for example, a plugin calling `NeonFS.FUSE.MountManager` on the same host). Make the target node configurable via `Application.get_env/3` so both co-located and split-BEAM deployments work — see `NeonFS.Docker.MountTracker`'s `default_mount_fn` for the canonical shape.

Expose the call as a test-injectable `_fn` in the caller (`core_create_fn`, `mount_fn`, `unmount_fn`). Tests mock the function; production uses the default that drives the real Router / GenServer.

## Extending metadata

Two common shapes:

### Adding a field to an existing resource

1. Update the struct/type definition (e.g. `NeonFS.Core.FileMeta`).
2. Update the encoder/decoder if the type crosses the NIF or wire boundary.
3. Update `Cluster.State`'s serialise/deserialise functions if the field is cluster-wide.
4. Add migration handling in `Cluster.State.migrate/2` for older state-machine versions.
5. Update default volume config in `NeonFS.Core.Volume.defaults/0` if applicable.
6. Update any CLI handler that constructs/parses the resource (partial maps from CLI must be merged with defaults).
7. Add a property or unit test that exercises both old and new format.

### Adding a new quorum-replicated index

1. New module under `neonfs_core/lib/neon_fs/core/<name>_index.ex`, modelled on `FileIndex` / `ChunkIndex` / `StripeIndex`.
2. Use `QuorumCoordinator.quorum_write/4` and `quorum_read/3`.
3. Persist via `persistent_term` for hot-path reads, erase in `terminate/2`.
4. Add the index to the supervisor tree in `NeonFS.Core.Application`.
5. Unit tests + a peer-cluster integration test covering quorum semantics.

### Adding a new Ra state machine field

Rare — most new state lives in quorum metadata rather than Ra. When you do need Ra state:

1. Update `Cluster.State` struct.
2. Add serialiser/deserialiser entries.
3. Add a load function in `Application` that reads the field from startup.
4. Wire a supervisor child if the field drives a GenServer (with an `opts` helper that reads from `Application.get_env/3`).
5. Bump the state-machine version and add a migration. Never break old snapshots — Ra will happily feed a 6-month-old snapshot to your code after a restart.

## Operational gotchas while developing

- **BEAM memory in containers.** Docker 25+ containers inherit the kernel's `nr_open` as `RLIMIT_NOFILE` (~1e9). OTP sizes its port table from that, capped at 2²⁷-1 entries — ~1.6 GB preallocated per BEAM before any code runs. We pin with `ERL_ZFLAGS="+Q 65536"` in `.devcontainer/devcontainer.json` and `.forgejo/workflows/ci.yml`. Don't remove these.
- **Git commit signing on devpod.** The `devpod-ssh-signature` tool doesn't support Git 2.42+'s `-U` flag, so GPG-signed commits fail with `fatal: failed to write commit object`. Commit with `git -c commit.gpgsign=false commit …`.
- **Forgejo (not GitHub).** Use the `fj` CLI for PRs and issues; the token for API calls lives at `~/.local/share/forgejo-cli/keys.json`. See [`CLAUDE.md`](../CLAUDE.md) for curl recipes (CI status queries, PR creation, issue comments).

## Further reading

- [`CLAUDE.md`](../CLAUDE.md) — authoritative agent-facing reference. Overlaps significantly with this guide; the two should not drift.
- [`AGENTS.md`](../AGENTS.md) — the agent-facing companion, slightly different tone.
- [Codebase Patterns](https://harton.dev/project-neon/neonfs/wiki/Codebase-Patterns) wiki page — living reference of gotchas. Update as you discover new ones.
- [Architecture](https://harton.dev/project-neon/neonfs/wiki/Architecture), [Packages](https://harton.dev/project-neon/neonfs/wiki/Packages), [Specification](https://harton.dev/project-neon/neonfs/wiki/Specification), [Testing](https://harton.dev/project-neon/neonfs/wiki/Testing) in the wiki.
- [`operator-guide.md`](operator-guide.md) — when a change needs operational documentation.
- [`user-guide.md`](user-guide.md) — when a change affects what consumers see.
