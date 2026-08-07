# AGENTS.md

This file provides guidance to coding agents when working with code in this repository.

## Project Overview

NeonFS is a BEAM-orchestrated distributed filesystem combining Elixir's coordination strengths with Rust's performance for storage operations. The project follows a strict separation of concerns: Elixir handles coordination, policy, and APIs; Rust handles I/O, chunking, and cryptography via Rustler NIFs.

**Current Status:** the storage engine (replication, erasure coding, tiering, compression, encryption), multi-node clustering (Ra consensus, leaderless quorum metadata, cluster CA, TLS distribution, mTLS data plane), and seven access interfaces (FUSE, NFSv3, S3, WebDAV, Docker volumes, containerd content store, Kubernetes CSI) are shipped. CIFS/SMB via Samba VFS is in progress (#116): the `vfs_neonfs.so` C shim and `neonfs_cifs` Elixir bridge are built and CI-tested (#383, #384), with packaging (#385) and the end-to-end `smbd` test (#386) outstanding. See the [issue tracker](https://harton.dev/project-neon/neonfs/issues) for active work.

## Experimental Project — No Backwards Compatibility

NeonFS is experimental. **There are no production clusters.** Do not preserve backwards compatibility with prior on-disk formats, RPC shapes, module APIs, or behaviour when migrating or refactoring. Delete old code paths outright rather than gating them behind feature flags or fallbacks. This applies to every migration, including (but not limited to) the per-volume metadata work — switch readers and writers to the new path in whatever order is mechanically convenient and remove the old path. Don't add dual-mode shims, "during the transition" fallbacks, or `# legacy` branches.

## Build Commands

### Elixir (from individual package directories)
```bash
mix check --no-retry           # Run all checks for this package
mix compile                    # Compile Elixir + Rustler NIFs
mix test                       # Run ExUnit tests
mix test path/to/test.exs      # Run specific test file
mix test path/to/test.exs:42   # Run test at specific line
mix format                     # Format code
mix format --check-formatted   # Check formatting
mix credo --strict             # Code style checker
mix dialyzer                   # Static type analysis
```

There is no Mix project at the repository root. To run a task in every subproject, use the fan-out script:
```bash
resources/scripts/neonfs-each mix check --no-retry   # All checks in all subprojects
resources/scripts/neonfs-each mix deps.get           # Fetch deps everywhere
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

| Package | Purpose |
|---------|---------|
| `neonfs_client/` | Shared library every other package builds on: shared types (`Volume`, `FileMeta`), service discovery (`Connection`, `Discovery`, `CostFunction`), RPC routing (`Router`), chunk streaming over the TLS data plane (`ChunkReader`, `ChunkWriter`), KV access, service registration (`Registrar`). Pure library — no OTP application. |
| `neonfs_core/` | Control plane and storage engine: blob storage NIFs (`native/neonfs_blob`), file/chunk/stripe indexes on leaderless quorum replication, Ra-backed service + volume registries, cluster CA, join flow, GC, scrub, repair, tiering. |
| `neonfs_fuse/` | FUSE interface (FUSE transport + protocol codec via the `wick` hex library). `Session` owns the `/dev/fuse` fd; `Handler` translates ops into core RPCs; `MountManager` owns mount lifecycle. |
| `neonfs_nfs/` | NFSv3 interface: `NFSServer.*.Backend` impls against `neonfs_client`, export lifecycle, inode table, metadata cache, NLM v4 locking. |
| `neonfs_s3/` | S3-compatible HTTP interface (Bandit + `firkin`): backend, multipart store. |
| `neonfs_webdav/` | WebDAV interface (Bandit + `davy`). |
| `neonfs_docker/` | Docker/Podman VolumeDriver plugin (HTTP over Unix socket, FUSE-backed mounts). |
| `neonfs_containerd/` | containerd content-store gRPC plugin (Unix socket). |
| `neonfs_csi/` | Kubernetes CSI driver (gRPC over Unix socket; Helm chart in `deploy/charts/neonfs-csi/`). |
| `neonfs_cifs/` | Samba VFS module backend (ETF over Unix socket) — in progress. `vfs_neonfs.so` C shim + Elixir bridge built and CI-tested (#383, #384); packaging (#385) and end-to-end `smbd` test (#386) outstanding. |
| `neonfs_iam/` | IAM Ash domain — scaffold, resources land via #288/#290/#291/#292. |
| `neonfs_omnibus/` | Single release bundling core + all shipped interfaces. |
| `neonfs-cli/` | Rust CLI; speaks Erlang distribution (TLS) directly via `erl_dist`. |
| `neonfs_test_support/` | Peer-cluster test scaffolding (`PeerCluster`, `ClusterCase`, …) shared by every package's integration tests. |
| `neonfs_integration/` | Cross-node cluster-correctness test suite (formation, replication, partitions, failure recovery). Per-interface e2e tests live with their packages. |

Architecture and design documentation lives in the [wiki](https://harton.dev/project-neon/neonfs/wiki) — start with [Specification](https://harton.dev/project-neon/neonfs/wiki/Specification).

### Dependency Graph

```
neonfs_client  ← neonfs_core
neonfs_client  ← every interface package (fuse, nfs, s3, webdav, docker,
                 containerd, csi, cifs, iam)
neonfs_core + interfaces              ← neonfs_omnibus
neonfs_test_support (test-only)       ← all packages with peer-cluster tests
all of the above                      ← neonfs_integration
```

Interface packages have **no dependency** on neonfs_core. All communication with core nodes happens via Erlang distribution, routed through the `NeonFS.Client.Router` module, with bulk chunk data on the TLS data plane.

### Key Design Principles
- All data flows through Elixir for single code path and consistency
- Content-addressed storage: immutable SHA-256 identified chunks
- Per-volume supervision trees for isolation
- Blocking Rust NIFs run on dirty schedulers (`DirtyIo` for disk/syscall work, `DirtyCpu` for hashing/chunking/erasure) so they don't stall normal BEAM schedulers; the async-runtime rework (#1197) is tracked separately

## No Whole-File Buffering (CRITICAL)

**Never load an entire file's contents into memory.** This is a recurring bug in this codebase and every instance is a correctness defect, not a performance nit.

A single volume can hold files much larger than available RAM. Buffering a whole file — as a binary, iolist, `Vec<u8>`, or any other "one value holding all the bytes" — will OOM the node under realistic workloads. It has already happened multiple times.

### The rule

When reading or writing file contents, process data as a stream of chunks with a bounded working set. The working set may be a single chunk, a small sliding window, or a fixed-size buffer — it must NOT scale with file size.

This applies everywhere: core read/write paths, interface packages (FUSE, NFS, S3, WebDAV, Docker, CSI), content-type detection, checksums, compression, encryption, backup/restore, copy/move, and any new feature that touches file bytes.

### Concrete guidance

Reads:

- On core, use `NeonFS.Core.read_file_stream/3` — returns a `Stream` that pulls chunks lazily. This is the canonical API.
- For byte-range reads, pass `:offset` and `:length` and consume the stream; don't `Enum.into(<<>>)` the whole thing.
- On interface nodes (S3, WebDAV, NFS, FUSE), use `NeonFS.Client.ChunkReader.read_file_stream/3` — it builds a distribution-safe stream locally and fetches each chunk via the TLS data plane (or a range-limited per-chunk RPC for compressed/encrypted chunks). If a callsite calls `read_file/2,3` and buffers the result, that's a bug — convert it to a stream.

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

Releases are cut with `resources/scripts/neonfs-release`, which runs `mix git_ops.release` in the release tooling project at `resources/release/` (the repository root has no Mix project).

`git_ops` bumps the `version =` field in every Elixir `mix.exs` and every Rust `Cargo.toml` it tracks, but **does not** regenerate the three workspace `Cargo.lock` files (`neonfs_core/`, `neonfs-cli/`, `neonfs_client/native/neonfs_chunker/`). After a release commit, run `cargo update -p <workspace-package>` in each workspace and commit the lockfile changes — otherwise the next clean checkout's first `cargo build` produces an uncommitted lockfile drift (`<pkg> v0.1.0 → v<new>`) that shows up in every subsequent `git status`.

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

**Test suite performance:** The full check suite (`resources/scripts/neonfs-each mix check --no-retry`) takes several minutes, and the integration tests (`neonfs_integration`) alone can take 6+ minutes. **Save test output to a file and grep it** rather than re-running the suite each time you need to inspect results:
```bash
resources/scripts/neonfs-each mix check --no-retry 2>&1 | tee /tmp/neonfs_check.txt
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

- Top-level: `NeonFS.Client.*`, `NeonFS.Core.*`, `NeonFS.FUSE.*`, `NeonFS.NFS.*`, `NeonFS.S3.*`, `NeonFS.WebDAV.*`, `NeonFS.Docker.*`, `NeonFS.Containerd.*`, `NeonFS.CSI.*`, `NeonFS.CIFS.*`, `NeonFS.IAM.*`, `NeonFS.Omnibus.*`, and `NeonFS.TestSupport.*`
- The standalone protocol libraries use their own namespaces: the `wick` hex library provides `Wick.*` (FUSE); the `tahr` hex library provides `Tahr.*` (NFSv3/ONC-RPC)
- File paths use underscore: `NeonFS.Core` → `lib/neon_fs/core.ex`
- Type specs required on all public Elixir functions (for Dialyzer)

## CI Structure

`.forgejo/workflows/ci.yml` runs one `package` job per entry in `.forgejo/ci-matrix.json` (a dynamic matrix), plus bespoke jobs for `neonfs_integration`, `neonfs-cli`, `vfs_neonfs_wire`, and `vfs_neonfs`. `resources/scripts/ci-affected` filters the matrix and gates the bespoke jobs by a PR's changed paths; the `canary` job aggregates everything into the single required check.

**When adding a package**: add a `ci-matrix.json` entry (schema documented at the top of `ci.yml`) and a path rule in `ci-affected`. **When adding a sibling or test-only dependency**: update the corresponding `ci-affected` rule and, if build artefacts are shared, the entry's `cache_paths`/`lockfiles`. Lockfiles listed in an entry must exist — the job fails on missing ones rather than silently weakening the cache key.

**The integration suite is sharded, one shard per runner.** `neonfs_integration` is a matrix over `partition: [1, 2]` running `mix test --partitions 2`; its static tools (compiler, credo, dialyzer, formatter, audit) live in a separate `neonfs_integration_checks` job so they run once rather than per shard. Three things must stay in step when changing the shard count: the literal `partition` list, the `--partitions N` argument, and **the number of amd64 runners** — there are two.

That last one is the constraint, not a guideline. A separate job is not a separate host: the scheduler will happily place two shards on one runner, where they share its disk and its ephemeral port range while each boots multi-node peer clusters. Four shards on two runners did exactly that and exhausted both — `No space left on device` under `/tmp/neonfs_mount_*/blobs`, then `PeerCluster could not allocate an ephemeral peer port: :eaddrinuse`. Wider sharding bought no wall clock either, because only two shards ever ran at once. Confirm placement from the first line of each job's log, which names the runner (`eivor`, `jeb`).

Mix distributes test *files* round-robin over their sorted names, so a shard's contents shift whenever a file is added or renamed — balance is approximate by design, and one shard running slower than its sibling is expected rather than a fault. Only the sharded job needs the privileged container, the loop devices and the registry sidecar.

**A matrix job that `canary` depends on must never expand to zero instances.** Forgejo instantiates a job whose `if` is false, and that skipped job satisfies `needs` — but a matrix over an empty list creates no job at all, so `needs` on it is unsatisfiable and `canary` sits at "Blocked by required conditions" indefinitely, leaving the PR unmergeable. This is why `package` matrixes over `setup`'s `package_matrix` output (the affected list, or a single placeholder when that list would be empty) rather than over `packages` directly, and why the job keeps its own `if` on `packages`. It bit `neonfs-cli`-only and docs-only changes (#1641).

## Forgejo

This repository is hosted on a Forgejo instance at `harton.dev`. Use the `fj` CLI (not `gh`) for the simple read operations it handles well:
```bash
fj issue search --state open           # List open issues
fj issue view 123                      # View issue details
fj pr view 123                         # View PR details
fj pr search --state open              # List open pull requests
```

For everything else — CI status, job logs, comments, PR creation with body, self-assignment, automated rebase/merge — use the helper scripts in [`resources/scripts/`](resources/scripts/) rather than hand-rolled `curl` commands. Add that directory to `$PATH` for the session, or invoke directly:

```bash
export PATH="$PWD/resources/scripts:$PATH"
```

Each script handles auth, error reporting, and JSON parsing; they read the token from `~/.local/share/forgejo-cli/keys.json` (the `fj` CLI's store — the git credential token does not have the scopes for issues/PRs).

| Script | Purpose |
| --- | --- |
| `fj-token` | Print the API token (for one-off `curl` if needed). |
| `fj-whoami` | Print the authenticated login. |
| `fj-pr-status <pr#-or-sha>` | Latest CI status per context, TSV. |
| `fj-run-jobs <pr#-or-sha>` | Per-job state of the latest Actions run, TSV — distinguishes `blocked` (waiting on `needs`) from `waiting` (no runner has claimed it) from `running`, which commit statuses flatten to `pending`. |
| `fj-pr-failing <pr#-or-sha>` | Only the failing contexts, with `target_url` for log retrieval. |
| `fj-job-logs <run-index> <job-index> [attempt]` | Download a single job's log. Also accepts a `target_url` from `fj-pr-failing`. |
| `fj-job-logs-failing <pr#-or-sha> [--tail N]` | Dump every failing job's log, one section per job. |
| `fj-issue-assign-self <n>` | Self-assign an issue. |
| `fj-issue-create <title> <body\|-> [label-id...]` | Open an issue (body `-` reads from stdin). Common labels: `128` enhancement, `126` bug. |
| `fj-pr-create <head> <title> <body\|-> [base]` | Open a PR. |
| `fj-pr-rebase-stale [author]` | For each open PR (optionally by author), rebase on `main` and force-push if conflict-free; comment + skip on conflict. |
| `fj-pr-merge-when-green <pr#> [--timeout S] [--poll S] [--no-merge]` | Poll until CI is green, then squash-merge. Exit codes encode outcome: 0 merged, 2 needs rebase, 3 failure, 4 timeout, 5 already closed. |
| `fj-branch-prune [--delete] [--include-unmerged]` | List (or `--delete`) branches whose PR is merged/closed and safe to remove — squash-merge defeats `git branch --merged`, so it matches branches to PRs via the API. Dry-run by default; TSV `<verb><TAB><branch><TAB><pr#><TAB><note>`. |

Debug knobs the scripts honour: `FJ_HOST` (default `harton.dev`), `FJ_REPO` (default `project-neon/neonfs`), and — for the request layer in `_fj-lib.sh` — `FJ_MAX_TIME`, `FJ_WRITE_MAX_TIME`, `FJ_RETRIES`, `FJ_RETRY_DELAY`.

**Reads and writes get different budgets.** `FJ_MAX_TIME` (45 s) bounds a request that reads a record back; `FJ_WRITE_MAX_TIME` (300 s) bounds one that makes the server *do* something — merge a branch, create a PR or issue. One budget for everything means the cheap calls set it and the expensive one fails first: with a degraded instance answering a plain `GET` in ~35 s, every merge overran the read budget, `curl` aborted, and the wrapper reported `HTTP 000` — indistinguishable from the server refusing. Three merges "failed" that way before the fourth went through on the larger budget.

Every request is bounded by `--max-time` and retries transient failures (connection error, 429, 5xx) before giving up, and `fj_json` additionally rejects a body that will not parse. Both matter because a degraded instance answers with HTML often enough that piping it into `jq` yields `null` for every field — and `null` reads as a legitimate value. That is how `fj-pr-merge-when-green` once announced an open, mid-CI PR as "already merged" and exited 5, which tells its caller the work is done. **When writing a new helper, decide on `fj_json`, not `fj_curl` + `jq`.**

A retry is only safe for reads: a POST whose response was lost has still been applied. **Create through `fj_create`**, which sends once and then asks the server what happened, taking a verify command that prints the resource's number if it exists. Do not reach for `fj_curl POST` directly — its retry will happily create the thing twice. One `fj-issue-create` invocation produced three identical issues that way, the instance answering `HTTP 000` while applying every attempt; `fj-pr-create` escaped the same fate only because Forgejo rejects a duplicate PR for one head/base with a 4xx, which is not retried. The merge call takes the same shape by hand, since a merge has no resource to look up — one attempt, then confirm `merged` on the PR.

### Reading failing-job logs

`fj-job-logs` and `fj-job-logs-failing` use the web download endpoint (`/<repo>/actions/runs/<N>/jobs/<i>/attempt/<a>/logs`), which accepts an `Authorization: token` header, so you don't need to reproduce a CI failure locally just to read its log:

```bash
fj-job-logs-failing 581 --tail 200    # last 200 lines of every failing job
```

If a particular failing job needs deeper inspection, get the `target_url` from `fj-pr-failing` and pass it to `fj-job-logs` directly.

A REST route for the same thing exists as of Forgejo 16 — `GET /api/v1/repos/{owner}/{repo}/actions/jobs/{job_id}/logs`, plain text, token auth, no `attempt` to guess. The web endpoint is kept because a `target_url` already carries the run index it wants; reach for the REST one when you have a `job_id` from `fj-run-jobs`.

### Actions API: two identifiers, and no rerun

A run has **two** identifiers and they are not interchangeable:

- `index_in_repo` — the number in web URLs (`/actions/runs/3624`), which is what `target_url` and `fj-job-logs` use;
- `id` — what the REST API keys on (`13801`).

Passing the web number to the REST API returns `{"message":"resource does not exist"}`, which is indistinguishable from an unimplemented endpoint and has been misread as one. Resolve the id from a commit instead of guessing: `GET /actions/runs?head_sha=<sha>` (`fj_run_id` in `_fj-lib.sh`).

**There is no rerun in the REST API.** `/actions/runs/{run_id}/cancel` exists; rerun does not. The web UI's per-job rerun button posts to `/<owner>/<repo>/actions/runs/<index>/jobs/<job-index>/rerun`, which is **session-cookie only** — `Authorization: token`, `Bearer`, `?token=`, `?access_token=` and anonymous all return a bare `404 Not found.`, and Cloudflare adds a `cf_clearance` requirement on top. So re-triggering CI from a script still means pushing a new SHA; since PRs here squash-merge, an empty commit does the job and disappears on merge.

Before spending a full suite re-run on a job that looks stuck, check `fj-run-jobs`: `blocked` means its `needs` haven't finished, and a job showing `task=0` was never dispatched to a runner — which is also why rerunning it fails with `task with job_id … and attempt 0: resource does not exist`. An undispatched job is not necessarily dead, either: jobs orphaned by a runner outage were picked up and completed once the runners returned.

## Container Building

Build containers for local testing (single-arch, loaded locally). Targets live in `containers/bake.hcl` (`base`, `core`, `fuse`, `nfs`, `s3`, `webdav`, `docker`, `csi`, `containerd`, `cifs`, `omnibus`, `cli`):
```bash
PLATFORMS='linux/amd64' docker buildx bake -f containers/bake.hcl --load core fuse nfs cli
```

The `--load` flag is required to load images into the local Docker daemon. Without it, images are only pushed to the registry. Multi-platform builds don't support `--load`, so override PLATFORMS for local testing.

## Multi-Node Architecture

Core and interface packages run as separate Erlang nodes communicating via distribution:
- Core node: `neonfs_core@<host>` (storage, metadata, CLI handler, service registry)
- Interface nodes: `neonfs_fuse@<host>`, `neonfs_nfs@<host>`, `neonfs_s3@<host>`, etc. — each routes to core via neonfs_client
- CLI connects to the core node; core makes RPC calls to interface nodes (e.g. FUSE mount operations)
- Node authentication is **cluster-managed TLS**, not cookies: distribution runs over TLS from first boot (local CA generated by the daemon wrapper), and joining nodes redeem single-use invite tokens to receive cluster-signed certificates. The Erlang cookie is the constant, non-secret value `neonfs` on every node (#1136) — there is no cookie file, no cookie exchange in the join flow, and nothing for operators to configure or match by hand.

### Cluster Trust Model (full mutual trust)

**Every node holding a valid cluster certificate is fully trusted by every other node.** The cluster certificate is the *only* trust boundary; there is no intra-cluster authorisation. Concretely, any connected node can:

- fetch any chunk of any volume over the TLS data plane (`neonfs_client/lib/neon_fs/transport/handler.ex` does no per-volume check);
- invoke any core function via `NeonFS.Client.Router` (an unrestricted `:rpc.call`);
- obtain any volume's encryption keys (`NeonFS.Core.KeyManager.get_volume_key/2` has no caller check).

This is **deliberate, not a gap.** It is inherent to the Erlang distribution model: a connected node can already spawn arbitrary code on its peers, so callee-side gating (per-volume checks, RPC allow-lists, key-access controls) would be security theatre — trivially bypassed by a node that is, by definition, running attacker code. The mitigation lives at the boundary (who gets a certificate: single-use invite tokens, cluster-signed mTLS), not inside it.

Consequences operators and contributors must internalise:

- **Interface nodes are not sandboxed.** A compromised FUSE/NFS/S3/WebDAV/Docker/CSI node is equivalent to a compromised *cluster*, including all volume key material — not just the data that interface serves.
- **Deployment guidance:** interface nodes belong in the **same trust and network zone as core nodes**. Never expose the distribution port to, or run an interface node in, a lower-trust zone. Per-interface client-facing access control (S3 credentials, NFS AUTH_SYS/IP allow-lists, WebDAV auth) gates *external clients of that interface* — it does **not** constrain what the interface node itself can do inside the cluster.

Do not add intra-cluster authorisation checks expecting them to be a security control; they cannot be, and they imply a sandbox guarantee that does not exist.

### Listener Posture (interface defaults)

Interface listeners bind **`127.0.0.1` (loopback)** by default — private-by-default (#1225). The HTTP ones are still **plain HTTP** (unencrypted), so any deployment that widens the bind must also restrict/terminate TLS at the boundary:

| Interface | App env (bind / port)                          | Default              | Transport |
|-----------|------------------------------------------------|----------------------|-----------|
| S3        | `:neonfs_s3` `:s3_bind` / `:s3_port`           | `127.0.0.1` / `8080` | plain HTTP |
| WebDAV    | `:neonfs_webdav` `:webdav_bind` / `:webdav_port` | `127.0.0.1` / `8081` | plain HTTP |
| NFSv3     | `:neonfs_nfs` `:bind_address` / `:port`        | `127.0.0.1` / `2049` | TCP        |
| NLM       | `:neonfs_nfs` `:nlm_bind` / `:nlm_port`        | `127.0.0.1` / `4045` | TCP        |

Override the bind env (`NEONFS_S3_BIND`, `NEONFS_WEBDAV_BIND`, `NEONFS_NFS_BIND`, `NEONFS_NLM_BIND`) to `0.0.0.0` (or a specific address) for multi-host access. The shipped **container images set `0.0.0.0`** so published images serve externally out of the box (`containers/Containerfile.{s3,webdav,nfs,omnibus}`); the systemd package ships commented loopback defaults (`packaging/systemd/neonfs.conf`). When widening: front the HTTP interfaces (S3, WebDAV) with a TLS-terminating reverse proxy or confine them to a trusted network, and firewall the NFS/NLM ports. (Metrics endpoints `:*_metrics_bind` still default to `0.0.0.0` — observability scrape targets, gated by the metrics-enabled flag.)

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

## Rustler NIF Return Values

Rustler wraps Rust `Result<T, E>` types:
- `Result<(), E>` success → `{:ok, {}}` (not `:ok`)
- `Result<T, E>` success → `{:ok, value}`
- `Result<T, E>` error → `{:error, reason}`

Handle the `{:ok, {}}` case explicitly when expecting simple `:ok`.

## Phase Completion Requirements

**A phase is NOT complete until all components are fully integrated and tested together.**

Before declaring any implementation phase complete:

1. **Run the full test suite**: `resources/scripts/neonfs-each mix check --no-retry` (runs checks in all subprojects)
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
- Erlang nodes not connected (need explicit `Node.connect/1`, or the node lacks cluster TLS credentials — check the join completed)
- Service discovery failing (check `NeonFS.Client.Discovery.get_core_nodes/0` or `Node.list()`)
- RPC calls returning `{:badrpc, _}` or `{:error, :all_nodes_unreachable}` (nodes not reachable)
- Client infrastructure not ready (Connection, Discovery, CostFunction need time to probe after startup)
