# AGENTS.md

`neonfs` is the NeonFS product repo. The packages — client, core, every
interface, the CLI and the omnibus release — each live in their own repo under
[`project-neon`](https://harton.dev/project-neon), with their own CI, releases,
images and Debian packages. This repo holds what spans them:

| Path | What |
|---|---|
| `neonfs_integration/` | The cross-node integration suite: cluster formation, replication, partitions, failure recovery, and the CLI against a real cluster. |
| `packaging/` | `neonfs-common` (shared config, TLS helper, the `neonfs` user), and `build-debs.sh` for building every package locally from a workspace. |
| `deploy/prometheus/` | Alerting rules. |
| `docs/` | Operator, user and developer guides, and runbooks. |
| `test-rig/` | The QEMU test rig, its acceptance suite and the benchmark harness. |
| `containers/Containerfile.deb-builder` | The image `build-debs.sh` cross-compiles in. |
| `resources/release/` | git_ops for this repo's own releases. |

Issues and the [wiki](https://harton.dev/project-neon/neonfs/wiki) for all of
NeonFS live here — start with
[Specification](https://harton.dev/project-neon/neonfs/wiki/Specification).

**Read the [workspace `AGENTS.md`](https://harton.dev/project-neon/neonfs_workspace/src/branch/main/AGENTS.md)
first.** It holds the rules for every NeonFS repo — no whole-file buffering,
the cluster trust model, test synchronisation, how sibling dependencies are
pinned, how to make a change that spans repos, and releases — and the Forgejo
helper scripts.

## The workspace

This repo is meant to be cloned inside a
[`neonfs_workspace`](https://harton.dev/project-neon/neonfs_workspace)
checkout, beside every package repo (`bin/neonfs-sync` clones them). The
integration suite builds against those checkouts under `NEONFS_DEPS=local`,
and `build-debs.sh` and the test rig always build from them.

## The integration suite

`neonfs_integration` pins every sibling it uses by tag, with `override: true`
— the combination that ships. Its CLI tests drive the binary built from the
pinned `neonfs_cli` (in `deps/`, or the sibling checkout under
`NEONFS_DEPS=local`); `test/test_helper.exs` builds it.

```bash
cd neonfs_integration
mix test                               # the whole suite (several minutes)
mix test test/integration/cli_test.exs # one file
mix check --no-retry --except ex_unit  # the static tools
```

To test a change in a package before releasing it: `export NEONFS_DEPS=local`
in the workspace, `bin/neonfs-deps-local`, then run the suite.

**A change to a package is not done until the integration suite passes with
it.** Unit tests passing is necessary but not sufficient — CLI → core over
distribution, core ↔ interface nodes, and multi-node Ra coordination only meet
here. Common causes of failure: nodes not connected (or the join never
completed, so the node lacks cluster TLS credentials), service discovery not
yet populated (`NeonFS.Client.Discovery.get_core_nodes/0`), `{:badrpc, _}` or
`{:error, :all_nodes_unreachable}` from unreachable nodes, and client
infrastructure that has not finished probing after startup.

## CI

`ci.yml` runs the workspace's static checks, the suite's static tools once
(`neonfs_integration_checks`), and the suite itself in two shards.

**The integration suite is sharded, one shard per runner.** The shard job is a
matrix over `partition: [1, 2]` running `mix test --partitions 2`. Three things
must stay in step when changing the shard count: the literal `partition` list,
the `--partitions N` argument, and **the number of amd64 runners** — there are
two.

That last one is the constraint, not a guideline. A separate job is not a
separate host: the scheduler will happily place two shards on one runner, where
they share its disk and its ephemeral port range while each boots multi-node
peer clusters. Four shards on two runners did exactly that and exhausted both —
`No space left on device` under `/tmp/neonfs_mount_*/blobs`, then
`PeerCluster could not allocate an ephemeral peer port: :eaddrinuse`. Confirm
placement from the first line of each job's log, which names the runner
(`eivor`, `jeb`).

Mix distributes test *files* round-robin over their sorted names, so a shard's
contents shift whenever a file is added or renamed — balance is approximate by
design. Only the sharded job needs the privileged container, the loop devices
and the registry sidecar.

**End-to-end jobs run nightly, never per push, and are not required checks.**
`e2e-nightly.yml` (the rig acceptance matrix, the CSI driver on k3s, an SMB
round trip through a real `smbd`) and `bench.yml` (weekly) boot QEMU VMs or a
Samba sidecar. Both assemble a workspace (the workspace's
`ci/assemble-workspace`) — this repo at the commit under test beside every
package repo at its default branch — and build with `NEONFS_DEPS=local`, so
they test what merged, not what was last released. A failure there is a signal
to investigate rather than a merge blocker.

## Releasing

This repo's version is `neonfs-common`'s. From `resources/release`:

```bash
MIX_ENV=dev mix git_ops.release
git push --follow-tags
```

The tag publishes `neonfs-common` to the Debian repository and creates the
Forgejo release (`release.yml`). Release notes live in
[`CHANGELOG.md`](CHANGELOG.md).

## Building packages locally

From a workspace checkout, `packaging/build-debs.sh` builds and packages the
CLI, `neonfs-common` and every daemon from their own checkouts, using each
repo's own nfpm configs:

```bash
VERSION=0.6.1 packaging/build-debs.sh                        # native
VERSION=0.6.1 ARCH=arm64 packaging/build-debs.sh             # via Docker
VERSION=0.6.1 COMPONENTS=neonfs_omnibus NEONFS_DEPS=local packaging/build-debs.sh
```

## Git Commit Signing

The DevPod environment configures `devpod-ssh-signature` as the GPG signing
program, which does not support the `-U` flag newer git passes. Use
`git -c commit.gpgsign=false commit -m "..."`.
