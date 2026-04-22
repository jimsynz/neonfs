# Namespace Coordinator Implementation Plan

- Date: 2026-04-22
- Scope: `neonfs_core/lib/neon_fs/core/namespace_coordinator/` (new module namespace)
- Issue: [#300](https://harton.dev/project-neon/neonfs/issues/300) — first slice of [#226](https://harton.dev/project-neon/neonfs/issues/226)

## Scope (what this PR delivers)

The Ra-backed `NamespaceCoordinator` GenServer plus the three first-wave primitives: `claim_path/2`, `claim_subtree/2`, `release/1`, and `list_claims/1`. Everything else in the #226 tracker is a separate sub-issue.

## State-machine shape

Follows the `AclManager` / `ServiceRegistry` / `VolumeRegistry` template. State lives in Ra; hot reads are served from an ETS mirror on each core node so `list_claims/1` and internal conflict checks don't round-trip through Ra.

```elixir
defmodule NeonFS.Core.NamespaceCoordinator.State do
  @type scope :: :exclusive | :shared
  @type claim_id :: binary()                     # UUID v7 for HLC-friendly ordering
  @type claim_kind :: :path | :subtree

  @type claim :: %{
          id: claim_id,
          path: String.t(),
          kind: claim_kind,
          scope: scope,
          holder: {node(), pid()},               # for :DOWN cleanup
          created_at: DateTime.t()
        }

  defstruct claims: %{},                         # claim_id => claim
            by_path: %{},                        # path    => MapSet.new(claim_id)
            by_holder: %{}                       # {node, pid} => MapSet.new(claim_id)
end
```

### Commands

```
{:claim_path,    path, scope, holder}    -> {:ok, claim_id} | {:error, :conflict}
{:claim_subtree, path, scope, holder}    -> {:ok, claim_id} | {:error, :conflict}
{:release,       claim_id}               -> :ok             # idempotent
{:release_holder, {node, pid}}           -> :ok             # on :DOWN
```

### Queries

```
{:list_claims, prefix} -> [claim, ...]                      # served from ETS mirror locally
```

Reads via `:ra.consistent_query/2` only when the caller specifically needs linearizability; almost every caller can use the local ETS mirror (strictly stale reads are acceptable for conflict *displays* — not for conflict *detection*, which happens inside the state machine itself).

## Conflict detection

Rule (for `claim_path(P, scope)`):

- Conflicts with any existing `claim_path` on `P` if either scope is `:exclusive`.
- Conflicts with any `claim_subtree(A, scope)` where `A` is `P` or an ancestor of `P`, if either scope is `:exclusive`.

Rule (for `claim_subtree(P, scope)`):

- Conflicts with any existing `claim_path` on any descendant of `P` (inclusive), if either scope is `:exclusive`.
- Conflicts with any existing `claim_subtree` on `P`, any ancestor of `P`, or any descendant of `P`, if either scope is `:exclusive`.

Implementation:

- Path claims are looked up via the `by_path` map — O(1).
- Ancestor subtree claims: walk the prefix segments of `P` (`/a/b/c` → check `/`, `/a`, `/a/b`, `/a/b/c`). Cheap for typical path depth.
- Descendant path/subtree claims: scan `by_path` / subtree-claim index keyed by path with a prefix match. For a shallow tree this is cheap; for a filesystem root claim it's O(N). **Open point: trie/radix tree for sub-O(N) prefix match if profiling shows it matters.** Leave as a map for MVP.

All shared-with-shared pairs pass. Only exclusive-involving pairs conflict.

## Lifetime tracking (`:DOWN` cleanup)

The GenServer wrapping the Ra machine monitors each holder pid once at `claim_path` / `claim_subtree` time (deduplicated — one monitor per `{node, pid}`, not one per claim). On `{:DOWN, _, :process, pid, _}` or `nodedown`, the GenServer sends `{:release_holder, {node, pid}}` to Ra.

`:DOWN` cleanup is best-effort — if the coordinator GenServer itself dies on the same node as the monitored pid, the cleanup is missed. Recovery: a `Process.whereis/1` check runs periodically (5 s default) against each distinct holder and issues `:release_holder` for any that no longer exist. Cheap because the holder set is tiny in practice.

## Supervision

Added to `NeonFS.Core.Supervisor`'s child list after Ra cluster init but before interface-adjacent children (FUSE handler, NFS handler, etc.). Specifically, same position as `NeonFS.Core.AclManager` — that's the closest analogue.

On core node startup:

1. Ra machine starts (either as cluster founder or joiner).
2. `NamespaceCoordinator` GenServer starts, subscribes to the Ra machine, and builds its ETS mirror from a consistent snapshot.
3. The `NamespaceCoordinator` starts monitoring the holder pids of any claims that survived the restart.

## Telemetry

Events emitted (for test synchronisation per the `CLAUDE.md` testing rule):

- `[:neonfs, :namespace_coordinator, :claim, :granted]`
- `[:neonfs, :namespace_coordinator, :claim, :rejected]`
- `[:neonfs, :namespace_coordinator, :claim, :released]`
- `[:neonfs, :namespace_coordinator, :holder_down]`

Metadata includes `path`, `scope`, `kind`, `holder`. Useful for both tests and a future operator dashboard of "what's holding the namespace right now".

## Test plan

1. **Unit tests** (`neonfs_core/test/neon_fs/core/namespace_coordinator_test.exs`):
   - Single-path exclusive claims: first succeeds, second gets `:conflict`.
   - Single-path shared claims: both succeed.
   - Subtree-path hierarchy: subtree on `/a` blocks path on `/a/b`; path on `/a/b` blocks subtree on `/a`.
   - Release: idempotent; re-claim after release succeeds.
   - `list_claims(prefix)` filters correctly.
2. **Property tests** (`StreamData`):
   - For random sequences of `claim_path` / `claim_subtree` / `release`, the reported conflict matches a reference implementation (sorted-list scan).
3. **`:DOWN` cleanup integration test** (`neonfs_integration/test/integration/namespace_coordinator_test.exs`):
   - Three-node peer cluster. Node A acquires a claim and dies. Node B sees the claim released within 1 s (monitor) to 6 s (periodic sweep worst case).
4. **Cross-node query test**: node A acquires; node B's `list_claims/1` sees it within the Ra apply latency (use telemetry `:claim_granted` to synchronise).

## Implementation sequence

Single PR, but internally staged so review is tractable:

1. State-machine module (`namespace_coordinator/state.ex` with pure functions for each command) + unit tests on the state machine alone — no Ra integration yet.
2. Ra wiring (`namespace_coordinator.ex` GenServer + Ra machine callbacks + ETS mirror).
3. Public API (`NeonFS.Core.NamespaceCoordinator.claim_path/2` etc.) + telemetry.
4. Supervision tree integration.
5. Integration test.

## Open points

1. **Trie/radix tree for large-fanout filesystems.** Maps work for MVP. If #306 (POSIX unlink-while-open) produces millions of simultaneous claims, switch to a trie. Leave as a follow-up; benchmark first.

2. **Persistent claim survival across leader failover.** Ra replicates state, so the claim survives. But the holder pid reference doesn't — it's a local `pid()` that has no meaning on the new leader's node. On leader failover, the new leader's GenServer should re-establish monitors against each `{node, pid}` (via `:erpc.call(node, Process, :alive?, [pid])`), cleaning up any that are dead. Covered by the periodic sweep already.

3. **Holder identity.** `{node, pid}` captures the current plan. If interface nodes ever hold claims via a pooled GenServer (rather than per-request processes), claim teardown might need a richer identity (e.g. `{node, pid, request_id}`). Out of scope for #300; revisit if a caller needs it.

## References

- Issue: [#300](https://harton.dev/project-neon/neonfs/issues/300)
- Parent: [#226](https://harton.dev/project-neon/neonfs/issues/226)
- Template: `NeonFS.Core.AclManager` — same Ra-state-machine + ETS-mirror pattern
- Sibling sub-issues that depend on this landing: #301, #302, #303, #304, #305, #306
