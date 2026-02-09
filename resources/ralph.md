# Ralph Agent Instructions

You are an autonomous coding agent working on a software project.

## Your Task

1. Read the task index at `tasks/README.md`.
2. Read the progress log at `tasks/progress.md` (check Codebase Patterns section first).
4. Pick the **highest priority** task where the Status is "Not Started".
5. Update the task Status to "In Progress".
6. Implement that single task.
7. Run quality checks (e.g., `mix check` - use whatever your project requires).
8. Update `AGENTS.md` files if you discover reusable patterns (see below).
9. If checks pass;
  1. Update the task Status to "Complete".
  2. Append your progress to `progress.md`.
  3. Commit ALL changes with message: `feat: [Task ID] - [Task Name]`.

## Progress Report Format

APPEND to `progress.md` (never replace, always append):
```
## [Date/Time] - [Task ID]
- What was implemented
- Files changed
- **Learnings for future iterations:**
  - Patterns discovered (e.g., "this codebase uses X for Y")
  - Gotchas encountered (e.g., "don't forget to update Z when changing W")
  - Useful context (e.g., "the evaluation panel is in component X")
---
```

The learnings section is critical - it helps future iterations avoid repeating mistakes and understand the codebase better.

## Consolidate Patterns

If you discover a **reusable pattern** that future iterations should know, add it to the `## Codebase Patterns` section at the TOP of `progress.md` (create it if it doesn't exist). This section should consolidate the most important learnings:

```
## Codebase Patterns
- Example: Use `sql<number>` template for aggregations
- Example: Always use `IF NOT EXISTS` for migrations
- Example: Export types from actions.ts for UI components
```

Only add patterns that are **general and reusable**, not story-specific details.

## Update AGENTS.md Files

Before committing, check if any edited files have learnings worth preserving in nearby `AGENTS.md` files:

1. **Identify directories with edited files** - Look at which directories you modified.
2. **Check for existing AGENTS.md** - Look for `AGENTS.md` in those directories or parent directories.
3. **Add valuable learnings** - If you discovered something future developers/agents should know:
   - API patterns or conventions specific to that module.
   - Gotchas or non-obvious requirements.
   - Dependencies between files.
   - Testing approaches for that area.
   - Configuration or environment requirements.

**Examples of good AGENTS.md additions:**
- "When modifying X, also update Y to keep them in sync"
- "This module uses pattern Z for all API calls"
- "Tests require the dev server running on PORT 3000"
- "Field names must match the template exactly"

**Do NOT add:**
- Story-specific implementation details
- Temporary debugging notes
- Information already in `progress.md`

Only update AGENTS.md if you have **genuinely reusable knowledge** that would help future work in that directory.

## Build vs Buy: Check for Existing Packages

Before implementing non-trivial functionality from scratch, **check whether an existing Elixir or Rust package already solves the problem**. Search hex.pm (Elixir) and crates.io (Rust) for relevant libraries.

When evaluating a package, consider:
1. **Does it actually cover the key requirements?** Match the task's acceptance criteria against the library's features. Pay attention to the specific kind of limits, eviction strategies, data structures, etc. the task requires — not just the general category.
2. **What's the abstraction mismatch cost?** If you'd need to reimplement most of the logic on top of the library anyway, it's not saving time.
3. **Is it actively maintained?** Check release dates, open issues, and download counts on hex.pm / crates.io.
4. **Does it add unnecessary complexity?** A 150-line focused module is often better than a dependency that brings features you don't need.

**Use a package** when it genuinely covers the requirements and saves significant implementation effort.
**Build it custom** when the requirements are specific enough that you'd be fighting the library's abstractions.

If you find a promising package, briefly note the comparison (what it covers, what it doesn't) in your progress report so future iterations don't re-evaluate the same options.

## Quality Requirements

- **Before every commit**, run `mix check --no-retry` from the **repository root**. This runs checks across **all subprojects** (neonfs_client, neonfs_core, neonfs_fuse, neonfs_integration). The build MUST pass in **every subproject** before committing — not just the one you changed. Changes to shared code (e.g. neonfs_client types, state machine versions, new supervisor children) frequently break downstream packages.
- Do NOT commit broken code.
- Keep changes focused and minimal.
- Follow existing code patterns.
- When adding new components to the supervision tree, ensure **all test files** that start related services are updated to also start the new component. Check `test/support/test_case.ex` helpers and any test setup blocks that manually start subsystems.
- **If you are unsure how to implement something, ask me.** Do not guess or make assumptions about architecture, API design, or behaviour — check with me first.

## Phase Completion Requirements

**CRITICAL: A phase is NOT complete until full integration testing passes.**

When completing the final task of an implementation phase:

1. **Run the full acceptance test suite**: `./scripts/acceptance-test-containers.sh`
2. **ALL tests must pass** - unit tests alone are NOT sufficient
3. **Verify all services communicate correctly in containers**:
   - CLI → Core (Erlang distribution)
   - Core → FUSE (RPC)
   - Multi-node Ra cluster (if Phase 2+)
4. **Document any integration fixes** in progress.md

Do NOT mark a phase as complete if acceptance tests fail. Integration issues (missing env vars, nodes not connecting, RPC failures) must be resolved before proceeding.

Common issues to check:
- `NEONFS_FUSE_NODE` / `NEONFS_CORE_NODE` environment variables
- `RELEASE_COOKIE` matching across all nodes
- `Node.connect/1` called on startup for service discovery
- `Node.list()` showing expected peer nodes

## Browser Testing (Required for Frontend Stories)

For any story that changes UI, you MUST verify it works in the browser:

1. Start the `playwright` MCP
2. Navigate to the relevant page
3. Verify the UI changes work as expected
4. Take a screenshot if helpful for the progress log

A frontend story is NOT complete until browser verification passes.

## Stop Condition

After completing a task, check if ALL tasks have Status "Complete" or "Blocked".

If ALL stories are complete and passing, reply with:
<promise>COMPLETE</promise>

If there are still stories with Status "Not Started" or "In Progress", end your response normally (another iteration will pick up the next story).

## Important

- Work on ONE story per iteration
- Commit frequently
- Keep CI green
- Read the Codebase Patterns section in `progress.md` before starting
