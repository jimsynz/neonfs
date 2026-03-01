# Task 0144: Convert Logger Calls to Structured Format

## Status
Complete

## Phase
Gap Analysis — M-7 (3/3)

## Description
Convert existing `Logger` calls across the codebase from string
interpolation to structured metadata format. This is the final step in
the structured logging effort — tasks 0142 and 0143 set up the formatter
and entry-point metadata; this task converts individual log messages.

The conversion is mechanical: replace string interpolation with keyword
metadata. For example:

```elixir
# Before
Logger.info("GCScheduler triggered GC job #{job.id}")

# After
Logger.info("GC job triggered", job_id: job.id)
```

The message becomes a static string (no interpolation), and dynamic values
move into the metadata keyword list. This allows the JSON formatter to
emit them as structured fields rather than embedded in a string.

## Acceptance Criteria
- [ ] All `Logger.*` calls in `neonfs_core/lib/` use structured metadata (no string interpolation in message)
- [ ] All `Logger.*` calls in `neonfs_fuse/lib/` use structured metadata
- [ ] All `Logger.*` calls in `neonfs_client/lib/` use structured metadata
- [ ] Static log messages are descriptive without the metadata (readable in isolation)
- [ ] Metadata keys use snake_case atoms (e.g. `volume_id:`, `chunk_hash:`, `node:`)
- [ ] `inspect/1` calls on complex terms are preserved in metadata values (not in message strings)
- [ ] No functional changes — only log formatting changes
- [ ] Existing tests still pass
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Run `mix test` in all packages to verify no functional regressions
- Grep for `Logger\.(info|warning|error|debug)\(.*#\{` to verify no string interpolation remains
- Spot-check a few converted calls to verify metadata is correctly structured

## Dependencies
- Task 0142 (JSON log formatter setup)
- Task 0143 (Logger metadata at entry points)

## Files to Create/Modify
- All files in `neonfs_core/lib/` containing `Logger.*` calls (~30 files, ~150 calls)
- All files in `neonfs_fuse/lib/` containing `Logger.*` calls (~5 files, ~15 calls)
- All files in `neonfs_client/lib/` containing `Logger.*` calls (~5 files, ~15 calls)

## Reference
- `spec/operations.md` lines 200–218
- `spec/gap-analysis.md` — M-7
- Elixir Logger documentation on metadata

## Notes
This is a large but mechanical task. Work through files alphabetically
within each package. The key rules:

1. **Message is a static string** — no `#{}` interpolation
2. **Dynamic values go in metadata** — `Logger.info("msg", key: value)`
3. **Keep messages short** — the metadata carries the detail
4. **Don't add new log calls** — only convert existing ones
5. **Preserve log levels** — don't change info to debug, etc.

Some `Logger.warning` and `Logger.error` calls include `inspect(reason)` —
move the reason into metadata: `Logger.error("operation failed", reason: inspect(reason))`.

For calls using multi-line strings or `"""` heredocs, convert to a single
static string with metadata.

Estimated scope: ~180 Logger calls across ~40 files. This is the largest
single task in the M-7 chain but is entirely mechanical.
