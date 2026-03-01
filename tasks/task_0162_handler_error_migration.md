# Task 0162: Migrate CLI Handler to Structured Errors

## Status
Complete

## Phase
Gap Analysis — M-16 (2/4)

## Description
Migrate the CLI handler (`NeonFS.CLI.Handler`) to use `NeonFS.Error` structs
for all error returns. The handler is the primary user-facing boundary in
the Elixir codebase — converting errors here gives the CLI clear, consistent
error messages and categories.

This is an incremental migration: internal modules can continue returning
legacy error formats. The handler converts them at the boundary using
`NeonFS.Error.from_legacy/1` and returns structured errors to the CLI.

## Acceptance Criteria
- [ ] All handler functions return `{:error, %NeonFS.Error{}}` instead of `{:error, atom | string}`
- [ ] Volume not found errors use `NeonFS.Error.VolumeNotFound`
- [ ] File not found errors use `NeonFS.Error.FileNotFound`
- [ ] Permission denied errors use `NeonFS.Error.PermissionDenied`
- [ ] Invalid input errors use `NeonFS.Error.InvalidConfig` or `NeonFS.Error.InvalidPath`
- [ ] Cluster/quorum errors use `NeonFS.Error.QuorumUnavailable`
- [ ] All `with` blocks catch legacy `{:error, reason}` and convert via `from_legacy/1`
- [ ] Error messages are human-readable and include relevant context (volume name, path, etc.)
- [ ] Handler functions that call remote nodes wrap `:badrpc` in `NeonFS.Error.Unavailable`
- [ ] Backward compatibility: handler still accepts legacy errors from internal modules
- [ ] Unit test: each handler error path returns an appropriate error struct
- [ ] Unit test: from_legacy conversion works for all error types encountered in handler
- [ ] Existing handler tests updated to match new error format
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Update tests in `neonfs_core/test/neon_fs/cli/handler_test.exs`:
  - Verify error returns are `%NeonFS.Error{}` structs with correct class
  - Verify error messages contain relevant context
  - Verify backward compatibility with legacy error formats from mocked modules

## Dependencies
- Task 0161 (NeonFS.Error struct and error catalogue)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — convert all error returns)
- `neonfs_core/test/neon_fs/cli/handler_test.exs` (modify — update error assertions)

## Reference
- `spec/gap-analysis.md` — M-16
- Existing: `neonfs_core/lib/neon_fs/cli/handler.ex`
- Task 0161 (error struct definitions)

## Notes
The handler currently has ~30 functions that can return errors. The
migration pattern for each is:

```elixir
# Before
def handle_volume_show(name) do
  case VolumeRegistry.get_by_name(name) do
    {:ok, volume} -> {:ok, format_volume(volume)}
    {:error, :not_found} -> {:error, "Volume '#{name}' not found"}
  end
end

# After
def handle_volume_show(name) do
  case VolumeRegistry.get_by_name(name) do
    {:ok, volume} -> {:ok, format_volume(volume)}
    {:error, :not_found} -> {:error, NeonFS.Error.VolumeNotFound.exception(volume_name: name)}
    {:error, reason} -> {:error, NeonFS.Error.from_legacy({:error, reason})}
  end
end
```

The catch-all `{:error, reason}` clause in each `with` block ensures that
unexpected errors from internal modules are still converted to structured
errors rather than leaking raw atoms/strings to the CLI.

This is a large but mechanical migration. Work through handler functions
alphabetically, converting error returns one by one.
