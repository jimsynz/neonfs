# Task 0161: NeonFS.Error Struct and Error Catalogue

## Status
Complete

## Phase
Gap Analysis — M-16 (1/4)

## Description
Create a structured error system using the `splode` hex package. Define a
`NeonFS.Error` base module with error classes and create error modules for
the major error categories used across the codebase.

Currently errors are ad-hoc atoms, strings, and tagged tuples with no
consistent structure. This task establishes the foundation — later tasks
will migrate existing error sites to use these structured errors.

The error hierarchy should cover the categories observed in the codebase:
- `:invalid` — bad input, validation failures
- `:not_found` — volume, file, chunk, or key not found
- `:forbidden` — ACL/permission denied
- `:unavailable` — quorum loss, all nodes unreachable
- `:internal` — unexpected failures, NIF errors

## Acceptance Criteria
- [x] `splode` added as a dependency to `neonfs_client/mix.exs` (shared error types)
- [x] `NeonFS.Error` module created using `use Splode, error_classes: [...]`
- [x] Error class `:invalid` with module `NeonFS.Error.Invalid`
- [x] Error class `:not_found` with module `NeonFS.Error.NotFound`
- [x] Error class `:forbidden` with module `NeonFS.Error.Forbidden`
- [x] Error class `:unavailable` with module `NeonFS.Error.Unavailable`
- [x] Error class `:internal` with module `NeonFS.Error.Internal`
- [x] Each error class module supports `message` field (human-readable description)
- [x] Each error class module supports `details` field (map with context: volume_id, path, node, etc.)
- [x] `NeonFS.Error.to_string/1` renders a human-readable error message
- [x] Error structs implement `String.Chars` protocol (for interpolation in logs)
- [x] Specific error modules for common cases:
  - `NeonFS.Error.VolumeNotFound`
  - `NeonFS.Error.FileNotFound`
  - `NeonFS.Error.ChunkNotFound`
  - `NeonFS.Error.QuorumUnavailable`
  - `NeonFS.Error.PermissionDenied`
  - `NeonFS.Error.InvalidPath`
  - `NeonFS.Error.InvalidConfig`
- [x] Unit test: each error class can be created and rendered
- [x] Unit test: `to_string/1` produces readable output
- [x] Unit test: error structs work in `{:error, struct}` tuples
- [x] `mix format` passes
- [x] `mix credo --strict` passes

## Testing Strategy
- Unit tests in `neonfs_client/test/neon_fs/error_test.exs`:
  - Create each error class, verify fields
  - Test `to_string/1` output
  - Test String.Chars protocol implementation

## Dependencies
- None

## Files to Create/Modify
- `neonfs_client/mix.exs` (modify — add splode dependency)
- `neonfs_client/lib/neon_fs/error.ex` (create — base error module)
- `neonfs_client/lib/neon_fs/error/invalid.ex` (create — invalid error class)
- `neonfs_client/lib/neon_fs/error/not_found.ex` (create — not found error class)
- `neonfs_client/lib/neon_fs/error/forbidden.ex` (create — forbidden error class)
- `neonfs_client/lib/neon_fs/error/unavailable.ex` (create — unavailable error class)
- `neonfs_client/lib/neon_fs/error/internal.ex` (create — internal error class)
- `neonfs_client/test/neon_fs/error_test.exs` (create — tests)

## Reference
- `spec/gap-analysis.md` — M-16
- https://hex2txt.fly.dev/splode/llms.txt (package documentation)
- Error patterns observed in codebase: `{:error, :atom}`, `{:error, "string"}`,
  `{:error, {:tag, reason}}`

## Notes
The error modules live in `neonfs_client` because they're shared types used
by all packages (same rationale as Volume and FileMeta living in
neonfs_client). This means `splode` is a runtime dependency of neonfs_client.

Error structs should be raised/returned from their sources rather than
converted at boundaries. Migration tasks (0162, 0163) will update call sites
to return structured errors directly.

Keep the specific error modules (VolumeNotFound, etc.) simple — they should
`use` the appropriate error class and add domain-specific fields:

```elixir
defmodule NeonFS.Error.VolumeNotFound do
  use Splode.Error, fields: [:volume_name, :volume_id], class: :not_found

  def message(%{volume_name: name}) when not is_nil(name) do
    "Volume '#{name}' not found"
  end

  def message(%{volume_id: id}) when not is_nil(id) do
    "Volume with ID '#{id}' not found"
  end
end
```

Check the `splode` documentation for the exact API — the `use Splode.Error`
macro and `error_classes` configuration may differ from this sketch.
