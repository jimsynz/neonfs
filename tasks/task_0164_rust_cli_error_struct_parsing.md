# Task 0164: Rust CLI Error Struct Parsing

## Status
Complete

## Phase
Gap Analysis — M-16 (4/4)

## Description
Update the Rust CLI's error extraction logic to parse `NeonFS.Error` structs
returned from the Elixir handler. Currently the CLI pattern-matches on
`{:error, atom}` and `{:error, binary_string}` — after the handler migration
(task 0162), errors arrive as `{:error, %NeonFS.Error{}}` structs encoded
as Erlang terms.

The CLI should extract the human-readable message from the error struct and
optionally use the error class for formatting (e.g. colour-coding, exit
codes).

## Acceptance Criteria
- [x] `extract_error/1` handles `NeonFS.Error` struct format (Erlang map with `__struct__` key)
- [x] Extracts `message` field from the error struct for display
- [x] Extracts `class` field (`:invalid`, `:not_found`, `:forbidden`, `:unavailable`, `:internal`)
- [x] `:invalid` and `:not_found` errors display as user errors (no "internal error" prefix)
- [x] `:forbidden` errors display with "Permission denied: " prefix
- [x] `:unavailable` errors display with "Cluster unavailable: " prefix
- [x] `:internal` errors display with "Internal error: " prefix
- [x] Backward compatibility: legacy `{:error, atom}` and `{:error, string}` formats still work
- [x] Exit code varies by error class: 1 for user errors, 2 for permission, 3 for unavailable, 4 for internal
- [x] `details` map from error struct is available for verbose mode (future)
- [x] Unit test: parse error struct with each class
- [x] Unit test: legacy error formats still work
- [x] Unit test: exit codes match error classes
- [x] `cargo clippy --all-targets -- -D warnings` passes
- [x] `cargo fmt --check` passes

## Testing Strategy
- Rust unit tests in the error extraction module:
  - Construct eetf terms representing each error struct type
  - Verify message extraction
  - Verify class detection
  - Verify exit code mapping
  - Test legacy format backward compatibility

## Dependencies
- Task 0162 (Handler error migration — defines what the CLI receives)

## Files to Create/Modify
- `neonfs-cli/src/error.rs` or equivalent error handling module (modify — add struct parsing)
- `neonfs-cli/src/main.rs` (modify — use class-based exit codes)

## Reference
- `spec/gap-analysis.md` — M-16
- Existing: `neonfs-cli/src/` (error extraction logic — find with `extract_error`)
- Task 0161 (error struct format)
- Task 0162 (handler returns these structs)

## Notes
Erlang term encoding of an Elixir struct is a map with a `__struct__` key:

```erlang
%{
  __struct__: NeonFS.Error.VolumeNotFound,
  message: "Volume 'mydata' not found",
  class: :not_found,
  details: %{volume_name: "mydata"},
  ...
}
```

In `eetf` (Rust Erlang term library), this is an `eetf::Map` with
`eetf::Atom` keys. The extraction logic should:
1. Check if the error term is a map with `__struct__` key
2. If so, extract `message` (binary) and `class` (atom)
3. If not, fall back to legacy extraction (atom → `to_string`, binary → as-is)

The backward compatibility is important during the migration period — not
all handler functions will be migrated simultaneously, so both old and new
error formats need to work.
