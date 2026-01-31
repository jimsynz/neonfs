# Task 0016: Implement File Metadata Structure

## Status
Complete

## Phase
1 - Foundation

## Description
Define the Elixir-side file metadata structure that tracks logical files. Files map paths to ordered lists of chunk hashes. This includes POSIX attributes (mode, uid, gid, timestamps) and version tracking for copy-on-write semantics.

## Acceptance Criteria
- [x] `NeonFS.Core.FileMeta` struct with all fields from spec
- [x] `NeonFS.Core.FileIndex` GenServer managing file metadata
- [x] ETS tables for lookups by id and by path
- [x] `create/2` - create new file metadata
- [x] `get/1` - retrieve by file_id
- [x] `get_by_path/2` - retrieve by volume + path
- [x] `update/2` - update file metadata
- [x] `delete/1` - remove file (soft delete for versioning)
- [x] `list_dir/2` - list files in a directory path
- [x] Path parsing and validation
- [x] Version tracking (previous_version_id link)

## FileMeta Struct
```elixir
defmodule NeonFS.Core.FileMeta do
  defstruct [
    :id,                  # Unique file ID
    :volume_id,           # Volume this file belongs to
    :path,                # String, e.g., "/documents/report.pdf"
    :chunks,              # [hash_binary] for replicated volumes
    :stripes,             # nil | [%{stripe_id, byte_range}] for EC volumes
    :size,                # Total file size in bytes
    :mode,                # POSIX mode (e.g., 0o644)
    :uid,                 # Owner user ID
    :gid,                 # Owner group ID
    :created_at,          # DateTime
    :modified_at,         # DateTime
    :accessed_at,         # DateTime
    :version,             # Integer, increments on modification
    :previous_version_id  # nil | file_id of previous version
  ]
end
```

## Testing Strategy
- Unit tests:
  - CRUD operations
  - Path lookups
  - Directory listing
  - Version chain navigation
  - Invalid path handling

## Dependencies
- task_0015_elixir_chunk_metadata (for understanding the relationship)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/core/file_meta.ex` (new)
- `neonfs_core/lib/neon_fs/core/file_index.ex` (new)
- `neonfs_core/lib/neon_fs/core/application.ex` (add to supervision)
- `neonfs_core/test/neon_fs/core/file_index_test.exs` (new)

## Reference
- spec/data-model.md - Files (Elixir Layer)
- spec/data-model.md - File References for Erasure-Coded Volumes

## Notes
For Phase 1, only replicated volumes are supported (chunks field). Stripe references for erasure coding come in Phase 4. The struct includes the field for forward compatibility.
