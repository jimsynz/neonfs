# Task 0149: FUSE setattr Integration Tests

## Status
Complete

## Phase
Gap Analysis — M-9

## Description
Add integration tests for `setattr` operations (`chmod`, `chown`,
`utimens`, `truncate`) to the FUSE handler integration test suite. These
operations have no integration test coverage despite being implemented in
the FUSE handler.

The existing `fuse_handler_test.exs` covers core CRUD operations (lookup,
getattr, create, read, write, readdir, unlink, rmdir, rename). This task
adds a `describe "setattr operations"` block covering all `setattr`
sub-operations including permission enforcement edge cases.

## Acceptance Criteria
- [ ] Test: `chmod` changes file mode and is reflected in subsequent `getattr`
- [ ] Test: `chmod` on directory changes mode correctly
- [ ] Test: `chown` changes UID/GID and is reflected in subsequent `getattr`
- [ ] Test: `truncate` to smaller size removes excess chunks and updates size in metadata
- [ ] Test: `truncate` to zero empties the file
- [ ] Test: `utimens` sets access and modification times correctly
- [ ] Test: `utimens` with `{sec, nsec}` tuple format works
- [ ] Test: non-owner UID cannot `chmod` (returns EACCES)
- [ ] Test: non-owner UID cannot `chown` (returns EACCES)
- [ ] Test: `setattr` with combined mode + timestamps in one call
- [ ] Test: `setattr` updates `changed_at` (ctime) field
- [ ] All tests follow existing test patterns (send `:fuse_op`, assert `:fuse_op_complete`)
- [ ] Existing tests still pass
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Integration tests in `neonfs_integration/test/integration/fuse_handler_test.exs` (extend):
  - Create files and directories using existing helpers
  - Send setattr operations via the handler message protocol
  - Verify results via `getattr` responses and direct RPC to core node's FileIndex
  - Use different UIDs to test permission enforcement

## Dependencies
- None (handler setattr operations already implemented)

## Files to Create/Modify
- `neonfs_integration/test/integration/fuse_handler_test.exs` (modify — add setattr test block)

## Reference
- `spec/testing.md` — integration test strategy
- `spec/gap-analysis.md` — M-9
- Existing: `neonfs_fuse/lib/neon_fs/fuse/handler.ex` (setattr handler at ~line 398)
- Existing: `neonfs_integration/test/integration/fuse_handler_test.exs` (test patterns)

## Notes
The setattr handler receives operations as:
```elixir
{:fuse_op, ref, {"setattr", %{inode: inode, attrs: attrs, to_set: flags}}}
```

Where `attrs` contains the new values and `to_set` is a bitmask/list
indicating which fields to set. Check the handler implementation for the
exact message format.

For permission tests, the handler checks `check_setattr_permission/3`
which compares the calling process's UID against the file owner. In
integration tests, this may require setting up the handler with a
specific UID context. Check how the existing tests handle UID — they may
use a default UID of 0 (root) which bypasses permission checks.
