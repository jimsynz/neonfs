# Task 0065: CLI and Volume Creation for Erasure Coding

## Status
Complete

## Phase
4 - Erasure Coding

## Description
Update the CLI handler and volume creation flow to support erasure-coded volumes. Users should be able to create volumes with `--durability erasure:D:P` (e.g. `erasure:10:4` for 10 data chunks + 4 parity chunks). Volume status display should show erasure coding configuration including data/parity counts and storage overhead ratio.

## Acceptance Criteria
- [ ] CLI handler `handle_create_volume/1` parses `durability: "erasure:D:P"` format into `%{type: :erasure, data_chunks: D, parity_chunks: P}`
- [ ] CLI handler `handle_create_volume/1` still accepts `durability: "replicate:N"` format (existing behaviour unchanged)
- [ ] Validation rejects invalid erasure configs: non-integer D/P, D < 1, P < 1, malformed string
- [ ] VolumeRegistry `create_volume/1` accepts erasure durability config and stores it correctly
- [ ] `handle_volume_status/1` displays erasure config: data chunks, parity chunks, total chunks, overhead ratio (e.g. "1.4x" for 10+4)
- [ ] `handle_volume_list/0` indicates durability type in listing (e.g. "replicate:3" or "erasure:10+4")
- [ ] Error messages are clear for invalid durability strings (e.g. "Invalid durability format. Use 'replicate:N' or 'erasure:D:P'")
- [ ] Unit tests for durability string parsing (both formats)
- [ ] Unit tests for volume creation with erasure durability
- [ ] Unit tests for volume status display with erasure config
- [ ] All existing CLI handler tests still pass

## Testing Strategy
- Unit test: parse "erasure:10:4" → `%{type: :erasure, data_chunks: 10, parity_chunks: 4}`
- Unit test: parse "erasure:4:2" → `%{type: :erasure, data_chunks: 4, parity_chunks: 2}`
- Unit test: parse "replicate:3" → `%{type: :replicate, factor: 3, min_copies: 2}` (existing, unchanged)
- Unit test: parse "erasure:0:4" → error
- Unit test: parse "erasure:abc" → error
- Unit test: create volume with erasure durability, read back, verify config stored
- Unit test: volume status shows "erasure:10+4 (1.40x overhead)"
- Unit test: volume list shows durability type for mixed volumes
- Run full `mix test` in neonfs_core

## Dependencies
- task_0058 (Volume durability extension with erasure type)

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cli/handler.ex` (modify — parse erasure durability, update status/list display)
- `neonfs_core/lib/neon_fs/core/volume_registry.ex` (modify — ensure erasure durability stored/retrieved correctly)
- `neonfs_core/test/neon_fs/cli/handler_test.exs` (modify — add erasure durability tests)

## Reference
- spec/implementation.md — Volume creation CLI examples
- spec/replication.md — Erasure coding configuration table (4+2, 10+4, 8+3)
- spec/deployment.md — CLI architecture

## Notes
The overhead ratio is calculated as `(data_chunks + parity_chunks) / data_chunks`. For 10+4 this is 14/10 = 1.4x. For 4+2 this is 6/4 = 1.5x. This should be formatted to 2 decimal places in the display. The CLI handler currently parses durability as a string from the Rust CLI tool — the format `"erasure:D:P"` was chosen to be consistent with the existing `"replicate:N"` format. The Rust CLI side (`neonfs-cli` crate) may also need minor updates to pass the new durability format, but that can be deferred if the CLI handler tests pass with direct Elixir calls.
