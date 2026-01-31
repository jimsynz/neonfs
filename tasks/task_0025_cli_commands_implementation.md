# Task 0025: Implement CLI Commands

## Status
Complete

## Phase
1 - Foundation

## Description
Implement the actual CLI commands that use the daemon connection to perform operations. Each command calls the appropriate RPC function and formats the output.

## Acceptance Criteria
- [x] `neonfs cluster status` - show cluster status
- [x] `neonfs volume list` - list all volumes
- [x] `neonfs volume create <name> [options]` - create volume
- [x] `neonfs volume delete <name>` - delete volume
- [x] `neonfs volume info <name>` - show volume details
- [x] `neonfs mount <volume> <path>` - mount volume
- [x] `neonfs unmount <path>` - unmount
- [x] `neonfs mount list` - list active mounts
- [x] Table output by default
- [x] JSON output with `--json` flag
- [x] Proper exit codes (0 success, 1 error)
- [x] Error messages to stderr

## Command Examples
```bash
$ neonfs cluster status
Cluster: home-lab
Node: neonfs@localhost
Status: running
Volumes: 3
Uptime: 2d 5h 30m

$ neonfs volume list
NAME        SIZE      CHUNKS  DURABILITY
documents   1.2 GB    4521    replicate:3
media       50.3 GB   12043   replicate:2
scratch     256 MB    102     replicate:1

$ neonfs volume list --json
[{"name":"documents","size":1288490188,...},...]
```

## Testing Strategy
- Integration tests (require daemon):
  - Run each command, verify output format
  - Verify JSON output is valid JSON
  - Verify error exit codes
- Mock tests:
  - Test output formatting with mock data

## Dependencies
- task_0023_cli_daemon_connection
- task_0024_cli_handler_module

## Files to Create/Modify
- `neonfs-cli/src/commands/cluster.rs` (implement)
- `neonfs-cli/src/commands/volume.rs` (implement)
- `neonfs-cli/src/commands/mount.rs` (implement)
- `neonfs-cli/src/term/mod.rs` (new - term conversion)
- `neonfs-cli/src/term/types.rs` (new - response types)

## Reference
- spec/deployment.md - CLI Commands section
- spec/deployment.md - Error Handling

## Notes
Term conversion from Erlang terms to Rust types is needed. Create `FromTerm` trait implementations for the response structures.
