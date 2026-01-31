# Task 0024: Implement CLI Handler Module in Elixir

## Status
Complete

## Phase
1 - Foundation

## Description
Implement the Elixir module that receives RPC calls from the CLI and dispatches them to the appropriate handlers. This is the daemon-side interface that the CLI connects to.

## Acceptance Criteria
- [x] `NeonFS.CLI.Handler` module with RPC-callable functions
- [x] `cluster_status/0` - return cluster status map
- [x] `list_volumes/0` - return list of volumes
- [x] `create_volume/2` - create volume with config
- [x] `delete_volume/1` - delete volume by name
- [x] `get_volume/1` - get volume details
- [x] `mount/3` - mount volume at path
- [x] `unmount/1` - unmount by mount_id or path
- [x] `list_mounts/0` - return active mounts
- [x] All functions return `{:ok, data}` or `{:error, reason}`
- [x] Data returned as maps (easy term conversion)

## Module Structure
```elixir
defmodule NeonFS.CLI.Handler do
  @moduledoc """
  RPC handler for CLI commands. Called via Erlang distribution.
  """

  def cluster_status do
    {:ok, %{
      name: get_cluster_name(),
      node: Node.self(),
      status: :running,
      volumes: VolumeRegistry.count(),
      uptime: get_uptime()
    }}
  end

  def list_volumes do
    {:ok, VolumeRegistry.list() |> Enum.map(&volume_to_map/1)}
  end

  def create_volume(name, config) do
    case VolumeRegistry.create(name, config) do
      {:ok, volume} -> {:ok, volume_to_map(volume)}
      {:error, reason} -> {:error, reason}
    end
  end

  # ... more functions
end
```

## Testing Strategy
- Unit tests:
  - Each handler function returns expected format
  - Error cases return proper tuples
  - Data is serialisable (no PIDs, references in output)

## Dependencies
- task_0017_elixir_volume_config
- task_0021_elixir_fuse_mount_manager

## Files to Create/Modify
- `neonfs_core/lib/neon_fs/cli/handler.ex` (new)
- `neonfs_core/test/neon_fs/cli/handler_test.exs` (new)

## Reference
- spec/deployment.md - Daemon-side RPC interface
- spec/deployment.md - Response format

## Notes
This module must be careful not to return process-specific data (PIDs, refs) that can't be serialised across distribution. All returns should be plain maps, lists, and atoms.
