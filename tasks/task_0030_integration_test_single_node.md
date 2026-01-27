# Task 0030: Phase 1 Integration Test - Single Node

## Status
Not Started

## Phase
1 - Foundation

## Description
Create an end-to-end integration test that validates the Phase 1 milestone: "Mount a directory, read/write files, data persists across restarts, CLI can query status."

## Acceptance Criteria
- [ ] Integration test module `NeonFS.Integration.Phase1Test`
- [ ] Start NeonFS in test mode
- [ ] Create a test volume via API
- [ ] Mount volume to temp directory
- [ ] Write file via mounted filesystem
- [ ] Read file back, verify content matches
- [ ] Verify file appears in directory listing
- [ ] Query volume status via CLI handler
- [ ] Stop NeonFS
- [ ] Restart NeonFS
- [ ] Re-mount volume
- [ ] Read file, verify data persisted
- [ ] Cleanup test artifacts

## Test Scenario
```elixir
defmodule NeonFS.Integration.Phase1Test do
  use ExUnit.Case, async: false

  @moduletag :integration
  @moduletag timeout: 120_000

  setup do
    # Start NeonFS with test configuration
    {:ok, _} = Application.ensure_all_started(:neonfs_fuse)

    temp_dir = create_temp_dir()
    mount_point = Path.join(temp_dir, "mount")
    File.mkdir_p!(mount_point)

    on_exit(fn ->
      cleanup(temp_dir)
    end)

    %{temp_dir: temp_dir, mount_point: mount_point}
  end

  test "write and read file persists across restart", ctx do
    # Create volume
    {:ok, _} = NeonFS.CLI.Handler.create_volume("test", %{})

    # Mount
    {:ok, _} = NeonFS.CLI.Handler.mount("test", ctx.mount_point, %{})

    # Write file
    test_data = :crypto.strong_rand_bytes(1024 * 100)  # 100KB
    file_path = Path.join(ctx.mount_point, "test.bin")
    File.write!(file_path, test_data)

    # Read and verify
    assert File.read!(file_path) == test_data

    # Check status
    {:ok, status} = NeonFS.CLI.Handler.cluster_status()
    assert status.volumes == 1

    # Unmount
    :ok = NeonFS.CLI.Handler.unmount(ctx.mount_point)

    # Restart (simulate by stopping and starting applications)
    :ok = Application.stop(:neonfs_fuse)
    :ok = Application.stop(:neonfs_core)
    {:ok, _} = Application.ensure_all_started(:neonfs_fuse)

    # Re-mount
    {:ok, _} = NeonFS.CLI.Handler.mount("test", ctx.mount_point, %{})

    # Verify data persisted
    assert File.read!(file_path) == test_data
  end
end
```

## Testing Strategy
- Run with `mix test --only integration`
- Requires FUSE permissions (may need root or fuse group)
- Uses temp directories for isolation
- Cleanup on test exit

## Dependencies
- All Phase 1 tasks complete

## Files to Create
- `test/integration/phase1_test.exs`
- `test/support/integration_helpers.ex`

## Reference
- spec/implementation.md - Phase 1 Milestone
- spec/testing.md - Integration Testing

## Notes
This test validates the complete Phase 1 deliverable. It should be the final task before Phase 1 is considered complete. May need to run as root for FUSE access.
