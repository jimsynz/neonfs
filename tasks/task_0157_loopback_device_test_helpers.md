# Task 0157: Loopback Device Test Helpers

## Status
Complete

## Phase
Gap Analysis — M-13 (1/2)

## Description
Create test helper modules for creating, mounting, and tearing down loopback
block devices. These helpers allow integration tests to exercise real
space-related behaviours (ENOSPC, capacity thresholds) using small real
filesystems rather than mocked blob stores.

Loopback devices use `dd` to create a file, `losetup` to bind it as a block
device, and `mkfs.ext4` to format it. The resulting mount point behaves
exactly like a real drive, including returning ENOSPC when full.

These operations require root privileges. Tests using loopback devices
should be tagged and skipped when not running as root.

## Acceptance Criteria
- [ ] `NeonFS.Integration.LoopbackDevice` module created in test support
- [ ] `create/1` creates a loopback device of specified size (e.g. `create(size_mb: 50)`)
- [ ] Returns `{:ok, %{path: mount_path, loop_device: "/dev/loopN", image_file: path}}`
- [ ] `destroy/1` unmounts, detaches loopback device, removes image file
- [ ] Cleanup is idempotent (safe to call multiple times)
- [ ] `available?/0` checks whether loopback device creation is possible (root + losetup present)
- [ ] Helper sets up ext4 filesystem on the loopback device
- [ ] Mount point is in a temporary directory unique to each test
- [ ] `ExUnit.Callbacks.on_exit/1` integration for automatic cleanup on test failure
- [ ] `@tag :loopback` module attribute for skipping when `available?/0` returns false
- [ ] Unit test: create and destroy a 10 MB loopback device (requires root)
- [ ] Unit test: write to loopback mount point, verify data persists
- [ ] Unit test: verify cleanup removes all artifacts
- [ ] `mix format` passes
- [ ] `mix credo --strict` passes

## Testing Strategy
- Tests in `neonfs_integration/test/integration/loopback_device_test.exs`:
  - Tagged with `@tag :loopback` and `@tag :requires_root`
  - Create a small (10 MB) loopback device
  - Write a file to the mount point
  - Read it back, verify contents
  - Destroy the device, verify cleanup
- Tests automatically skipped in CI unless running as root

## Dependencies
- None

## Files to Create/Modify
- `neonfs_integration/test/support/loopback_device.ex` (create — helper module)
- `neonfs_integration/test/integration/loopback_device_test.exs` (create — tests)

## Reference
- `spec/testing.md` — failure injection
- `spec/gap-analysis.md` — M-13
- Linux loopback device documentation

## Notes
The implementation uses system commands via `System.cmd/3`:

```elixir
# Create image file
System.cmd("dd", ["if=/dev/zero", "of=#{image_path}", "bs=1M", "count=#{size_mb}"])

# Set up loopback device
{output, 0} = System.cmd("losetup", ["--find", "--show", image_path])
loop_device = String.trim(output)

# Format
System.cmd("mkfs.ext4", ["-q", loop_device])

# Mount
File.mkdir_p!(mount_path)
System.cmd("mount", [loop_device, mount_path])
```

For cleanup:
```elixir
System.cmd("umount", [mount_path], stderr_to_stdout: true)
System.cmd("losetup", ["-d", loop_device], stderr_to_stdout: true)
File.rm(image_path)
File.rmdir(mount_path)
```

The `on_exit` callback ensures cleanup even if the test crashes:
```elixir
setup do
  {:ok, device} = LoopbackDevice.create(size_mb: 50)
  on_exit(fn -> LoopbackDevice.destroy(device) end)
  {:ok, device: device}
end
```

These helpers require root privileges. In CI, either run the integration
test container as root or skip these tests. The `available?/0` check makes
this transparent.
