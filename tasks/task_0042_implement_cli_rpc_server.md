# Task 0042: Implement CLI RPC Server and Fix CLI Integration

## Status

Complete

## Phase

1 (Foundation Addendum - must complete before Phase 2)

## Description

The CLI cannot connect to the daemon because there is no TCP RPC server listening. The Rust CLI expects to connect via TCP and send ETF-encoded RPC requests, but the Elixir side has no server to accept these connections.

Additionally, there are several bugs and stub implementations in the CLI that need fixing.

## Acceptance Criteria

### RPC Server (Critical - Blocks All CLI)

- [ ] New `NeonFS.Core.RpcServer` GenServer module
- [ ] Listens on configurable TCP port (default: EPMD port + 1, or explicit config)
- [ ] Accepts connections and reads ETF-encoded requests
- [ ] Request format: `{:rpc, cookie, module, function, args}`
- [ ] Validates cookie matches `RELEASE_COOKIE`
- [ ] Dispatches to `NeonFS.CLI.Handler` functions
- [ ] Returns ETF-encoded `{:ok, result}` or `{:error, reason}`
- [ ] Added to supervision tree in `NeonFS.Core.Supervisor`
- [ ] Writes port to `/run/neonfs/rpc_port` for CLI discovery

### CLI Bug Fixes

- [ ] Fix mount command: send 3 arguments (volume, mountpoint, options map)
- [ ] Fix response field names to match handler output:
  - `uptime` not `uptime_seconds`
  - `id` not `mount_id`
- [ ] Implement `cluster init` properly (currently placeholder)
- [ ] Implement `cluster join` properly (currently placeholder)
- [ ] Implement `node status` with real data (currently hardcoded)
- [ ] Implement `node list` with real data (currently hardcoded)

### Integration Verification

- [ ] CLI can connect to running daemon
- [ ] `neonfs cluster status` returns real cluster info
- [ ] `neonfs volume list` returns volumes from registry
- [ ] `neonfs volume create test-vol` creates a volume
- [ ] `neonfs volume info test-vol` shows volume details
- [ ] `neonfs mount test-vol /mnt/test` succeeds (if FUSE available)

## Implementation Notes

### RPC Server Architecture

```elixir
defmodule NeonFS.Core.RpcServer do
  use GenServer
  require Logger

  @default_port 4370  # EPMD is 4369, we use 4370

  def start_link(opts) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  def init(opts) do
    port = Keyword.get(opts, :port, @default_port)
    {:ok, listen_socket} = :gen_tcp.listen(port, [
      :binary,
      packet: 4,  # 4-byte length prefix
      active: false,
      reuseaddr: true
    ])

    # Write port for CLI discovery
    File.write!("/run/neonfs/rpc_port", to_string(port))

    # Start acceptor
    spawn_link(fn -> accept_loop(listen_socket) end)

    {:ok, %{socket: listen_socket, port: port}}
  end

  defp accept_loop(listen_socket) do
    {:ok, client} = :gen_tcp.accept(listen_socket)
    spawn(fn -> handle_client(client) end)
    accept_loop(listen_socket)
  end

  defp handle_client(socket) do
    case :gen_tcp.recv(socket, 0) do
      {:ok, data} ->
        response = process_request(data)
        :gen_tcp.send(socket, response)
        handle_client(socket)
      {:error, :closed} ->
        :ok
    end
  end

  defp process_request(data) do
    case :erlang.binary_to_term(data) do
      {:rpc, cookie, module, function, args} ->
        if valid_cookie?(cookie) do
          result = apply_rpc(module, function, args)
          :erlang.term_to_binary(result)
        else
          :erlang.term_to_binary({:error, :invalid_cookie})
        end
      _ ->
        :erlang.term_to_binary({:error, :invalid_request})
    end
  end
end
```

### CLI Mount Fix

```rust
// neonfs-cli/src/commands/mount.rs
// Change from:
conn.call("Elixir.NeonFS.CLI.Handler", "mount", vec![volume_term, mountpoint_term])
// To:
let options = Term::Map(/* empty map */);
conn.call("Elixir.NeonFS.CLI.Handler", "mount", vec![volume_term, mountpoint_term, options])
```

## Testing Strategy

1. Unit tests for RpcServer request/response handling
2. Integration test: start daemon, run CLI commands, verify results
3. Test invalid cookie rejection
4. Test malformed request handling
5. Test concurrent CLI connections

## Dependencies

- Task 0023 (CLI daemon connection) - Complete
- Task 0024 (CLI handler) - Complete
- Task 0025 (CLI commands) - Complete

## Files to Create/Modify

- `neonfs_core/lib/neon_fs/core/rpc_server.ex` (new)
- `neonfs_core/lib/neon_fs/core/supervisor.ex` (add RpcServer)
- `neonfs_core/test/neon_fs/core/rpc_server_test.exs` (new)
- `neonfs-cli/src/commands/mount.rs` (fix argument count)
- `neonfs-cli/src/commands/cluster.rs` (implement init/join)
- `neonfs-cli/src/commands/node.rs` (implement status/list)
- `neonfs-cli/src/term/types.rs` (fix field names)

## Reference

- Task 0023 for CLI connection protocol
- Task 0024 for handler function signatures
