# Task 0023: Implement CLI Daemon Connection

## Status
Complete

## Phase
1 - Foundation

## Description
Implement the connection logic that allows the CLI to communicate with the NeonFS daemon via Erlang distribution protocol. This uses erl_dist/erl_rpc crates to connect and make RPC calls.

## Acceptance Criteria
- [x] Add `erl_dist` and `erl_rpc` dependencies (or equivalent)
- [x] `DaemonConnection` struct managing connection state
- [x] Read cookie from `/var/lib/neonfs/.erlang.cookie`
- [x] Connect to EPMD on localhost:4369
- [x] Query for `neonfs` node name
- [x] Establish Erlang distribution connection
- [x] `call/4` method for RPC calls (module, function, args)
- [x] Proper error handling with user-friendly messages
- [x] Connection timeout handling
- [ ] Automatic retry logic (configurable)

## Connection Flow
```rust
impl DaemonConnection {
    pub async fn connect() -> Result<Self, CliError> {
        let cookie = read_cookie()?;
        let node_name = epmd_lookup("neonfs")?;
        let conn = erl_dist::connect(&node_name, &cookie).await?;
        Ok(Self { conn })
    }

    pub async fn call<T>(&self, module: &str, function: &str, args: Vec<Term>) -> Result<T, CliError>
    where T: FromTerm
    {
        let result = self.conn.call(module, function, args).await?;
        T::from_term(result)
    }
}
```

## Error Messages
| Scenario | User Message |
|----------|--------------|
| EPMD not running | "Cannot connect to NeonFS daemon. Is the service running?" |
| Node not found | "NeonFS daemon not found. The service may be starting." |
| Cookie permission denied | "Permission denied reading cookie. Run as neonfs user or use sudo." |
| Cookie not found | "NeonFS not initialised. Run: neonfs cluster init" |
| Connection timeout | "Connection timed out. The daemon may be overloaded." |

## Testing Strategy
- Unit tests:
  - Cookie reading (mock file)
  - Error message formatting
- Integration tests (require running daemon):
  - Connect to daemon
  - Make simple RPC call
  - Handle connection errors gracefully

## Dependencies
- task_0022_cli_crate_scaffolding

## Files to Create/Modify
- `neonfs-cli/Cargo.toml` (add dependencies)
- `neonfs-cli/src/daemon.rs` (new)
- `neonfs-cli/src/error.rs` (add connection errors)

## Reference
- spec/deployment.md - Communication Protocol section
- spec/deployment.md - Cookie Management

## Notes
The erl_dist/erl_rpc crates may need evaluation. Alternatives include implementing a minimal Erlang distribution client or using a different IPC mechanism. The spec recommends erl_dist/erl_rpc for native integration.
