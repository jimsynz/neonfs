# NeonFS Development Progress

## Codebase Patterns
- Use `mix rustler.new` to create new Rust NIF crates - the default add/2 function is created automatically
- Rustler 0.37+ no longer requires explicit NIF function lists in `rustler::init!` macro - just the module name
- NIF modules use `use Rustler, otp_app: :neonfs_core, crate: :neonfs_blob` - no need to modify mix.exs compilers
- Use `mix check --no-retry` to run all quality checks (Elixir + Rust) - configured via `.check.exs`
- Rust tools in `.check.exs` use `enabled: File.dir?("native/crate_name")` for graceful skip when crate doesn't exist
- To return binary from Rustler NIF: use `NewBinary::new(env, len)`, copy data, then `.into()` to convert to `Binary<'a>`

---

## 2026-01-27 - Task 0001
- What was implemented:
  - Created neonfs_blob Rust crate using `mix rustler.new`
  - Created Elixir NIF module at `lib/neon_fs/core/blob/native.ex`
  - Added test file for NIF verification
- Files changed:
  - `neonfs_core/native/neonfs_blob/` (new crate)
  - `neonfs_core/lib/neon_fs/core/blob/native.ex` (new module)
  - `neonfs_core/test/neon_fs/core/blob/native_test.exs` (new test)
  - `neonfs_core/Cargo.toml` (workspace config added by rustler)
- **Learnings for future iterations:**
  - Rustler 0.37+ automatically discovers NIF functions via `#[rustler::nif]` attribute - no explicit list needed
  - The `use Rustler` macro in Elixir module handles compilation - no need to add `:rustler` to compilers list
  - Must install Hex (`mix local.hex --force`) before running deps.get if not already available
---

## 2026-01-27 - Task 0002
- What was implemented:
  - Created `.check.exs` for neonfs_core with Rust tool integration (cargo fmt, clippy, test)
  - Created `.check.exs` for neonfs_fuse with Rust tool integration (gracefully skipped until native crate exists)
  - Fixed credo alias suggestion in `neonfs_core/test/neon_fs/core/blob/native_test.exs`
- Files changed:
  - `neonfs_core/.check.exs` (new file)
  - `neonfs_fuse/.check.exs` (new file)
  - `neonfs_core/test/neon_fs/core/blob/native_test.exs` (added alias for credo compliance)
- **Learnings for future iterations:**
  - ex_check uses keyword list syntax for conditional tools: `{:tool_name, command: "...", enabled: condition}`
  - `File.dir?("path")` is evaluated at config load time, making it suitable for conditional tool enabling
  - Credo with `--strict` flag reports design suggestions as errors - use `alias` for nested module references
---

## 2026-01-27 - Task 0003
- What was implemented:
  - SHA-256 hashing module in Rust at `neonfs_blob/src/hash.rs`
  - `Hash` type with Display, Debug, Clone, PartialEq, Eq, Hash traits
  - Functions: `sha256()`, `to_hex()`, `from_hex()`, `from_bytes()`, `as_bytes()`
  - `HashError` type for invalid hex handling
  - NIF function `compute_hash/1` exported to Elixir returning proper binary
  - Comprehensive Rust unit tests including property tests with proptest
  - Elixir tests comparing NIF output with `:crypto.hash(:sha256, data)`
- Files changed:
  - `neonfs_core/native/neonfs_blob/Cargo.toml` (added sha2, hex, thiserror, proptest deps)
  - `neonfs_core/native/neonfs_blob/src/hash.rs` (new module)
  - `neonfs_core/native/neonfs_blob/src/lib.rs` (added module and NIF export)
  - `neonfs_core/lib/neon_fs/core/blob/native.ex` (added compute_hash/1)
  - `neonfs_core/test/neon_fs/core/blob/native_test.exs` (added hash tests)
- **Learnings for future iterations:**
  - To return binary from NIF, use `NewBinary` and convert to `Binary` - `Vec<u8>` returns as Erlang list
  - `hex::FromHexError` doesn't implement `Eq`, so custom error handling needed for `#[derive(Eq)]` on error types
  - Use thiserror 2.0 (not 1.0 as specified in task) for current Rust ecosystem compatibility
---
