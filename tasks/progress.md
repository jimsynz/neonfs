# NeonFS Development Progress

## Codebase Patterns
- Use `mix rustler.new` to create new Rust NIF crates - the default add/2 function is created automatically
- Rustler 0.37+ no longer requires explicit NIF function lists in `rustler::init!` macro - just the module name
- NIF modules use `use Rustler, otp_app: :neonfs_core, crate: :neonfs_blob` - no need to modify mix.exs compilers
- Use `mix check --no-retry` to run all quality checks (Elixir + Rust) - configured via `.check.exs`
- Rust tools in `.check.exs` use `enabled: File.dir?("native/crate_name")` for graceful skip when crate doesn't exist

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
