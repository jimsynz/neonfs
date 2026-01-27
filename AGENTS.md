# CLAUDE.md

This file provides guidance to coding agents when working with code in this repository.

## Project Overview

NeonFS is a BEAM-orchestrated distributed filesystem combining Elixir's coordination strengths with Rust's performance for storage operations. The project follows a strict separation of concerns: Elixir handles coordination, policy, and APIs; Rust handles I/O, chunking, and cryptography via Rustler NIFs.

**Current Status:** Phase 1 (Foundation) - building basic single-node operation with local storage and CLI.

## Build Commands

### Elixir (from neonfs_core/ or neonfs_fuse/)
```bash
mix compile                    # Compile Elixir + Rustler NIFs
mix test                       # Run ExUnit tests
mix test path/to/test.exs      # Run specific test file
mix test path/to/test.exs:42   # Run test at specific line
mix format                     # Format code
mix format --check-formatted   # Check formatting
mix credo --strict             # Code style checker
mix dialyzer                   # Static type analysis
```

### Rust (from native/ crates)
```bash
cargo test                                        # Run tests
cargo clippy --all-targets -- -D warnings         # Linting
cargo fmt --check                                 # Format check
```

## Architecture

```
neonfs_core/          # Elixir control plane
├── lib/neon_fs/core/
│   ├── application.ex    # Supervision tree root
│   └── core.ex           # Main module
├── native/               # Rustler NIFs (via mix rustler.new)
└── test/

neonfs_fuse/          # FUSE filesystem package
├── lib/neon_fs/fuse/
│   ├── application.ex
│   └── fuse.ex
├── native/
└── test/

spec/                 # Specification documents (start here)
├── specification.md     # Entry point & overview
├── architecture.md      # System design, supervision trees
├── implementation.md    # Phases 1-7, dependencies
└── testing.md           # Testing strategy

tasks/                # Implementation task specifications
├── README.md         # Task overview & dependency graph
└── task_NNNN_*.md    # Individual tasks with acceptance criteria
```

### Key Design Principles
- All data flows through Elixir for single code path and consistency
- Content-addressed storage: immutable SHA-256 identified chunks
- Per-volume supervision trees for isolation
- Async Rust NIFs for backpressure via BEAM scheduler

## Task System

Implementation follows 37 task specifications in `/workspace/tasks/`. Each task has:
- Status, phase, description
- Acceptance criteria checkboxes
- Testing strategy
- Dependencies on other tasks

**Parallel work streams (Phase 1):**
- Tasks 0001-0009: neonfs_blob Rust crate (independent)
- Tasks 0010-0013: neonfs_fuse Rust crate (independent)
- Tasks 0014-0019: Elixir metadata & read/write (depends on blob scaffolding)
- Tasks 0022-0025: CLI (can develop alongside core)

## Testing

**Testing layers (bottom to top):**
1. Static analysis: Dialyzer, Clippy, Credo
2. Unit/property tests: ExUnit + StreamData (Elixir), cargo test + proptest (Rust)
3. NIF boundary tests: Elixir calling Rust NIFs
4. Integration tests: Containerized multi-node clusters

**Running tests:**
```bash
mix test                       # All Elixir tests
mix test --only integration    # Integration tests only
mix test --exclude integration # Unit tests only
cargo test                     # Rust tests
```

## Version Requirements

From `.tool-versions`:
- Elixir 1.19.5 (OTP 28)
- Erlang 28.3.1
- Rust 1.93.0

## Key Specification Documents

Always consult these before implementing:
- `spec/specification.md` - Start here for overview
- `spec/architecture.md` - System design, NIF boundaries
- `spec/implementation.md` - Phase roadmap, dependency tables
- `spec/testing.md` - Test examples and patterns
- `tasks/README.md` - Task dependency graph

## Module Naming

- Top-level: `NeonFS.Core.*` and `NeonFS.FUSE.*`
- File paths use underscore: `NeonFS.Core` → `lib/neon_fs/core.ex`
- Type specs required on all public Elixir functions (for Dialyzer)
