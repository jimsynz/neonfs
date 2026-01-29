# Task 0044: Full-Suite Acceptance Testing and Forgejo Actions CI

## Status

Complete

## Phase

1 (Foundation Addendum - must complete before Phase 2)

## Description

Create a comprehensive acceptance testing system that can be run both locally by developers and in CI via Forgejo Actions. This ensures the full system works end-to-end before claiming Phase 1 is complete.

## Acceptance Criteria

### Acceptance Test Script

- [x] `scripts/acceptance-test.sh` runs complete Phase 1 test suite
- [x] Builds both releases (`neonfs_core`, `neonfs_fuse`)
- [x] Starts both services (either via systemd or direct execution)
- [x] Runs CLI commands to verify functionality
- [x] Tests FUSE mount/unmount cycle (if FUSE available)
- [x] Tests file write/read through FUSE
- [x] Tests data persistence across restart
- [x] Produces clear pass/fail output with details on failures
- [x] Exits with non-zero code on any failure
- [x] Can run in containerized environment (for CI)

### Forgejo Actions CI

- [x] `.forgejo/workflows/ci.yml` workflow file
- [x] Jobs for: lint, unit tests, integration tests, release build
- [x] Lint job runs: `mix format --check-formatted`, `mix credo`, `cargo fmt --check`, `cargo clippy`
- [x] Unit test job runs: `mix test --exclude integration`, `cargo test`
- [x] Dialyzer job with PLT caching
- [x] Integration test job runs acceptance script
- [x] Release build job produces artifacts
- [x] FUSE tests run when FUSE available (may need privileged container)
- [x] Caching for Mix deps, Cargo deps, Dialyzer PLTs
- [x] Runs on push to main and on pull requests

### Container Support

- [x] `Dockerfile.ci` for CI runner environment
- [x] Includes: Elixir 1.19.5, Erlang/OTP 28, Rust 1.93.0
- [x] Includes: FUSE libraries, build essentials
- [x] Published to container registry or built inline

## Implementation Notes

### Acceptance Test Script Structure

```bash
#!/usr/bin/env bash
# scripts/acceptance-test.sh

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }

# Test tracking
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0

run_test() {
    local name="$1"
    local cmd="$2"
    TESTS_RUN=$((TESTS_RUN + 1))
    log_info "Running: $name"
    if eval "$cmd"; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_info "PASSED: $name"
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_error "FAILED: $name"
    fi
}

# Phase 1: Build
log_info "=== Building Releases ==="
cd "$PROJECT_ROOT/neonfs_core"
run_test "Build neonfs_core release" "MIX_ENV=prod mix release"

cd "$PROJECT_ROOT/neonfs_fuse"
run_test "Build neonfs_fuse release" "MIX_ENV=prod mix release"

# Phase 2: Start services
log_info "=== Starting Services ==="
# ... service startup logic

# Phase 3: CLI tests
log_info "=== CLI Tests ==="
run_test "Cluster status" "neonfs cluster status"
run_test "Volume create" "neonfs volume create test-acceptance"
run_test "Volume list" "neonfs volume list | grep test-acceptance"
run_test "Volume info" "neonfs volume info test-acceptance"

# Phase 4: FUSE tests (if available)
if command -v fusermount &> /dev/null; then
    log_info "=== FUSE Tests ==="
    MOUNT_POINT=$(mktemp -d)
    run_test "Mount volume" "neonfs mount test-acceptance $MOUNT_POINT"
    run_test "Write file" "echo 'test content' > $MOUNT_POINT/test.txt"
    run_test "Read file" "grep 'test content' $MOUNT_POINT/test.txt"
    run_test "Unmount volume" "neonfs unmount $MOUNT_POINT"
    rmdir "$MOUNT_POINT"
else
    log_warn "FUSE not available, skipping FUSE tests"
fi

# Phase 5: Persistence test
log_info "=== Persistence Tests ==="
# ... restart services, verify data still exists

# Summary
log_info "=== Test Summary ==="
echo "Tests run: $TESTS_RUN"
echo "Passed: $TESTS_PASSED"
echo "Failed: $TESTS_FAILED"

if [ $TESTS_FAILED -gt 0 ]; then
    exit 1
fi
```

### Forgejo Actions Workflow

```yaml
# .forgejo/workflows/ci.yml
name: CI

on:
  push:
    branches: [main, develop]
  pull_request:
    branches: [main]

jobs:
  lint:
    runs-on: docker
    container:
      image: ghcr.io/neonfs/ci:latest
    steps:
      - uses: actions/checkout@v4

      - name: Elixir format check
        run: |
          cd neonfs_core && mix format --check-formatted
          cd ../neonfs_fuse && mix format --check-formatted

      - name: Credo
        run: |
          cd neonfs_core && mix credo --strict
          cd ../neonfs_fuse && mix credo --strict

      - name: Rust format check
        run: |
          cd neonfs_core/native/neonfs_blob && cargo fmt --check
          cd ../../../neonfs_fuse/native/neonfs_fuse && cargo fmt --check
          cd ../../../neonfs-cli && cargo fmt --check

      - name: Clippy
        run: |
          cd neonfs_core/native/neonfs_blob && cargo clippy --all-targets -- -D warnings
          cd ../../../neonfs_fuse/native/neonfs_fuse && cargo clippy --all-targets --features fuse -- -D warnings
          cd ../../../neonfs-cli && cargo clippy --all-targets -- -D warnings

  unit-tests:
    runs-on: docker
    container:
      image: ghcr.io/neonfs/ci:latest
    steps:
      - uses: actions/checkout@v4

      - name: Get deps
        run: |
          cd neonfs_core && mix deps.get
          cd ../neonfs_fuse && mix deps.get

      - name: Elixir unit tests
        run: |
          cd neonfs_core && mix test --exclude integration
          cd ../neonfs_fuse && mix test --exclude integration --exclude fuse

      - name: Rust unit tests
        run: |
          cd neonfs_core/native/neonfs_blob && cargo test
          cd ../../../neonfs_fuse/native/neonfs_fuse && cargo test
          cd ../../../neonfs-cli && cargo test

  dialyzer:
    runs-on: docker
    container:
      image: ghcr.io/neonfs/ci:latest
    steps:
      - uses: actions/checkout@v4

      - uses: actions/cache@v4
        with:
          path: |
            neonfs_core/priv/plts
            neonfs_fuse/priv/plts
          key: plt-${{ hashFiles('**/mix.lock') }}

      - name: Dialyzer
        run: |
          cd neonfs_core && mix dialyzer
          cd ../neonfs_fuse && mix dialyzer

  integration-tests:
    runs-on: docker
    needs: [lint, unit-tests]
    container:
      image: ghcr.io/neonfs/ci:latest
      options: --privileged  # Required for FUSE
    steps:
      - uses: actions/checkout@v4

      - name: Run acceptance tests
        run: ./scripts/acceptance-test.sh

  build-release:
    runs-on: docker
    needs: [lint, unit-tests]
    container:
      image: ghcr.io/neonfs/ci:latest
    steps:
      - uses: actions/checkout@v4

      - name: Build releases
        run: |
          cd neonfs_core && MIX_ENV=prod mix release
          cd ../neonfs_fuse && MIX_ENV=prod mix release

      - name: Upload artifacts
        uses: actions/upload-artifact@v4
        with:
          name: releases
          path: |
            neonfs_core/_build/prod/*.tar.gz
            neonfs_fuse/_build/prod/*.tar.gz
```

### CI Dockerfile

```dockerfile
# Dockerfile.ci
FROM debian:bookworm

# Install build dependencies
RUN apt-get update && apt-get install -y \
    curl wget git build-essential \
    libssl-dev libncurses-dev \
    libfuse3-dev fuse3 \
    && rm -rf /var/lib/apt/lists/*

# Install asdf for version management
RUN git clone https://github.com/asdf-vm/asdf.git ~/.asdf --branch v0.14.0
ENV PATH="/root/.asdf/bin:/root/.asdf/shims:$PATH"

# Install Erlang, Elixir, Rust via asdf
COPY .tool-versions /tmp/
RUN asdf plugin add erlang && \
    asdf plugin add elixir && \
    asdf plugin add rust && \
    cd /tmp && asdf install

# Setup Hex and Rebar
RUN mix local.hex --force && mix local.rebar --force
```

## Testing Strategy

1. Run acceptance script locally, verify all tests pass
2. Set up Forgejo runner, verify CI workflow executes
3. Intentionally break something, verify CI catches it
4. Test CI caching by running twice, verify faster second run

## Dependencies

- Task 0029 (Elixir release) - Complete
- Task 0030 (Integration test) - Complete
- Task 0040 (Persistence) - Required for restart tests
- Task 0041 (FUSE fixes) - Required for FUSE tests
- Task 0042 (CLI RPC) - Required for CLI tests
- Task 0043 (systemd split) - Required for service tests

## Files to Create

- `scripts/acceptance-test.sh` (new)
- `.forgejo/workflows/ci.yml` (new)
- `Dockerfile.ci` (new)
- `scripts/start-test-services.sh` (new, helper for acceptance tests)
- `scripts/stop-test-services.sh` (new, helper for acceptance tests)

## Reference

- Forgejo Actions documentation: https://forgejo.org/docs/latest/user/actions/
- Forgejo Runner setup: https://forgejo.org/docs/latest/admin/actions/
