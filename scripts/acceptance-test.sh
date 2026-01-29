#!/usr/bin/env bash
# scripts/acceptance-test.sh
# Full-suite acceptance testing for NeonFS Phase 1

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PROJECT_ROOT="$(dirname "$SCRIPT_DIR")"

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_section() { echo -e "\n${BLUE}=== $1 ===${NC}"; }

# Test tracking
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
FAILED_TESTS=()

# Cleanup tracking
CORE_PID=""
FUSE_PID=""
MOUNT_POINT=""
TEST_DATA_DIR=""

cleanup() {
    log_section "Cleanup"

    # Unmount FUSE if mounted
    if [ -n "$MOUNT_POINT" ] && mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        log_info "Unmounting $MOUNT_POINT"
        fusermount -u "$MOUNT_POINT" 2>/dev/null || umount "$MOUNT_POINT" 2>/dev/null || true
    fi

    # Remove mount point
    if [ -n "$MOUNT_POINT" ] && [ -d "$MOUNT_POINT" ]; then
        rmdir "$MOUNT_POINT" 2>/dev/null || true
    fi

    # Stop services
    if [ -n "$FUSE_PID" ] && kill -0 "$FUSE_PID" 2>/dev/null; then
        log_info "Stopping neonfs_fuse (PID: $FUSE_PID)"
        kill "$FUSE_PID" 2>/dev/null || true
        sleep 2
        kill -9 "$FUSE_PID" 2>/dev/null || true
    fi

    if [ -n "$CORE_PID" ] && kill -0 "$CORE_PID" 2>/dev/null; then
        log_info "Stopping neonfs_core (PID: $CORE_PID)"
        kill "$CORE_PID" 2>/dev/null || true
        sleep 2
        kill -9 "$CORE_PID" 2>/dev/null || true
    fi

    # Clean up test data
    if [ -n "$TEST_DATA_DIR" ] && [ -d "$TEST_DATA_DIR" ]; then
        log_info "Removing test data: $TEST_DATA_DIR"
        rm -rf "$TEST_DATA_DIR"
    fi
}

trap cleanup EXIT

run_test() {
    local name="$1"
    local cmd="$2"
    TESTS_RUN=$((TESTS_RUN + 1))
    log_info "Running: $name"

    if eval "$cmd" > /tmp/test_output_$TESTS_RUN.log 2>&1; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_info "✓ PASSED: $name"
        return 0
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_error "✗ FAILED: $name"
        FAILED_TESTS+=("$name")
        log_error "Output: $(cat /tmp/test_output_$TESTS_RUN.log)"
        return 1
    fi
}

# Check for required tools
check_dependencies() {
    log_section "Checking Dependencies"

    local missing=()

    command -v mix >/dev/null 2>&1 || missing+=("mix (Elixir)")
    command -v cargo >/dev/null 2>&1 || missing+=("cargo (Rust)")

    if [ ${#missing[@]} -gt 0 ]; then
        log_error "Missing required tools: ${missing[*]}"
        exit 1
    fi

    log_info "All required tools found"

    # Check for FUSE
    if command -v fusermount >/dev/null 2>&1; then
        log_info "FUSE support detected"
        FUSE_AVAILABLE=1
    else
        log_warn "FUSE not available, will skip FUSE tests"
        FUSE_AVAILABLE=0
    fi
}

# Build releases
build_releases() {
    log_section "Building Releases"

    # Build neonfs_core
    cd "$PROJECT_ROOT/neonfs_core"
    run_test "Build neonfs_core release" "MIX_ENV=prod mix do deps.get, compile, release --overwrite"

    # Build neonfs_fuse
    cd "$PROJECT_ROOT/neonfs_fuse"
    run_test "Build neonfs_fuse release" "MIX_ENV=prod mix do deps.get, compile, release --overwrite"

    # Build CLI
    cd "$PROJECT_ROOT/neonfs-cli"
    run_test "Build neonfs CLI" "cargo build --release"

    cd "$PROJECT_ROOT"
}

# Start services
start_services() {
    log_section "Starting Services"

    # Create test data directory
    TEST_DATA_DIR=$(mktemp -d -t neonfs-test-XXXXXX)
    export NEONFS_DATA_DIR="$TEST_DATA_DIR"
    export NEONFS_META_DIR="$TEST_DATA_DIR/meta"
    mkdir -p "$NEONFS_META_DIR"

    # Generate Erlang cookie
    COOKIE_FILE="$TEST_DATA_DIR/.erlang.cookie"
    tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 20 > "$COOKIE_FILE"
    chmod 400 "$COOKIE_FILE"
    export ERLANG_COOKIE=$(cat "$COOKIE_FILE")

    log_info "Test data directory: $TEST_DATA_DIR"
    log_info "Erlang cookie: $ERLANG_COOKIE"

    # Start neonfs_core
    log_info "Starting neonfs_core..."
    cd "$PROJECT_ROOT/neonfs_core"
    RELEASE_NODE="neonfs_core@localhost" \
    RELEASE_COOKIE="$ERLANG_COOKIE" \
    RELEASE_DISTRIBUTION="sname" \
    NEONFS_DATA_DIR="$TEST_DATA_DIR" \
    NEONFS_META_DIR="$NEONFS_META_DIR" \
    _build/prod/rel/neonfs_core/bin/neonfs_core start &
    CORE_PID=$!

    log_info "neonfs_core started (PID: $CORE_PID)"
    sleep 5

    # Verify core is running
    if ! kill -0 "$CORE_PID" 2>/dev/null; then
        log_error "neonfs_core failed to start"
        exit 1
    fi

    # Start neonfs_fuse if FUSE is available
    if [ "$FUSE_AVAILABLE" -eq 1 ]; then
        log_info "Starting neonfs_fuse..."
        cd "$PROJECT_ROOT/neonfs_fuse"
        RELEASE_NODE="neonfs_fuse@localhost" \
        RELEASE_COOKIE="$ERLANG_COOKIE" \
        RELEASE_DISTRIBUTION="sname" \
        NEONFS_CORE_NODE="neonfs_core@localhost" \
        _build/prod/rel/neonfs_fuse/bin/neonfs_fuse start &
        FUSE_PID=$!

        log_info "neonfs_fuse started (PID: $FUSE_PID)"
        sleep 5

        # Verify FUSE is running
        if ! kill -0 "$FUSE_PID" 2>/dev/null; then
            log_error "neonfs_fuse failed to start"
            exit 1
        fi
    fi

    cd "$PROJECT_ROOT"
}

# CLI tests
cli_tests() {
    log_section "CLI Tests"

    CLI="$PROJECT_ROOT/neonfs-cli/target/release/neonfs"
    export NEONFS_COOKIE_FILE="$COOKIE_FILE"

    # Test cluster status
    run_test "CLI: Cluster status" "$CLI cluster status"

    # Test volume creation
    run_test "CLI: Create volume 'test-acceptance'" "$CLI volume create test-acceptance"

    # Test volume listing
    run_test "CLI: List volumes" "$CLI volume list | grep -q test-acceptance"

    # Test volume info
    run_test "CLI: Get volume info" "$CLI volume show test-acceptance"
}

# FUSE tests
fuse_tests() {
    if [ "$FUSE_AVAILABLE" -eq 0 ]; then
        log_warn "Skipping FUSE tests (FUSE not available)"
        return 0
    fi

    log_section "FUSE Tests"

    CLI="$PROJECT_ROOT/neonfs-cli/target/release/neonfs"
    MOUNT_POINT=$(mktemp -d -t neonfs-mount-XXXXXX)

    # Mount volume
    run_test "FUSE: Mount volume" "$CLI mount test-acceptance $MOUNT_POINT"

    # Wait for mount
    sleep 2

    # Verify mount point
    if ! mountpoint -q "$MOUNT_POINT" 2>/dev/null; then
        log_warn "Mount point not detected by mountpoint command, checking manually"
        if ! mount | grep -q "$MOUNT_POINT"; then
            log_error "FUSE mount failed"
            return 1
        fi
    fi

    # Write file
    run_test "FUSE: Write file" "echo 'test content from acceptance test' > $MOUNT_POINT/test.txt"

    # Read file
    run_test "FUSE: Read file" "grep -q 'test content' $MOUNT_POINT/test.txt"

    # Create directory
    run_test "FUSE: Create directory" "mkdir -p $MOUNT_POINT/testdir"

    # Write file in directory
    run_test "FUSE: Write file in directory" "echo 'nested content' > $MOUNT_POINT/testdir/nested.txt"

    # List directory
    run_test "FUSE: List directory" "ls -la $MOUNT_POINT/testdir | grep -q nested.txt"

    # Read nested file
    run_test "FUSE: Read nested file" "grep -q 'nested content' $MOUNT_POINT/testdir/nested.txt"

    # Unmount
    run_test "FUSE: Unmount volume" "$CLI unmount $MOUNT_POINT"

    # Wait for unmount
    sleep 2

    # Clean up mount point
    rmdir "$MOUNT_POINT" 2>/dev/null || true
    MOUNT_POINT=""
}

# Persistence tests
persistence_tests() {
    log_section "Persistence Tests"

    CLI="$PROJECT_ROOT/neonfs-cli/target/release/neonfs"

    # Verify volume exists before restart
    run_test "Persistence: Volume exists before restart" "$CLI volume list | grep -q test-acceptance"

    # Stop services
    log_info "Stopping services for restart test..."
    if [ -n "$FUSE_PID" ] && kill -0 "$FUSE_PID" 2>/dev/null; then
        kill "$FUSE_PID" 2>/dev/null || true
        sleep 3
    fi
    if [ -n "$CORE_PID" ] && kill -0 "$CORE_PID" 2>/dev/null; then
        kill "$CORE_PID" 2>/dev/null || true
        sleep 3
    fi

    # Restart core
    log_info "Restarting neonfs_core..."
    cd "$PROJECT_ROOT/neonfs_core"
    RELEASE_NODE="neonfs_core@localhost" \
    RELEASE_COOKIE="$ERLANG_COOKIE" \
    RELEASE_DISTRIBUTION="sname" \
    NEONFS_DATA_DIR="$TEST_DATA_DIR" \
    NEONFS_META_DIR="$NEONFS_META_DIR" \
    _build/prod/rel/neonfs_core/bin/neonfs_core start &
    CORE_PID=$!
    sleep 5

    # Verify core is running
    if ! kill -0 "$CORE_PID" 2>/dev/null; then
        log_error "neonfs_core failed to restart"
        return 1
    fi

    # Verify volume persisted
    run_test "Persistence: Volume exists after restart" "$CLI volume list | grep -q test-acceptance"

    log_info "✓ Data persistence verified"
}

# Run all tests
main() {
    log_section "NeonFS Phase 1 Acceptance Tests"

    check_dependencies
    build_releases
    start_services
    cli_tests
    fuse_tests
    persistence_tests

    # Summary
    log_section "Test Summary"
    echo "Tests run:    $TESTS_RUN"
    echo "Tests passed: $TESTS_PASSED"
    echo "Tests failed: $TESTS_FAILED"

    if [ $TESTS_FAILED -gt 0 ]; then
        log_error "Failed tests:"
        for test in "${FAILED_TESTS[@]}"; do
            echo "  - $test"
        done
        exit 1
    else
        log_info "✓ All tests passed!"
        exit 0
    fi
}

main
