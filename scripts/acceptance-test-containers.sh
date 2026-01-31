#!/bin/sh
# scripts/acceptance-test-containers.sh
# Container-based acceptance testing for NeonFS
# Tests the actual release containers (core, fuse, cli) in CI

set -eu

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Colours for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

log_info() { printf "${GREEN}[INFO]${NC} %s\n" "$1"; }
log_warn() { printf "${YELLOW}[WARN]${NC} %s\n" "$1"; }
log_error() { printf "${RED}[ERROR]${NC} %s\n" "$1"; }
log_section() { printf "\n${BLUE}=== %s ===${NC}\n" "$1"; }

# Test tracking
TESTS_RUN=0
TESTS_PASSED=0
TESTS_FAILED=0
FAILED_TESTS=""

run_test() {
    name="$1"
    shift
    TESTS_RUN=$((TESTS_RUN + 1))
    log_info "Running: $name"

    output_file="/tmp/test_output_$TESTS_RUN.log"
    if "$@" > "$output_file" 2>&1; then
        TESTS_PASSED=$((TESTS_PASSED + 1))
        log_info "✓ PASSED: $name"
        return 0
    else
        TESTS_FAILED=$((TESTS_FAILED + 1))
        log_error "✗ FAILED: $name"
        if [ -n "$FAILED_TESTS" ]; then
            FAILED_TESTS="$FAILED_TESTS
$name"
        else
            FAILED_TESTS="$name"
        fi
        log_error "Output:"
        cat "$output_file"
        return 1
    fi
}

# Cleanup on exit
cleanup() {
    log_section "Cleanup"
    log_info "Stopping containers..."
    docker stop neonfs-fuse neonfs-core 2>/dev/null || true
    docker rm neonfs-fuse neonfs-core 2>/dev/null || true
    docker volume rm neonfs-data 2>/dev/null || true
    docker network rm neonfs-test 2>/dev/null || true
    log_info "Cleanup complete"
}
trap cleanup EXIT

# Configuration
TAG="${TAG:-latest}"
COOKIE=$(tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 20)

log_section "NeonFS Container Acceptance Tests"
log_info "Using tag: $TAG"
log_info "Generated cookie: $COOKIE"

# ─────────────────────────────────────────────────────────────
# Setup
# ─────────────────────────────────────────────────────────────
log_section "Setup"

log_info "Creating docker network..."
docker network create neonfs-test || true

# Create a persistent volume for core data (survives container restart)
log_info "Creating data volume..."
docker volume create neonfs-data
# Set correct ownership (nobody:nogroup = 65534:65534)
docker run --rm -v neonfs-data:/data alpine chown -R 65534:65534 /data

# Start neonfs_core
log_info "Starting neonfs_core..."
docker run -d --name neonfs-core \
    --network neonfs-test \
    -v neonfs-data:/var/lib/neonfs \
    -e RELEASE_COOKIE="$COOKIE" \
    -e RELEASE_NODE="neonfs_core@neonfs-core" \
    -e NEONFS_FUSE_NODE="neonfs_fuse@neonfs-fuse" \
    forgejo.dmz/project-neon/neonfs/core:${TAG}

log_info "Waiting for neonfs_core to start..."
sleep 5

# Verify core is running
if ! docker ps | grep -q neonfs-core; then
    log_error "neonfs_core failed to start"
    docker logs neonfs-core
    exit 1
fi
log_info "neonfs_core is running"

# Start neonfs_fuse (privileged for FUSE)
log_info "Starting neonfs_fuse..."
docker run -d --name neonfs-fuse \
    --network neonfs-test \
    --privileged \
    -e RELEASE_COOKIE="$COOKIE" \
    -e RELEASE_NODE="neonfs_fuse@neonfs-fuse" \
    -e NEONFS_CORE_NODE="neonfs_core@neonfs-core" \
    forgejo.dmz/project-neon/neonfs/fuse:${TAG}

log_info "Waiting for neonfs_fuse to start..."
sleep 5

# Verify fuse is running
if ! docker ps | grep -q neonfs-fuse; then
    log_error "neonfs_fuse failed to start"
    docker logs neonfs-fuse
    exit 1
fi
log_info "neonfs_fuse is running"

# CLI image reference (used to avoid repetition)
CLI_IMAGE="forgejo.dmz/project-neon/neonfs/cli:${TAG}"

# Helper to run CLI commands
run_cli() {
    docker run --rm --network neonfs-test \
        -e NEONFS_COOKIE="$COOKIE" \
        -e NEONFS_NODE="neonfs_core@neonfs-core" \
        "$CLI_IMAGE" "$@"
}

# Helper to exec into fuse container
fuse_exec() {
    docker exec neonfs-fuse "$@"
}

# Helper for tests that need piping (wraps command in sh -c)
assert_output_contains() {
    pattern="$1"
    shift
    "$@" | grep -q "$pattern"
}

# ─────────────────────────────────────────────────────────────
# CLI Tests
# ─────────────────────────────────────────────────────────────
log_section "CLI Tests"

run_test "CLI: Cluster status" run_cli cluster status
run_test "CLI: Create volume 'test-acceptance'" run_cli volume create test-acceptance
run_test "CLI: List volumes contains test-acceptance" assert_output_contains "test-acceptance" run_cli volume list
run_test "CLI: Show volume info" run_cli volume show test-acceptance

# ─────────────────────────────────────────────────────────────
# FUSE Tests (exec into fuse container)
# ─────────────────────────────────────────────────────────────
log_section "FUSE Tests"

# Mount the volume inside the fuse container
MOUNT_POINT="/mnt/neonfs/test"
log_info "Creating mount point: $MOUNT_POINT"
fuse_exec mkdir -p "$MOUNT_POINT"

run_test "FUSE: Mount volume" run_cli mount mount test-acceptance "$MOUNT_POINT"
sleep 2

# Write and read file
run_test "FUSE: Write file" fuse_exec sh -c "echo 'test content from acceptance test' > $MOUNT_POINT/test.txt"
run_test "FUSE: Read file" fuse_exec grep -q 'test content' "$MOUNT_POINT/test.txt"

# Create directory and nested file
run_test "FUSE: Create directory" fuse_exec mkdir -p "$MOUNT_POINT/testdir"
run_test "FUSE: Write file in directory" fuse_exec sh -c "echo 'nested content' > $MOUNT_POINT/testdir/nested.txt"
run_test "FUSE: List directory contains nested.txt" fuse_exec sh -c "ls -la $MOUNT_POINT/testdir | grep -q nested.txt"
run_test "FUSE: Read nested file" fuse_exec grep -q 'nested content' "$MOUNT_POINT/testdir/nested.txt"

# Unmount
run_test "FUSE: Unmount volume" run_cli mount unmount "$MOUNT_POINT"

# ─────────────────────────────────────────────────────────────
# Persistence Tests (restart and verify)
# ─────────────────────────────────────────────────────────────
log_section "Persistence Tests"

log_info "Stopping neonfs_core for restart test..."
# Use the release stop command for graceful shutdown (triggers terminate callbacks)
docker exec neonfs-core /app/bin/neonfs_core stop || true
# Wait for container to actually stop
sleep 5
# If still running, force stop
docker stop -t 10 neonfs-core 2>/dev/null || true
log_info "Container logs before restart:"
docker logs --tail 30 neonfs-core
sleep 2

log_info "Restarting neonfs_core..."
docker start neonfs-core
sleep 5

# Verify core is running
if ! docker ps | grep -q neonfs-core; then
    log_error "neonfs_core failed to restart"
    docker logs neonfs-core
    exit 1
fi

log_info "Container logs after restart:"
docker logs --tail 20 neonfs-core

# Check if data directory has files (path is /var/lib/neonfs/data/meta in container)
log_info "Checking data volume contents:"
docker run --rm -v neonfs-data:/data alpine ls -laR /data 2>/dev/null || log_warn "Volume empty or not accessible"

run_test "Persistence: Volume exists after restart" assert_output_contains "test-acceptance" run_cli volume list

# ─────────────────────────────────────────────────────────────
# Summary
# ─────────────────────────────────────────────────────────────
log_section "Test Summary"
echo "Tests run:    $TESTS_RUN"
echo "Tests passed: $TESTS_PASSED"
echo "Tests failed: $TESTS_FAILED"

if [ $TESTS_FAILED -gt 0 ]; then
    log_error "Failed tests:"
    printf "%s\n" "$FAILED_TESTS" | while read -r test; do
        echo "  - $test"
    done
    exit 1
else
    log_info "✓ All acceptance tests passed!"
    exit 0
fi
