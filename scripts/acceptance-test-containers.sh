#!/bin/sh
# scripts/acceptance-test-containers.sh
# Container-based acceptance testing for NeonFS
# Tests the actual release containers (core, fuse, cli) in CI
#
# Phase 1 tests: Single-node CLI, FUSE, persistence
# Phase 2 tests: Multi-node cluster, replication, failure recovery

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
    # Phase 2 containers
    docker stop neonfs-core-1 neonfs-core-2 neonfs-core-3 2>/dev/null || true
    docker rm neonfs-core-1 neonfs-core-2 neonfs-core-3 2>/dev/null || true
    docker volume rm neonfs-data-1 neonfs-data-2 neonfs-data-3 2>/dev/null || true
    # Phase 1 containers
    docker stop neonfs-fuse neonfs-core 2>/dev/null || true
    docker rm neonfs-fuse neonfs-core 2>/dev/null || true
    docker volume rm neonfs-data 2>/dev/null || true
    # Network
    docker network rm neonfs-test 2>/dev/null || true
    log_info "Cleanup complete"
}
trap cleanup EXIT

# Configuration
TAG="${TAG:-latest}"
COOKIE=$(tr -dc 'A-Za-z0-9' < /dev/urandom | head -c 20)
REGISTRY="${REGISTRY:-forgejo.dmz/project-neon/neonfs}"

log_section "NeonFS Container Acceptance Tests"
log_info "Using tag: $TAG"
log_info "Using registry: $REGISTRY"
log_info "Generated cookie: $COOKIE"

# Image references
CLI_IMAGE="${REGISTRY}/cli:${TAG}"
CORE_IMAGE="${REGISTRY}/core:${TAG}"
FUSE_IMAGE="${REGISTRY}/fuse:${TAG}"

# ─────────────────────────────────────────────────────────────
# Helper Functions
# ─────────────────────────────────────────────────────────────

# Helper to run CLI commands against single-node (Phase 1)
run_cli_single() {
    docker run --rm --network neonfs-test \
        -e NEONFS_COOKIE="$COOKIE" \
        -e NEONFS_NODE="neonfs_core@neonfs-core" \
        "$CLI_IMAGE" "$@"
}

# Helper to run CLI commands against a specific node (Phase 2)
run_cli_node() {
    node="$1"
    shift
    docker run --rm --network neonfs-test \
        -e NEONFS_COOKIE="$COOKIE" \
        -e NEONFS_NODE="neonfs_core@$node" \
        "$CLI_IMAGE" "$@"
}

# Helper to run CLI commands (defaults to core-1 for Phase 2)
run_cli() {
    run_cli_node neonfs-core-1 "$@"
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

# Helper to create and configure a data volume
create_data_volume() {
    volume_name="$1"
    docker volume create "$volume_name"
    # Set correct ownership (nobody:nogroup = 65534:65534)
    docker run --rm -v "$volume_name":/data alpine chown -R 65534:65534 /data
}

# Helper to start a core node (Phase 2)
start_core_node() {
    node_num="$1"
    container_name="neonfs-core-$node_num"
    volume_name="neonfs-data-$node_num"

    log_info "Starting $container_name..."
    docker run -d --name "$container_name" \
        --network neonfs-test \
        --hostname "$container_name" \
        -v "$volume_name":/var/lib/neonfs \
        -e RELEASE_COOKIE="$COOKIE" \
        -e RELEASE_NODE="neonfs_core@$container_name" \
        -e NEONFS_FUSE_NODE="neonfs_fuse@neonfs-fuse" \
        "$CORE_IMAGE"
}

# Helper to wait for a core node to be ready (Phase 2)
wait_for_core_node() {
    node_num="$1"
    container_name="neonfs-core-$node_num"
    max_attempts="${2:-30}"

    log_info "Waiting for $container_name to be ready..."
    attempts=0
    while [ $attempts -lt $max_attempts ]; do
        if docker ps | grep -q "$container_name"; then
            # Try to get cluster status to verify the node is responsive
            if run_cli_node "$container_name" cluster status >/dev/null 2>&1; then
                log_info "$container_name is ready"
                return 0
            fi
        fi
        attempts=$((attempts + 1))
        sleep 1
    done

    log_error "$container_name failed to become ready"
    docker logs "$container_name" 2>&1 | tail -50
    return 1
}

# ═══════════════════════════════════════════════════════════════
# PHASE 1: Single-Node Tests
# ═══════════════════════════════════════════════════════════════
log_section "PHASE 1: Single-Node Tests"

# ─────────────────────────────────────────────────────────────
# Phase 1 Setup
# ─────────────────────────────────────────────────────────────
log_section "Phase 1 Setup"

log_info "Creating docker network..."
docker network create neonfs-test || true

# Create a persistent volume for core data
log_info "Creating data volume..."
create_data_volume neonfs-data

# Start neonfs_core (single node)
log_info "Starting neonfs_core..."
docker run -d --name neonfs-core \
    --network neonfs-test \
    -v neonfs-data:/var/lib/neonfs \
    -e RELEASE_COOKIE="$COOKIE" \
    -e RELEASE_NODE="neonfs_core@neonfs-core" \
    -e NEONFS_FUSE_NODE="neonfs_fuse@neonfs-fuse" \
    "$CORE_IMAGE"

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
    "$FUSE_IMAGE"

log_info "Waiting for neonfs_fuse to start..."
sleep 5

# Verify fuse is running
if ! docker ps | grep -q neonfs-fuse; then
    log_error "neonfs_fuse failed to start"
    docker logs neonfs-fuse
    exit 1
fi
log_info "neonfs_fuse is running"

# ─────────────────────────────────────────────────────────────
# Phase 1: CLI Tests
# ─────────────────────────────────────────────────────────────
log_section "Phase 1: CLI Tests"

run_test "CLI: Cluster status" run_cli_single cluster status
run_test "CLI: Create volume 'test-acceptance'" run_cli_single volume create test-acceptance
run_test "CLI: List volumes contains test-acceptance" assert_output_contains "test-acceptance" run_cli_single volume list
run_test "CLI: Show volume info" run_cli_single volume show test-acceptance

# ─────────────────────────────────────────────────────────────
# Phase 1: FUSE Tests
# ─────────────────────────────────────────────────────────────
log_section "Phase 1: FUSE Tests"

# Mount the volume inside the fuse container
MOUNT_POINT="/mnt/neonfs/test"
log_info "Creating mount point: $MOUNT_POINT"
fuse_exec mkdir -p "$MOUNT_POINT"

run_test "FUSE: Mount volume" run_cli_single mount mount test-acceptance "$MOUNT_POINT"
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
run_test "FUSE: Unmount volume" run_cli_single mount unmount "$MOUNT_POINT"

# ─────────────────────────────────────────────────────────────
# Phase 1: Persistence Tests
# ─────────────────────────────────────────────────────────────
log_section "Phase 1: Persistence Tests"

log_info "Stopping neonfs_core for restart test..."
# Use the release stop command for graceful shutdown
docker exec neonfs-core /app/bin/neonfs_core stop || true
sleep 5
docker stop -t 10 neonfs-core 2>/dev/null || true

log_info "Restarting neonfs_core..."
docker start neonfs-core
sleep 5

# Verify core is running
if ! docker ps | grep -q neonfs-core; then
    log_error "neonfs_core failed to restart"
    docker logs neonfs-core
    exit 1
fi

run_test "Persistence: Volume exists after restart" assert_output_contains "test-acceptance" run_cli_single volume list

# Remount and verify data still exists
log_info "Remounting volume to verify data persistence..."
fuse_exec mkdir -p "$MOUNT_POINT"
run_test "Persistence: Remount volume" run_cli_single mount mount test-acceptance "$MOUNT_POINT"
sleep 2

run_test "Persistence: File still exists" fuse_exec grep -q 'test content' "$MOUNT_POINT/test.txt"
run_test "Persistence: Directory still exists" fuse_exec test -d "$MOUNT_POINT/testdir"
run_test "Persistence: Nested file still exists" fuse_exec grep -q 'nested content' "$MOUNT_POINT/testdir/nested.txt"

run_test "Persistence: Unmount" run_cli_single mount unmount "$MOUNT_POINT"

# ─────────────────────────────────────────────────────────────
# Phase 1: Cleanup
# ─────────────────────────────────────────────────────────────
log_section "Phase 1: Cleanup"

run_test "Cleanup: Delete test volume" run_cli_single volume delete test-acceptance

log_info "Stopping Phase 1 containers..."
docker stop neonfs-fuse neonfs-core 2>/dev/null || true
docker rm neonfs-fuse neonfs-core 2>/dev/null || true
docker volume rm neonfs-data 2>/dev/null || true

log_info "Phase 1 complete"

# ═══════════════════════════════════════════════════════════════
# PHASE 2: Multi-Node Cluster Tests
# ═══════════════════════════════════════════════════════════════
log_section "PHASE 2: Multi-Node Cluster Tests"

# ─────────────────────────────────────────────────────────────
# Phase 2 Setup: Multi-Node Cluster
# ─────────────────────────────────────────────────────────────
log_section "Phase 2 Setup: Multi-Node Cluster"

# Create data volumes for all nodes
log_info "Creating data volumes..."
create_data_volume neonfs-data-1
create_data_volume neonfs-data-2
create_data_volume neonfs-data-3

# Start all core nodes
start_core_node 1
start_core_node 2
start_core_node 3

# Wait for nodes to be ready
sleep 5
wait_for_core_node 1
wait_for_core_node 2
wait_for_core_node 3

# Start FUSE node (connected to core-1)
log_info "Starting neonfs-fuse..."
docker run -d --name neonfs-fuse \
    --network neonfs-test \
    --privileged \
    -e RELEASE_COOKIE="$COOKIE" \
    -e RELEASE_NODE="neonfs_fuse@neonfs-fuse" \
    -e NEONFS_CORE_NODE="neonfs_core@neonfs-core-1" \
    "$FUSE_IMAGE"

log_info "Waiting for neonfs-fuse to start..."
sleep 5

if ! docker ps | grep -q neonfs-fuse; then
    log_error "neonfs_fuse failed to start"
    docker logs neonfs-fuse
    exit 1
fi
log_info "neonfs-fuse is running"

# ─────────────────────────────────────────────────────────────
# Phase 2: Cluster Initialisation
# ─────────────────────────────────────────────────────────────
log_section "Phase 2: Cluster Initialisation"

run_test "Cluster: Init on node 1" run_cli cluster init --name test-cluster

run_test "Cluster: Status shows initialised" assert_output_contains "test-cluster\|neonfs_core" run_cli cluster status

# ─────────────────────────────────────────────────────────────
# Phase 2: Node Join Flow
# ─────────────────────────────────────────────────────────────
log_section "Phase 2: Node Join Flow"

# Create invite token and capture it
log_info "Creating invite token..."
INVITE_OUTPUT=$(run_cli cluster create-invite --expires 1h)
echo "$INVITE_OUTPUT"

# Extract token from output (matches nfs_inv_random_expiry_signature format)
INVITE_TOKEN=$(echo "$INVITE_OUTPUT" | grep -oE 'nfs_inv_[a-z0-9]+_[0-9]+_[a-z0-9]+' | head -1)

if [ -z "$INVITE_TOKEN" ]; then
    log_error "Failed to extract invite token from output"
    exit 1
fi
log_info "Extracted invite token successfully"

# Join node 2 to cluster
run_test "Cluster: Join node 2" run_cli_node neonfs-core-2 cluster join \
    --token "$INVITE_TOKEN" \
    --via "neonfs_core@neonfs-core-1"

# Give cluster time to stabilise (Ra join happens async after 500ms delay)
sleep 5

# Join node 3 to cluster
run_test "Cluster: Join node 3" run_cli_node neonfs-core-3 cluster join \
    --token "$INVITE_TOKEN" \
    --via "neonfs_core@neonfs-core-1"

# Give cluster time to stabilise and Ra to synchronize
# The async Ra join takes time: 500ms delay + Ra reconfiguration + state sync
sleep 10

# Verify all nodes can report cluster status
run_test "Cluster: Node 1 status" run_cli_node neonfs-core-1 cluster status
run_test "Cluster: Node 2 status" run_cli_node neonfs-core-2 cluster status
run_test "Cluster: Node 3 status" run_cli_node neonfs-core-3 cluster status

# ─────────────────────────────────────────────────────────────
# Phase 2: Replicated Volume
# ─────────────────────────────────────────────────────────────
log_section "Phase 2: Replicated Volume"

run_test "Volume: Create replicated volume" run_cli volume create test-replicated
run_test "Volume: List shows volume" assert_output_contains "test-replicated" run_cli volume list
run_test "Volume: Show volume info" run_cli volume show test-replicated

# Wait for Ra replication to propagate the volume to other nodes
# The async Ra join + state replication can take time
log_info "Waiting for Ra replication to complete..."
sleep 5

# Verify volume visible from all nodes
run_test "Volume: Visible from node 2" assert_output_contains "test-replicated" run_cli_node neonfs-core-2 volume list
run_test "Volume: Visible from node 3" assert_output_contains "test-replicated" run_cli_node neonfs-core-3 volume list

# ─────────────────────────────────────────────────────────────
# Phase 2: FUSE with Replication
# ─────────────────────────────────────────────────────────────
log_section "Phase 2: FUSE with Replication"

MOUNT_POINT="/mnt/neonfs/test"
log_info "Creating mount point: $MOUNT_POINT"
fuse_exec mkdir -p "$MOUNT_POINT"

run_test "FUSE: Mount replicated volume" run_cli mount mount test-replicated "$MOUNT_POINT"
sleep 2

# Write test data (larger file to ensure chunking/replication)
TEST_CONTENT="This is test content for replication testing. $(date)"
run_test "FUSE: Write file" fuse_exec sh -c "echo '$TEST_CONTENT' > $MOUNT_POINT/replicated.txt"
run_test "FUSE: Read file" fuse_exec grep -q 'replication testing' "$MOUNT_POINT/replicated.txt"

# Write a larger file to trigger chunking
run_test "FUSE: Write large file" fuse_exec sh -c "dd if=/dev/urandom of=$MOUNT_POINT/large.bin bs=1024 count=100 2>/dev/null"
run_test "FUSE: Verify large file size" fuse_exec sh -c "test \$(stat -c%s $MOUNT_POINT/large.bin) -eq 102400"

# Create directory structure
run_test "FUSE: Create directory" fuse_exec mkdir -p "$MOUNT_POINT/subdir"
run_test "FUSE: Write nested file" fuse_exec sh -c "echo 'nested content' > $MOUNT_POINT/subdir/nested.txt"
run_test "FUSE: Read nested file" fuse_exec grep -q 'nested content' "$MOUNT_POINT/subdir/nested.txt"

# Give replication time to complete
log_info "Waiting for replication to complete..."
sleep 5

# Unmount
run_test "FUSE: Unmount volume" run_cli mount unmount "$MOUNT_POINT"

# ─────────────────────────────────────────────────────────────
# Phase 2: Node Failure and Recovery
# ─────────────────────────────────────────────────────────────
log_section "Phase 2: Node Failure and Recovery"

# Kill node 3 (simulate crash)
log_info "Killing node 3 to simulate failure..."
docker kill neonfs-core-3

# Give cluster time to detect the failure
sleep 5

# Verify cluster still operational with 2 nodes
run_test "Failure: Node 1 still operational" run_cli_node neonfs-core-1 cluster status
run_test "Failure: Node 2 still operational" run_cli_node neonfs-core-2 cluster status
run_test "Failure: Volume still accessible" run_cli_node neonfs-core-1 volume list

# Remount and verify data still accessible
log_info "Remounting volume to verify data accessibility..."
fuse_exec mkdir -p "$MOUNT_POINT"
run_test "Failure: Can remount volume" run_cli mount mount test-replicated "$MOUNT_POINT"
sleep 2

run_test "Failure: Data still readable" fuse_exec grep -q 'replication testing' "$MOUNT_POINT/replicated.txt"
run_test "Failure: Large file still readable" fuse_exec sh -c "test \$(stat -c%s $MOUNT_POINT/large.bin) -eq 102400"
run_test "Failure: Nested file still readable" fuse_exec grep -q 'nested content' "$MOUNT_POINT/subdir/nested.txt"

run_test "Failure: Unmount" run_cli mount unmount "$MOUNT_POINT"

# Restart node 3
log_info "Restarting node 3..."
docker start neonfs-core-3

# Wait for node to rejoin
log_info "Waiting for node 3 to rejoin cluster..."
sleep 10

# Verify node 3 has rejoined
run_test "Recovery: Node 3 operational" wait_for_core_node 3 30
run_test "Recovery: Node 3 sees cluster" run_cli_node neonfs-core-3 cluster status
run_test "Recovery: Node 3 sees volume" assert_output_contains "test-replicated" run_cli_node neonfs-core-3 volume list

# ─────────────────────────────────────────────────────────────
# Phase 2: Persistence (restart node 1)
# ─────────────────────────────────────────────────────────────
log_section "Phase 2: Persistence Tests"

log_info "Stopping neonfs-core-1 for restart test..."
# Use the release stop command for graceful shutdown
docker exec neonfs-core-1 /app/bin/neonfs_core stop || true
sleep 5
docker stop -t 10 neonfs-core-1 2>/dev/null || true

log_info "Restarting neonfs-core-1..."
docker start neonfs-core-1

# Wait for node to be ready
wait_for_core_node 1 30

run_test "Persistence: Volume exists after restart" assert_output_contains "test-replicated" run_cli volume list
run_test "Persistence: Cluster status after restart" run_cli cluster status

# ─────────────────────────────────────────────────────────────
# Cleanup test volumes
# ─────────────────────────────────────────────────────────────
log_section "Cleanup Test Data"

run_test "Cleanup: Delete test volume" run_cli volume delete test-replicated

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
