#!/bin/bash
# Build Debian packages for NeonFS
#
# Usage:
#   VERSION=0.1.0 ./packaging/build-debs.sh            # native build
#   VERSION=0.1.0 ARCH=arm64 ./packaging/build-debs.sh  # cross-compile via Docker
#   VERSION=0.1.0 USE_DOCKER=1 ./packaging/build-debs.sh # force Docker for native arch
#
# Environment variables:
#   VERSION    - Package version (required)
#   ARCH       - Target architecture: amd64, arm64 (default: auto-detect)
#   OUT_DIR    - Output directory for .deb files (default: ./dist)
#   USE_DOCKER - Set to 1 to force Docker even for native architecture

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

if [ -z "${VERSION:-}" ]; then
    echo "Error: VERSION is required"
    echo "Usage: VERSION=0.1.0 $0"
    exit 1
fi

if [ -z "${ARCH:-}" ]; then
    case "$(uname -m)" in
        x86_64)  ARCH="amd64" ;;
        aarch64) ARCH="arm64" ;;
        *)
            echo "Error: unsupported architecture $(uname -m)"
            exit 1
            ;;
    esac
fi

OUT_DIR="${OUT_DIR:-${REPO_ROOT}/dist}"

# --- Docker-based cross-compilation ---

host_arch=""
case "$(uname -m)" in
    x86_64)  host_arch="amd64" ;;
    aarch64) host_arch="arm64" ;;
esac

if [ "${_NEONFS_IN_DOCKER:-}" != "1" ] && { [ "$ARCH" != "$host_arch" ] || [ "${USE_DOCKER:-}" = "1" ]; }; then
    BUILDER_IMAGE="neonfs-deb-builder:${ARCH}"

    ELIXIR_VERSION=$(awk '/^elixir/ {print $2}' "${REPO_ROOT}/.tool-versions")
    ERLANG_VERSION=$(awk '/^erlang/ {print $2}' "${REPO_ROOT}/.tool-versions")
    RUST_VERSION=$(awk '/^rust/ {print $2}' "${REPO_ROOT}/.tool-versions")

    echo "==> Building ${ARCH} debs via Docker (host is ${host_arch})"
    echo "==> Building builder image for ${ARCH} (cached after first run)..."
    docker buildx build \
        --platform "linux/${ARCH}" \
        --load \
        --build-arg "ELIXIR_VERSION=${ELIXIR_VERSION}" \
        --build-arg "ERLANG_VERSION=${ERLANG_VERSION}" \
        --build-arg "RUST_VERSION=${RUST_VERSION}" \
        -t "${BUILDER_IMAGE}" \
        -f "${REPO_ROOT}/containers/Containerfile.deb-builder" \
        "${REPO_ROOT}"

    mkdir -p "${OUT_DIR}"

    echo "==> Running build inside Docker..."
    docker run --rm \
        --platform "linux/${ARCH}" \
        -v "${REPO_ROOT}:/src:ro" \
        -v "${OUT_DIR}:/out" \
        -e VERSION="${VERSION}" \
        -e ARCH="${ARCH}" \
        -e OUT_DIR=/out \
        -e _NEONFS_IN_DOCKER=1 \
        "${BUILDER_IMAGE}" \
        /src/packaging/build-debs.sh

    echo "==> Done. Packages in ${OUT_DIR}:"
    ls -lh "${OUT_DIR}"/*.deb
    exit 0
fi

# --- If running inside Docker, copy source to a writable location ---

if [ "${_NEONFS_IN_DOCKER:-}" = "1" ]; then
    echo "==> Copying source to build directory..."
    cp -a /src /build
    REPO_ROOT=/build
    SCRIPT_DIR=/build/packaging
fi

# --- Native build (or inside Docker container) ---

mkdir -p "${OUT_DIR}"
export VERSION ARCH BUILD_DIR="${REPO_ROOT}"

echo "==> Building NeonFS ${VERSION} for ${ARCH}"

# Step 1: Build CLI
echo "==> Building CLI..."
cd "${REPO_ROOT}/neonfs-cli"
cargo build --release

# Step 2: Build Elixir releases
for component in neonfs_core neonfs_fuse neonfs_nfs neonfs_omnibus; do
    echo "==> Building ${component} release..."
    cd "${REPO_ROOT}/${component}"
    MIX_ENV=prod mix deps.get --only prod
    MIX_ENV=prod mix release --overwrite
done

# Step 3: Package with nfpm
# nfpm resolves relative paths (../systemd/, ../scripts/) from CWD,
# so we must cd into the nfpm config directory first.
echo "==> Packaging .deb files..."

cd "${SCRIPT_DIR}/nfpm"

cleanup_generated_configs() {
    rm -f "${SCRIPT_DIR}"/nfpm/.generated-*.yaml
}
trap cleanup_generated_configs EXIT

for config in neonfs-cli neonfs-common neonfs-core neonfs-fuse neonfs-nfs neonfs-omnibus; do
    echo "    ${config}..."
    envsubst < "${config}.yaml" > ".generated-${config}.yaml"
    nfpm package \
        --config ".generated-${config}.yaml" \
        --packager deb \
        --target "${OUT_DIR}/"
done

echo "==> Done. Packages in ${OUT_DIR}:"
ls -lh "${OUT_DIR}"/*.deb
