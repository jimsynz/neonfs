#!/bin/bash
# Build Debian packages for NeonFS
#
# Usage:
#   VERSION=0.1.0 ./packaging/build-debs.sh
#
# Environment variables:
#   VERSION  - Package version (required)
#   ARCH     - Target architecture: amd64, arm64 (default: auto-detect)
#   OUT_DIR  - Output directory for .deb files (default: ./dist)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

# Validate required inputs
if [ -z "${VERSION:-}" ]; then
    echo "Error: VERSION is required"
    echo "Usage: VERSION=0.1.0 $0"
    exit 1
fi

# Auto-detect architecture
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
# nfpm doesn't expand environment variables in YAML, so we use envsubst
echo "==> Packaging .deb files..."

for config in neonfs-cli neonfs-common neonfs-core neonfs-fuse neonfs-nfs neonfs-omnibus; do
    echo "    ${config}..."
    envsubst < "${SCRIPT_DIR}/nfpm/${config}.yaml" > "/tmp/${config}.yaml"
    nfpm package \
        --config "/tmp/${config}.yaml" \
        --packager deb \
        --target "${OUT_DIR}/"
done

echo "==> Done. Packages in ${OUT_DIR}:"
ls -lh "${OUT_DIR}"/*.deb
