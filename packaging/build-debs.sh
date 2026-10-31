#!/bin/bash
# Build NeonFS Debian packages from a workspace checkout.
#
# This repo sits inside a neonfs_workspace checkout alongside every NeonFS
# package repo (`bin/neonfs-sync` clones them). Each package is built in its
# own checkout and packaged with its own `packaging/nfpm/` configs; the shared
# maintainer scripts come from the workspace, and `neonfs-common` from here.
# CI publishes these same packages from each repo's tags — this script exists
# for building them locally, as the test rig does.
#
# Usage:
#   VERSION=0.1.0 ./packaging/build-debs.sh            # native build
#   VERSION=0.1.0 ARCH=arm64 ./packaging/build-debs.sh  # cross-compile via Docker
#   VERSION=0.1.0 USE_DOCKER=1 ./packaging/build-debs.sh # force Docker for native arch
#
# Environment variables:
#   VERSION          - Package version (required)
#   ARCH             - Target architecture: amd64, arm64 (default: auto-detect)
#   OUT_DIR          - Output directory for .deb files (default: ./dist)
#   USE_DOCKER       - Set to 1 to force Docker even for native architecture
#   COMPONENTS       - Service repos to build (default: every daemon)
#   NEONFS_BUILD_CIFS - Also build neonfs_cifs and the samba-vfs-neonfs deb
#   NEONFS_DEPS      - `local` builds every package against its sibling
#                      checkouts rather than the tags each pins

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
WORKSPACE="$(cd "${REPO_ROOT}/.." && pwd)"

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
COMPONENTS="${COMPONENTS:-neonfs_core neonfs_fuse neonfs_nfs neonfs_s3 neonfs_webdav neonfs_docker neonfs_containerd neonfs_block neonfs_omnibus}"
[ -n "${NEONFS_BUILD_CIFS:-}" ] && COMPONENTS="${COMPONENTS} neonfs_cifs"

for repo in neonfs_workspace neonfs_cli ${COMPONENTS}; do
    dir="${WORKSPACE}/${repo}"
    [ "${repo}" = neonfs_workspace ] && dir="${WORKSPACE}"
    [ -d "${dir}" ] || {
        echo "Error: ${dir} not found — run bin/neonfs-sync in the workspace first"
        exit 1
    }
done
[ -d "${WORKSPACE}/packaging/scripts" ] || {
    echo "Error: ${WORKSPACE} is not a neonfs_workspace checkout (no packaging/scripts)"
    exit 1
}

# --- Docker-based cross-compilation ---

host_arch=""
case "$(uname -m)" in
    x86_64)  host_arch="amd64" ;;
    aarch64) host_arch="arm64" ;;
esac

if [ "${_NEONFS_IN_DOCKER:-}" != "1" ] && { [ "$ARCH" != "$host_arch" ] || [ "${USE_DOCKER:-}" = "1" ]; }; then
    BUILDER_IMAGE="neonfs-deb-builder:${ARCH}"
    ELIXIR_VERSION=$(awk '/^elixir/ {sub(/-otp-[0-9]+$/, "", $2); print $2}' "${REPO_ROOT}/.tool-versions")
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
        "${REPO_ROOT}/containers"

    mkdir -p "${OUT_DIR}"
    echo "==> Running build inside Docker..."
    docker run --rm \
        --platform "linux/${ARCH}" \
        -v "${WORKSPACE}:/src:ro" \
        -v "${OUT_DIR}:/out" \
        -e VERSION="${VERSION}" \
        -e ARCH="${ARCH}" \
        -e OUT_DIR=/out \
        -e COMPONENTS="${COMPONENTS}" \
        -e NEONFS_DEPS="${NEONFS_DEPS:-}" \
        -e _NEONFS_IN_DOCKER=1 \
        "${BUILDER_IMAGE}" \
        "/src/$(basename "${REPO_ROOT}")/packaging/build-debs.sh"

    echo "==> Done. Packages in ${OUT_DIR}:"
    ls -lh "${OUT_DIR}"/*.deb
    exit 0
fi

# --- Native build (or running inside Docker) ---

if [ "${_NEONFS_IN_DOCKER:-}" = "1" ]; then
    echo "==> Copying the workspace to a build directory..."
    cp -a /src /build
    WORKSPACE=/build
    REPO_ROOT="/build/$(basename "${REPO_ROOT}")"
fi

mkdir -p "${OUT_DIR}"
export VERSION ARCH
export COMMON="${WORKSPACE}/packaging"

# nfpm resolves `${BUILD_DIR}` paths in each config; every repo's configs are
# written relative to that repo's root.
package_repo() {
    local dir="$1" config generated
    for config in "${dir}"/packaging/nfpm/*.yaml; do
        echo "    $(basename "${config}" .yaml)..."
        generated="$(mktemp --suffix=.yaml -p "${dir}/packaging/nfpm")"
        BUILD_DIR="${dir}" envsubst < "${config}" > "${generated}"
        nfpm package --config "${generated}" --packager deb --target "${OUT_DIR}/"
        rm -f "${generated}"
    done
}

echo "==> Building NeonFS ${VERSION} for ${ARCH}"

echo "==> Building and packaging the CLI..."
( cd "${WORKSPACE}/neonfs_cli" && cargo build --release )
package_repo "${WORKSPACE}/neonfs_cli"

echo "==> Packaging neonfs-common..."
package_repo "${REPO_ROOT}"

# `mix release --overwrite` does not delete a previous build's
# `lib/<app>-<old_vsn>/` directories, and a stale one has shipped an empty
# `priv/` that way, so release builds start clean.
for component in ${COMPONENTS}; do
    echo "==> Building and packaging ${component}..."
    dir="${WORKSPACE}/${component}"
    (
        cd "${dir}"
        MIX_ENV=prod mix deps.get --only prod
        rm -rf _build/prod/rel _build/prod/lib/neonfs_*
        MIX_ENV=prod mix release --overwrite
    )
    package_repo "${dir}"
done

if [ -n "${NEONFS_BUILD_CIFS:-}" ]; then
    echo "==> Building samba-vfs-neonfs.deb..."
    OUT_DIR="${OUT_DIR}" VERSION="${VERSION}" \
        bash "${WORKSPACE}/neonfs_cifs/packaging/build-vfs-deb.sh" >/dev/null
fi

echo "==> Done. Packages in ${OUT_DIR}:"
ls -lh "${OUT_DIR}"/*.deb
