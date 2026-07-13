#!/bin/bash
# Build the Samba VFS module (neonfs.so) for the neonfs-cifs Debian package.
#
# A Samba VFS module is ABI-locked to the Samba version it was compiled against
# (SMB_VFS_INTERFACE_VERSION is a compile-time macro) and cannot be built
# out-of-tree, so `build-in-tree.sh` fetches the matching Samba *source* and
# builds against it. Option A (#1527): match the **target Debian release's**
# Samba, discovered at build time from apt, so the module loads in the host
# smbd. This therefore requires the build environment's apt to resolve `samba`
# to the target release's version (the deb targets Debian trixie).
#
# Prints the path of the built neonfs.so on stdout; all diagnostics go to
# stderr. Shared by packaging/build-debs.sh and .forgejo/workflows/release.yml.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

log() { echo "$@" >&2; }

# Samba build prerequisites (build-in-tree.sh apt-installs these via sudo when
# SKIP_APT is unset; the deb builder runs as root without sudo, so install
# here and pass SKIP_APT=1). Mirrors the `vfs_neonfs` CI job.
log "==> Installing Samba build prerequisites..."
apt-get update -qq >&2
apt-get install -y --no-install-recommends >&2 \
    curl python3-dev pkg-config gcc make perl bison flex \
    libjansson-dev libgnutls28-dev libtasn1-6-dev libtasn1-bin \
    libpopt-dev libbsd-dev libcap-dev libacl1-dev libblkid-dev \
    libparse-yapp-perl zlib1g-dev libtirpc-dev liblmdb-dev

# Discover the target distro's Samba upstream version (strip epoch + Debian
# revision / +dfsg suffix) so the module's interface version matches the host.
candidate="$(apt-cache policy samba 2>/dev/null | awk '/Candidate:/ {print $2}')"
if [ -z "${candidate}" ] || [ "${candidate}" = "(none)" ]; then
    log "Error: could not determine the target Samba version (apt-cache policy samba)."
    log "The neonfs-cifs deb must be built where apt resolves 'samba' to the target"
    log "Debian release's version — the module's ABI must match the host smbd (#1527)."
    exit 1
fi
samba_version="${candidate#*:}"        # strip epoch (e.g. 2:)
samba_version="${samba_version%%+*}"   # strip +dfsg...
samba_version="${samba_version%%-*}"   # strip Debian revision
log "==> Target Samba ${samba_version} (from ${candidate})"

work_dir="${REPO_ROOT}/.samba-build"
SAMBA_VERSION="${samba_version}" WORKDIR="${work_dir}" SKIP_APT=1 \
    bash "${REPO_ROOT}/neonfs_cifs/native/vfs_neonfs/build-in-tree.sh" >&2

so="$(find "${work_dir}" -name neonfs.so -path '*modules/vfs*' | head -1)"
if [ -z "${so}" ]; then
    log "Error: neonfs.so was not produced by build-in-tree.sh"
    exit 1
fi
log "==> Built ${so}"
printf '%s\n' "${so}"
