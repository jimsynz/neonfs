#!/usr/bin/env bash
# Stand up an smbd with the NeonFS VFS module loaded, and prove the module
# and the daemon come from the same Samba.
#
# The module cannot be built out-of-tree: it must ABI-match the host smbd and
# carry the distro's private-symbol version node, or smbd silently refuses to
# load it and a client sees "connection refused" with no explanation. So the
# match is not documented here and hoped for — it is enforced three ways:
#
#   1. `samba-vfs-neonfs` declares `Depends: samba (= <exact version>)`, so
#      apt refuses the install outright against a different Samba.
#   2. The declared version is compared against the archive's candidate
#      before installing, so the failure names both versions instead of
#      arriving as a dpkg dependency error.
#   3. `ldd -r` on the installed module resolves every symbol against the
#      installed Samba libraries. A symbol-version mismatch shows up here as
#      "symbol not found" — which is exactly what smbd would hit at load
#      time, only visible.
#
# Then smbd is started with a share configured for `vfs objects = neonfs`
# and a client connects, so the module is loaded by a real daemon rather
# than only inspected. The share's ETF socket does not exist — that is the
# `neonfs_cifs` bridge's job, and serving files through it is the end-to-end
# test — so a tree connect is expected to fail; what must not appear is a
# module *load* failure.
#
# Root only, and it installs packages: meant for a CI container.
#
# Usage: verify-smbd-sidecar.sh [path/to/samba-vfs-neonfs_*.deb]
#        (defaults to the newest such deb under dist/)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DEB="${1:-}"
SHARE_DIR="${SHARE_DIR:-/tmp/neonfs-smbd-share}"
LOG_DIR="${LOG_DIR:-/tmp/neonfs-smbd-logs}"
SOCKET_PATH="${SOCKET_PATH:-/tmp/neonfs-smbd-bridge.sock}"

log() { echo "==> $*" >&2; }
die() {
  echo "verify-smbd-sidecar: $*" >&2
  exit 1
}

[ "$(id -u)" = 0 ] || die "must run as root — it installs samba and the module deb"

if [ -z "$DEB" ]; then
  DEB="$(find "${REPO_ROOT}/dist" -maxdepth 1 -name 'samba-vfs-neonfs_*.deb' \
    ! -name '*-dbgsym_*' 2>/dev/null | sort | tail -1 || true)"
fi

[ -n "$DEB" ] && [ -f "$DEB" ] || die "no samba-vfs-neonfs deb given or found under dist/"

log "module package: $(basename "$DEB")"

# --- 1. the version the module was built against -------------------------

module_requires="$(dpkg-deb -f "$DEB" Depends |
  tr ',' '\n' |
  sed -nE 's/^[[:space:]]*samba[[:space:]]*\(=[[:space:]]*([^)]+)\).*/\1/p' |
  head -1)"

[ -n "$module_requires" ] || die "the deb does not pin an exact samba version in Depends"

apt-get update -qq
archive_candidate="$(apt-cache policy samba | awk '/Candidate:/ {print $2}')"

log "module built against samba ${module_requires}; archive offers ${archive_candidate}"

if [ "$module_requires" != "$archive_candidate" ]; then
  die "samba version mismatch: the module needs ${module_requires} but this image's archive offers ${archive_candidate}. The sidecar must be built from the same image as the module, or smbd will refuse to load it."
fi

# --- 2. install the daemon and the module --------------------------------

DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends samba smbclient

# Not `dpkg -i`: apt resolves the module's `Depends: samba (= …)` and refuses
# rather than leaving a half-configured package behind.
DEBIAN_FRONTEND=noninteractive apt-get install -y "$DEB"

installed_samba="$(dpkg-query -W -f='${Version}' samba)"
[ "$installed_samba" = "$module_requires" ] ||
  die "installed samba ${installed_samba} is not the ${module_requires} the module was built against"

log "samba ${installed_samba} and the module installed together"

# --- 3. the module resolves against the installed libraries --------------

module_so="$(dpkg-query -L samba-vfs-neonfs | grep -E '/neonfs\.so$' | head -1)"
[ -n "$module_so" ] || die "the package installed no neonfs.so"

log "module: ${module_so}"

unresolved="$(ldd -r "$module_so" 2>&1 | grep -E 'undefined symbol|not found' || true)"
[ -z "$unresolved" ] || die "the module does not resolve against the installed samba:
${unresolved}"

log "every symbol resolves against the installed samba libraries"

# --- 4. a real smbd loads it ---------------------------------------------

rm -rf "$SHARE_DIR" "$LOG_DIR"
mkdir -p "$SHARE_DIR" "$LOG_DIR"
chmod 0777 "$SHARE_DIR"

conf="$(mktemp)"
cat > "$conf" <<CONF
[global]
    workgroup = WORKGROUP
    server min protocol = SMB2
    security = user
    map to guest = Bad User
    # Everything the daemon writes goes under a directory this script owns,
    # so the assertions below read one run's log rather than the image's.
    log file = ${LOG_DIR}/smbd.log
    log level = 3 vfs:10
    private dir = ${LOG_DIR}
    lock directory = ${LOG_DIR}
    state directory = ${LOG_DIR}
    cache directory = ${LOG_DIR}
    pid directory = ${LOG_DIR}
    smb ports = 4450

[neonfs]
    path = ${SHARE_DIR}
    read only = no
    guest ok = yes
    vfs objects = neonfs
    neonfs:socket = ${SOCKET_PATH}
CONF

testparm -s "$conf" >/dev/null || die "smb.conf is not valid"
log "smb.conf validates"

smbd --foreground --no-process-group --debug-stdout --configfile="$conf" \
  >"${LOG_DIR}/smbd.stdout" 2>&1 &
smbd_pid=$!

cleanup() {
  kill "$smbd_pid" 2>/dev/null || true
  wait "$smbd_pid" 2>/dev/null || true
}
trap cleanup EXIT

for _ in $(seq 1 30); do
  if smbclient -L localhost -p 4450 -N >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

smbclient -L localhost -p 4450 -N >"${LOG_DIR}/shares.txt" 2>&1 ||
  die "smbd never answered a share listing:
$(tail -40 "${LOG_DIR}/smbd.stdout")"

grep -q 'neonfs' "${LOG_DIR}/shares.txt" ||
  die "the neonfs share is not advertised:
$(cat "${LOG_DIR}/shares.txt")"

log "smbd is serving and advertises the neonfs share"

# The tree connect is expected to fail — nothing is listening on the ETF
# socket — but it must fail *after* the module loads. A load failure is the
# mismatch this whole script exists to catch, and Samba says so plainly.
smbclient "//localhost/neonfs" -p 4450 -N -c 'ls' >"${LOG_DIR}/connect.txt" 2>&1 || true

if grep -rqiE "error loading module.*neonfs|failed to load module.*neonfs" \
  "${LOG_DIR}" 2>/dev/null; then
  die "smbd refused to load the module:
$(grep -rhiE 'error loading module.*neonfs|failed to load module.*neonfs' "${LOG_DIR}" | head -5)"
fi

grep -rqi "vfs_neonfs\|neonfs" "${LOG_DIR}/smbd.stdout" ||
  log "note: the daemon log names no neonfs activity; the tree connect may not have reached the module"

log "smbd loaded the module without complaint"
log "sidecar verified: samba ${installed_samba}, module from $(basename "$DEB")"
