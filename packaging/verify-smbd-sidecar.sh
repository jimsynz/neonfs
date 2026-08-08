#!/usr/bin/env bash
# Stand up an smbd with the NeonFS VFS module loaded, and prove the module
# and the daemon come from the same Samba.
#
# The module cannot be built out-of-tree: it must ABI-match the host smbd and
# carry the distro's private-symbol version node, or smbd silently refuses to
# load it and a client sees "connection refused" with no explanation. So the
# match is not documented here and hoped for — it is enforced three ways:
#
#   1. The daemon is not installed from the archive. Building the module
#      inside the distro's Samba source produces that entire Samba, so the
#      sidecar installs *those* debs — the match is structural rather than
#      pinned, and immune to the archive moving on between the source fetch
#      and the install.
#   2. `samba-vfs-neonfs` declares `Depends: samba (= <exact version>)`, and
#      the version it names is compared against the Samba deb beside it
#      before anything is installed, so a mismatch names both versions
#      instead of arriving as a dpkg dependency error.
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
#        (defaults to the newest such deb under dist/; the Samba debs from
#        the same build are read from $WORKDIR, default .samba-build/)

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"

DEB="${1:-}"
WORKDIR="${WORKDIR:-${REPO_ROOT}/.samba-build}"
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

# The daemon comes from the build, not from the archive. Building the module
# inside the distro's Samba source produces that whole Samba too, so
# installing those debs makes the match structural — no pin to keep current,
# and no exposure to the archive moving on between the source fetch and the
# install, which is a real gap: the binary package can be a point release
# ahead of the source index the module was built from.
# Match on the version, not on whichever file `find` returns first: a
# restored build cache can still hold a previous point release's debs beside
# this build's. dpkg drops the epoch from filenames, and every package from
# one Samba build carries that version — either whole or as a `+samba…`
# suffix — so the version string selects this build's set exactly.
build_version="${module_requires#*:}"

samba_deb="$(find "$WORKDIR" -maxdepth 1 -name "samba_${build_version}_*.deb" \
  ! -name '*-dbgsym_*' | head -1)"

if [ -z "$samba_deb" ]; then
  die "no samba ${build_version} deb in ${WORKDIR} to match the module. Present:
$(find "$WORKDIR" -maxdepth 1 -name 'samba_*.deb' -printf '  %f\n' 2>/dev/null)"
fi

built_samba="$(dpkg-deb -f "$samba_deb" Version)"
[ "$module_requires" = "$built_samba" ] ||
  die "the module needs samba ${module_requires} but the matching file declares ${built_samba}"

log "module and daemon both from samba ${built_samba}"

# --- 2. install that Samba, then the module ------------------------------

apt-get update -qq

# Everything the build produced except the debug companions. Installing the
# whole set rather than picking a subset keeps the inter-package `(= version)`
# dependencies satisfiable from these files; apt fills in the rest from the
# archive.
# The module itself is deliberately not in this set: it collides with the
# `samba` package below and is installed separately, under a check.
mapfile -t built_debs < <(find "$WORKDIR" -maxdepth 1 -name "*${build_version}*.deb" \
  ! -name '*-dbgsym_*' ! -name 'samba-vfs-neonfs_*')
[ "${#built_debs[@]}" -gt 0 ] || die "no debs in ${WORKDIR}"

log "installing ${#built_debs[@]} packages from the build"
DEBIAN_FRONTEND=noninteractive apt-get install -y "${built_debs[@]}"

# Registering the module with `--with-shared-modules` puts `neonfs.so` in the
# `samba` package's own file list as well as in `samba-vfs-neonfs`, so the two
# co-built packages claim the same path and dpkg refuses the second. That
# never bites the release path, where the module is installed against the
# archive's samba, which has no such file.
#
# Overwriting is only safe because both copies come from this one build, so
# assert that rather than assume it: identical bytes, then force.
module_path="$(dpkg-deb -c "$DEB" | awk '$NF ~ /\/neonfs\.so$/ {print $NF}' | sed 's|^\.||')"
[ -n "$module_path" ] || die "the module deb contains no neonfs.so"

extract_dir="$(mktemp -d)"
dpkg-deb -x "$DEB" "${extract_dir}/module"

# Compare build IDs, not bytes: `dh_strip` splits debug symbols per binary
# package, so one build's two copies of the same object differ on disk while
# still being the same compilation. The build ID survives stripping and is
# what actually identifies it.
build_id() {
  readelf -n "$1" 2>/dev/null | sed -nE 's/.*Build ID: ([0-9a-f]+).*/\1/p' | head -1
}

install_opts=()
if [ -e "${module_path}" ]; then
  installed_id="$(build_id "${module_path}")"
  package_id="$(build_id "${extract_dir}/module${module_path}")"

  [ -n "$installed_id" ] && [ -n "$package_id" ] ||
    die "cannot read a build ID from ${module_path} — refusing to overwrite blind"

  [ "$installed_id" = "$package_id" ] ||
    die "${module_path} is already installed with build ID ${installed_id}, but $(basename "$DEB") carries ${package_id} — these are not from the same build"

  log "the co-built samba ships the same module (build ID ${package_id}); overwriting it"
  install_opts+=(-o Dpkg::Options::=--force-overwrite)
fi
rm -rf "$extract_dir"

# Not `dpkg -i`: apt resolves the module's `Depends: samba (= …)` and refuses
# rather than leaving a half-configured package behind.
DEBIAN_FRONTEND=noninteractive apt-get install -y "${install_opts[@]}" "$DEB"

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

# Stopping the daemon has to be explicit, and has to leave the script's exit
# status alone: a trap whose last command is `wait` on a process just killed
# hands the shell that process's 143, so a fully passing run reports SIGTERM.
cleanup() {
  [ -n "${smbd_pid:-}" ] || return 0
  kill "$smbd_pid" 2>/dev/null || true
  wait "$smbd_pid" 2>/dev/null || true
  smbd_pid=""
  return 0
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

cleanup
log "sidecar verified: samba ${installed_samba}, module from $(basename "$DEB")"
exit 0
