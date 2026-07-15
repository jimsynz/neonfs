#!/usr/bin/env bash
# Build the samba-vfs-neonfs Debian package.
#
# A Samba VFS module can't be built out-of-tree, must ABI-match the host smbd,
# and — critically — must be linked against the *same* Samba private libraries
# with the *same* symbol-version node the distro's samba-libs provides (a
# bespoke ./configure produces `SAMBA_<upstream>_PRIVATE_SAMBA`, which the
# distro's `SAMBA_<upstream>_DEBIAN_<debver>_PRIVATE_SAMBA` does not satisfy —
# so smbd refuses to load it, #1548). The only robust way to get all of that is
# to build the module inside the distro's own samba source package: we
# `apt-get source samba`, drop `vfs_neonfs` into `source3/modules`, add it to
# `--with-shared-modules`, add a `samba-vfs-neonfs` binary package that
# `Depends: samba (= exact version)` (mirroring `samba-vfs-ceph`), and build it
# with the distro's `debian/rules` via dpkg-buildpackage. The module then links
# and symbol-versions exactly like the distro's own VFS modules.
#
# Must run where apt resolves `samba` to the target release's version (the deb
# targets Debian trixie). Requires: apt (deb-src is enabled here), a C
# toolchain, and Erlang on PATH (the ei headers/libs for the ETF wire client
# are discovered from the running Erlang install and linked statically).
#
# Places samba-vfs-neonfs_*.deb in OUT_DIR and prints the main .deb path.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
# Overridable so the container build can point at a copied-in module dir
# without the full repo layout.
NATIVE="${NATIVE:-${REPO_ROOT}/neonfs_cifs/native/vfs_neonfs}"
OUT_DIR="${OUT_DIR:-${REPO_ROOT}/dist}"
WORKDIR="${WORKDIR:-${REPO_ROOT}/.samba-build}"
ERL="${ERL:-erl}"

log() { echo "$@" >&2; }
SUDO=""; [ "$(id -u)" = 0 ] || SUDO=sudo

EI_DIR="$("$ERL" -noshell -eval 'io:format("~ts",[code:lib_dir(erl_interface)]),halt().' 2>/dev/null || true)"
[ -f "${EI_DIR}/include/ei.h" ] || { log "erl_interface not found (need Erlang with ei on PATH; ERL=${ERL})"; exit 2; }

enable_deb_src() {
  local f extra
  for f in /etc/apt/sources.list.d/*.sources; do
    [ -f "$f" ] || continue
    if grep -qE '^Types:' "$f" && ! grep -qE '^Types:.* deb-src' "$f"; then
      $SUDO sed -i -E 's/^(Types:.*)$/\1 deb-src/' "$f"
    fi
  done
  for f in /etc/apt/sources.list /etc/apt/sources.list.d/*.list; do
    [ -f "$f" ] || continue
    if grep -qE '^deb ' "$f" && ! grep -qE '^deb-src ' "$f"; then
      extra="$(sed -nE 's/^deb (.*)/deb-src \1/p' "$f")"
      [ -n "$extra" ] && printf '%s\n' "$extra" | $SUDO tee -a "$f" >/dev/null
    fi
  done
}

mkdir -p "${WORKDIR}" "${OUT_DIR}"

# The build toolchain + samba build-deps are needed on every run — a cached
# tree ($WORKDIR is cached in CI) still has to relink the module and repackage
# in a fresh container that has none of them installed.
log "==> enabling deb-src + installing the samba build toolchain"
enable_deb_src
$SUDO apt-get update -qq
$SUDO apt-get install -y --no-install-recommends dpkg-dev >/dev/null
$SUDO apt-get build-dep -y samba >/dev/null

# Reuse a cached source tree only if it still matches the distro's samba (a
# stale tree would build against the wrong private-symbol version). Otherwise
# fetch the source fresh.
fresh=0
cand="$(apt-cache policy samba 2>/dev/null | awk '/Candidate:/ {print $2}')"
SRC="$(find "${WORKDIR}" -mindepth 1 -maxdepth 1 -type d -name 'samba-*' 2>/dev/null | head -1 || true)"
if [ -n "${SRC}" ]; then
  treever="$(sed -nE '1s/^[^(]*\(([^)]+)\).*/\1/p' "${SRC}/debian/changelog" 2>/dev/null || true)"
  if [ -z "${treever}" ] || { [ -n "${cand}" ] && [ "${cand}" != "(none)" ] && [ "${treever}" != "${cand}" ]; }; then
    log "==> cached tree (${treever:-unknown}) != distro samba (${cand}); refetching"
    rm -rf "${SRC}"; SRC=""
  fi
fi
if [ -z "${SRC}" ]; then
  fresh=1
  ( cd "${WORKDIR}" && apt-get source samba )
  SRC="$(find "${WORKDIR}" -mindepth 1 -maxdepth 1 -type d -name 'samba-*' 2>/dev/null | head -1 || true)"
  [ -n "${SRC}" ] || { log "apt-get source samba produced no samba-* tree"; exit 1; }
fi
log "==> samba source: ${SRC}  (ei: ${EI_DIR})"

# --- drop the module + wire client into the tree ---
cp "${NATIVE}/vfs_neonfs.c" "${NATIVE}/wire.c" "${NATIVE}/wire.h" "${SRC}/source3/modules/"

# --- register ei as a link dep in source3 configure (idempotent) ---
python3 - "${SRC}/source3/wscript" "${EI_DIR}" <<'PY'
import sys
ws, ei = sys.argv[1], sys.argv[2]
s = open(ws).read()
if "LIBPATH_EI_NEONFS" not in s:
    needle = "def configure(conf):\n"
    i = s.find(needle) + len(needle)
    s = s[:i] + ("    conf.env.append_value('LIBPATH', '%s/lib')  # LIBPATH_EI_NEONFS\n"
                 "    conf.CHECK_LIB('ei', shlib=False)\n" % ei) + s[i:]
    open(ws, "w").write(s)
PY

# --- register the module (idempotent) ---
python3 - "${SRC}/source3/modules/wscript_build" "${EI_DIR}" <<'PY'
import sys
wsb, ei = sys.argv[1], sys.argv[2]
s = open(wsb).read()
i = s.find("bld.SAMBA3_MODULE('vfs_neonfs'")
if i != -1:
    s = s[:i].rstrip() + "\n"
s += ("\nbld.SAMBA3_MODULE('vfs_neonfs',\n"
"                  subsystem='vfs',\n"
"                  source='vfs_neonfs.c wire.c',\n"
"                  deps='samba-util ei',\n"
"                  includes='%s/include',\n"
"                  init_function='',\n"
"                  internal_module=bld.SAMBA3_IS_STATIC_MODULE('vfs_neonfs'),\n"
"                  enabled=bld.SAMBA3_IS_ENABLED_MODULE('vfs_neonfs'))\n" % ei)
open(wsb, "w").write(s)
PY

# --- build vfs_neonfs as a shared module ---
grep -q ',vfs_neonfs' debian/rules 2>/dev/null || \
  sed -i 's/--with-shared-modules=vfs_dfs_samba4,vfs_nfs4acl_xattr,auth_samba4/&,vfs_neonfs/' "${SRC}/debian/rules"

# --- new binary package (mirrors samba-vfs-ceph) ---
if ! grep -q '^Package: samba-vfs-neonfs' "${SRC}/debian/control"; then
cat >> "${SRC}/debian/control" <<'CTL'

Package: samba-vfs-neonfs
Architecture: any
Depends: samba (= ${binary:Version}), ${misc:Depends}, ${shlibs:Depends}
Enhances: samba
Description: Samba Virtual FileSystem module for NeonFS
 A stacked VFS module bridging Samba's smbd to a NeonFS volume over the
 neonfs_cifs ETF socket. Built in-tree against this Samba source so its ABI
 and private-symbol versions match the host smbd.
CTL
fi
# DEB_HOST_MULTIARCH is a dh substitution, resolved by dh_install at build
# time — it must stay literal in the .install file, hence single quotes.
# shellcheck disable=SC2016
echo 'usr/lib/${DEB_HOST_MULTIARCH}/samba/vfs/neonfs.so' > "${SRC}/debian/samba-vfs-neonfs.install"

# --- build ---
cd "${SRC}"
if [ "${fresh}" = 1 ]; then
  log "==> dpkg-buildpackage (clean build — first run)"
  DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage -b -uc -us
else
  # Cached tree: force the module (and only it) to relink, keep the rest of the
  # samba build, and skip the clean step so the cache is actually reused.
  log "==> dpkg-buildpackage -nc (incremental — cached tree)"
  rm -f bin/built.stamp
  DEB_BUILD_OPTIONS=nocheck dpkg-buildpackage -b -nc -uc -us
fi

# Ship only the module package, not its -dbgsym companion (nothing else in the
# release produces a dbgsym; no need to publish one).
deb="$(find "${WORKDIR}" -maxdepth 1 -name 'samba-vfs-neonfs_*.deb' ! -name '*-dbgsym_*' | head -1)"
[ -n "${deb}" ] || { log "samba-vfs-neonfs deb was not produced"; exit 1; }
cp -f "${deb}" "${OUT_DIR}/"
log "==> built $(basename "${deb}") -> ${OUT_DIR}"
printf '%s\n' "${OUT_DIR}/$(basename "${deb}")"
