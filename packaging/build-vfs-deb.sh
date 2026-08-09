#!/usr/bin/env bash
# Build the samba-vfs-neonfs Debian package.
#
# A Samba VFS module can't be built out-of-tree, must ABI-match the host smbd,
# and — critically — must be linked against the *same* Samba private libraries
# with the *same* symbol-version node the distro's samba-libs provides (a
# bespoke ./configure produces `SAMBA_<upstream>_PRIVATE_SAMBA`, which the
# distro's `SAMBA_<upstream>_DEBIAN_<debver>_PRIVATE_SAMBA` does not satisfy —
# so smbd refuses to load it). The only robust way to get all of that is
# to build the module inside the distro's own samba source package: we
# `apt-get source samba`, drop `vfs_neonfs` into `source3/modules`, add it to
# `--with-shared-modules`, add a `samba-vfs-neonfs` binary package that
# `Depends: samba (= exact version)` (mirroring `samba-vfs-ceph`), and build it
# with the distro's `debian/rules` via dpkg-buildpackage. The module then links
# and symbol-versions exactly like the distro's own VFS modules.
#
# The build environment must resolve `samba` to the target release's version
# (the deb targets Debian trixie) and provide the project's Erlang version for
# the statically-linked ei client. Non-root local invocations therefore rerun
# automatically in the same versioned Debian image used by CI; CI/release
# containers already run as root and execute the build directly.
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

run_in_build_container() {
  local elixir_version erlang_version rust_version image host_uid host_gid

  command -v docker >/dev/null 2>&1 || {
    log "docker is required for a non-root samba-vfs-neonfs build"
    exit 2
  }

  elixir_version="$(awk '/^elixir/ {print $2}' "${REPO_ROOT}/.tool-versions")"
  erlang_version="$(awk '/^erlang/ {print $2}' "${REPO_ROOT}/.tool-versions")"
  rust_version="$(awk '/^rust/ {print $2}' "${REPO_ROOT}/.tool-versions")"
  image="harton.dev/james/workflows/elixir-rust:${elixir_version}-erlang-${erlang_version}-rust-${rust_version}"
  host_uid="$(id -u)"
  host_gid="$(id -g)"

  mkdir -p "${WORKDIR}" "${OUT_DIR}"
  NATIVE="$(cd "${NATIVE}" && pwd)"
  OUT_DIR="$(cd "${OUT_DIR}" && pwd)"
  WORKDIR="$(cd "${WORKDIR}" && pwd)"
  log "==> building in ${image}"

  docker run --rm \
    -v "${REPO_ROOT}:/workspaces/neonfs:ro" \
    -v "${NATIVE}:/native:ro" \
    -v "${OUT_DIR}:/out" \
    -v "${WORKDIR}:/workspaces/neonfs/.samba-build" \
    -e NATIVE=/native \
    -e OUT_DIR=/out \
    -e WORKDIR=/workspaces/neonfs/.samba-build \
    -e HOST_UID="${host_uid}" \
    -e HOST_GID="${host_gid}" \
    "${image}" \
    bash -c 'status=0; bash /workspaces/neonfs/packaging/build-vfs-deb.sh || status=$?; chown -R "${HOST_UID}:${HOST_GID}" /out /workspaces/neonfs/.samba-build; exit "${status}"'
}

if [ "$(id -u)" -ne 0 ]; then
  run_in_build_container
  exit $?
fi

EI_DIR="$("$ERL" -noshell -eval 'io:format("~ts",[code:lib_dir(erl_interface)]),halt().' 2>/dev/null || true)"
[ -f "${EI_DIR}/include/ei.h" ] || { log "erl_interface not found (need Erlang with ei on PATH; ERL=${ERL})"; exit 2; }

enable_deb_src() {
  local f extra
  for f in /etc/apt/sources.list.d/*.sources; do
    [ -f "$f" ] || continue
    if grep -qE '^Types:' "$f" && ! grep -qE '^Types:.* deb-src' "$f"; then
      sed -i -E 's/^(Types:.*)$/\1 deb-src/' "$f"
    fi
  done
  for f in /etc/apt/sources.list /etc/apt/sources.list.d/*.list; do
    [ -f "$f" ] || continue
    if grep -qE '^deb ' "$f" && ! grep -qE '^deb-src ' "$f"; then
      extra="$(sed -nE 's/^deb (.*)/deb-src \1/p' "$f")"
      [ -n "$extra" ] && printf '%s\n' "$extra" | tee -a "$f" >/dev/null
    fi
  done
}

mkdir -p "${WORKDIR}" "${OUT_DIR}"

# The build toolchain + samba build-deps are needed on every run — a cached
# tree ($WORKDIR is cached in CI) still has to relink the module and repackage
# in a fresh container that has none of them installed.
log "==> enabling deb-src + installing the samba build toolchain"
enable_deb_src
apt-get update -qq
apt-get install -y --no-install-recommends dpkg-dev >/dev/null
apt-get build-dep -y samba >/dev/null

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

# A restored cache brings the previous run's .debs back with it. They are all
# regenerated below, and leaving them is how a stale module (or a stale samba
# beside it) gets picked up after a point release moves the version.
rm -f "${WORKDIR}"/*.deb

# --- drop the module + wire client into the tree ---
cp "${NATIVE}/vfs_neonfs.c" "${NATIVE}/wire.c" "${NATIVE}/wire.h" "${SRC}/source3/modules/"

# --- register ei as a link dep in source3 configure (idempotent) ---
python3 - "${SRC}/source3/wscript" "${EI_DIR}" <<'PY'
import sys
ws, ei = sys.argv[1], sys.argv[2]
s = open(ws).read()
s = "".join(line for line in s.splitlines(keepends=True)
            if "LIBPATH_EI_NEONFS" not in line
            and "conf.CHECK_LIB('ei', shlib=False)" not in line)
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

# --- build vfs_neonfs as a shared module (and repair previously cached trees) ---
python3 - "${SRC}/debian/rules" <<'PY'
import sys
rules = sys.argv[1]
s = open(rules).read().replace(",vfs_neonfs", "")
needle = "--with-shared-modules=vfs_dfs_samba4,vfs_nfs4acl_xattr,auth_samba4"
if needle not in s:
    raise SystemExit("Samba shared-module configuration not found")
s = s.replace(needle, needle + ",vfs_neonfs", 1)
open(rules, "w").write(s)
PY

# --- keep the module out of the samba package (idempotent) ---
#
# `debian/samba.install` claims `samba/vfs/*.so` wholesale, so without this
# `neonfs.so` ships in both `samba` and `samba-vfs-neonfs` and the two cannot
# be installed together. Debian solves it for its own split-out modules by
# deleting them from the samba staging tree after dh_install — join that list
# rather than inventing a mechanism.
python3 - "${SRC}/debian/rules" <<'RULES'
import sys

rules = sys.argv[1]
s = open(rules).read()

# Drop any line this script added to a previously cached tree.
s = "".join(l for l in s.splitlines(keepends=True) if "samba/vfs/neonfs.so" not in l)

needle = "execute_after_dh_install-arch:"
i = s.find(needle)
if i == -1:
    raise SystemExit("execute_after_dh_install-arch not found in debian/rules")

marker = "rm -f \\\n"
j = s.find(marker, i)
if j == -1:
    raise SystemExit("no `rm -f` module removal after execute_after_dh_install-arch")

j += len(marker)
line = "\t    debian/samba/usr/lib/${DEB_HOST_MULTIARCH}/samba/vfs/neonfs.so \\\n"
open(rules, "w").write(s[:j] + line + s[j:])
RULES

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
#
# Build the whole Samba source package without debug info. Debian's default
# `-g -O2` puts DWARF in every object of a 6600-step build and then has
# `dh_strip` copy it back out into a `-dbgsym` deb per binary package, and
# nothing here consumes any of it — the module ships stripped and the dbgsym
# companions are discarded. Dropping it halves the peak footprint, measured
# on a cold amd64 build: 1.6G of tree becomes 828M, `bin/default` 911M becomes
# 365M, and 27 dbgsym debs stop being built at all. `-g0` wins over the
# earlier `-g` from dpkg-buildflags; `noautodbgsym` handles dh_strip.
export DEB_BUILD_OPTIONS="nocheck noautodbgsym"
export DEB_CFLAGS_APPEND="-g0"
export DEB_CXXFLAGS_APPEND="-g0"

cd "${SRC}"
if [ "${fresh}" = 1 ]; then
  log "==> dpkg-buildpackage (clean build — first run)"
  dpkg-buildpackage -b -uc -us
else
  # Cached tree: reconfigure for the current versioned Erlang image, force the
  # module to relink, and skip the clean step so compiled Samba objects remain.
  #
  # `debian/files` has to go with them. It is the previous build's manifest,
  # `-nc` skips the `dh_clean` that would remove it, and this run deletes the
  # debs it names — so every entry that the run does not regenerate dangles and
  # `dpkg-genbuildinfo` fails trying to stat it. Nothing regenerates the
  # `-dbgsym` entries now that `noautodbgsym` is set.
  log "==> dpkg-buildpackage -nc (incremental — cached tree)"
  rm -f bin/configured.stamp bin/built.stamp debian/files
  dpkg-buildpackage -b -nc -uc -us
fi

# Ship only the module package, not its -dbgsym companion (nothing else in the
# release produces a dbgsym; no need to publish one).
#
# Pick it by the version the tree just built, not by whichever `find` returns
# first. A cached `.samba-build` still holds the *previous* run's deb beside
# the new one, and when the distro moves (u1 → u2) those differ: the stale
# module then goes out against a Samba it does not match, which is the exact
# mismatch smbd refuses to load. Seen for real — a u1 module deb selected next
# to freshly built u2 Samba binaries.
tree_version="$(sed -nE '1s/^[^(]*\(([^)]+)\).*/\1/p' "${SRC}/debian/changelog")"
[ -n "${tree_version}" ] || { log "cannot read the built version from debian/changelog"; exit 1; }

# dpkg drops the epoch from filenames, so match on what the name can carry.
file_version="${tree_version#*:}"
deb="$(find "${WORKDIR}" -maxdepth 1 \
  -name "samba-vfs-neonfs_${file_version}_*.deb" ! -name '*-dbgsym_*' | head -1)"
if [ -z "${deb}" ]; then
  log "no samba-vfs-neonfs deb at the built version ${tree_version}; the workdir holds:"
  find "${WORKDIR}" -maxdepth 1 -name 'samba-vfs-neonfs_*.deb' -printf '  %f\n' >&2 || true
  exit 1
fi
cp -f "${deb}" "${OUT_DIR}/"
log "==> built $(basename "${deb}") -> ${OUT_DIR}"
printf '%s\n' "${OUT_DIR}/$(basename "${deb}")"
