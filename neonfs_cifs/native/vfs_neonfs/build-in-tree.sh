#!/usr/bin/env bash
# Build vfs_neonfs.so against a pinned, in-tree Samba source.
#
# Samba VFS modules cannot be built out-of-tree (SambaWiki: "Writing a Samba
# VFS Module"), so this fetches the pinned Samba release, drops the module +
# ETF wire client into source3/modules/, registers erl_interface (ei) as a
# link dependency in configure, configures a minimal file-server build, and
# builds just the vfs_neonfs target.
#
# Verified against Samba 4.24.3. Used by CI (cache $WORKDIR) and for local
# iteration. Requires: sudo apt, a C toolchain, python3, perl, and Erlang on
# PATH (the ei headers/libs are discovered from the running Erlang install).
set -euo pipefail

SAMBA_VERSION="${SAMBA_VERSION:-4.24.3}"
WORKDIR="${WORKDIR:-$PWD/.samba-build}"
HERE="$(cd "$(dirname "$0")" && pwd)"
SRC="$WORKDIR/samba-${SAMBA_VERSION}"

ERL="${ERL:-erl}"
EI_DIR="$("$ERL" -noshell -eval 'io:format("~ts",[code:lib_dir(erl_interface)]),halt().')"
if [ ! -f "$EI_DIR/include/ei.h" ]; then
  echo "erl_interface not found at $EI_DIR (need Erlang with ei on PATH)" >&2
  exit 2
fi

mkdir -p "$WORKDIR"

if [ ! -d "$SRC" ]; then
  tarball="$WORKDIR/samba-${SAMBA_VERSION}.tar.gz"
  [ -f "$tarball" ] || curl -fsSL \
    "https://download.samba.org/pub/samba/stable/samba-${SAMBA_VERSION}.tar.gz" \
    -o "$tarball"
  tar -C "$WORKDIR" -xzf "$tarball"
fi

# Minimal build deps for a file-server-only Samba (no AD DC).
if [ "${SKIP_APT:-}" != "1" ]; then
  sudo apt-get update -qq
  sudo apt-get install -y --no-install-recommends \
    python3-dev pkg-config gcc make perl bison flex \
    libjansson-dev libgnutls28-dev libtasn1-6-dev libtasn1-bin \
    libpopt-dev libbsd-dev libcap-dev libacl1-dev libblkid-dev \
    libparse-yapp-perl zlib1g-dev libtirpc-dev liblmdb-dev
fi

# Drop the module + wire client into the Samba source tree.
cp "$HERE/vfs_neonfs.c" "$HERE/wire.c" "$HERE/wire.h" "$SRC/source3/modules/"

# Register erl_interface (ei) as a linkable dependency in source3 configure,
# so `deps='ei'` on the module links -lei from the Erlang install (idempotent).
python3 - "$SRC/source3/wscript" "$EI_DIR" <<'PY'
import sys
ws, ei = sys.argv[1], sys.argv[2]
s = open(ws).read()
if "LIBPATH_EI_NEONFS" not in s:
    needle = "def configure(conf):\n"
    i = s.find(needle) + len(needle)
    snippet = (
        "    # vfs_neonfs: register erl_interface (ei) for the ETF wire client.\n"
        "    conf.env.append_value('LIBPATH', '%s/lib')  # LIBPATH_EI_NEONFS\n"
        "    conf.CHECK_LIB('ei', shlib=False)\n" % ei
    )
    s = s[:i] + snippet + s[i:]
    open(ws, "w").write(s)
PY

# Register the module (idempotent — replace any prior block).
python3 - "$SRC/source3/modules/wscript_build" "$EI_DIR" <<'PY'
import sys
wsb, ei = sys.argv[1], sys.argv[2]
s = open(wsb).read()
i = s.find("bld.SAMBA3_MODULE('vfs_neonfs'")
if i != -1:
    s = s[:i].rstrip() + "\n"
s += (
"\nbld.SAMBA3_MODULE('vfs_neonfs',\n"
"                  subsystem='vfs',\n"
"                  source='vfs_neonfs.c wire.c',\n"
"                  deps='samba-util ei',\n"
"                  includes='%s/include',\n"
"                  init_function='',\n"
"                  internal_module=bld.SAMBA3_IS_STATIC_MODULE('vfs_neonfs'),\n"
"                  enabled=bld.SAMBA3_IS_ENABLED_MODULE('vfs_neonfs'))\n" % ei
)
open(wsb, "w").write(s)
PY

cd "$SRC"
./configure --without-ad-dc --without-ldap --without-ads --without-winbind \
  --without-libarchive --without-pam --without-systemd --without-regedit \
  --with-shared-modules='!vfs_snapper,!vfs_ceph,!vfs_glusterfs,!vfs_gpfs,vfs_neonfs'

# waf refuses to run without PYTHONHASHSEED set (Samba's configure/make sets it).
PYTHONHASHSEED=1 python3 ./buildtools/bin/waf build --targets=vfs_neonfs

so="$(find bin -name 'neonfs.so' -path '*modules/vfs*' | head -1)"
if [ -z "$so" ]; then
  echo "vfs_neonfs.so was not produced" >&2
  exit 1
fi
echo "built: $so"
file -L "$so"
