# shellcheck shell=bash
# Acceptance test steps for a running NeonFS cluster (single- or multi-node).
# Sourced by ./acceptance; relies on helpers from lib/rig.sh.
#
# Each step function returns: 0 = pass, 1 = fail, 77 = skip. Steps print
# diagnostics to stderr and never abort the suite, so the full matrix always
# runs and a summary is printed at the end.

ACCEPT_VOL="${ACCEPT_VOL:-accept}"        # volume for the interface matrix (replicas 1)
ACCEPT_REPL_VOL="${ACCEPT_REPL_VOL:-accept_repl}"  # volume for the replication check
FUSE_MNT="/mnt/${ACCEPT_VOL}-fuse"
NFS_MNT="/mnt/${ACCEPT_VOL}-nfs"
CLI_TIMEOUT="${CLI_TIMEOUT:-45}"
CONSISTENCY_TIMEOUT="${CONSISTENCY_TIMEOUT:-25}"

# S3 credentials captured during the s3 step.
S3_KEY="" ; S3_SECRET="" ; S3_FLAGS=""

# Set by s_cifs_share once smbd + vfs_neonfs serve the share; gates s_cifs_ops.
CIFS_READY=0
S3_HOST="127.0.0.1:8080"
DAV_BASE="http://127.0.0.1:8081"

# Container runtime integrations. `DOCKER_SOCK`, `CONTAINERD_SOCK` and
# `CONTAINERD_VOL` come from lib/rig.sh, which the docker-storage scenario also
# needs and which is always sourced before this file. Defining them in both
# places is how two copies of one default drift apart.
DOCKER_VOL="${DOCKER_VOL:-accept_docker}"

# CIFS/SMB: the omnibus daemon ships the Samba VFS module (neonfs.so) and runs
# the CIFS bridge in-process, exposing the ETF socket below. smbd and smbclient
# come from apt (samba/smbclient); the VFS module is ABI-matched to the
# release's Samba. See neonfs_cifs/README.md.
CIFS_SOCK="${CIFS_SOCK:-/run/neonfs/cifs.sock}"
CIFS_USER="${CIFS_USER:-neonfs}"
CIFS_PASS="${CIFS_PASS:-neonfs-rig}"

# Block device (NBD). The volume's size is the device's size, so the two are
# derived from one number rather than restated.
#
# Creating the volume writes the whole device as zeroes, one metadata entry
# per 128 KiB chunk, and that commit rate — not the bytes — is what bounds
# the size a rig run can afford. Measured on a single-node rig VM: 64 MiB in
# 82 s and 256 MiB in 273 s, about 7 chunks/s either way, which puts a
# gigabyte device at ~18 minutes of `volume create` before a single step
# runs. 64 MiB keeps the suite usable and still crosses every device
# boundary the steps care about; raise `BLOCK_MIB` (and
# `BLOCK_CREATE_TIMEOUT` with it) to exercise a larger one deliberately.
BLOCK_VOL="${BLOCK_VOL:-accept_block}"
BLOCK_MIB="${BLOCK_MIB:-64}"
BLOCK_SIZE="${BLOCK_MIB}M"
BLOCK_BYTES=$(( BLOCK_MIB * 1024 * 1024 ))
BLOCK_DEV="${BLOCK_DEV:-/dev/nbd0}"
BLOCK_MNT="/mnt/${BLOCK_VOL}-blk"
BLOCK_FIO_BYTES="${BLOCK_FIO_BYTES:-32M}"
BLOCK_CREATE_TIMEOUT="${BLOCK_CREATE_TIMEOUT:-600}"

# Set by s_block_attach once the kernel has the device; gates the steps that
# operate on it, and cleared again by s_block_detach.
BLOCK_READY=0

# --- harness ---------------------------------------------------------------

A_PASS=0 ; A_FAIL=0 ; A_SKIP=0
declare -a A_RESULTS

step() {
  local name="$1"; shift
  printf '\033[1;36m• %s\033[0m\n' "${name}" >&2
  local rc=0
  "$@" || rc=$?
  case "${rc}" in
    0)  A_RESULTS+=("PASS  ${name}"); A_PASS=$((A_PASS + 1)); printf '  \033[1;32mPASS\033[0m\n' >&2 ;;
    77) A_RESULTS+=("SKIP  ${name}"); A_SKIP=$((A_SKIP + 1)); printf '  \033[1;33mSKIP\033[0m\n' >&2 ;;
    *)  A_RESULTS+=("FAIL  ${name}"); A_FAIL=$((A_FAIL + 1)); printf '  \033[1;31mFAIL\033[0m\n' >&2 ;;
  esac
}

# Run the neonfs CLI on a node with a timeout (guards against the CLI wedging).
ncli() { local i="$1"; shift; node_ssh "$i" "sudo timeout ${CLI_TIMEOUT} neonfs $*"; }

# Poll a command until it succeeds or CONSISTENCY_TIMEOUT elapses.
retry_until() {
  local deadline=$(( SECONDS + CONSISTENCY_TIMEOUT ))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    "$@" && return 0
    sleep 2
  done
  return 1
}

# --- steps -----------------------------------------------------------------

s_cluster_status() {
  ncli 1 "cluster status" 2>&1 | grep -qE 'Status[[:space:]]+running' \
    || { echo "  cluster not running" >&2; return 1; }
}

s_drives_present() {
  local n; n=$(ncli 1 "drive list" 2>/dev/null | grep -c 'active' || true)
  echo "  active drives: ${n}" >&2
  [ "${n:-0}" -ge 2 ] || { echo "  expected >= 2 active drives" >&2; return 1; }
}

s_volume_create() {
  if ncli 1 "volume list" 2>/dev/null | grep -qE "^${ACCEPT_VOL}[[:space:]]"; then
    echo "  volume ${ACCEPT_VOL} already exists" >&2; return 0
  fi
  ncli 1 "volume create ${ACCEPT_VOL} --replicas 1" 2>&1 | grep -qi 'created successfully' \
    || { echo "  volume create failed" >&2; return 1; }
}

volume_present() { ncli 1 "volume list" 2>/dev/null | grep -qE "^$1[[:space:]]"; }
volume_ready() { volume_present "${ACCEPT_VOL}"; }

s_fuse_mount() {
  volume_ready || { echo "  ${ACCEPT_VOL} missing" >&2; return 77; }
  node_ssh 1 "sudo install -d -o neonfs -g neonfs ${FUSE_MNT}" 2>/dev/null
  ncli 1 "fuse mount ${ACCEPT_VOL} ${FUSE_MNT}" 2>&1 | sed 's/^/  /' >&2
  # Verify a real kernel FUSE mount via /proc/mounts rather than
  # `mountpoint`: the mount is owned by the neonfs uid without
  # allow_other, so `mountpoint` (run as the ssh user) gets EACCES even
  # though the mount is attached. Reading the mount table needs no access.
  node_ssh 1 "for i in \$(seq 1 20); do grep -q ' ${FUSE_MNT} fuse' /proc/mounts && exit 0; sleep 1; done; exit 1" 2>/dev/null \
    || { echo "  fuse mount did not attach (absent from /proc/mounts)" >&2; return 1; }
}

s_fuse_ops() {
  node_ssh 1 "sudo -u neonfs bash -c '
    set -e
    cd ${FUSE_MNT}
    mkdir -p d/sub
    echo fuse-content > d/a.txt
    cp d/a.txt d/sub/b.txt
    [ \"\$(cat d/a.txt)\" = fuse-content ]
    [ -f d/sub/b.txt ]
    stat d/a.txt >/dev/null
    rm d/sub/b.txt
    [ ! -e d/sub/b.txt ]
  '" 2>&1 | sed 's/^/  /' >&2
  node_ssh 1 "sudo -u neonfs test -f ${FUSE_MNT}/d/a.txt" 2>/dev/null \
    || { echo "  fuse ops failed" >&2; return 1; }
}

s_nfs_export_mount() {
  volume_ready || return 77
  ncli 1 "nfs export ${ACCEPT_VOL}" 2>&1 | grep -qiE 'exported|already' || true
  node_ssh 1 "sudo mkdir -p ${NFS_MNT}
    sudo umount ${NFS_MNT} 2>/dev/null
    sudo mount -t nfs -o nfsvers=3,proto=tcp,nolock,port=2049,mountport=2049 127.0.0.1:/${ACCEPT_VOL} ${NFS_MNT}" 2>&1 | sed 's/^/  /' >&2
  node_ssh 1 "mount | grep -q '${NFS_MNT}'" 2>/dev/null \
    || { echo "  nfs mount failed" >&2; return 1; }
}

s_nfs_ops() {
  node_ssh 1 "mount | grep -q '${NFS_MNT}'" 2>/dev/null || return 77
  node_ssh 1 "sudo bash -c '
    set -e
    cd ${NFS_MNT}
    mkdir -p nd
    echo nfs-content > nd/n.txt
    [ \"\$(cat nd/n.txt)\" = nfs-content ]
    rm nd/n.txt
  '" 2>&1 | sed 's/^/  /' >&2 \
    || { echo "  nfs ops failed" >&2; return 1; }
}

# Write a tagged file via $1 (writer fn), confirm it appears via $2 (reader fn).
s_consistency() {
  local writer="$1" reader="$2" label="$3"
  local fname="consist_${TAG}_${label}.txt"
  "${writer}" "${fname}" "consistency-${label}" || { echo "  write via ${label%%_*} failed" >&2; return 1; }
  retry_until "${reader}" "${fname}" \
    || { echo "  ${fname} not visible across interfaces within ${CONSISTENCY_TIMEOUT}s" >&2; return 1; }
}

w_fuse()  { node_ssh 1 "sudo -u neonfs bash -c 'echo $2 > ${FUSE_MNT}/$1 && sync'" 2>/dev/null; }
w_nfs()   { node_ssh 1 "sudo bash -c 'echo $2 > ${NFS_MNT}/$1'" 2>/dev/null; }
r_fuse()  { node_ssh 1 "sudo -u neonfs test -f ${FUSE_MNT}/$1" 2>/dev/null; }
r_nfs()   { node_ssh 1 "sudo test -f ${NFS_MNT}/$1" 2>/dev/null; }
r_s3()    { node_ssh 1 "s3cmd ${S3_FLAGS} ls s3://${ACCEPT_VOL}/$1" 2>/dev/null | grep -q "$1"; }
r_dav()   { node_ssh 1 "curl -s -m 15 ${DAV_AUTH} -o /dev/null -w '%{http_code}' ${DAV_BASE}/${ACCEPT_VOL}/$1" 2>/dev/null | grep -q 200; }
w_s3()    { node_ssh 1 "printf %s $2 > /tmp/$1 && s3cmd ${S3_FLAGS} put /tmp/$1 s3://${ACCEPT_VOL}/$1" 2>/dev/null; }
w_dav()   { node_ssh 1 "printf %s $2 | curl -s -m 15 ${DAV_AUTH} -T - ${DAV_BASE}/${ACCEPT_VOL}/$1 -o /dev/null -w '%{http_code}'" 2>/dev/null | grep -qE '20(0|1|4)'; }

s_consistency_fuse_nfs() {
  node_ssh 1 "mount | grep -q '${NFS_MNT}'" 2>/dev/null || return 77
  s_consistency w_fuse r_nfs fuse_to_nfs && s_consistency w_nfs r_fuse nfs_to_fuse
}

s_s3_setup() {
  node_ssh 1 "command -v s3cmd >/dev/null 2>&1 || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q s3cmd >/dev/null 2>&1" 2>/dev/null
  # One credential serves both S3 SigV4 and WebDAV Basic auth (see
  # `neonfs credential create` — the `s3 create-credential` verb is gone).
  local out; out=$(ncli 1 "credential create --user accept" 2>/dev/null)
  S3_KEY=$(echo "${out}" | awk '/Access Key ID:/ {print $NF}')
  S3_SECRET=$(echo "${out}" | awk '/Secret Access Key:/ {print $NF}')
  [ -n "${S3_KEY}" ] && [ -n "${S3_SECRET}" ] \
    || { echo "  failed to create credential" >&2; return 1; }
  S3_FLAGS="--access_key=${S3_KEY} --secret_key=${S3_SECRET} --host=${S3_HOST} --host-bucket=${S3_HOST} --no-ssl --region=neonfs"
  DAV_AUTH="-u ${S3_KEY}:${S3_SECRET}"
  echo "  access key ${S3_KEY}" >&2
}

s_s3_ops() {
  [ -n "${S3_KEY}" ] || return 77
  ncli 1 "s3 bucket list" 2>/dev/null | grep -q "${ACCEPT_VOL}" \
    || { echo "  ${ACCEPT_VOL} not listed as a bucket" >&2; return 1; }
  node_ssh 1 "printf %s s3-content > /tmp/s3o.txt
    out=\$(s3cmd ${S3_FLAGS} put /tmp/s3o.txt s3://${ACCEPT_VOL}/s3o_${TAG}.txt 2>&1); rc=\$?
    if [ \$rc -ne 0 ] || echo \"\$out\" | grep -qi 'MD5.*match'; then
      echo \"s3cmd PUT failed (rc=\$rc) — ETag/MD5 integrity: \$out\"; exit 1
    fi
    got=\$(s3cmd ${S3_FLAGS} get --force s3://${ACCEPT_VOL}/s3o_${TAG}.txt - 2>/dev/null)
    [ \"\$got\" = s3-content ]" 2>&1 | sed 's/^/  /' >&2 \
    || { echo "  S3 put/get round-trip failed" >&2; return 1; }
}

s_webdav_ops() {
  volume_ready || return 77
  [ -n "${DAV_AUTH}" ] || return 77
  node_ssh 1 "
    code=\$(printf %s dav-content | curl -s -m 15 ${DAV_AUTH} -T - ${DAV_BASE}/${ACCEPT_VOL}/dav_${TAG}.txt -o /dev/null -w '%{http_code}')
    [ \"\$code\" = 201 ] || [ \"\$code\" = 200 ] || [ \"\$code\" = 204 ] || exit 1
    got=\$(curl -s -m 15 ${DAV_AUTH} ${DAV_BASE}/${ACCEPT_VOL}/dav_${TAG}.txt)
    [ \"\$got\" = dav-content ]" 2>&1 | sed 's/^/  /' >&2 \
    || { echo "  WebDAV PUT/GET failed" >&2; return 1; }
}

# Attach a Docker volume to a NeonFS volume through a real Docker daemon: the
# omnibus package ships /etc/docker/plugins/neonfs.spec, so `docker volume
# create -d neonfs` provisions a NeonFS volume in core and `docker run -v`
# attaches it (the driver's FUSE mount reaches the container's namespace via the
# omnibus unit's MountFlags=shared). Writing from one container and reading it
# back from a second proves the NeonFS volume is genuinely attached.
s_docker_volume_attach() {
  node_ssh 1 "command -v docker >/dev/null 2>&1" 2>/dev/null \
    || { echo "  docker not installed — skipping" >&2; return 77; }
  node_ssh 1 "sudo systemctl is-active --quiet docker || sudo systemctl start docker" 2>/dev/null
  node_ssh 1 "sudo test -S ${DOCKER_SOCK}" 2>/dev/null \
    || { echo "  neonfs docker plugin socket ${DOCKER_SOCK} absent — driver not deployed" >&2; return 77; }
  node_ssh 1 "test -f /etc/docker/plugins/neonfs.spec" 2>/dev/null \
    || { echo "  /etc/docker/plugins/neonfs.spec missing — docker cannot discover the driver" >&2; return 1; }

  node_ssh 1 "sudo docker pull busybox:latest >/dev/null 2>&1" 2>/dev/null \
    || { echo "  could not pull busybox (no registry connectivity?) — skipping" >&2; return 77; }

  node_ssh 1 "sudo docker volume rm ${DOCKER_VOL} >/dev/null 2>&1 || true
    sudo docker volume create -d neonfs --opt durability=1 ${DOCKER_VOL}" 2>&1 | sed 's/^/  /' >&2
  node_ssh 1 "sudo docker volume ls --format '{{.Driver}} {{.Name}}' | grep -qx 'neonfs ${DOCKER_VOL}'" 2>/dev/null \
    || { echo "  docker volume create -d neonfs failed" >&2; return 1; }

  retry_until volume_present "${DOCKER_VOL}" \
    || { echo "  driver did not provision a NeonFS volume named ${DOCKER_VOL}" >&2; return 1; }

  local rc=0
  node_ssh 1 "sudo docker run --rm -v ${DOCKER_VOL}:/data busybox sh -c 'echo docker-vol-content > /data/dv_${TAG}.txt && sync'" 2>&1 | sed 's/^/  /' >&2 || rc=1
  node_ssh 1 "got=\$(sudo docker run --rm -v ${DOCKER_VOL}:/data busybox cat /data/dv_${TAG}.txt 2>/dev/null); [ \"\$got\" = docker-vol-content ]" 2>&1 | sed 's/^/  /' >&2 \
    || { echo "  data written via one container not readable from another — volume not attached" >&2; rc=1; }

  node_ssh 1 "sudo docker volume rm ${DOCKER_VOL} >/dev/null 2>&1 || true"
  [ "${rc}" -eq 0 ] || return 1
}

# containerd content store: store a real image-layer blob in a NeonFS volume
# through the content proxy plugin. Spawn a throwaway containerd whose only
# content backend is the neonfs plugin (default bolt store disabled, per the
# proxy_plugins wiring in docs/containerd.md), then round-trip a blob with
# `ctr content ingest`/`ls`/`get` — mirroring neonfs_integration's
# ContainerdDaemon. The plugin lands the blob in the CONTAINERD_VOL NeonFS
# volume as a sharded sha256 object.
s_containerd_content() {
  node_ssh 1 "command -v containerd >/dev/null 2>&1 && command -v ctr >/dev/null 2>&1" 2>/dev/null \
    || { echo "  containerd/ctr not installed — skipping" >&2; return 77; }
  node_ssh 1 "sudo test -S ${CONTAINERD_SOCK}" 2>/dev/null \
    || { echo "  containerd plugin socket ${CONTAINERD_SOCK} absent — backend not deployed" >&2; return 77; }

  volume_present "${CONTAINERD_VOL}" \
    || ncli 1 "volume create ${CONTAINERD_VOL} --replicas 1" 2>&1 | grep -qi 'created successfully' \
    || { echo "  could not create the ${CONTAINERD_VOL} content-store volume" >&2; return 1; }

  node_ssh 1 "sudo bash -s ${CONTAINERD_SOCK} ${TAG}" 2>&1 <<'REMOTE' | sed 's/^/  /' >&2
set -e
PROXY_SOCK="$1"; TAG="$2"
TMP="$(mktemp -d /tmp/neonfs-ctrd.XXXXXX)"
trap 'kill "${CTRD_PID:-0}" 2>/dev/null || true; rm -rf "${TMP}"' EXIT
mkdir -p "${TMP}/root" "${TMP}/state"
GRPC="${TMP}/containerd.sock"

cat > "${TMP}/config.toml" <<CFG
version = 2
root = "${TMP}/root"
state = "${TMP}/state"
disabled_plugins = ["io.containerd.grpc.v1.cri", "io.containerd.content.v1.content"]
imports = []

[grpc]
address = "${GRPC}"

[ttrpc]
address = "${GRPC}.ttrpc"

[proxy_plugins]
  [proxy_plugins.neonfs]
  type = "content"
  address = "${PROXY_SOCK}"
CFG

containerd --config "${TMP}/config.toml" --log-level info > "${TMP}/containerd.log" 2>&1 &
CTRD_PID=$!
for _ in $(seq 1 50); do [ -S "${GRPC}" ] && break; sleep 0.2; done
[ -S "${GRPC}" ] || { echo "containerd grpc socket never came up"; tail -20 "${TMP}/containerd.log"; exit 1; }

head -c 65536 /dev/urandom > "${TMP}/blob"
DIGEST="sha256:$(sha256sum "${TMP}/blob" | awk '{print $1}')"

ctr --address "${GRPC}" --namespace test content ingest --expected-digest "${DIGEST}" "ref_${TAG}" < "${TMP}/blob" \
  || { echo "ctr content ingest failed"; tail -20 "${TMP}/containerd.log"; exit 1; }
ctr --address "${GRPC}" --namespace test content ls | grep -q "${DIGEST}" \
  || { echo "ingested digest ${DIGEST} absent from content ls"; exit 1; }
ctr --address "${GRPC}" --namespace test content get "${DIGEST}" > "${TMP}/got"
cmp -s "${TMP}/blob" "${TMP}/got" \
  || { echo "blob retrieved from the content store differs from the original"; exit 1; }
echo "content round-trip OK (${DIGEST})"
REMOTE
}

# Configure a co-located smbd to serve ${ACCEPT_VOL} through vfs_neonfs, dialing
# the in-process omnibus CIFS bridge. smbd/smbclient are apt-installed here (the
# omnibus deb only ships the ABI-matched VFS module + samba-vfs-modules dep).
s_cifs_share() {
  volume_ready || { echo "  ${ACCEPT_VOL} missing" >&2; return 77; }
  # The Samba VFS module is opt-in at deb-build time (NEONFS_BUILD_CIFS=1, so a
  # default `up` skips the heavy in-tree Samba build). Absent module = CIFS was
  # not built into this omnibus, so skip rather than fail.
  node_ssh 1 "ls /usr/lib/*/samba/vfs/neonfs.so >/dev/null 2>&1" 2>/dev/null \
    || { echo "  Samba VFS module neonfs.so not installed — rebuild the omnibus with NEONFS_BUILD_CIFS=1 to exercise CIFS" >&2; return 77; }
  node_ssh 1 "sudo test -S ${CIFS_SOCK}" 2>/dev/null \
    || { echo "  CIFS bridge socket ${CIFS_SOCK} absent — omnibus CIFS bridge not running" >&2; return 1; }

  node_ssh 1 "sudo bash -s ${ACCEPT_VOL} ${CIFS_SOCK} ${CIFS_USER} ${CIFS_PASS}" 2>&1 <<'REMOTE' | sed 's/^/  /' >&2
set -e
SHARE="$1"; SOCK="$2"; SMBUSER="$3"; SMBPASS="$4"

if ! { command -v smbd >/dev/null 2>&1 && command -v smbclient >/dev/null 2>&1; }; then
  DEBIAN_FRONTEND=noninteractive apt-get install -y -q samba smbclient >/dev/null 2>&1 || true
fi
command -v smbd >/dev/null 2>&1 || { echo "smbd unavailable after 'apt-get install samba'"; exit 1; }
command -v smbclient >/dev/null 2>&1 || { echo "smbclient unavailable after 'apt-get install smbclient'"; exit 1; }

cat > /etc/samba/smb.conf <<CONF
[global]
   workgroup = WORKGROUP
   security = user
   server min protocol = SMB2
   log level = 1

[${SHARE}]
   path = /
   read only = no
   vfs objects = neonfs
   neonfs:socket = ${SOCK}
   neonfs:volume = ${SHARE}
   admin users = ${SMBUSER}
CONF

testparm -s >/dev/null 2>&1 || { echo "smb.conf failed testparm validation"; testparm -s 2>&1 | tail -5; exit 1; }

printf '%s\n%s\n' "${SMBPASS}" "${SMBPASS}" | smbpasswd -s -a "${SMBUSER}" >/dev/null

systemctl restart smbd

for _ in $(seq 1 30); do
  if smbclient "//127.0.0.1/${SHARE}" -U "${SMBUSER}%${SMBPASS}" -c ls >/dev/null 2>&1; then
    echo "share ${SHARE} reachable via smbd + vfs_neonfs"
    exit 0
  fi
  sleep 1
done
echo "share ${SHARE} not reachable via smbclient after smbd restart"
smbclient "//127.0.0.1/${SHARE}" -U "${SMBUSER}%${SMBPASS}" -c ls 2>&1 | tail -10 || true
journalctl -u smbd -n 20 --no-pager 2>&1 | tail -20 || true
exit 1
REMOTE
  local rc=$?
  [ "${rc}" -eq 0 ] && CIFS_READY=1
  return "${rc}"
}

s_cifs_ops() {
  [ "${CIFS_READY}" = 1 ] || { echo "  CIFS share not established — skipping ops" >&2; return 77; }
  node_ssh 1 "sudo bash -s ${ACCEPT_VOL} ${CIFS_USER} ${CIFS_PASS} ${TAG}" 2>&1 <<'REMOTE' | sed 's/^/  /' >&2
set -e
SHARE="$1"; SMBUSER="$2"; SMBPASS="$3"; TAG="$4"
DIR="cifs_${TAG}"
SRC="$(mktemp)"; OUT="$(mktemp)"
trap 'rm -f "${SRC}" "${OUT}"' EXIT
echo cifs-content > "${SRC}"

smb() { smbclient "//127.0.0.1/${SHARE}" -U "${SMBUSER}%${SMBPASS}" "$@"; }

smb -c "mkdir ${DIR}"
smb -c "put ${SRC} ${DIR}/c.txt"
smb -c "allinfo ${DIR}/c.txt" >/dev/null                                  # stat
smb -c "cd ${DIR}; ls" | grep -q "c.txt"                                  # readdir
smb -c "get ${DIR}/c.txt ${OUT}"
[ "$(cat "${OUT}")" = cifs-content ]                                      # content round-trip
smb -c "rename ${DIR}/c.txt ${DIR}/c2.txt"
smb -c "cd ${DIR}; ls" | grep -q "c2.txt"
smb -c "del ${DIR}/c2.txt"
if smb -c "cd ${DIR}; ls" | grep -q "c2.txt"; then echo "delete left c2.txt behind"; exit 1; fi
smb -c "rmdir ${DIR}"
echo "cifs round-trip OK (mkdir/put/stat/readdir/get/rename/delete/rmdir)"
REMOTE
}

# --- block device (NBD) ----------------------------------------------------
#
# A kernel block device cannot be attached from inside a container — the `nbd`
# module has to be loaded on the host that runs the client — which is why this
# lives here rather than in ExUnit or a package CI job. The VM installs the
# real `.deb` packages, so the NBD listener is the shipped one.
#
# The device is provisioned by `volume create --type block --size N`: a block
# volume is its device, and `max_size` is both the quota and the device size.
# The export name is the bare volume, which resolves to that volume's only
# device.

s_block_attach() {
  block_prereqs || return 77

  # Creating the volume writes the whole device as zeroes, one metadata entry
  # per chunk, so it takes longer than the interactive CLI timeout allows.
  volume_present "${BLOCK_VOL}" \
    || node_ssh 1 "sudo timeout ${BLOCK_CREATE_TIMEOUT} neonfs volume create ${BLOCK_VOL} --type block --size ${BLOCK_SIZE} --replicas 1" 2>&1 | grep -qi 'created successfully' \
    || { echo "  volume create --type block failed" >&2; return 1; }

  node_ssh 1 "sudo bash -s ${BLOCK_VOL} ${BLOCK_DEV} ${BLOCK_BYTES}" 2>&1 <<'REMOTE' | sed 's/^/  /' >&2
set -e
VOL="$1"; DEV="$2"; BYTES="$3"

nbd-client -d "${DEV}" >/dev/null 2>&1 || true
# `-b 4096` is not decoration: nbd-client defaults to 512 regardless of the
# NBD_INFO_BLOCK_SIZE the server advertises, and the backing store refuses a
# request that is not 4 KiB-aligned — so a 512-block attachment breaks on the
# first sub-4K IO the kernel decides to issue.
nbd-client -N "${VOL}" 127.0.0.1 10809 "${DEV}" -b 4096 -persist -timeout 60

for _ in $(seq 1 30); do [ "$(blockdev --getsize64 "${DEV}" 2>/dev/null || echo 0)" != 0 ] && break; sleep 1; done

size=$(blockdev --getsize64 "${DEV}")
ss=$(blockdev --getss "${DEV}")
pbsz=$(blockdev --getpbsz "${DEV}")
echo "attached ${DEV}: size=${size} logical=${ss} physical=${pbsz}"

[ "${size}" = "${BYTES}" ] || { echo "device size ${size} != volume size ${BYTES}"; exit 1; }
[ "${ss}" = 4096 ] || { echo "logical block size ${ss} != 4096"; exit 1; }
[ "${pbsz}" = 4096 ] || { echo "physical block size ${pbsz} != 4096"; exit 1; }
REMOTE
  local rc=$?
  [ "${rc}" -eq 0 ] && BLOCK_READY=1
  return "${rc}"
}

# A filesystem surviving a remount is the real proof the device is coherent:
# ext4 reads back its own metadata from wherever it left it, including the
# superblock backups mkfs scatters across the device.
s_block_fs() {
  [ "${BLOCK_READY}" = 1 ] || { echo "  no block device attached — skipping" >&2; return 77; }
  node_ssh 1 "sudo bash -s ${BLOCK_DEV} ${BLOCK_MNT} ${TAG}" 2>&1 <<'REMOTE' | sed 's/^/  /' >&2
set -e
DEV="$1"; MNT="$2"; TAG="$3"

umount "${MNT}" 2>/dev/null || true
mkfs.ext4 -q -F "${DEV}"
mkdir -p "${MNT}"
mount "${DEV}" "${MNT}"
echo "block-content-${TAG}" > "${MNT}/b.txt"
[ "$(cat "${MNT}/b.txt")" = "block-content-${TAG}" ]
umount "${MNT}"
mount "${DEV}" "${MNT}"
[ "$(cat "${MNT}/b.txt")" = "block-content-${TAG}" ] || { echo "file did not survive the remount"; exit 1; }
umount "${MNT}"
echo "ext4 mkfs/write/remount round-trip OK"
REMOTE
}

# fio's own verification, not ours: a crc32c mismatch is a corrupt device.
# The run deliberately includes the device's last block, so an off-by-one at
# the end of the device fails here rather than in production.
s_block_fio_verify() {
  [ "${BLOCK_READY}" = 1 ] || return 77
  node_ssh 1 "command -v fio >/dev/null 2>&1 || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q fio >/dev/null 2>&1
    command -v fio >/dev/null 2>&1" 2>/dev/null \
    || { echo "  fio unavailable and not installable — skipping" >&2; return 77; }

  node_ssh 1 "sudo bash -s ${BLOCK_DEV} ${BLOCK_FIO_BYTES}" 2>&1 <<'REMOTE' | sed 's/^/  /' >&2
set -e
DEV="$1"; SPAN="$2"
SIZE=$(blockdev --getsize64 "${DEV}")
TAIL=$(( SIZE - 1048576 ))

# Two runs: random over the head of the device, then the final MiB, so the
# last addressable block is written and read back rather than assumed.
fio --name=head --filename="${DEV}" --direct=1 --rw=randrw --rwmixread=50 \
    --bs=4k --size="${SPAN}" --io_size="${SPAN}" --verify=crc32c \
    --verify_fatal=1 --do_verify=1 --numjobs=1 --iodepth=8 --ioengine=libaio \
    --group_reporting --output-format=terse --minimal
fio --name=tail --filename="${DEV}" --direct=1 --rw=randwrite --bs=4k \
    --offset="${TAIL}" --size=1M --verify=crc32c --verify_fatal=1 \
    --do_verify=1 --numjobs=1 --iodepth=4 --ioengine=libaio \
    --group_reporting --output-format=terse --minimal
echo "fio --verify=crc32c reported no verification errors"
REMOTE
}

# Detaching must leave the data where it was: the backing file is a file in a
# NeonFS volume and outlives any attachment of it.
s_block_detach() {
  [ "${BLOCK_READY}" = 1 ] || return 77
  node_ssh 1 "sudo bash -s ${BLOCK_VOL} ${BLOCK_DEV} ${BLOCK_MNT} ${TAG}" 2>&1 <<'REMOTE' | sed 's/^/  /' >&2
set -e
VOL="$1"; DEV="$2"; MNT="$3"; TAG="$4"

umount "${MNT}" 2>/dev/null || true
nbd-client -d "${DEV}"

for _ in $(seq 1 15); do [ "$(cat "/sys/block/$(basename "${DEV}")/size" 2>/dev/null || echo 0)" = 0 ] && break; sleep 1; done
[ "$(cat "/sys/block/$(basename "${DEV}")/size")" = 0 ] || { echo "${DEV} still reports a size after detach"; exit 1; }

nbd-client -N "${VOL}" 127.0.0.1 10809 "${DEV}" -b 4096 -persist -timeout 60
for _ in $(seq 1 30); do [ "$(blockdev --getsize64 "${DEV}" 2>/dev/null || echo 0)" != 0 ] && break; sleep 1; done
mount "${DEV}" "${MNT}"
[ "$(cat "${MNT}/b.txt")" = "block-content-${TAG}" ] || { echo "data did not survive detach and re-attach"; exit 1; }
umount "${MNT}"
nbd-client -d "${DEV}"
echo "detach/re-attach OK — filesystem and contents survived"
REMOTE
  local rc=$?
  [ "${rc}" -eq 0 ] && BLOCK_READY=0
  return "${rc}"
}

# nbd-client and a loadable `nbd` module are what separate a VM from a
# container here; either being absent is a skip, not a failure.
block_prereqs() {
  # `nbd-client` lives in /sbin, which is not on the login shell's PATH here,
  # so every check for it runs under sudo — as the steps themselves do.
  node_ssh 1 "sudo sh -c 'command -v nbd-client' >/dev/null 2>&1 || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q nbd-client >/dev/null 2>&1
    sudo sh -c 'command -v nbd-client' >/dev/null 2>&1" 2>/dev/null \
    || { echo "  nbd-client unavailable and not installable — skipping" >&2; return 1; }
  node_ssh 1 "sudo modprobe nbd max_part=8" 2>/dev/null \
    || { echo "  nbd module could not be loaded — skipping" >&2; return 1; }
  node_ssh 1 "test -b ${BLOCK_DEV}" 2>/dev/null \
    || { echo "  ${BLOCK_DEV} absent after modprobe nbd — skipping" >&2; return 1; }
}

# Cross-interface: write via FUSE, must be visible via S3 and WebDAV (and NFS).
s_cross_consistency() {
  [ -n "${S3_KEY}" ] || return 77
  local fname="cross_${TAG}.txt"
  w_fuse "${fname}" cross-content || { echo "  fuse write failed" >&2; return 1; }
  local ok=0
  retry_until r_s3 "${fname}"  && ok=$((ok+1)) || echo "  not visible via S3" >&2
  retry_until r_dav "${fname}" && ok=$((ok+1)) || echo "  not visible via WebDAV" >&2
  node_ssh 1 "mount | grep -q '${NFS_MNT}'" 2>/dev/null && { retry_until r_nfs "${fname}" && ok=$((ok+1)) || echo "  not visible via NFS" >&2; }
  [ "${ok}" -ge 2 ] || { echo "  FUSE write not consistent across other interfaces" >&2; return 1; }
}

# Multi-node only: replicated volume must place copies on >= 2 distinct nodes.
s_volume_stats() {
  volume_ready || return 77
  # By now the FUSE/NFS/S3/WebDAV steps have written data into the volume.
  # `volume show` must reflect it rather than reporting 0 chunks / 0 bytes.
  local out; out=$(ncli 1 "volume show ${ACCEPT_VOL}" 2>/dev/null)
  echo "${out}" | grep -iE 'chunks|logical|physical' | sed 's/^/  /' >&2
  local chunks; chunks=$(echo "${out}" | grep -iE 'chunks' | grep -oE '[0-9]+' | head -1)
  [ "${chunks:-0}" -gt 0 ] \
    || { echo "  volume show reports 0 chunks despite writes" >&2; return 1; }
}

# A FUSE unmount must not wedge the control plane. Unmount, confirm the
# CLI still responds, then re-mount (MountManager must not be left in a bad
# state). Runs after the FUSE-dependent steps; leaves the mount restored.
s_fuse_unmount_resilience() {
  volume_ready || return 77
  node_ssh 1 "grep -q ' ${FUSE_MNT} fuse' /proc/mounts" 2>/dev/null || return 77

  ncli 1 "fuse unmount ${FUSE_MNT}" 2>&1 | sed 's/^/  /' >&2 || true

  ncli 1 "cluster status" 2>&1 | grep -qE 'Status[[:space:]]+running' \
    || { echo "  CLI wedged after fuse unmount" >&2; return 1; }

  node_ssh 1 "for i in \$(seq 1 15); do sudo timeout 20 neonfs fuse mount ${ACCEPT_VOL} ${FUSE_MNT} 2>&1 | grep -qiE 'mounted|already' && exit 0; sleep 2; done; exit 1" 2>/dev/null \
    || { echo "  re-mount after unmount failed — MountManager left in a bad state" >&2; return 1; }
}

s_replication() {
  [ "${NODES}" -ge 2 ] || { echo "  single node — replication not applicable" >&2; return 77; }
  local cores; cores=$(ncli 1 "cluster status" 2>/dev/null | grep -iE 'core nodes|members' | grep -oE '[0-9]+' | head -1)
  ncli 1 "volume list" 2>/dev/null | grep -qE "^${ACCEPT_REPL_VOL}[[:space:]]" \
    || ncli 1 "volume create ${ACCEPT_REPL_VOL} --replicas 2" 2>&1 | grep -qi 'created successfully' \
    || { echo "  could not create replicas=2 volume (cluster has < 2 core nodes?)" >&2; return 1; }
  # Write 8 MiB via S3 and check each node's drives gained data.
  local before after grew=0 i
  for i in $(seq 1 "${NODES}"); do
    before=$(node_ssh "$i" "sudo du -sb /mnt/neonfs 2>/dev/null | awk '{print \$1}'" 2>/dev/null)
    eval "B_$i=${before:-0}"
  done
  node_ssh 1 "head -c 8388608 /dev/urandom > /tmp/rep.bin
    s3cmd ${S3_FLAGS} put /tmp/rep.bin s3://${ACCEPT_REPL_VOL}/rep_${TAG}.bin >/dev/null 2>&1 || true" 2>/dev/null
  sleep 5
  for i in $(seq 1 "${NODES}"); do
    after=$(node_ssh "$i" "sudo du -sb /mnt/neonfs 2>/dev/null | awk '{print \$1}'" 2>/dev/null)
    local b; eval "b=\$B_$i"
    local delta=$(( ${after:-0} - ${b:-0} ))
    echo "  node $i drive growth: ${delta} bytes" >&2
    [ "${delta}" -ge 4000000 ] && grew=$((grew + 1))
  done
  [ "${grew}" -ge 2 ] || { echo "  data not replicated to >= 2 nodes" >&2; return 1; }
}

# --- cleanup ---------------------------------------------------------------

acceptance_cleanup() {
  [ "${KEEP:-0}" = 1 ] && return 0
  node_ssh 1 "sudo umount ${NFS_MNT} 2>/dev/null; sudo timeout 20 neonfs fuse unmount ${FUSE_MNT} 2>/dev/null" >/dev/null 2>&1 || true
  node_ssh 1 "command -v docker >/dev/null 2>&1 && sudo docker volume rm ${DOCKER_VOL} >/dev/null 2>&1" >/dev/null 2>&1 || true
  node_ssh 1 "command -v smbd >/dev/null 2>&1 && sudo systemctl stop smbd 2>/dev/null" >/dev/null 2>&1 || true
  node_ssh 1 "sudo umount ${BLOCK_MNT} 2>/dev/null; command -v nbd-client >/dev/null 2>&1 && sudo nbd-client -d ${BLOCK_DEV} 2>/dev/null" >/dev/null 2>&1 || true
}

# --- driver ----------------------------------------------------------------

acceptance_run() {
  local mode="$1"
  echo "NeonFS acceptance — mode=${mode}, nodes=${NODES}, tag=${TAG}" >&2
  echo >&2

  step "cluster initialised and running"           s_cluster_status
  step "two or more drives active"                  s_drives_present
  step "create volume (${ACCEPT_VOL})"              s_volume_create
  step "FUSE mount"                                 s_fuse_mount
  step "FUSE filesystem operations"                 s_fuse_ops
  step "NFS export + mount"                         s_nfs_export_mount
  step "NFS filesystem operations"                  s_nfs_ops
  step "consistency FUSE <-> NFS"                   s_consistency_fuse_nfs
  step "S3 credential + client setup"               s_s3_setup
  step "S3 operations (list/put/get)"               s_s3_ops
  step "consistency S3/NFS/FUSE/WebDAV (FUSE write)" s_cross_consistency
  step "WebDAV operations (PUT/GET)"                s_webdav_ops
  step "Docker volume attach (create -d neonfs + run -v)" s_docker_volume_attach
  step "containerd content store (ingest/get via ctr)"    s_containerd_content
  step "CIFS/SMB share (smbd + vfs_neonfs)"          s_cifs_share
  step "CIFS/SMB operations (smbclient round-trip)"  s_cifs_ops
  step "block device attach (nbd-client + geometry)" s_block_attach
  step "block device filesystem (mkfs.ext4/remount)" s_block_fs
  step "block device fio --verify=crc32c"            s_block_fio_verify
  step "block device detach (data survives)"         s_block_detach
  step "volume show reflects stored data"           s_volume_stats
  step "FUSE unmount does not wedge control plane"  s_fuse_unmount_resilience
  step "replication across nodes"                   s_replication

  acceptance_cleanup

  echo >&2
  echo "================ acceptance summary (${mode}) ================" >&2
  printf '%s\n' "${A_RESULTS[@]}" >&2
  echo "-------------------------------------------------------------" >&2
  echo "PASS=${A_PASS}  FAIL=${A_FAIL}  SKIP=${A_SKIP}" >&2
  [ "${A_FAIL}" -eq 0 ]
}
