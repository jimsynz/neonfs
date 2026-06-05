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
S3_HOST="127.0.0.1:8080"
DAV_BASE="http://127.0.0.1:8081"

# Container runtime integrations: the omnibus daemon owns these Unix sockets.
DOCKER_SOCK="${DOCKER_SOCK:-/run/neonfs/docker.sock}"
CONTAINERD_SOCK="${CONTAINERD_SOCK:-/run/neonfs/containerd.sock}"
DOCKER_VOL="${DOCKER_VOL:-accept_docker}"

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

volume_ready() { ncli 1 "volume list" 2>/dev/null | grep -qE "^${ACCEPT_VOL}[[:space:]]"; }

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
r_dav()   { node_ssh 1 "curl -s -m 15 -o /dev/null -w '%{http_code}' ${DAV_BASE}/${ACCEPT_VOL}/$1" 2>/dev/null | grep -q 200; }
w_s3()    { node_ssh 1 "printf %s $2 > /tmp/$1 && s3cmd ${S3_FLAGS} put /tmp/$1 s3://${ACCEPT_VOL}/$1" 2>/dev/null; }
w_dav()   { node_ssh 1 "printf %s $2 | curl -s -m 15 -T - ${DAV_BASE}/${ACCEPT_VOL}/$1 -o /dev/null -w '%{http_code}'" 2>/dev/null | grep -qE '20(0|1|4)'; }

s_consistency_fuse_nfs() {
  node_ssh 1 "mount | grep -q '${NFS_MNT}'" 2>/dev/null || return 77
  s_consistency w_fuse r_nfs fuse_to_nfs && s_consistency w_nfs r_fuse nfs_to_fuse
}

s_s3_setup() {
  node_ssh 1 "command -v s3cmd >/dev/null 2>&1 || sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q s3cmd >/dev/null 2>&1" 2>/dev/null
  local out; out=$(ncli 1 "s3 create-credential --user accept" 2>/dev/null)
  S3_KEY=$(echo "${out}" | awk '/Access Key ID:/ {print $NF}')
  S3_SECRET=$(echo "${out}" | awk '/Secret Access Key:/ {print $NF}')
  [ -n "${S3_KEY}" ] && [ -n "${S3_SECRET}" ] \
    || { echo "  failed to create S3 credential" >&2; return 1; }
  S3_FLAGS="--access_key=${S3_KEY} --secret_key=${S3_SECRET} --host=${S3_HOST} --host-bucket=${S3_HOST} --no-ssl --region=neonfs"
  echo "  access key ${S3_KEY}" >&2
}

s_s3_ops() {
  [ -n "${S3_KEY}" ] || return 77
  ncli 1 "s3 bucket list" 2>/dev/null | grep -q "${ACCEPT_VOL}" \
    || { echo "  ${ACCEPT_VOL} not listed as a bucket" >&2; return 1; }
  node_ssh 1 "printf %s s3-content > /tmp/s3o.txt
    out=\$(s3cmd ${S3_FLAGS} put /tmp/s3o.txt s3://${ACCEPT_VOL}/s3o_${TAG}.txt 2>&1); rc=\$?
    if [ \$rc -ne 0 ] || echo \"\$out\" | grep -qi 'MD5.*match'; then
      echo \"s3cmd PUT failed (rc=\$rc) — ETag/MD5 integrity (#1037): \$out\"; exit 1
    fi
    got=\$(s3cmd ${S3_FLAGS} get --force s3://${ACCEPT_VOL}/s3o_${TAG}.txt - 2>/dev/null)
    [ \"\$got\" = s3-content ]" 2>&1 | sed 's/^/  /' >&2 \
    || { echo "  S3 put/get round-trip failed" >&2; return 1; }
}

s_webdav_ops() {
  volume_ready || return 77
  node_ssh 1 "
    code=\$(printf %s dav-content | curl -s -m 15 -T - ${DAV_BASE}/${ACCEPT_VOL}/dav_${TAG}.txt -o /dev/null -w '%{http_code}')
    [ \"\$code\" = 201 ] || [ \"\$code\" = 200 ] || [ \"\$code\" = 204 ] || exit 1
    got=\$(curl -s -m 15 ${DAV_BASE}/${ACCEPT_VOL}/dav_${TAG}.txt)
    [ \"\$got\" = dav-content ]" 2>&1 | sed 's/^/  /' >&2 \
    || { echo "  WebDAV PUT/GET failed" >&2; return 1; }
}

# POST a Docker Volume Plugin request to the driver's Unix socket. $1 is the
# endpoint, $2 the JSON body (single-quote it at the call site). Runs as root
# because the socket is neonfs-owned and not world-writable.
dvd() {
  node_ssh 1 "sudo curl -s --max-time 20 --unix-socket ${DOCKER_SOCK} \
    -H 'Content-Type: application/json' -d '$2' http://plugin/$1" 2>/dev/null
}

# Docker/Podman VolumeDriver: drive the full lifecycle over the plugin socket —
# Activate, Create (which provisions a replicas-1 NeonFS volume in core), Mount
# (a real FUSE-backed mountpoint), a read/write round-trip at that mountpoint,
# Path/List, then Unmount + Remove.
s_docker_volume_driver() {
  node_ssh 1 "sudo test -S ${DOCKER_SOCK}" 2>/dev/null \
    || { echo "  docker plugin socket ${DOCKER_SOCK} absent — driver not deployed" >&2; return 77; }

  dvd Plugin.Activate '{}' | grep -q 'VolumeDriver' \
    || { echo "  Plugin.Activate did not advertise VolumeDriver" >&2; return 1; }

  local out
  out=$(dvd VolumeDriver.Create "{\"Name\":\"${DOCKER_VOL}\",\"Opts\":{\"durability\":\"1\"}}")
  echo "${out}" | grep -q '"Err":""' \
    || { echo "  VolumeDriver.Create failed: ${out}" >&2; return 1; }

  out=$(dvd VolumeDriver.Mount "{\"Name\":\"${DOCKER_VOL}\"}")
  local mp; mp=$(echo "${out}" | grep -oE '"Mountpoint":"[^"]+"' | head -1 | cut -d'"' -f4)
  [ -n "${mp}" ] \
    || { echo "  VolumeDriver.Mount returned no mountpoint: ${out}" >&2; return 1; }
  echo "  mountpoint ${mp}" >&2

  local rc=0
  node_ssh 1 "sudo -u neonfs bash -c '
    set -e
    echo docker-content > ${mp}/dvd_${TAG}.txt
    sync
    [ \"\$(cat ${mp}/dvd_${TAG}.txt)\" = docker-content ]
  '" 2>&1 | sed 's/^/  /' >&2 || rc=1

  dvd VolumeDriver.Path "{\"Name\":\"${DOCKER_VOL}\"}" | grep -q "${mp}" \
    || { echo "  VolumeDriver.Path did not return the mountpoint" >&2; rc=1; }
  dvd VolumeDriver.List '{}' | grep -q "${DOCKER_VOL}" \
    || { echo "  VolumeDriver.List did not include ${DOCKER_VOL}" >&2; rc=1; }

  dvd VolumeDriver.Unmount "{\"Name\":\"${DOCKER_VOL}\"}" | grep -q '"Err":""' \
    || { echo "  VolumeDriver.Unmount failed" >&2; rc=1; }
  dvd VolumeDriver.Remove "{\"Name\":\"${DOCKER_VOL}\"}" >/dev/null 2>&1

  [ "${rc}" -eq 0 ] || return 1
}

# containerd content store: the plugin speaks the gRPC content protocol over a
# Unix socket that a containerd daemon dials as a proxy plugin. Content RPCs
# (Read/Write/Info) are not yet implemented (#549/#550), so this verifies the
# endpoint is bound, listening, and the service is registered + healthy — not a
# content round-trip.
s_containerd_endpoint() {
  node_ssh 1 "sudo test -S ${CONTAINERD_SOCK}" 2>/dev/null \
    || { echo "  containerd socket ${CONTAINERD_SOCK} absent — backend not deployed" >&2; return 77; }

  node_ssh 1 "sudo ss -lxn 2>/dev/null | grep -q '${CONTAINERD_SOCK}'" 2>/dev/null \
    || { echo "  ${CONTAINERD_SOCK} is not a listening socket" >&2; return 1; }

  local status; status=$(ncli 1 "node status" 2>&1)
  echo "${status}" | grep -iE 'containerd_(cluster|registrar)' | sed 's/^/  /' >&2 || true
  if echo "${status}" | grep -iE 'containerd_(cluster|registrar)' | grep -qi 'unhealthy'; then
    echo "  containerd service reported unhealthy" >&2; return 1
  fi

  echo "  note: content Read/Write unimplemented (#549/#550) — endpoint liveness only" >&2
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
  # `volume show` must reflect it rather than reporting 0 chunks / 0 bytes (#1036).
  local out; out=$(ncli 1 "volume show ${ACCEPT_VOL}" 2>/dev/null)
  echo "${out}" | grep -iE 'chunks|logical|physical' | sed 's/^/  /' >&2
  local chunks; chunks=$(echo "${out}" | grep -iE 'chunks' | grep -oE '[0-9]+' | head -1)
  [ "${chunks:-0}" -gt 0 ] \
    || { echo "  volume show reports 0 chunks despite writes (#1036)" >&2; return 1; }
}

# #1035: a FUSE unmount must not wedge the control plane. Unmount, confirm the
# CLI still responds, then re-mount (MountManager must not be left in a bad
# state). Runs after the FUSE-dependent steps; leaves the mount restored.
s_fuse_unmount_resilience() {
  volume_ready || return 77
  node_ssh 1 "grep -q ' ${FUSE_MNT} fuse' /proc/mounts" 2>/dev/null || return 77

  ncli 1 "fuse unmount ${FUSE_MNT}" 2>&1 | sed 's/^/  /' >&2 || true

  ncli 1 "cluster status" 2>&1 | grep -qE 'Status[[:space:]]+running' \
    || { echo "  CLI wedged after fuse unmount (#1035)" >&2; return 1; }

  node_ssh 1 "for i in \$(seq 1 15); do sudo timeout 20 neonfs fuse mount ${ACCEPT_VOL} ${FUSE_MNT} 2>&1 | grep -qiE 'mounted|already' && exit 0; sleep 2; done; exit 1" 2>/dev/null \
    || { echo "  re-mount after unmount failed — MountManager left in a bad state (#1035)" >&2; return 1; }
}

s_replication() {
  [ "${NODES}" -ge 2 ] || { echo "  single node — replication not applicable" >&2; return 77; }
  local cores; cores=$(ncli 1 "cluster status" 2>/dev/null | grep -iE 'core nodes|members' | grep -oE '[0-9]+' | head -1)
  ncli 1 "volume list" 2>/dev/null | grep -qE "^${ACCEPT_REPL_VOL}[[:space:]]" \
    || ncli 1 "volume create ${ACCEPT_REPL_VOL} --replicas 2" 2>&1 | grep -qi 'created successfully' \
    || { echo "  could not create replicas=2 volume (cluster has < 2 core nodes? see #1033)" >&2; return 1; }
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
  if node_ssh 1 "sudo test -S ${DOCKER_SOCK}" 2>/dev/null; then
    dvd VolumeDriver.Unmount "{\"Name\":\"${DOCKER_VOL}\"}" >/dev/null 2>&1 || true
    dvd VolumeDriver.Remove "{\"Name\":\"${DOCKER_VOL}\"}" >/dev/null 2>&1 || true
  fi
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
  step "Docker volume driver (create/mount/io)"     s_docker_volume_driver
  step "containerd content store endpoint"          s_containerd_endpoint
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
