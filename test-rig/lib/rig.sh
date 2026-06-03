# shellcheck shell=bash
# Shared configuration and helpers for the NeonFS QEMU test rig.
# Sourced by ./neonfs-rig; not meant to be run directly.

RIG_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_ROOT="$(cd "${RIG_ROOT}/.." && pwd)"

CACHE_DIR="${RIG_ROOT}/.cache"
IMAGE_DIR="${CACHE_DIR}/images"
DEB_DIR="${CACHE_DIR}/debs"
RUN_DIR="${CACHE_DIR}/run"
SSH_KEY="${RUN_DIR}/id_ed25519"

BASE_IMAGE_NAME="debian-13-genericcloud-amd64.qcow2"
BASE_IMAGE_URL="https://cloud.debian.org/images/cloud/trixie/latest/${BASE_IMAGE_NAME}"
BASE_IMAGE="${IMAGE_DIR}/${BASE_IMAGE_NAME}"

# Tunables — override from the environment, e.g. `NODES=3 ./neonfs-rig up`.
NODES="${NODES:-1}"
DRIVES_PER_NODE="${DRIVES_PER_NODE:-2}"
DRIVE_SIZE="${DRIVE_SIZE:-2G}"
ROOT_SIZE="${ROOT_SIZE:-12G}"
VM_MEM="${VM_MEM:-2048}"
VM_CPUS="${VM_CPUS:-2}"
SSH_BASE_PORT="${SSH_BASE_PORT:-2230}"
DIST_PORT="${DIST_PORT:-9100}"
CLUSTER_API_PORT="${CLUSTER_API_PORT:-9568}"
MCAST_ADDR="${MCAST_ADDR:-230.13.37.1:6555}"
CLUSTER_NAME="${CLUSTER_NAME:-rig}"
CLUSTER_COOKIE="${CLUSTER_COOKIE:-neonfs-rig-cookie}"
VOLUME_NAME="${VOLUME_NAME:-test}"
# Default replication factor tracks the node count so `volume create` is
# satisfiable without --allow-under-replicated.
REPLICAS="${REPLICAS:-${NODES}}"

VERSION="$(grep -m1 '@version "' "${REPO_ROOT}/neonfs_omnibus/mix.exs" | sed -E 's/.*"([^"]+)".*/\1/')"

# --- logging ---------------------------------------------------------------

log()  { printf '\033[1;34m==>\033[0m %s\n' "$*" >&2; }
warn() { printf '\033[1;33mwarn:\033[0m %s\n' "$*" >&2; }
die()  { printf '\033[1;31merror:\033[0m %s\n' "$*" >&2; exit 1; }

# --- per-node addressing ---------------------------------------------------

node_dir()      { echo "${RUN_DIR}/node-$1"; }
node_ip()       { echo "10.10.10.$(( 10 + $1 ))"; }
node_erl()      { echo "neonfs@$(node_ip "$1")"; }
node_ssh_port() { echo "$(( SSH_BASE_PORT + $1 ))"; }
node_mac_nat()  { printf '52:54:00:13:37:%02x' "$1"; }
node_mac_clus() { printf '52:54:00:13:38:%02x' "$1"; }

ssh_opts=(-i "${SSH_KEY}"
  -o StrictHostKeyChecking=no
  -o UserKnownHostsFile=/dev/null
  -o LogLevel=ERROR
  -o ConnectTimeout=10)

node_ssh() {
  local i="$1"; shift
  ssh "${ssh_opts[@]}" -p "$(node_ssh_port "$i")" "rig@127.0.0.1" "$@"
}

node_scp() {
  local i="$1"; shift
  scp "${ssh_opts[@]}" -P "$(node_ssh_port "$i")" "$@"
}

# Run the neonfs CLI on a node (as root, so it reads the cookie/dist files
# the daemon wrote under /run/neonfs and /var/lib/neonfs).
node_cli() {
  local i="$1"; shift
  node_ssh "$i" "sudo neonfs $*"
}

# --- prerequisites ---------------------------------------------------------

require_tools() {
  local missing=()
  for t in qemu-system-x86_64 qemu-img cloud-localds ssh scp ssh-keygen; do
    command -v "$t" >/dev/null 2>&1 || missing+=("$t")
  done
  [ "${#missing[@]}" -eq 0 ] || die "missing tools: ${missing[*]} (see test-rig/README.md)"
  [ -w /dev/kvm ] || warn "/dev/kvm not writable — VMs will fall back to slow TCG emulation"
}

ensure_image() {
  [ -f "${BASE_IMAGE}" ] && return 0
  mkdir -p "${IMAGE_DIR}"
  log "downloading base image ${BASE_IMAGE_NAME}"
  curl -fSL -o "${BASE_IMAGE}.part" "${BASE_IMAGE_URL}"
  mv "${BASE_IMAGE}.part" "${BASE_IMAGE}"
}

ensure_debs() {
  local deb newer
  deb="$(ls -t "${DEB_DIR}"/neonfs-omnibus_*.deb 2>/dev/null | head -1 || true)"
  if [ -n "${deb}" ]; then
    newer="$(find "${REPO_ROOT}" -type f \
      \( -name '*.ex' -o -name '*.exs' -o -name '*.rs' -o -name '*.toml' \
         -o -name '*.service' -o -name '*.yaml' -o -name '*.sh' \) \
      -not -path '*/_build/*' -not -path '*/deps/*' -not -path '*/target/*' \
      -newer "${deb}" -print -quit 2>/dev/null || true)"
    [ -z "${newer}" ] && return 0
    log "source changed since last build — rebuilding .debs"
  else
    log "building .debs (VERSION=${VERSION}) — this takes several minutes"
  fi
  mkdir -p "${DEB_DIR}"
  VERSION="${VERSION}" OUT_DIR="${DEB_DIR}" bash "${REPO_ROOT}/packaging/build-debs.sh"
}

ensure_ssh_key() {
  [ -f "${SSH_KEY}" ] && return 0
  mkdir -p "${RUN_DIR}"
  ssh-keygen -t ed25519 -N '' -f "${SSH_KEY}" -C "neonfs-rig" >/dev/null
}

# --- cloud-init seed -------------------------------------------------------

write_seed() {
  local i="$1" dir; dir="$(node_dir "$i")"
  local pubkey; pubkey="$(cat "${SSH_KEY}.pub")"

  cat > "${dir}/meta-data" <<EOF
instance-id: neonfs-${i}
local-hostname: neonfs-${i}
EOF

  {
    echo "#cloud-config"
    echo "hostname: neonfs-${i}"
    echo "fqdn: neonfs-${i}.${CLUSTER_NAME}.local"
    echo "users:"
    echo "  - name: rig"
    echo "    sudo: 'ALL=(ALL) NOPASSWD:ALL'"
    echo "    shell: /bin/bash"
    echo "    ssh_authorized_keys:"
    echo "      - ${pubkey}"
    echo "ssh_pwauth: false"
    # Data drives appear as vdb, vdc, ... (root is vda, seed is last).
    local letters=({b..z}) d dev
    echo "fs_setup:"
    for d in $(seq 1 "${DRIVES_PER_NODE}"); do
      dev="/dev/vd${letters[$((d - 1))]}"
      echo "  - {device: '${dev}', filesystem: ext4, label: 'nfsdrive${d}', overwrite: true}"
    done
    echo "mounts:"
    for d in $(seq 1 "${DRIVES_PER_NODE}"); do
      dev="/dev/vd${letters[$((d - 1))]}"
      echo "  - ['${dev}', '/mnt/neonfs/drive${d}', 'ext4', 'defaults,noatime', '0', '2']"
    done
  } > "${dir}/user-data"

  cat > "${dir}/network-config" <<EOF
version: 2
ethernets:
  nat:
    match: {macaddress: "$(node_mac_nat "$i")"}
    set-name: nat
    dhcp4: true
  clus:
    match: {macaddress: "$(node_mac_clus "$i")"}
    set-name: clus
    addresses: ["$(node_ip "$i")/24"]
EOF

  cloud-localds --network-config="${dir}/network-config" \
    "${dir}/seed.iso" "${dir}/user-data" "${dir}/meta-data"
}

# --- VM lifecycle ----------------------------------------------------------

create_node_disks() {
  local i="$1" dir; dir="$(node_dir "$i")"
  mkdir -p "${dir}"
  qemu-img create -q -f qcow2 -F qcow2 -b "${BASE_IMAGE}" "${dir}/root.qcow2" "${ROOT_SIZE}"
  local d
  for d in $(seq 1 "${DRIVES_PER_NODE}"); do
    qemu-img create -q -f raw "${dir}/drive-${d}.img" "${DRIVE_SIZE}"
  done
}

node_running() {
  local pidfile; pidfile="$(node_dir "$1")/qemu.pid"
  [ -f "${pidfile}" ] && kill -0 "$(cat "${pidfile}")" 2>/dev/null
}

boot_node() {
  local i="$1" dir; dir="$(node_dir "$i")"
  if node_running "$i"; then warn "node ${i} already running"; return 0; fi

  local accel cpu
  if [ -w /dev/kvm ]; then
    accel="kvm"; cpu="host"
  else
    accel="tcg"; cpu="max"
    warn "no writable /dev/kvm — node ${i} uses slow TCG emulation (see README for KVM access)"
  fi
  local args=(
    -name "neonfs-${i}"
    -machine "q35,accel=${accel}"
    -cpu "${cpu}"
    -smp "${VM_CPUS}" -m "${VM_MEM}"
    -display none
    -drive "if=virtio,file=${dir}/root.qcow2,format=qcow2"
  )

  local d
  for d in $(seq 1 "${DRIVES_PER_NODE}"); do
    args+=(-drive "if=virtio,file=${dir}/drive-${d}.img,format=raw")
  done
  args+=(-drive "if=virtio,file=${dir}/seed.iso,format=raw,readonly=on")

  args+=(
    -netdev "user,id=nat,hostfwd=tcp:127.0.0.1:$(node_ssh_port "$i")-:22"
    -device "virtio-net-pci,netdev=nat,mac=$(node_mac_nat "$i")"
    -netdev "socket,id=clus,mcast=${MCAST_ADDR}"
    -device "virtio-net-pci,netdev=clus,mac=$(node_mac_clus "$i")"
    -serial "file:${dir}/serial.log"
    -qmp "unix:${dir}/qmp.sock,server,nowait"
    -pidfile "${dir}/qemu.pid"
    -daemonize
  )

  log "booting node ${i} ($(node_erl "$i"), ssh 127.0.0.1:$(node_ssh_port "$i"), accel=${accel})"
  qemu-system-x86_64 "${args[@]}"
}

wait_ssh() {
  local i="$1" deadline=$(( SECONDS + 360 ))
  log "waiting for ssh on node ${i}"
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if node_ssh "$i" true 2>/dev/null; then return 0; fi
    sleep 3
  done
  die "node ${i} did not become reachable over ssh (see $(node_dir "$i")/serial.log)"
}

wait_cloud_init() {
  local i="$1"
  log "waiting for cloud-init to finish on node ${i}"
  node_ssh "$i" "sudo cloud-init status --wait >/dev/null 2>&1 || true"
}

# --- provisioning ----------------------------------------------------------

provision_node() {
  local i="$1" ip; ip="$(node_ip "$i")"
  log "installing neonfs_omnibus on node ${i}"

  node_ssh "$i" "sudo mkdir -p /tmp/debs && sudo chown rig:rig /tmp/debs"
  node_scp "$i" \
    "${DEB_DIR}/neonfs-common_${VERSION}_amd64.deb" \
    "${DEB_DIR}/neonfs-cli_${VERSION}_amd64.deb" \
    "${DEB_DIR}/neonfs-omnibus_${VERSION}_amd64.deb" \
    "rig@127.0.0.1:/tmp/debs/"

  node_ssh "$i" "sudo apt-get update -qq"
  node_ssh "$i" "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
    /tmp/debs/neonfs-common_${VERSION}_amd64.deb \
    /tmp/debs/neonfs-cli_${VERSION}_amd64.deb \
    /tmp/debs/neonfs-omnibus_${VERSION}_amd64.deb"

  node_ssh "$i" "sudo systemctl stop neonfs-omnibus"
  node_ssh "$i" "sudo install -d -m 0755 /etc/neonfs"
  node_ssh "$i" "sudo tee /etc/neonfs/neonfs.conf >/dev/null" <<EOF
RELEASE_DISTRIBUTION=name
RELEASE_NODE=$(node_erl "$i")
RELEASE_COOKIE=${CLUSTER_COOKIE}
NEONFS_DIST_PORT=${DIST_PORT}
NEONFS_CORE_NODE=$(node_erl "$i")
EOF

  node_ssh "$i" "printf %s '${CLUSTER_COOKIE}' | sudo tee /var/lib/neonfs/.erlang.cookie >/dev/null \
    && sudo chown neonfs:neonfs /var/lib/neonfs/.erlang.cookie \
    && sudo chmod 600 /var/lib/neonfs/.erlang.cookie"
  node_ssh "$i" "sudo chown -R neonfs:neonfs /mnt/neonfs"

  node_ssh "$i" "sudo systemctl start neonfs-omnibus"
  wait_daemon "$i"
}

wait_daemon() {
  local i="$1" deadline=$(( SECONDS + 240 ))
  log "waiting for neonfs daemon on node ${i}"
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    if node_ssh "$i" "sudo neonfs node status >/dev/null 2>&1"; then return 0; fi
    sleep 3
  done
  warn "neonfs daemon on node ${i} not answering CLI yet (continuing)"
}

# --- cluster bootstrap -----------------------------------------------------

cluster_bootstrap() {
  # Init the system volume at replicas 1: only node 1's first drive is registered
  # at this point. It auto-adjusts up to the core-node count as nodes join.
  log "initialising cluster '${CLUSTER_NAME}' on node 1"
  node_cli 1 "cluster init --name '${CLUSTER_NAME}' --drive /mnt/neonfs/drive1 --system-replicas 1"
  add_extra_drives 1

  local i
  for i in $(seq 2 "${NODES}"); do
    log "joining node ${i} to the cluster"
    local token
    token="$(node_cli 1 "cluster create-invite --expires 1h" | grep -oE 'nfs_inv_[A-Za-z0-9_]+' | head -1)"
    [ -n "${token}" ] || die "could not obtain invite token from node 1"
    node_cli "$i" "cluster join --token '${token}' --via $(node_ip 1):${CLUSTER_API_PORT}"
    node_cli "$i" "drive add --path /mnt/neonfs/drive1"
    add_extra_drives "$i"
  done

  log "creating volume '${VOLUME_NAME}' (replicas ${REPLICAS})"
  node_cli 1 "volume create '${VOLUME_NAME}' --replicas ${REPLICAS}"
}

add_extra_drives() {
  local i="$1" d
  for d in $(seq 2 "${DRIVES_PER_NODE}"); do
    node_cli "$i" "drive add --path /mnt/neonfs/drive${d}"
  done
}

# --- teardown --------------------------------------------------------------

stop_node() {
  local i="$1" pidfile; pidfile="$(node_dir "$i")/qemu.pid"
  if node_running "$i"; then
    log "stopping node ${i}"
    kill "$(cat "${pidfile}")" 2>/dev/null || true
  fi
  rm -f "${pidfile}"
}

discovered_nodes() {
  ls -d "${RUN_DIR}"/node-* 2>/dev/null | sed -E 's/.*node-//' | sort -n
}
