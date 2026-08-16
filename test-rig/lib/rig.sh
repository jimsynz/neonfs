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
VOLUME_NAME="${VOLUME_NAME:-test}"
# Default replication factor tracks the node count so `volume create` is
# satisfiable without --allow-under-replicated.
REPLICAS="${REPLICAS:-${NODES}}"
# Codec/tiering knobs applied to volumes the rig creates. compression +
# encryption are `volume create` flags; tiering is a post-create `volume update`
# (no create flag). Erasure has no CLI create flag yet (tracked separately), so
# it isn't a rig knob.
# Refuse to boot without /dev/kvm rather than falling through to TCG, which
# is roughly ten times slower. The interactive default is to warn and carry
# on — slow beats refusing to start on a laptop — but a non-interactive
# caller usually cannot tolerate it: a CI job times out with nothing in the
# log to say why, and a benchmark produces numbers ~10x off that read as a
# regression rather than as emulation.
REQUIRE_KVM="${REQUIRE_KVM:-0}"

COMPRESSION="${COMPRESSION:-zstd}"
ENCRYPTION="${ENCRYPTION:-none}"
INITIAL_TIER="${INITIAL_TIER:-}"

# --- docker-storage scenario ----------------------------------------------
# Image blobs live in NeonFS via the containerd content proxy; unpacked layers
# and container rootfs stay on local disk. Putting docker's whole `data-root`
# on a FUSE mount was abandoned: dockerd's boltdb volume store needs
# `MAP_SHARED` mmap, which the FUSE mount refuses because it sets
# `FOPEN_DIRECT_IO` for cross-interface coherence.
CONTAINERD_SOCK="${CONTAINERD_SOCK:-/run/neonfs/containerd.sock}"
DOCKER_SOCK="${DOCKER_SOCK:-/run/neonfs/docker.sock}"
# The volume the containerd content plugin stores blobs in — matches
# :neonfs_containerd, :volume, whose default is "containerd".
CONTAINERD_VOL="${CONTAINERD_VOL:-containerd}"
DOCKER_STORAGE_VOL="${DOCKER_STORAGE_VOL:-rig_docker_shared}"
# Pulled through the proxy, so it must be small and multi-layer-free enough to
# stay quick, but a *real* image — the point is exercising manifest, config and
# layer blobs, which a synthetic blob ingest does not.
DOCKER_STORAGE_IMAGE="${DOCKER_STORAGE_IMAGE:-docker.io/library/busybox:latest}"

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

# Run the neonfs CLI on a node (as root, so it reads the runtime files the
# daemon wrote under /run/neonfs and the TLS material under /var/lib/neonfs).
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
  [ -w /dev/kvm ] || no_kvm
}

# Truthy spellings beyond `1` are accepted because the alternative is a
# caller who asked for the check, did not get it, and cannot tell.
require_kvm_requested() {
  case "$(printf '%s' "${REQUIRE_KVM}" | tr '[:upper:]' '[:lower:]')" in
    1 | true | yes | on) return 0 ;;
    *) return 1 ;;
  esac
}

no_kvm() {
  if require_kvm_requested; then
    die "/dev/kvm is not present or not writable, and REQUIRE_KVM is set — \
TCG emulation is ~10x slower, so this would not fail so much as never finish"
  fi

  warn "/dev/kvm not writable — VMs will fall back to slow TCG emulation"
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
    # Docker + containerd back the container-runtime acceptance steps: the
    # NeonFS Docker volume driver (docker volume create -d neonfs) and the
    # containerd content-store proxy plugin (ctr content ingest/get).
    echo "package_update: true"
    echo "packages:"
    echo "  - docker.io"
    echo "  - containerd"
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

# True when the cached Samba VFS module's pinned Samba version is the one the
# node's apt would install. Anything else — a distro point release, a deb built
# on a different base — means it cannot be installed there.
vfs_deb_matches_node() {
  local i="$1" deb="$2" required candidate

  required="$(dpkg-deb -f "${deb}" Depends 2>/dev/null |
    tr ',' '\n' | sed -n 's/.*samba (= \(.*\))/\1/p' | head -1)"
  [ -n "${required}" ] || return 0

  candidate="$(node_ssh "$i" "apt-cache policy samba 2>/dev/null | awk '/Candidate:/{print \$2}'" | tr -d '\r')"
  [ -n "${candidate}" ] || return 0

  [ "${required}" = "${candidate}" ]
}

provision_node() {
  local i="$1" ip; ip="$(node_ip "$i")"
  log "installing neonfs_omnibus on node ${i}"

  node_ssh "$i" "sudo mkdir -p /tmp/debs && sudo chown rig:rig /tmp/debs"
  # samba-vfs-neonfs pins the exact distro Samba it was built against, so a
  # cached one goes stale the moment the distro publishes a point release and
  # can no longer be installed. `neonfs-omnibus` only *recommends* it, so the
  # right answer then is to ship no VFS module and carry on without CIFS —
  # apt skips an unsatisfiable recommendation silently. Passing the .deb to
  # `apt-get install` regardless makes it a direct target, and its own hard
  # dependency takes the whole transaction down with it.
  local vfs_deb; vfs_deb="$(ls -t "${DEB_DIR}"/samba-vfs-neonfs_*.deb 2>/dev/null | head -1 || true)"
  if [ -n "${vfs_deb}" ] && ! vfs_deb_matches_node "$i" "${vfs_deb}"; then
    log "skipping stale ${vfs_deb##*/} (built for a different Samba); this rig has no CIFS"
    vfs_deb=""
  fi
  node_scp "$i" \
    "${DEB_DIR}/neonfs-common_${VERSION}_amd64.deb" \
    "${DEB_DIR}/neonfs-cli_${VERSION}_amd64.deb" \
    "${DEB_DIR}/neonfs-omnibus_${VERSION}_amd64.deb" \
    ${vfs_deb:+"${vfs_deb}"} \
    "rig@127.0.0.1:/tmp/debs/"

  node_ssh "$i" "sudo apt-get update -qq"
  node_ssh "$i" "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q /tmp/debs/*.deb"

  node_ssh "$i" "sudo systemctl stop neonfs-omnibus"
  node_ssh "$i" "sudo install -d -m 0755 /etc/neonfs"
  node_ssh "$i" "sudo tee /etc/neonfs/neonfs.conf >/dev/null" <<EOF
RELEASE_DISTRIBUTION=name
RELEASE_NODE=$(node_erl "$i")
NEONFS_DIST_PORT=${DIST_PORT}
NEONFS_CORE_NODE=$(node_erl "$i")
EOF

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

# --- standalone interface node ----------------------------------------------

# Fixed index for the interface-only VM so it never collides with core
# nodes (which count up from 1).
IFACE_INDEX="${IFACE_INDEX:-9}"

iface_erl() { echo "neonfs_nfs@$(node_ip "$1")"; }

# Install only neonfs-common + neonfs-cli + neonfs-nfs — no core, no
# omnibus. NEONFS_CORE_NODE deliberately points at the remote core: the
# pre-join CLI must not be able to authenticate there, proving
# `cluster join` drives the local interface daemon instead.
provision_iface_node() {
  local i="$1" ip; ip="$(node_ip "$i")"
  log "installing neonfs-nfs (interface-only) on node ${i}"

  node_ssh "$i" "sudo mkdir -p /tmp/debs && sudo chown rig:rig /tmp/debs"
  node_scp "$i" \
    "${DEB_DIR}/neonfs-common_${VERSION}_amd64.deb" \
    "${DEB_DIR}/neonfs-cli_${VERSION}_amd64.deb" \
    "${DEB_DIR}/neonfs-nfs_${VERSION}_amd64.deb" \
    "rig@127.0.0.1:/tmp/debs/"

  node_ssh "$i" "sudo apt-get update -qq"
  node_ssh "$i" "sudo DEBIAN_FRONTEND=noninteractive apt-get install -y -q \
    /tmp/debs/neonfs-common_${VERSION}_amd64.deb \
    /tmp/debs/neonfs-cli_${VERSION}_amd64.deb \
    /tmp/debs/neonfs-nfs_${VERSION}_amd64.deb"

  node_ssh "$i" "sudo systemctl stop neonfs-nfs 2>/dev/null || true"
  node_ssh "$i" "sudo install -d -m 0755 /etc/neonfs"
  node_ssh "$i" "sudo tee /etc/neonfs/neonfs.conf >/dev/null" <<EOF
RELEASE_DISTRIBUTION=name
NEONFS_NFS_NODE=$(iface_erl "$i")
NEONFS_CORE_NODE=$(node_erl 1)
NEONFS_DIST_PORT=${DIST_PORT}
EOF

  node_ssh "$i" "sudo systemctl start neonfs-nfs"
  log "interface daemon started on node ${i} ($(iface_erl "$i"))"
}

# The acceptance: the interface node joins with an invite token,
# shows up in `node list` as an nfs service, serves the NFS port, and
# survives a daemon restart with no manual intervention.
iface_join_scenario() {
  local i="${IFACE_INDEX}" ip; ip="$(node_ip "${IFACE_INDEX}")"

  log "joining interface node ${i} ($(iface_erl "$i")) via invite token"
  local token
  token="$(node_cli 1 "cluster create-invite --expires 1h" | grep -oE 'nfs_inv_[A-Za-z0-9_]+' | head -1)"
  [ -n "${token}" ] || die "could not obtain invite token from node 1"
  node_cli "$i" "cluster join --token '${token}' --via $(node_ip 1):${CLUSTER_API_PORT}"

  iface_assert_serving "registered and serving after join"

  log "restarting the interface daemon (restart survivability)"
  node_ssh "$i" "sudo systemctl restart neonfs-nfs"

  iface_assert_serving "registered and serving after restart"

  log "interface-node join scenario passed"
}

# Poll until node 1's `node list` shows the interface node as a non-offline
# nfs service AND its NFS port answers, or die with diagnostics.
iface_assert_serving() {
  local label="$1" ip row; ip="$(node_ip "${IFACE_INDEX}")"
  log "verifying: ${label}"

  local deadline=$(( SECONDS + 120 ))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    row="$(node_cli 1 "node list" 2>/dev/null | grep "neonfs_nfs@${ip}" || true)"

    if [ -n "${row}" ] && ! echo "${row}" | grep -q offline &&
      node_ssh 1 "timeout 5 bash -c 'exec 3<>/dev/tcp/${ip}/2049'" 2>/dev/null; then
      log "OK: ${label}"
      return 0
    fi

    sleep 3
  done

  node_cli 1 "node list" >&2 || true
  die "interface node not serving: ${label}"
}

# --- cluster bootstrap -----------------------------------------------------

cluster_bootstrap() {
  # Init the system volume at replicas 1: only node 1's first drive is registered
  # at this point. It auto-adjusts up to the core-node count as nodes join.
  log "initialising cluster '${CLUSTER_NAME}' on node 1"
  node_cli 1 "cluster init --name '${CLUSTER_NAME}' --drive /mnt/neonfs/drive1 --system-replicas 1"
  # `cluster init` restarts the node to bring the cluster TLS config into effect
  # so it briefly drops out. Wait for it to come back before issuing
  # any further CLI commands, otherwise they hit the node mid-restart.
  wait_init_restart 1
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

  log "creating volume '${VOLUME_NAME}' (replicas ${REPLICAS}, compression ${COMPRESSION}, encryption ${ENCRYPTION})"
  node_cli 1 "volume create '${VOLUME_NAME}' --replicas ${REPLICAS} $(codec_create_flags)"
  apply_initial_tier 1 "${VOLUME_NAME}"
}

# Volume codec flags for `volume create`. Tiering is applied separately
# via `apply_initial_tier` because `initial_tier` is a `volume update` field.
codec_create_flags() {
  printf -- '--compression %s --encryption %s' "${COMPRESSION}" "${ENCRYPTION}"
}

apply_initial_tier() {
  local node="$1" vol="$2"
  [ -n "${INITIAL_TIER}" ] || return 0
  node_cli "${node}" "volume update '${vol}' --initial-tier ${INITIAL_TIER}" >/dev/null 2>&1 \
    || warn "failed to set initial tier '${INITIAL_TIER}' on '${vol}'"
}

add_extra_drives() {
  local i="$1" d
  for d in $(seq 2 "${DRIVES_PER_NODE}"); do
    node_cli "$i" "drive add --path /mnt/neonfs/drive${d}"
  done
}

# `cluster init` triggers a full node restart. Wait for the node to drop
# out (restart started) and then recover, so subsequent CLI commands don't race
# the reboot.
wait_init_restart() {
  local i="$1" deadline
  log "waiting for node ${i} to restart after cluster init"
  deadline=$(( SECONDS + 30 ))
  while [ "${SECONDS}" -lt "${deadline}" ]; do
    node_ssh "$i" "sudo timeout 5 neonfs node status >/dev/null 2>&1" || break
    sleep 1
  done
  wait_daemon "$i"
}

# --- docker-storage scenario -----------------------------------------------

# Docker image/layer storage on NeonFS, in the shape that actually works:
# content-addressed blobs through the containerd content proxy, unpacked
# layers and rootfs on local disk, plus a NeonFS docker volume shared across
# nodes. Needs NODES>=2 — the shared-volume half is about cross-node
# visibility, which a single node cannot demonstrate.
docker_storage_scenario() {
  local nodes; nodes="$(discovered_nodes | wc -l)"
  [ "${nodes}" -ge 2 ] \
    || die "docker-storage needs a NODES>=2 cluster (found ${nodes}); try: NODES=2 ./neonfs-rig up"

  docker_image_storage_step
  docker_shared_volume_step

  log "docker-storage scenario passed"
}

# Pull and run a real image through a throwaway containerd whose content store
# is the NeonFS proxy, then prove the blobs are in the volume rather than on
# local disk.
#
# The containerd config mirrors `acceptance.sh`'s `s_containerd_content`:
# `io.containerd.content.v1.content` disabled so the proxy is the *only*
# content store, and the CRI plugin disabled because we drive it with `ctr`.
# It differs in keeping the default overlayfs snapshotter — layers unpack
# locally, which is the whole point of this shape.
docker_image_storage_step() {
  log "image blobs through the containerd content proxy"

  node_ssh 1 "command -v containerd >/dev/null 2>&1 && command -v ctr >/dev/null 2>&1" \
    || die "containerd/ctr not installed on node 1"
  node_ssh 1 "sudo test -S ${CONTAINERD_SOCK}" \
    || die "containerd proxy socket ${CONTAINERD_SOCK} absent — the omnibus content plugin is not running"

  node_cli 1 "volume show ${CONTAINERD_VOL}" >/dev/null 2>&1 \
    || node_cli 1 "volume create ${CONTAINERD_VOL} --replicas 1" >/dev/null \
    || die "could not create the ${CONTAINERD_VOL} content-store volume"

  local before after
  before="$(docker_storage_chunk_count)"

  node_ssh 1 "sudo bash -s ${CONTAINERD_SOCK} ${DOCKER_STORAGE_IMAGE}" <<'REMOTE' 2>&1 | sed 's/^/  /' >&2
set -e
PROXY_SOCK="$1"; IMAGE="$2"
TMP="$(mktemp -d /tmp/neonfs-ctrd.XXXXXX)"
trap 'kill "${CTRD_PID:-0}" 2>/dev/null || true; rm -rf "${TMP}"' EXIT
mkdir -p "${TMP}/root" "${TMP}/state"
GRPC="${TMP}/containerd.sock"

# No `io.containerd.snapshotter.v1.overlayfs` in disabled_plugins: layers must
# unpack to local disk. Only the content store is remote.
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

CTR="ctr --address ${GRPC} --namespace rig"

${CTR} image pull "${IMAGE}" \
  || { echo "ctr image pull failed"; tail -40 "${TMP}/containerd.log"; exit 1; }

# The manifest, config and layer blobs all had to travel through the proxy to
# get here, so a populated content ls is the proxy having served the pull.
BLOBS="$(${CTR} content ls -q | wc -l)"
[ "${BLOBS}" -gt 0 ] || { echo "content store empty after pulling ${IMAGE}"; exit 1; }
echo "content store holds ${BLOBS} blob(s) after the pull"

MARKER="neonfs-rig-container-ran"
OUT="$(${CTR} run --rm --snapshotter overlayfs "${IMAGE}" rig_probe echo "${MARKER}" 2>&1)" \
  || { echo "ctr run failed: ${OUT}"; tail -40 "${TMP}/containerd.log"; exit 1; }
echo "${OUT}" | grep -q "${MARKER}" \
  || { echo "container ran but did not print its marker: ${OUT}"; exit 1; }
echo "container ran from locally-unpacked layers"
REMOTE

  # The blobs are content-addressed files in the volume, so the volume's own
  # accounting is the check that they are really in NeonFS and not merely
  # cached by containerd — `ctr content ls` alone would pass against a store
  # that never persisted anything.
  after="$(docker_storage_chunk_count)"
  log "content-store volume chunks: ${before} → ${after}"
  [ "${after}" -gt "${before}" ] \
    || die "pulling ${DOCKER_STORAGE_IMAGE} added no chunks to ${CONTAINERD_VOL} — blobs did not reach NeonFS"

  log "OK: image blobs landed in ${CONTAINERD_VOL} and the container ran"
}

# Chunk count for the content-store volume, or 0 before it exists.
docker_storage_chunk_count() {
  node_cli 1 "volume show ${CONTAINERD_VOL}" 2>/dev/null \
    | grep -iE 'chunks' | grep -oE '[0-9]+' | head -1 || true
}

# A NeonFS docker volume attached on both nodes: a write through node 1's
# container must be visible to a container on node 2. This is the half that
# needs the multi-node cluster.
docker_shared_volume_step() {
  log "shared NeonFS docker volume across nodes"

  local i
  for i in 1 2; do
    node_ssh "$i" "command -v docker >/dev/null 2>&1" \
      || die "docker not installed on node ${i}"
    node_ssh "$i" "sudo systemctl is-active --quiet docker || sudo systemctl start docker"
    node_ssh "$i" "test -f /etc/docker/plugins/neonfs.spec" \
      || die "/etc/docker/plugins/neonfs.spec missing on node ${i} — docker cannot discover the driver"
    node_ssh "$i" "sudo test -S ${DOCKER_SOCK}" \
      || die "neonfs docker plugin socket ${DOCKER_SOCK} absent on node ${i}"
    node_ssh "$i" "sudo docker pull busybox:latest >/dev/null 2>&1" \
      || die "could not pull busybox on node ${i}"
  done

  node_ssh 1 "sudo docker volume rm ${DOCKER_STORAGE_VOL} >/dev/null 2>&1 || true
    sudo docker volume create -d neonfs ${DOCKER_STORAGE_VOL}" 2>&1 | sed 's/^/  /' >&2
  node_ssh 1 "sudo docker volume ls --format '{{.Driver}} {{.Name}}' | grep -qx 'neonfs ${DOCKER_STORAGE_VOL}'" \
    || die "docker volume create -d neonfs failed on node 1"

  local marker="shared-${RANDOM}"
  node_ssh 1 "sudo docker run --rm -v ${DOCKER_STORAGE_VOL}:/data busybox \
    sh -c 'echo ${marker} > /data/shared.txt && sync'" 2>&1 | sed 's/^/  /' >&2 \
    || die "writing to the shared volume from node 1 failed"

  docker_volume_read_expect 2 "${marker}"

  # And back the other way, so the volume is not merely readable but writable
  # from either side.
  local reply="reply-${RANDOM}"
  node_ssh 2 "sudo docker run --rm -v ${DOCKER_STORAGE_VOL}:/data busybox \
    sh -c 'echo ${reply} > /data/reply.txt && sync'" 2>&1 | sed 's/^/  /' >&2 \
    || die "writing to the shared volume from node 2 failed"

  docker_volume_read_expect 1 "${reply}" reply.txt

  log "OK: shared volume ${DOCKER_STORAGE_VOL} carries mutations both ways"
}

# Poll a container on node <i> until it reads <expected> from the shared
# volume. Polls rather than reads once: the write on the other node commits
# through quorum, and the read side is a fresh mount each time.
docker_volume_read_expect() {
  local i="$1" expected="$2" file="${3:-shared.txt}"
  local deadline=$(( SECONDS + 60 )) got=""

  while [ "${SECONDS}" -lt "${deadline}" ]; do
    got="$(node_ssh "$i" "sudo docker run --rm -v ${DOCKER_STORAGE_VOL}:/data busybox \
      cat /data/${file} 2>/dev/null" 2>/dev/null | tr -d '\r\n')"
    [ "${got}" = "${expected}" ] && { log "OK: node ${i} sees ${file} = ${expected}"; return 0; }
    sleep 3
  done

  die "node ${i} never saw ${file} = '${expected}' (last read: '${got}')"
}

docker_storage_cleanup() {
  log "cleaning up docker-storage scenario state"
  local i
  for i in 1 2; do
    node_ssh "$i" "command -v docker >/dev/null 2>&1 && \
      sudo docker volume rm ${DOCKER_STORAGE_VOL} >/dev/null 2>&1" >/dev/null 2>&1 || true
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
