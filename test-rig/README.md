# NeonFS QEMU test rig

Spin up one or more throwaway VMs, attach raw disk images as NeonFS drives,
install `neonfs_omnibus` from `.deb` packages built out of this repository, and
form a cluster — end to end, with one command.

This exercises the **real install path** an operator follows on bare metal: a
base OS, the `.deb` packages, the `neonfs-omnibus` systemd unit, and the
`neonfs` CLI driving `cluster init` / `drive add` / `volume create`. It is
deliberately separate from the in-BEAM `:peer` integration tests
(`neonfs_integration`), which test code, not packaging or the on-host runtime.

## Quick start

```bash
cd test-rig
./neonfs-rig up                 # single-node cluster (default)
./neonfs-rig status
./neonfs-rig cli 1 -- volume list
./neonfs-rig ssh 1              # shell into node 1
./neonfs-rig down               # stop the VM(s)
./neonfs-rig clean              # stop and delete all runtime state
```

Multi-node:

```bash
NODES=3 ./neonfs-rig up         # 3 core nodes, replicas=3
```

## Requirements

The rig needs QEMU and the cloud-init seed tooling on the host:

```bash
sudo apt-get install qemu-system-x86 qemu-utils cloud-image-utils
```

It also needs `nfpm` to build the `.deb` packages (only the first time, or
after a clean):

```bash
# pinned release, checksum-verified — see packaging/build-debs.sh
curl -fsSL -o /tmp/nfpm.tgz \
  https://github.com/goreleaser/nfpm/releases/download/v2.46.3/nfpm_2.46.3_Linux_x86_64.tar.gz
sudo tar -C /usr/local/bin -xzf /tmp/nfpm.tgz nfpm
```

### KVM acceleration (strongly recommended)

Without write access to `/dev/kvm` the VMs run under TCG software emulation,
which is **10–30× slower** — a single-node bring-up can take 20+ minutes
instead of ~2. Grant your user access via the `kvm` group (the device is
`root:kvm`, mode `0660`):

```bash
sudo usermod -aG kvm "$USER"     # then log out / back in, or: newgrp kvm
```

If the `kvm` group doesn't exist yet (some containers), create it matching the
device's GID first:

```bash
sudo groupadd -g "$(stat -c %g /dev/kvm)" kvm
sudo usermod -aG kvm "$USER"
```

The rig auto-detects a writable `/dev/kvm` and uses `accel=kvm` when available,
falling back to TCG otherwise.

## Commands

| Command | Description |
| --- | --- |
| `up` | Boot `NODES` VMs, install `neonfs_omnibus`, form the cluster, create a volume |
| `boot` | Boot + provision VMs only (no cluster init) |
| `init` | Initialise/join the cluster on already-provisioned VMs |
| `status` | Show VM state plus `cluster status` / `volume list` from node 1 |
| `ssh <n> [cmd...]` | SSH into node `<n>` (default 1) |
| `cli <n> -- <args>` | Run the `neonfs` CLI on node `<n>` |
| `down` | Stop all VMs, keep their disks |
| `clean` | Stop all VMs and delete runtime state (disks, seeds, ssh key) |

## Configuration

All knobs are environment variables (defaults in parentheses):

| Variable | Default | Meaning |
| --- | --- | --- |
| `NODES` | `1` | Number of core VMs |
| `DRIVES_PER_NODE` | `2` | Extra disk images per node, each formatted + registered as a drive |
| `DRIVE_SIZE` | `2G` | Size of each extra disk image |
| `ROOT_SIZE` | `12G` | Virtual size of each node's root disk (grown from the base image) |
| `VM_MEM` | `2048` | RAM (MiB) per VM |
| `VM_CPUS` | `2` | vCPUs per VM |
| `REPLICAS` | `=NODES` | Replication factor for the system volume and the created volume |
| `CLUSTER_NAME` | `rig` | Cluster name |
| `CLUSTER_COOKIE` | `neonfs-rig-cookie` | Shared Erlang distribution cookie |
| `VOLUME_NAME` | `test` | Name of the volume created at the end of `up` |
| `DIST_PORT` | `9100` | Pinned Erlang distribution port (`NEONFS_DIST_PORT`) |
| `SSH_BASE_PORT` | `2230` | Host port for node `n` SSH is `SSH_BASE_PORT + n` |

`REPLICAS` defaults to the node count so `volume create` is satisfiable without
`--allow-under-replicated` (NeonFS refuses a volume whose replication factor
exceeds the number of core nodes).

## How it works

1. **Base image** — Debian 13 (`genericcloud` amd64) qcow2, downloaded once into
   `.cache/images/`. Each node gets a copy-on-write overlay backed by it, grown
   to `ROOT_SIZE` (cloud-init `growpart` expands the root filesystem on boot).
2. **Packages** — `packaging/build-debs.sh` builds `neonfs-common`, `neonfs-cli`
   and `neonfs-omnibus` (plus the other service packages) into `.cache/debs/`.
3. **Disks** — `DRIVES_PER_NODE` raw images per node, attached as virtio block
   devices (`/dev/vdb`, `/dev/vdc`, …). cloud-init formats them `ext4` and mounts
   them at `/mnt/neonfs/drive1`, `/mnt/neonfs/drive2`, … cloud-init also installs
   `docker.io` and `containerd`, which back the container-runtime acceptance
   steps (the NeonFS Docker volume driver and the containerd content store).
4. **Networking** — two NICs per VM:
   - a user-mode (NAT) NIC for outbound internet + an SSH port forward to the host;
   - a socket/multicast NIC giving all VMs a shared L2 segment with static IPs
     (`10.10.10.1<n>`), used for inter-node Erlang distribution. No host bridge
     or root privileges required.
5. **Provisioning** (over SSH) — install the three `.deb`s, write
   `/etc/neonfs/neonfs.conf` (node name `neonfs@10.10.10.1<n>`, shared cookie,
   pinned `NEONFS_DIST_PORT`), seed the shared `/var/lib/neonfs/.erlang.cookie`,
   `chown` the drive mountpoints to `neonfs`, and start `neonfs-omnibus`.
6. **Cluster** — node 1 runs `cluster init --drive /mnt/neonfs/drive1`; extra
   nodes `cluster join --via 10.10.10.11:9568` using an invite token; every node
   registers its remaining drives with `drive add`; finally a volume is created.

Node names are bare IPs (`neonfs@10.10.10.12`), so no DNS or `/etc/hosts` entries
are needed — `NeonFS.Epmd` feeds the IP straight to the resolver and learns each
peer's distribution port through the `--via host:9568` join handshake.

## Acceptance suite

`./acceptance` drives a **running** cluster through the full acceptance matrix —
cluster/drives/volume checks, FUSE, NFS, S3 and WebDAV operations,
cross-interface consistency, container-runtime integration, and (multi-node)
replication — reporting `PASS`/`FAIL`/`SKIP` per step and exiting non-zero if any
step fails.

The container-runtime steps exercise NeonFS as backing storage for Docker and
containerd:

- **Docker volume attach** — `docker volume create -d neonfs` provisions a
  NeonFS volume through the driver the omnibus package registers at
  `/etc/docker/plugins/neonfs.spec`, then `docker run -v` attaches it; writing
  in one container and reading it back in a second proves the volume is
  attached.
- **containerd content store** — a throwaway `containerd` configured with the
  NeonFS content proxy plugin (default store disabled) stores and retrieves an
  image-layer blob via `ctr content ingest`/`get`, landing it in the
  `containerd` NeonFS volume as a sharded `sha256` object.

Both steps `SKIP` rather than `FAIL` where their prerequisites are absent
(Docker/containerd not installed, the plugin socket not deployed, or no registry
connectivity to pull the test image).

```bash
./neonfs-rig up            # bring a single-node cluster up first
./acceptance single

NODES=3 ./neonfs-rig up     # or a multi-node cluster
./acceptance multi          # node count defaults to the running nodes
```

Each interface step writes uniquely-tagged files and consistency checks poll for
up to `CONSISTENCY_TIMEOUT` seconds (default 25) before failing, so async
propagation isn't mistaken for inconsistency. CLI calls are wrapped in
`CLI_TIMEOUT` (default 45 s). `KEEP=1` leaves the test mounts in place.

The suite is intended to fail loudly on real defects. Against the current build
it reports the cross-interface consistency failures (issue #1034) as `FAIL`, and
in `multi` mode the replication step exercises multi-node formation (issues
#1032 / #1033). Steps whose preconditions aren't met (e.g. replication on a
single node, or a volume that failed to create) are reported `SKIP`.

## Layout

```
test-rig/
├── neonfs-rig            # cluster lifecycle dispatcher (up/down/ssh/cli/...)
├── acceptance            # acceptance test suite (single | multi)
├── lib/rig.sh            # configuration + helpers
├── lib/acceptance.sh     # acceptance step definitions + harness
├── README.md
└── .cache/               # (gitignored) base images, built .debs, per-cluster runtime
    ├── images/
    ├── debs/
    └── run/node-<n>/      # root.qcow2, drive-*.img, seed.iso, serial.log, qemu.pid
```

## Troubleshooting

- **Boot/console**: each node's serial console is logged to
  `.cache/run/node-<n>/serial.log`.
- **`Could not access KVM kernel module: Permission denied`**: see *KVM
  acceleration* above, or accept the TCG fallback.
- **SSH never comes up**: check the serial log; cloud-init failures show there.
  `./neonfs-rig ssh <n> -- sudo cloud-init status --long` once reachable.
- **Daemon not answering the CLI**: `./neonfs-rig ssh <n> -- sudo journalctl -u neonfs-omnibus -n 100`.

### Multi-node note

The single-node path is the default and the most exercised. The multi-node join
path relies on node 1's cluster API (port `9568`) being reachable from peers on
the shared segment; if a join stalls, confirm that port is listening on
`10.10.10.11` inside node 1 (`ss -ltnp` over SSH) and check the joining node's
journal.
