# NeonFS Omnibus

All of NeonFS in one process. A single Erlang release bundles
`neonfs_core` with every interface package — `neonfs_fuse`,
`neonfs_nfs`, `neonfs_s3`, `neonfs_webdav`, `neonfs_docker`, and
`neonfs_containerd` — so a small installation runs the full stack
without operating a multi-node constellation.

This is the fastest way to a working NeonFS: one package, one systemd
unit, every access method.

Intended use:

- **Single-node and small-cluster deployments** — one binary exposes
  FUSE, NFSv3, S3, WebDAV, the Docker VolumeDriver plugin, and the
  containerd content store alongside the storage engine.
- **Development and CI** — avoids starting a multi-node cluster
  locally when you only need to exercise the interfaces.
- **Appliance images** — the `neonfs-omnibus` Debian package and the
  `ghcr.io/jimsynz/neonfs/omnibus` container bundle everything as one
  systemd unit / `docker run` target.

Each interface is still a separate OTP application under the hood —
they start in sequence from `NeonFS.Omnibus.Application` after core
finishes initialising, and omnibus nodes cluster with other core or
interface nodes like any other member. The omnibus Debian package
conflicts with the split-service packages (`neonfs-core`,
`neonfs-fuse`, …); pick one or the other.

## Install

```bash
sudo apt install neonfs-omnibus
sudo systemctl enable --now neonfs-omnibus
```

See [`packaging/README.md`](../packaging/README.md) for the repository
setup, [`docs/deployment.md`](../docs/deployment.md) for configuration
and topology examples, and the top-level [`README.md`](../README.md)
for the architecture overview.

## Licence

Apache-2.0 — see [LICENSE](../LICENSE) for details.
