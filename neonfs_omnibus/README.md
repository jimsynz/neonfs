# NeonFS Omnibus

All-in-one NeonFS deployment. A single Erlang release bundles
`neonfs_core` with every interface package (`neonfs_fuse`,
`neonfs_nfs`, `neonfs_s3`, `neonfs_webdav`, and `neonfs_docker`) so a
small installation can run the full stack from one process.

Intended use:

- **Single-node / small cluster deployments** — one binary exposes
  FUSE, NFSv3, S3, WebDAV, and the Docker VolumeDriver plugin
  alongside the storage engine.
- **Development and CI** — avoids starting a multi-node cluster
  locally when you only need to exercise the interfaces.
- **Appliance images** — Debian `neonfs-omnibus` package and
  container `ghcr.io/jimsynz/neonfs/omnibus` bundle everything as
  one systemd unit / `docker run` target.

Each interface is still a separate OTP application under the hood —
they start in sequence from `NeonFS.Omnibus.Application` after core
finishes initialising. Omnibus conflicts with the split-service
Debian packages (`neonfs-core`, `neonfs-fuse`, etc.); pick one.

See [`docs/deployment.md`](../docs/deployment.md) for configuration
and topology examples, and the top-level [`README.md`](../README.md)
for the architecture overview.
