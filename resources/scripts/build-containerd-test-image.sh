#!/usr/bin/env bash
# Re-builds the OCI test image used by `neonfs_integration`'s
# containerd integration tests (#728 onwards).
#
# The output tarball lives at
# `neonfs_integration/test/fixtures/test-image.tar` and is loaded
# into the integration job's `registry:2` sidecar at startup. Two
# layers (alpine base + a tiny marker layer) so the test exercises
# the per-layer `Info` → `Read` flow rather than just a single
# blob.
#
# Pre-requisites: `buildah` (or any tool that can produce an
# `oci-archive`). Run from the repo root:
#
#     ./resources/scripts/build-containerd-test-image.sh
#
# The script needs root for `buildah`; CI never runs it — the
# committed tarball is the source of truth for fixture stability.

set -euo pipefail

OUT="$(git rev-parse --show-toplevel)/neonfs_integration/test/fixtures/test-image.tar"
mkdir -p "$(dirname "$OUT")"

ctr=$(sudo buildah from docker.io/library/alpine:3.19)
sudo buildah run "$ctr" sh -c 'echo "neonfs-test-image v1" > /etc/neonfs-marker'
sudo buildah commit "$ctr" neonfs-test-image:v1
sudo buildah push neonfs-test-image:v1 "oci-archive:${OUT}"
sudo buildah rm "$ctr"
sudo buildah rmi neonfs-test-image:v1

sudo chown "$(id -u):$(id -g)" "$OUT"

echo "Wrote $OUT ($(du -h "$OUT" | cut -f1))"
