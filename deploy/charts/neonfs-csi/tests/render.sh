#!/usr/bin/env bash
# Render the neonfs-csi chart with the snapshotted defaults and
# diff against `tests/default-values.yaml`. Pass `update` to refresh
# the snapshot after intentional changes.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
chart_dir="$(dirname "$here")"
fixture="$here/default-values.yaml"

mode="${1:-check}"

# `coreNode` is required, and `bootstrap.uses` is required alongside the token
# this snapshot sets. The chart refuses to render without them rather than
# producing pods that come up and never reach the cluster, so the snapshot has
# to supply both.
render() {
  helm template release "$chart_dir" \
    --namespace neonfs-csi \
    --set coreNode=neonfs_core@neonfs-core.example \
    --set bootstrap.uses=3 \
    --set bootstrap.value=test-bootstrap-token-redacted
}

case "$mode" in
  check)
    diff -u "$fixture" <(render) || {
      echo
      echo "snapshot mismatch — re-run '$0 update' if the change was intentional" >&2
      exit 1
    }
    ;;
  update)
    render > "$fixture"
    echo "updated $fixture"
    ;;
  *)
    echo "usage: $0 [check|update]" >&2
    exit 2
    ;;
esac
