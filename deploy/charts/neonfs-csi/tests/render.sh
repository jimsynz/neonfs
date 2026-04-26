#!/usr/bin/env bash
# Render the neonfs-csi chart with the snapshotted defaults and
# diff against `tests/default-values.yaml`. Pass `update` to refresh
# the snapshot after intentional changes.
set -euo pipefail

here="$(cd "$(dirname "$0")" && pwd)"
chart_dir="$(dirname "$here")"
fixture="$here/default-values.yaml"

mode="${1:-check}"

render() {
  helm template release "$chart_dir" \
    --namespace neonfs-csi \
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
