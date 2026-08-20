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
    --set bootstrap.value=test-bootstrap-token-redacted \
    "$@"
}

# `node.hostDevices` is the one value whose *absence* from the default render is
# the point — it grants the node plugin the host's `/dev`, which only
# `volumeMode: Block` needs. A snapshot proves it is off by default; it cannot
# prove the switch still works, and a switch that quietly stops rendering leaves
# block staging failing at `nbd-client` with a chart that looks configured.
check_host_devices() {
  local off on
  off="$(render)"
  on="$(render --set node.hostDevices.enabled=true)"

  if grep -q 'name: host-devices' <<<"$off"; then
    echo "node.hostDevices is off by default but rendered anyway" >&2
    return 1
  fi

  # Both halves, because a volume without its mount is invisible to the
  # container and a mount without its volume fails the pod outright.
  if ! grep -q 'name: host-devices' <<<"$on"; then
    echo "node.hostDevices.enabled=true rendered no host-devices volume" >&2
    return 1
  fi

  if ! grep -qE 'mountPath: /dev$' <<<"$on"; then
    echo "node.hostDevices.enabled=true rendered no /dev mount" >&2
    return 1
  fi
}

case "$mode" in
  check)
    diff -u "$fixture" <(render) || {
      echo
      echo "snapshot mismatch — re-run '$0 update' if the change was intentional" >&2
      exit 1
    }
    check_host_devices
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
