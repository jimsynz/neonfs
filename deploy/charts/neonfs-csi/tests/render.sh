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

# `sidecars.resizer` is off because the driver implements no expansion, and
# `csi-resizer` exits fatally when it finds none — which left the controller
# pod crash-looping and never Ready. So the *absence* is load-bearing here in
# the same way `hostDevices` is, and the switch has to keep working for
# whenever `EXPAND_VOLUME` does land.
check_resizer() {
  local off on
  off="$(render)"
  on="$(render --set sidecars.resizer.enabled=true)"

  if grep -q 'name: csi-resizer' <<<"$off"; then
    echo "sidecars.resizer is off by default but rendered anyway" >&2
    return 1
  fi

  if ! grep -q 'name: csi-resizer' <<<"$on"; then
    echo "sidecars.resizer.enabled=true rendered no csi-resizer container" >&2
    return 1
  fi

  # The StorageClass must not advertise what the driver cannot do, whatever
  # the sidecar is set to.
  if ! grep -q 'allowVolumeExpansion: false' <<<"$off"; then
    echo "the StorageClass claims volume expansion the driver does not implement" >&2
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
    check_resizer
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
