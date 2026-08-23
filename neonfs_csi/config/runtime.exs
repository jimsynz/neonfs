import Config

# `:mode` decides which socket the plugin opens — the controller's
# `/var/lib/csi/sockets/pluginproxy/csi.sock` or the node's
# `/var/lib/kubelet/plugins/<driver>/csi.sock` — and whether the node's
# registration metadata is advertised. A release had no way to set it, so
# every pod defaulted to `:controller`: the node DaemonSet opened the
# controller's path and `csi-node-driver-registrar` sat dialling a socket
# nothing had created, until it timed out and crashlooped.
case System.get_env("NEONFS_CSI_MODE") do
  nil ->
    :ok

  "controller" ->
    config :neonfs_csi, mode: :controller

  "node" ->
    config :neonfs_csi, mode: :node

  other ->
    raise "NEONFS_CSI_MODE must be \"controller\" or \"node\", got #{inspect(other)}"
end

# `NeonFS.Client.Connection` dials `:neonfs_client, :bootstrap_nodes`, which
# nothing populated here — so a release had no idea which core node to reach
# and `Discovery` reported "No core node connection" forever, whatever
# `NEONFS_CORE_NODE` said. `neonfs_nfs`'s runtime config has always mapped it;
# this brings the CSI driver into line.
case System.get_env("NEONFS_CORE_NODE") do
  nil ->
    :ok

  core_node ->
    config :neonfs_client, bootstrap_nodes: [String.to_atom(core_node)]
    config :neonfs_csi, core_node: String.to_atom(core_node)
end

# `CSI_ENDPOINT` is the CO's own way of telling a plugin where to listen, and
# the chart has always set it — `unix:///csi/csi.sock` for the node plugin,
# whose `/csi` is the kubelet's plugin directory mounted in. Nothing read it,
# so the plugin opened its compiled-in default instead: a path inside the
# container rather than on the hostPath, where `csi-node-driver-registrar`
# could never see it. The socket existed and the driver was unreachable.
case System.get_env("CSI_ENDPOINT") do
  nil ->
    :ok

  "unix://" <> path ->
    config :neonfs_csi, socket_path: Path.absname(path)

  path ->
    if String.contains?(path, "://") do
      raise "CSI_ENDPOINT must be a unix:// endpoint, got #{inspect(path)}"
    else
      config :neonfs_csi, socket_path: Path.absname(path)
    end
end

# Which frontend a block volume is staged over. `auto` uses ublk when a block
# target on this very host advertises it, and NBD otherwise — which in the
# shipped chart is always, because the node DaemonSet runs no block target.
# Forcing `ublk` fails the stage naming what was missing rather than staging
# over NBD and reporting ublk, since that is how a comparison of the two ends
# up measuring one of them twice.
case System.get_env("NEONFS_CSI_BLOCK_FRONTEND") do
  nil ->
    :ok

  value when value in ["auto", "ublk", "nbd"] ->
    config :neonfs_csi, block_frontend: String.to_atom(value)

  other ->
    raise "NEONFS_CSI_BLOCK_FRONTEND must be \"auto\", \"ublk\" or \"nbd\", got #{inspect(other)}"
end
