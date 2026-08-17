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
