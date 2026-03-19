import Config

# Runtime configuration for neonfs_nfs
# Evaluated at runtime when the release starts

if config_env() == :prod do
  # Structured JSON logging for production — human-readable console in dev/test
  config :logger, :default_handler, formatter: LoggerJSON.Formatters.Basic.new(metadata: :all)

  # Node name for Erlang distribution
  node_name = System.get_env("RELEASE_NODE", "neonfs_nfs@localhost")

  # Core node to connect to for metadata operations
  core_node = System.get_env("NEONFS_CORE_NODE", "neonfs_core@localhost")

  # NFS server bind address and port
  nfs_bind = System.get_env("NEONFS_NFS_BIND", "0.0.0.0")
  nfs_port = String.to_integer(System.get_env("NEONFS_NFS_PORT", "2049"))

  # Metrics endpoint (disabled by default, set NEONFS_NFS_METRICS=true to enable)
  metrics_enabled = System.get_env("NEONFS_NFS_METRICS", "false") == "true"
  metrics_port = String.to_integer(System.get_env("NEONFS_NFS_METRICS_PORT", "9570"))
  metrics_bind = System.get_env("NEONFS_NFS_METRICS_BIND", "0.0.0.0")

  # Client infrastructure — bootstrap against core node
  config :neonfs_client,
    bootstrap_nodes: [String.to_atom(core_node)]

  config :neonfs_nfs,
    core_node: String.to_atom(core_node),
    metrics_bind: metrics_bind,
    metrics_enabled: metrics_enabled,
    metrics_port: metrics_port,
    nfs_bind: nfs_bind,
    nfs_port: nfs_port,
    node_name: node_name

  # BEAM VM will use RELEASE_NODE environment variable for actual node name
  # This is set via rel/env.sh.eex or systemd environment
end
