import Config

# Runtime configuration for neonfs_fuse
# Evaluated at runtime when the release starts

if config_env() == :prod do
  # Structured JSON logging for production — human-readable console in dev/test
  config :logger, :default_handler, formatter: LoggerJSON.Formatters.Basic.new(metadata: :all)

  # Node name for Erlang distribution
  node_name = System.get_env("RELEASE_NODE", "neonfs_fuse@localhost")

  # Core node to connect to for metadata operations
  core_node = System.get_env("NEONFS_CORE_NODE", "neonfs_core@localhost")

  # Unmount command - fusermount3 for fuse3 systems, fusermount for fuse2
  fusermount_cmd = System.get_env("NEONFS_FUSERMOUNT_CMD", "fusermount3")

  # Metrics endpoint (disabled by default, set NEONFS_FUSE_METRICS=true to enable)
  metrics_enabled = System.get_env("NEONFS_FUSE_METRICS", "false") == "true"
  metrics_port = String.to_integer(System.get_env("NEONFS_FUSE_METRICS_PORT", "9569"))
  metrics_bind = System.get_env("NEONFS_FUSE_METRICS_BIND", "0.0.0.0")

  config :neonfs_fuse,
    core_node: String.to_atom(core_node),
    fusermount_cmd: fusermount_cmd,
    metrics_bind: metrics_bind,
    metrics_enabled: metrics_enabled,
    metrics_port: metrics_port,
    node_name: node_name

  # Default mount options
  config :neonfs_fuse, NeonFS.FUSE.MountManager, default_mount_options: []

  # BEAM VM will use RELEASE_NODE environment variable for actual node name
  # This is set via rel/env.sh.eex or systemd environment
end
