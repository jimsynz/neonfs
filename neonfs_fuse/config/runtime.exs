import Config

# Runtime configuration for neonfs_fuse
# Evaluated at runtime when the release starts

if config_env() == :prod do
  # Node name for Erlang distribution
  node_name = System.get_env("RELEASE_NODE", "neonfs_fuse@localhost")

  # Core node to connect to for metadata operations
  core_node = System.get_env("NEONFS_CORE_NODE", "neonfs_core@localhost")

  # Unmount command - fusermount3 for fuse3 systems, fusermount for fuse2
  fusermount_cmd = System.get_env("NEONFS_FUSERMOUNT_CMD", "fusermount3")

  config :neonfs_fuse,
    node_name: node_name,
    core_node: String.to_atom(core_node),
    fusermount_cmd: fusermount_cmd

  # Default mount options
  config :neonfs_fuse, NeonFS.FUSE.MountManager, default_mount_options: []

  # BEAM VM will use RELEASE_NODE environment variable for actual node name
  # This is set via rel/env.sh.eex or systemd environment
end
