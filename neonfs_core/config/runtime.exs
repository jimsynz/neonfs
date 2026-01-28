import Config

# Runtime configuration for neonfs_core
# Evaluated at runtime when the release starts

if config_env() == :prod do
  # Data directory for blob storage
  data_dir = System.get_env("NEONFS_DATA_DIR", "/var/lib/neonfs/data")

  # Node name for Erlang distribution
  node_name = System.get_env("RELEASE_NODE", "neonfs_core@localhost")

  # Blob store configuration
  config :neonfs_core, NeonFS.Core.BlobStore,
    data_dir: data_dir,
    prefix_depth: String.to_integer(System.get_env("NEONFS_PREFIX_DEPTH", "2"))

  # Configure node name
  config :neonfs_core,
    node_name: node_name

  # BEAM VM will use RELEASE_NODE environment variable for actual node name
  # This is set via rel/env.sh.eex or systemd environment
end
