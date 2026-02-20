import Config

# Runtime configuration for neonfs_core
# Evaluated at runtime when the release starts

if config_env() == :prod do
  # Data directory for blob storage
  data_dir = System.get_env("NEONFS_DATA_DIR", "/var/lib/neonfs/data")

  # Node name for Erlang distribution
  node_name = System.get_env("RELEASE_NODE", "neonfs_core@localhost")

  # FUSE node name for RPC calls (default assumes single-host deployment)
  fuse_node =
    System.get_env("NEONFS_FUSE_NODE", "neonfs_fuse@localhost")
    |> String.to_atom()

  # Enable Ra for distributed cluster coordination (Phase 2+)
  enable_ra = System.get_env("NEONFS_ENABLE_RA", "true") == "true"

  # Configure Ra data directory (must be set before Ra starts)
  # IMPORTANT: Ra (Erlang) expects a charlist, not a binary string
  config :ra,
    data_dir: String.to_charlist("#{data_dir}/ra")

  # Default drive configuration — used when cluster.json has no drives
  # (fresh install, before `neonfs-cli drive add`).
  # At startup, Application.start/2 loads drives from cluster.json and
  # overrides this default via Application.put_env/3.
  drives = [%{id: "default", path: "#{data_dir}/blobs", tier: :hot, capacity: 0}]

  # Core configuration
  config :neonfs_core,
    blob_store_base_dir: "#{data_dir}/blobs",
    blob_store_prefix_depth: String.to_integer(System.get_env("NEONFS_PREFIX_DEPTH", "2")),
    drives: drives,
    enable_ra: enable_ra,
    meta_dir: "#{data_dir}/meta",
    ra_data_dir: "#{data_dir}/ra",
    snapshot_interval_ms:
      String.to_integer(System.get_env("NEONFS_SNAPSHOT_INTERVAL_MS", "30000")),
    node_name: node_name,
    fuse_node: fuse_node

  # BEAM VM will use RELEASE_NODE environment variable for actual node name
  # This is set via rel/env.sh.eex or systemd environment
end
