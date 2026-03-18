import Config

if config_env() == :prod do
  config :logger, :default_handler, formatter: LoggerJSON.Formatters.Basic.new(metadata: :all)

  data_dir = System.get_env("NEONFS_DATA_DIR", "/var/lib/neonfs/data")
  node_name = System.get_env("RELEASE_NODE", "neonfs@localhost")

  # In omnibus mode, core is local — FUSE/NFS client points at self
  core_node =
    System.get_env("NEONFS_CORE_NODE", node_name)
    |> String.to_atom()

  enable_ra = System.get_env("NEONFS_ENABLE_RA", "true") == "true"

  config :ra,
    data_dir: String.to_charlist("#{data_dir}/ra")

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
    fuse_node: core_node

  # Auto-bootstrap
  auto_bootstrap = System.get_env("NEONFS_AUTO_BOOTSTRAP", "false") == "true"

  if auto_bootstrap do
    cluster_name =
      System.get_env("NEONFS_CLUSTER_NAME") ||
        raise "NEONFS_CLUSTER_NAME required when NEONFS_AUTO_BOOTSTRAP=true"

    bootstrap_expect =
      System.get_env("NEONFS_BOOTSTRAP_EXPECT") ||
        raise "NEONFS_BOOTSTRAP_EXPECT required when NEONFS_AUTO_BOOTSTRAP=true"

    bootstrap_peers =
      System.get_env("NEONFS_BOOTSTRAP_PEERS") ||
        raise "NEONFS_BOOTSTRAP_PEERS required when NEONFS_AUTO_BOOTSTRAP=true"

    config :neonfs_core,
      auto_bootstrap: true,
      cluster_name: cluster_name,
      bootstrap_expect: String.to_integer(bootstrap_expect),
      bootstrap_peers:
        bootstrap_peers
        |> String.split(",", trim: true)
        |> Enum.map(&String.to_atom(String.trim(&1))),
      bootstrap_timeout: String.to_integer(System.get_env("NEONFS_BOOTSTRAP_TIMEOUT", "300000"))
  end

  # FUSE configuration
  fusermount_cmd = System.get_env("NEONFS_FUSERMOUNT_CMD", "fusermount3")
  fuse_metrics_enabled = System.get_env("NEONFS_FUSE_METRICS", "false") == "true"
  fuse_metrics_port = String.to_integer(System.get_env("NEONFS_FUSE_METRICS_PORT", "9569"))
  fuse_metrics_bind = System.get_env("NEONFS_FUSE_METRICS_BIND", "0.0.0.0")

  config :neonfs_fuse,
    core_node: core_node,
    fusermount_cmd: fusermount_cmd,
    metrics_bind: fuse_metrics_bind,
    metrics_enabled: fuse_metrics_enabled,
    metrics_port: fuse_metrics_port,
    node_name: node_name

  config :neonfs_fuse, NeonFS.FUSE.MountManager, default_mount_options: []

  # NFS configuration
  nfs_bind = System.get_env("NEONFS_NFS_BIND", "0.0.0.0")
  nfs_port = String.to_integer(System.get_env("NEONFS_NFS_PORT", "2049"))
  nfs_metrics_enabled = System.get_env("NEONFS_NFS_METRICS", "false") == "true"
  nfs_metrics_port = String.to_integer(System.get_env("NEONFS_NFS_METRICS_PORT", "9570"))
  nfs_metrics_bind = System.get_env("NEONFS_NFS_METRICS_BIND", "0.0.0.0")

  config :neonfs_nfs,
    core_node: core_node,
    metrics_bind: nfs_metrics_bind,
    metrics_enabled: nfs_metrics_enabled,
    metrics_port: nfs_metrics_port,
    nfs_bind: nfs_bind,
    nfs_port: nfs_port,
    node_name: node_name
end
