import Config

if config_env() == :prod do
  config :logger, :default_handler, formatter: LoggerJSON.Formatters.Basic.new(metadata: :all)

  node_name = System.get_env("RELEASE_NODE", "neonfs_s3@localhost")
  core_node = System.get_env("NEONFS_CORE_NODE", "neonfs_core@localhost")

  s3_bind = System.get_env("NEONFS_S3_BIND", "0.0.0.0")
  s3_port = String.to_integer(System.get_env("NEONFS_S3_PORT", "8080"))
  s3_region = System.get_env("NEONFS_S3_REGION", "neonfs")

  metrics_enabled = System.get_env("NEONFS_S3_METRICS", "false") == "true"
  metrics_port = String.to_integer(System.get_env("NEONFS_S3_METRICS_PORT", "9571"))
  metrics_bind = System.get_env("NEONFS_S3_METRICS_BIND", "0.0.0.0")

  config :neonfs_client,
    bootstrap_nodes: [String.to_atom(core_node)]

  config :neonfs_s3,
    core_node: String.to_atom(core_node),
    metrics_bind: metrics_bind,
    metrics_enabled: metrics_enabled,
    metrics_port: metrics_port,
    node_name: node_name,
    region: s3_region,
    s3_bind: s3_bind,
    s3_port: s3_port
end
