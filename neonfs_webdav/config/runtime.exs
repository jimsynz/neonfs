import Config

if config_env() == :prod do
  config :logger, :default_handler, formatter: LoggerJSON.Formatters.Basic.new(metadata: :all)

  node_name = System.get_env("RELEASE_NODE", "neonfs_webdav@localhost")
  core_node = System.get_env("NEONFS_CORE_NODE", "neonfs_core@localhost")

  webdav_bind = System.get_env("NEONFS_WEBDAV_BIND", "0.0.0.0")
  webdav_port = String.to_integer(System.get_env("NEONFS_WEBDAV_PORT", "8081"))

  metrics_enabled = System.get_env("NEONFS_WEBDAV_METRICS", "false") == "true"
  metrics_port = String.to_integer(System.get_env("NEONFS_WEBDAV_METRICS_PORT", "9572"))
  metrics_bind = System.get_env("NEONFS_WEBDAV_METRICS_BIND", "0.0.0.0")

  config :neonfs_client,
    bootstrap_nodes: [String.to_atom(core_node)]

  config :neonfs_webdav,
    core_node: String.to_atom(core_node),
    metrics_bind: metrics_bind,
    metrics_enabled: metrics_enabled,
    metrics_port: metrics_port,
    node_name: node_name,
    webdav_bind: webdav_bind,
    webdav_port: webdav_port
end
