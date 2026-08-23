import Config

if config_env() == :prod do
  config :logger, :default_handler, formatter: LoggerJSON.Formatters.Basic.new(metadata: :all)

  node_name = System.get_env("RELEASE_NODE", "neonfs_block@localhost")
  core_node = System.get_env("NEONFS_CORE_NODE", "neonfs_core@localhost")

  block_bind = System.get_env("NEONFS_BLOCK_BIND", "127.0.0.1")
  block_port = String.to_integer(System.get_env("NEONFS_BLOCK_PORT", "10809"))

  # `auto` prefers ublk where the driver and the helper are both present.
  # Forcing one that is not available fails the attach naming what is
  # missing, rather than serving the other and reporting this value.
  frontend =
    case System.get_env("NEONFS_BLOCK_FRONTEND", "auto") do
      value when value in ["auto", "ublk", "nbd"] -> String.to_atom(value)
      other -> raise "NEONFS_BLOCK_FRONTEND must be auto, ublk or nbd, got #{inspect(other)}"
    end

  metrics_enabled = System.get_env("NEONFS_BLOCK_METRICS", "false") == "true"
  metrics_bind = System.get_env("NEONFS_BLOCK_METRICS_BIND", "0.0.0.0")
  metrics_port = String.to_integer(System.get_env("NEONFS_BLOCK_METRICS_PORT", "9573"))

  config :neonfs_client,
    bootstrap_nodes: [String.to_atom(core_node)]

  config :neonfs_client, :chunk_cache,
    max_bytes: String.to_integer(System.get_env("NEONFS_CHUNK_CACHE_MAX_BYTES", "134217728"))

  config :neonfs_block,
    bind: block_bind,
    core_node: String.to_atom(core_node),
    frontend: frontend,
    metrics_bind: metrics_bind,
    metrics_enabled: metrics_enabled,
    metrics_port: metrics_port,
    node_name: node_name,
    port: block_port
end
