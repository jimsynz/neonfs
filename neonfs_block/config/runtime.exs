import Config

if config_env() == :prod do
  config :logger, :default_handler, formatter: LoggerJSON.Formatters.Basic.new(metadata: :all)

  node_name = System.get_env("RELEASE_NODE", "neonfs_block@localhost")
  core_node = System.get_env("NEONFS_CORE_NODE", "neonfs_core@localhost")

  block_bind = System.get_env("NEONFS_BLOCK_BIND", "127.0.0.1")
  block_port = String.to_integer(System.get_env("NEONFS_BLOCK_PORT", "10809"))

  config :neonfs_client,
    bootstrap_nodes: [String.to_atom(core_node)]

  config :neonfs_client, :chunk_cache,
    max_bytes: String.to_integer(System.get_env("NEONFS_CHUNK_CACHE_MAX_BYTES", "134217728"))

  config :neonfs_block,
    bind: block_bind,
    core_node: String.to_atom(core_node),
    node_name: node_name,
    port: block_port
end
