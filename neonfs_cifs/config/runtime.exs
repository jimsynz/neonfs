import Config

# Runtime configuration for neonfs_cifs
# Evaluated at runtime when the release starts

if config_env() == :prod do
  # Structured JSON logging for production — human-readable console in dev/test
  config :logger, :default_handler, formatter: LoggerJSON.Formatters.Basic.new(metadata: :all)

  # Node name for Erlang distribution
  node_name = System.get_env("RELEASE_NODE", "neonfs_cifs@localhost")

  # Core node to connect to for metadata operations
  core_node = System.get_env("NEONFS_CORE_NODE", "neonfs_core@localhost")

  # Unix socket the Samba `vfs_neonfs.so` shim connects to (ETF wire protocol)
  socket_path = System.get_env("NEONFS_CIFS_SOCKET", "/run/neonfs/cifs.sock")

  # Client infrastructure — bootstrap against core node
  config :neonfs_client,
    bootstrap_nodes: [String.to_atom(core_node)]

  config :neonfs_client, :chunk_cache,
    max_bytes: String.to_integer(System.get_env("NEONFS_CHUNK_CACHE_MAX_BYTES", "134217728"))

  config :neonfs_cifs,
    core_node: String.to_atom(core_node),
    node_name: node_name,
    socket_path: socket_path

  # BEAM VM will use RELEASE_NODE environment variable for actual node name
  # This is set via rel/env.sh.eex or systemd environment
end
