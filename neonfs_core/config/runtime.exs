import Config

# Runtime configuration for neonfs_core
# Evaluated at runtime when the release starts

if config_env() == :prod do
  # Structured JSON logging for production — human-readable console in dev/test
  config :logger, :default_handler, formatter: LoggerJSON.Formatters.Basic.new(metadata: :all)

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

  # ChunkCache memory limit (bytes) — default 256 MiB
  # config :neonfs_core, chunk_cache_max_memory: 536_870_912  # 512 MiB

  # Client infrastructure — core uses ServiceRegistry directly for peer discovery
  config :neonfs_client,
    service_list_fn: {NeonFS.Core.ServiceRegistry, :list, []}

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

  # Auto-bootstrap: autonomous cluster formation (Consul/Nomad bootstrap_expect pattern)
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

    # Parse peer entries which may include dist_port: "neonfs@node1:9100,neonfs@node2:9200"
    parsed_peers =
      bootstrap_peers
      |> String.split(",", trim: true)
      |> Enum.map(fn entry ->
        entry = String.trim(entry)

        case Regex.run(~r/^(.+@.+):(\d+)$/, entry) do
          [_, node_name, port_str] ->
            {String.to_atom(node_name), String.to_integer(port_str)}

          _ ->
            {String.to_atom(entry), 0}
        end
      end)

    peer_atoms = Enum.map(parsed_peers, fn {node, _port} -> node end)

    peer_ports =
      parsed_peers
      |> Enum.filter(fn {_node, port} -> port > 0 end)
      |> Map.new()

    config :neonfs_core,
      auto_bootstrap: true,
      cluster_name: cluster_name,
      bootstrap_expect: String.to_integer(bootstrap_expect),
      bootstrap_peers: peer_atoms,
      bootstrap_peer_ports: peer_ports,
      bootstrap_timeout: String.to_integer(System.get_env("NEONFS_BOOTSTRAP_TIMEOUT", "300000"))
  end

  # BEAM VM will use RELEASE_NODE environment variable for actual node name
  # This is set via rel/env.sh.eex or systemd environment
end
