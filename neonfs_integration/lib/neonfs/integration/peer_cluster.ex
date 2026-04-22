defmodule NeonFS.Integration.PeerCluster do
  @moduledoc """
  Provides helpers for peer nodes in integration testing.

  Each peer node is started via `:peer.start_link/1` and tracked for cleanup.

  ## Example

      # Use the cluster helper
      cluster = PeerCluster.start_cluster!(3)
      # cluster contains node info and shared cookie

      # Execute RPC on nodes
      PeerCluster.rpc(cluster, :node1, Module, :function, [args])
  """

  require Logger

  @type node_info :: %{
          name: atom(),
          peer: pid(),
          node: node(),
          dist_port: non_neg_integer(),
          metrics_port: non_neg_integer() | nil
        }

  @type cluster :: %{
          id: String.t(),
          cookie: atom(),
          nodes: [node_info()],
          data_dir: String.t()
        }

  @doc """
  Starts a peer node and its applications.

  Returns `{:ok, peer, node}` on success.
  """
  @spec start_peer(map(), [atom()], keyword()) :: {:ok, pid(), node()} | {:error, term()}
  def start_peer(peer_opts, applications, app_config \\ []) do
    case span_peer_spawn(peer_opts) do
      {:ok, peer, node} ->
        # Suppress debug/info logs on peer nodes during tests
        # Configure both Elixir's Logger and Erlang's :logger
        :peer.call(peer, Logger, :configure, [[level: :warning]])
        :peer.call(peer, :logger, :set_primary_config, [:level, :warning])

        # Use :peer.call since nodes aren't connected yet (connection: 0)
        span_apply_config(peer, node, app_config)
        span_start_applications(peer, node, applications)

        {:ok, peer, node}

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp span_peer_spawn(peer_opts) do
    :telemetry.span([:neonfs, :peer_cluster, :node, :spawn], %{}, fn ->
      case :peer.start_link(peer_opts) do
        {:ok, peer, node} -> {{:ok, peer, node}, %{node: node}}
        {:error, reason} -> {{:error, reason}, %{error: reason}}
      end
    end)
  end

  defp span_apply_config(peer, node, app_config) do
    :telemetry.span([:neonfs, :peer_cluster, :node, :apply_config], %{node: node}, fn ->
      {apply_app_config(peer, app_config), %{}}
    end)
  end

  defp span_start_applications(peer, node, applications) do
    :telemetry.span([:neonfs, :peer_cluster, :node, :start_applications], %{node: node}, fn ->
      {start_applications(peer, node, applications), %{applications: applications}}
    end)
  end

  defp span_wait_for_ra(peer, node) do
    :telemetry.span([:neonfs, :peer_cluster, :node, :wait_for_ra], %{node: node}, fn ->
      {wait_for_ra_ready(peer), %{}}
    end)
  end

  @doc """
  Start a complete cluster with N nodes. Returns cluster info.

  Registers cleanup via `on_exit` callback for ExUnit.

  ## Options
  - `:cookie` - Erlang cookie (default: randomly generated)
  - `:applications` - Applications to start on each node (default: [:neonfs_core])
  - `:base_dir` - Base directory for cluster data (default: creates temp dir)
  - `:enable_ra` - Enable Ra consensus (default: true)
  - `:drives` - Custom drive configs per node. A function `(node_name, data_dir) -> [drive_config]`
    that returns drive config maps. When not provided, uses a single default drive.
  - `:metrics_port` - Base port for HTTP metrics/API server (Bandit). When set, each node
    gets `metrics_enabled: true` with sequential ports (node1 = base, node2 = base+1, etc.).
    The allocated port is stored in `node_info.metrics_port`.

  ## Ra Notes

  Ra requires data_dir to be a charlist (not a binary) because DETS internally
  requires charlist file paths. This is handled automatically by this module.
  """
  @spec start_cluster!(pos_integer(), keyword()) :: cluster()
  def start_cluster!(node_count, opts \\ []) do
    cluster_id = generate_cluster_id()
    cookie = Keyword.get_lazy(opts, :cookie, &generate_cookie/0)
    applications = Keyword.get(opts, :applications, [:neonfs_core])
    # Enable Ra by default
    enable_ra = Keyword.get(opts, :enable_ra, true)
    drives_fn = Keyword.get(opts, :drives, nil)
    formation_config = Keyword.get(opts, :formation, nil)
    metrics_base_port = Keyword.get(opts, :metrics_port, nil)

    # Ensure controller is distributed
    ensure_distributed!()

    # Set the cookie on the controller node
    :erlang.set_cookie(cookie)

    # Use provided base_dir or create a new one
    base_dir = Keyword.get_lazy(opts, :base_dir, fn -> create_cluster_dir(cluster_id) end)

    # Pre-compute all peer node names and distribution ports
    # (formation needs the full list before any node starts)
    all_peer_info =
      Enum.map(1..node_count, fn i ->
        {:"node#{i}_#{cluster_id}@localhost", allocate_peer_port()}
      end)

    all_peer_names = Enum.map(all_peer_info, fn {name, _port} -> name end)

    # Build NEONFS_PEER_PORTS for all nodes (used by custom EPMD module).
    # Set on the controller so it propagates to peer env via build_peer_opts.
    all_peer_ports_env =
      Enum.map_join(all_peer_info, ",", fn {name, port} -> "#{name}:#{port}" end)

    if all_peer_ports_env != "" do
      System.put_env("NEONFS_PEER_PORTS", all_peer_ports_env)
    end

    # Start nodes sequentially to avoid DETS name conflicts during Ra initialization
    nodes =
      Enum.reduce(1..node_count, [], fn i, acc ->
        alias_name = :"node#{i}"
        # Use cluster_id in the EPMD name to avoid collisions with stale nodes
        # from previous tests whose on_exit cleanup hasn't completed yet
        peer_name = :"node#{i}_#{cluster_id}"
        data_dir = Path.join(base_dir, "node#{i}")
        meta_dir = Path.join(data_dir, "meta")
        ra_dir = Path.join(data_dir, "ra")

        # Create all directories upfront
        File.mkdir_p!(data_dir)
        File.mkdir_p!(meta_dir)
        File.mkdir_p!(ra_dir)

        dist_port = lookup_peer_port(all_peer_info, :"#{peer_name}@localhost")

        {peer_opts, dist_port} = build_peer_opts(peer_name, cookie, data_dir, dist_port)

        # Configure neonfs_core to use the test data directories
        # IMPORTANT: Ra expects data_dir as a charlist, not a binary!
        # DETS in Erlang requires charlist file paths.
        node_metrics_port =
          if metrics_base_port, do: metrics_base_port + i - 1

        core_config = [
          data_dir: data_dir,
          meta_dir: meta_dir,
          blob_store_base_dir: Path.join(data_dir, "blobs"),
          metrics_enabled: metrics_base_port != nil,
          ra_data_dir: to_charlist(ra_dir),
          enable_ra: enable_ra,
          quorum_timeout_ms: 15_000
        ]

        core_config =
          if node_metrics_port do
            core_config ++ [metrics_port: node_metrics_port, metrics_bind: "127.0.0.1"]
          else
            core_config
          end

        core_config =
          if drives_fn do
            Keyword.put(core_config, :drives, drives_fn.(alias_name, data_dir))
          else
            core_config
          end

        core_config =
          if formation_config do
            core_config ++
              [
                auto_bootstrap: true,
                cluster_name: Keyword.get(formation_config, :cluster_name, cluster_id),
                bootstrap_expect: Keyword.get(formation_config, :bootstrap_expect, node_count),
                bootstrap_peers: all_peer_names,
                bootstrap_timeout: Keyword.get(formation_config, :bootstrap_timeout, 120_000)
              ]
          else
            core_config
          end

        client_config = [
          tls_dir: Path.join(data_dir, "tls"),
          partition_recovery_debounce_ms: 200,
          service_list_fn: {NeonFS.Core.ServiceRegistry, :list, []}
        ]

        app_config = [
          logger: [level: :warning],
          neonfs_client: client_config,
          neonfs_core: core_config,
          ra: [data_dir: to_charlist(ra_dir)]
        ]

        start_cluster_node(
          alias_name,
          peer_opts,
          applications,
          app_config,
          enable_ra,
          node_metrics_port,
          dist_port,
          acc
        )
      end)

    %{
      id: cluster_id,
      cookie: cookie,
      nodes: nodes,
      data_dir: base_dir
    }
  end

  @doc """
  Stop all nodes in a cluster and clean up resources.
  """
  @spec stop_cluster(cluster()) :: :ok
  def stop_cluster(cluster) do
    for node_info <- cluster.nodes do
      try do
        # Stop Ra system first to ensure DETS tables are closed
        :peer.call(node_info.peer, :ra_system, :stop, [:default])
      catch
        _, _ -> :ok
      end

      try do
        # Stop all applications gracefully
        :peer.call(node_info.peer, :application, :stop, [:neonfs_core])
        :peer.call(node_info.peer, :application, :stop, [:ra])
      catch
        _, _ -> :ok
      end

      try do
        :peer.stop(node_info.peer)
      catch
        :exit, _ -> :ok
      end
    end

    # Wait for all peer nodes to actually deregister from EPMD,
    # otherwise the next test might fail to start nodes with the same names
    wait_for_peers_gone(cluster.nodes)

    File.rm_rf(cluster.data_dir)
    :ok
  end

  @doc """
  Execute an RPC call on a cluster node.

  Default timeout is 30 seconds to allow for Ra operations with timeouts,
  error handling with cleanup, and any retries.
  """
  @spec rpc(cluster(), atom(), module(), atom(), [term()], timeout()) :: term()
  def rpc(cluster, node_name, module, function, args, timeout \\ 30_000) do
    node_info = get_node!(cluster, node_name)
    :rpc.call(node_info.node, module, function, args, timeout)
  end

  @doc """
  Get node info by name.
  """
  @spec get_node(cluster(), atom()) :: node_info() | nil
  def get_node(cluster, node_name) do
    Enum.find(cluster.nodes, &(&1.name == node_name))
  end

  @doc """
  Get node info by name, raising if not found.
  """
  @spec get_node!(cluster(), atom()) :: node_info()
  def get_node!(cluster, node_name) do
    get_node(cluster, node_name) ||
      raise ArgumentError, "Node #{node_name} not found in cluster"
  end

  @doc """
  Stop a specific peer node (simulates crash).
  """
  @spec stop_node(cluster(), atom()) :: :ok
  def stop_node(cluster, node_name) do
    node_info = get_node!(cluster, node_name)
    :peer.stop(node_info.peer)
    :ok
  end

  @doc """
  Restart a stopped (or running) peer node with the same configuration.

  Stops the old peer if still running, waits for it to leave EPMD, then
  starts a fresh peer with the same EPMD name, cookie, and data directory.
  The new peer's persisted Ra state and DETS tables are preserved on disk.

  Returns `{:ok, updated_cluster}` with the new peer PID in the nodes list.
  The caller must connect the node to the mesh and rebuild quorum rings
  (e.g. via `wait_for_full_mesh/1` and `rebuild_quorum_rings/1`).

  ## Options
  - `:applications` - Applications to start (default: `[:neonfs_core]`)
  """
  @spec restart_node(cluster(), atom(), keyword()) :: {:ok, cluster()}
  def restart_node(cluster, node_name, opts \\ []) do
    applications = Keyword.get(opts, :applications, [:neonfs_core])
    node_info = get_node!(cluster, node_name)

    stop_peer_gracefully(node_info)

    deadline = System.monotonic_time(:millisecond) + 10_000
    wait_for_node_gone(node_info.node, deadline)

    {peer_opts, app_config} = build_restart_config(cluster, node_name)

    {:ok, peer, node} = start_peer(peer_opts, applications, app_config)
    wait_for_ra_ready(peer)

    new_cluster = replace_node_info(cluster, node_name, peer, node)
    connect_restarted_node(new_cluster, node_name)

    {:ok, new_cluster}
  end

  @doc """
  Disconnect two nodes bidirectionally (simulates network partition between a pair).

  Sets an invalid per-node cookie on each side to prevent Erlang distribution
  from auto-reconnecting (e.g. when Ra sends heartbeats), then disconnects.
  Idempotent — does nothing if the nodes are already disconnected.
  """
  @spec disconnect_nodes(cluster(), atom(), atom()) :: :ok
  def disconnect_nodes(cluster, node_a_name, node_b_name) do
    info_a = get_node!(cluster, node_a_name)
    info_b = get_node!(cluster, node_b_name)

    # Use :peer.call to bypass distribution — the peer control channel is
    # independent of cookies and unaffected by global's partition prevention.

    # Set DIFFERENT wrong per-node cookies on each side so auto-reconnection
    # fails. Both sides must disagree on the cookie: if both use the same
    # wrong cookie, Erlang distribution will happily reconnect them.
    :peer.call(info_a.peer, :erlang, :set_cookie, [info_b.node, :block_a_to_b])
    :peer.call(info_b.peer, :erlang, :set_cookie, [info_a.node, :block_b_to_a])

    :peer.call(info_a.peer, Node, :disconnect, [info_b.node])
    :peer.call(info_b.peer, Node, :disconnect, [info_a.node])
    :ok
  end

  @doc """
  Reconnect two nodes bidirectionally.

  Restores the cluster cookie on each side, then reconnects.
  Idempotent — does nothing if the nodes are already connected.
  """
  @spec reconnect_nodes(cluster(), atom(), atom()) :: :ok
  def reconnect_nodes(cluster, node_a_name, node_b_name) do
    info_a = get_node!(cluster, node_a_name)
    info_b = get_node!(cluster, node_b_name)

    # Use :peer.call to bypass distribution — the peer control channel is
    # independent of cookies and unaffected by global's partition prevention.

    # Restore the correct per-node cookies before reconnecting
    :peer.call(info_a.peer, :erlang, :set_cookie, [info_b.node, cluster.cookie])
    :peer.call(info_b.peer, :erlang, :set_cookie, [info_a.node, cluster.cookie])

    :peer.call(info_a.peer, Node, :connect, [info_b.node])
    :peer.call(info_b.peer, Node, :connect, [info_a.node])
    :ok
  end

  @doc """
  Partition a cluster into two groups by disconnecting all cross-group node pairs.

  Each group is a list of node names (atoms like `:node1`, `:node2`).
  Nodes within the same group remain connected.

  ## Example

      partition_cluster(cluster, [[:node1], [:node2, :node3]])
  """
  @spec partition_cluster(cluster(), [[atom()]]) :: :ok
  def partition_cluster(cluster, groups) when is_list(groups) do
    for group_a <- groups,
        group_b <- groups,
        group_a != group_b,
        name_a <- group_a,
        name_b <- group_b do
      {name_a, name_b}
    end
    |> Enum.uniq_by(fn {a, b} -> Enum.sort([a, b]) end)
    |> Enum.each(fn {name_a, name_b} ->
      disconnect_nodes(cluster, name_a, name_b)
    end)

    :ok
  end

  @doc """
  Heal all partitions by reconnecting every node pair in the cluster.
  """
  @spec heal_partition(cluster()) :: :ok
  def heal_partition(cluster) do
    names = Enum.map(cluster.nodes, & &1.name)

    for [name_a, name_b] <- combinations(names, 2) do
      reconnect_nodes(cluster, name_a, name_b)
    end

    # Nudge global on each peer to reconcile after cookie restoration
    for node_info <- cluster.nodes do
      :peer.call(node_info.peer, :global, :sync, [])
    end

    :ok
  end

  @doc """
  Return the list of connected nodes as seen from the given node.

  Returns Erlang node atoms (not alias names).
  """
  @spec visible_nodes(cluster(), atom()) :: [node()]
  def visible_nodes(cluster, node_name) do
    node_info = get_node!(cluster, node_name)
    :rpc.call(node_info.node, Node, :list, [])
  end

  @doc """
  Connect all peer nodes to each other for Erlang distribution.

  This must be called after starting nodes with `connection: 0` to enable
  normal RPC communication between them.
  """
  @spec connect_nodes(cluster()) :: :ok
  def connect_nodes(cluster) do
    # Build peer port mapping for the custom EPMD module
    peer_ports =
      Enum.map_join(cluster.nodes, ",", fn info -> "#{info.node}:#{info.dist_port}" end)

    # Set on the controller so it can resolve peer addresses
    System.put_env("NEONFS_PEER_PORTS", peer_ports)

    # Set on each peer so they can resolve each other
    for node_info <- cluster.nodes do
      :peer.call(node_info.peer, System, :put_env, ["NEONFS_PEER_PORTS", peer_ports])
    end

    # Verify each peer's distribution port is reachable before connecting.
    # Without EPMD, our custom module returns the port immediately from env
    # even if a previous test's peer hasn't fully released it yet. A quick
    # TCP probe avoids a long kernel-level connect timeout.
    for node_info <- cluster.nodes do
      wait_for_dist_port(node_info.dist_port)
    end

    # Connect the controller to all peer nodes
    for node_info <- cluster.nodes do
      Node.connect(node_info.node)
    end

    # Get all node atoms
    all_nodes = Enum.map(cluster.nodes, & &1.node)

    # Have each node connect to all other nodes
    for node_info <- cluster.nodes do
      other_nodes = Enum.reject(all_nodes, &(&1 == node_info.node))

      for other_node <- other_nodes do
        :rpc.call(node_info.node, Node, :connect, [other_node])
      end
    end

    :ok
  end

  # Private helpers

  defp start_cluster_node(
         node_name,
         peer_opts,
         applications,
         app_config,
         enable_ra,
         metrics_port,
         dist_port,
         acc
       ) do
    case start_peer(peer_opts, applications, app_config) do
      {:ok, peer, node} ->
        if enable_ra, do: span_wait_for_ra(peer, node)

        acc ++
          [
            %{
              name: node_name,
              peer: peer,
              node: node,
              dist_port: dist_port,
              metrics_port: metrics_port
            }
          ]

      {:error, reason} ->
        raise "Failed to start peer node #{node_name}: #{inspect(reason)}"
    end
  end

  defp apply_app_config(peer, app_config) do
    for {app, config} <- app_config, {key, value} <- config do
      :peer.call(peer, Application, :put_env, [app, key, value])
    end

    :ok
  end

  defp start_applications(peer, node, applications) do
    for app <- applications do
      start_application_on_peer(peer, node, app)
    end

    :ok
  end

  defp start_application_on_peer(peer, node, app) do
    # Under heavy load (e.g. full integration suite), application startup can
    # exceed the default 5s :peer.call timeout. Use 30s to be safe.
    case :peer.call(peer, :application, :ensure_all_started, [app], 30_000) do
      {:ok, _} -> :ok
      {:error, reason} -> Logger.warning("Failed to start #{app} on #{node}: #{inspect(reason)}")
    end
  end

  defp build_peer_opts(node_name, cookie, data_dir, dist_port) do
    code_paths = build_code_paths()

    # Ensure directories exist
    meta_dir = Path.join(data_dir, "meta")
    File.mkdir_p!(meta_dir)

    # Build args as a list of charlists (OTP 25+ peer module format)
    args =
      [
        ~c"-setcookie",
        to_charlist(cookie),
        # Custom EPMD module — no external EPMD daemon needed
        ~c"-start_epmd",
        ~c"false",
        ~c"-epmd_module",
        ~c"Elixir.NeonFS.Epmd",
        # Disable global's partition prevention — tests rapidly create/destroy
        # clusters and global misinterprets this as overlapping partitions
        ~c"-kernel",
        ~c"prevent_overlapping_partitions",
        ~c"false"
      ] ++
        Enum.flat_map(code_paths, fn path ->
          [~c"-pa", to_charlist(path)]
        end)

    # Environment variables passed via env option.
    # NEONFS_PEER_PORTS is set on the controller before node startup and
    # propagated here so each peer's custom EPMD module can resolve others.
    env =
      [
        {~c"NEONFS_DATA_DIR", to_charlist(data_dir)},
        {~c"NEONFS_META_DIR", to_charlist(meta_dir)},
        {~c"NEONFS_DIST_PORT", to_charlist(Integer.to_string(dist_port))}
      ] ++
        case System.get_env("NEONFS_PEER_PORTS") do
          nil -> []
          ports -> [{~c"NEONFS_PEER_PORTS", to_charlist(ports)}]
        end

    {%{
       name: node_name,
       host: ~c"localhost",
       args: args,
       env: env,
       # Use 0 (no auto-connection) to avoid DETS table name conflicts during startup
       # We'll connect nodes manually after Ra has initialized
       connection: 0,
       # Default wait_boot is 15s which is too short on slow CI runners
       wait_boot: 60_000
     }, dist_port}
  end

  defp wait_for_dist_port(port, attempts \\ 0)

  defp wait_for_dist_port(_port, attempts) when attempts >= 50 do
    :ok
  end

  defp wait_for_dist_port(port, attempts) do
    case :gen_tcp.connect(~c"localhost", port, [], 200) do
      {:ok, sock} ->
        :gen_tcp.close(sock)

      {:error, _} ->
        Process.sleep(100)
        wait_for_dist_port(port, attempts + 1)
    end
  end

  defp lookup_peer_port(all_peer_info, node_name) do
    case Enum.find(all_peer_info, fn {name, _port} -> name == node_name end) do
      {_, port} -> port
      nil -> allocate_peer_port()
    end
  end

  defp allocate_peer_port do
    # Bind-and-release to get a guaranteed free port from the OS.
    # This avoids collisions with ports still in TIME_WAIT from
    # previous test clusters that haven't fully released yet.
    {:ok, socket} = :gen_tcp.listen(0, reuseaddr: true)
    {:ok, port} = :inet.port(socket)
    :gen_tcp.close(socket)
    port
  end

  defp build_code_paths do
    # Include ALL code paths, not just _build - we need Elixir's stdlib too
    :code.get_path()
    |> Enum.map(&to_string/1)
  end

  defp wait_for_ra_ready(peer) do
    # Wait for Ra system to be ready on the peer. `:ra_system.fetch/1`
    # returns the system's config map once Ra is up, or the atom
    # `:undefined` before it is. Try up to 30 times with a 100ms
    # delay (3 seconds total). See #429.
    Enum.reduce_while(1..30, :not_ready, fn _i, _acc ->
      try do
        case :peer.call(peer, :ra_system, :fetch, [:default]) do
          system when is_map(system) ->
            {:halt, :ok}

          _ ->
            Process.sleep(100)
            {:cont, :not_ready}
        end
      catch
        :exit, _ ->
          Process.sleep(100)
          {:cont, :not_ready}
      end
    end)
  end

  defp wait_for_peers_gone(nodes) do
    # Wait up to 5 seconds for all peer nodes to fully terminate.
    deadline = System.monotonic_time(:millisecond) + 5_000

    Enum.each(nodes, fn node_info ->
      wait_for_node_gone(node_info.node, deadline)
    end)
  end

  defp wait_for_node_gone(node_atom, deadline) do
    if System.monotonic_time(:millisecond) > deadline do
      Logger.warning("Peer node #{node_atom} still registered after timeout")
    else
      case Node.ping(node_atom) do
        :pang ->
          :ok

        :pong ->
          Node.disconnect(node_atom)
          Process.sleep(100)
          wait_for_node_gone(node_atom, deadline)
      end
    end
  end

  defp generate_cluster_id do
    :crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)
  end

  defp generate_cookie do
    :crypto.strong_rand_bytes(16)
    |> Base.encode32(case: :lower, padding: false)
    |> String.to_atom()
  end

  defp ensure_distributed! do
    unless Node.alive?() do
      raise """
      Erlang distribution is not enabled.

      Integration tests require distribution to be enabled from the start.
      Run tests with:

          elixir --sname test_runner -S mix test

      Or for a specific test file:

          elixir --sname test_runner -S mix test test/integration/cluster_formation_test.exs
      """
    end
  end

  defp create_cluster_dir(cluster_id) do
    dir = Path.join(System.tmp_dir!(), "neonfs_test_#{cluster_id}")
    File.mkdir_p!(dir)
    dir
  end

  defp combinations(_, 0), do: [[]]
  defp combinations([], _), do: []

  defp combinations([head | tail], k) do
    Enum.map(combinations(tail, k - 1), &[head | &1]) ++ combinations(tail, k)
  end

  # ─── restart_node helpers ──────────────────────────────────────────

  defp stop_peer_gracefully(node_info) do
    try do
      :peer.call(node_info.peer, :ra_system, :stop, [:default])
    catch
      _, _ -> :ok
    end

    try do
      :peer.stop(node_info.peer)
    catch
      :exit, _ -> :ok
    end
  end

  defp build_restart_config(cluster, node_name) do
    peer_name = :"#{node_name}_#{cluster.id}"
    data_dir = Path.join(cluster.data_dir, Atom.to_string(node_name))
    meta_dir = Path.join(data_dir, "meta")
    ra_dir = Path.join(data_dir, "ra")

    # Reuse the same dist_port the node had before restart
    old_info = get_node!(cluster, node_name)

    {peer_opts, _dist_port} =
      build_peer_opts(peer_name, cluster.cookie, data_dir, old_info.dist_port)

    app_config = [
      logger: [level: :warning],
      neonfs_client: [
        tls_dir: Path.join(data_dir, "tls"),
        partition_recovery_debounce_ms: 200,
        service_list_fn: {NeonFS.Core.ServiceRegistry, :list, []}
      ],
      neonfs_core: [
        data_dir: data_dir,
        meta_dir: meta_dir,
        blob_store_base_dir: Path.join(data_dir, "blobs"),
        metrics_enabled: false,
        ra_data_dir: to_charlist(ra_dir),
        enable_ra: true,
        quorum_timeout_ms: 15_000
      ],
      ra: [data_dir: to_charlist(ra_dir)]
    ]

    {peer_opts, app_config}
  end

  defp replace_node_info(cluster, node_name, peer, node) do
    new_nodes =
      Enum.map(cluster.nodes, fn
        %{name: ^node_name} = old -> %{old | peer: peer, node: node}
        other -> other
      end)

    %{cluster | nodes: new_nodes}
  end

  defp connect_restarted_node(cluster, restarted_name) do
    restarted = get_node!(cluster, restarted_name)

    # Set peer ports on the restarted node so its EPMD module can resolve peers
    peer_ports =
      Enum.map_join(cluster.nodes, ",", fn info -> "#{info.node}:#{info.dist_port}" end)

    :peer.call(restarted.peer, System, :put_env, ["NEONFS_PEER_PORTS", peer_ports])

    Node.connect(restarted.node)

    for node_info <- cluster.nodes, node_info.name != restarted_name do
      :rpc.call(restarted.node, Node, :connect, [node_info.node])
      :rpc.call(node_info.node, Node, :connect, [restarted.node])
    end

    :ok
  end
end
