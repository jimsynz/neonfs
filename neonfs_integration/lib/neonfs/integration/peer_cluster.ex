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
          node: node()
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
    case :peer.start_link(peer_opts) do
      {:ok, peer, node} ->
        # Suppress debug/info logs on peer nodes during tests
        # Configure both Elixir's Logger and Erlang's :logger
        :peer.call(peer, Logger, :configure, [[level: :warning]])
        :peer.call(peer, :logger, :set_primary_config, [:level, :warning])

        # Use :peer.call since nodes aren't connected yet (connection: 0)
        apply_app_config(peer, app_config)
        start_applications(peer, node, applications)
        {:ok, peer, node}

      {:error, reason} ->
        {:error, reason}
    end
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

    # Ensure controller is distributed
    ensure_distributed!()

    # Set the cookie on the controller node
    :erlang.set_cookie(cookie)

    # Use provided base_dir or create a new one
    base_dir = Keyword.get_lazy(opts, :base_dir, fn -> create_cluster_dir(cluster_id) end)

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

        peer_opts = build_peer_opts(peer_name, cookie, data_dir)

        # Configure neonfs_core to use the test data directories
        # IMPORTANT: Ra expects data_dir as a charlist, not a binary!
        # DETS in Erlang requires charlist file paths.
        core_config = [
          data_dir: data_dir,
          meta_dir: meta_dir,
          blob_store_base_dir: Path.join(data_dir, "blobs"),
          ra_data_dir: to_charlist(ra_dir),
          enable_ra: enable_ra,
          # Increase quorum timeout for integration tests where 3 peer nodes
          # share a single machine and BEAM schedulers are heavily contended.
          quorum_timeout_ms: 15_000
        ]

        core_config =
          if drives_fn do
            Keyword.put(core_config, :drives, drives_fn.(alias_name, data_dir))
          else
            core_config
          end

        app_config = [
          logger: [level: :warning],
          neonfs_core: core_config,
          ra: [data_dir: to_charlist(ra_dir)]
        ]

        start_cluster_node(alias_name, peer_opts, applications, app_config, enable_ra, acc)
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
  Connect all peer nodes to each other for Erlang distribution.

  This must be called after starting nodes with `connection: 0` to enable
  normal RPC communication between them.
  """
  @spec connect_nodes(cluster()) :: :ok
  def connect_nodes(cluster) do
    # First, connect the controller to all peer nodes
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

  defp start_cluster_node(node_name, peer_opts, applications, app_config, enable_ra, acc) do
    case start_peer(peer_opts, applications, app_config) do
      {:ok, peer, node} ->
        if enable_ra, do: wait_for_ra_ready(peer)
        acc ++ [%{name: node_name, peer: peer, node: node}]

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

  defp build_peer_opts(node_name, cookie, data_dir) do
    code_paths = build_code_paths()

    # Ensure directories exist
    meta_dir = Path.join(data_dir, "meta")
    File.mkdir_p!(meta_dir)

    # Build args as a list of charlists (OTP 25+ peer module format)
    args =
      [
        ~c"-setcookie",
        to_charlist(cookie),
        # Disable global's partition prevention — tests rapidly create/destroy
        # clusters and global misinterprets this as overlapping partitions
        ~c"-kernel",
        ~c"prevent_overlapping_partitions",
        ~c"false"
      ] ++
        Enum.flat_map(code_paths, fn path ->
          [~c"-pa", to_charlist(path)]
        end)

    # Environment variables passed via env option
    env = [
      {~c"NEONFS_DATA_DIR", to_charlist(data_dir)},
      {~c"NEONFS_META_DIR", to_charlist(meta_dir)}
    ]

    %{
      name: node_name,
      host: ~c"localhost",
      args: args,
      env: env,
      # Use 0 (no auto-connection) to avoid DETS table name conflicts during startup
      # We'll connect nodes manually after Ra has initialized
      connection: 0
    }
  end

  defp build_code_paths do
    # Include ALL code paths, not just _build - we need Elixir's stdlib too
    :code.get_path()
    |> Enum.map(&to_string/1)
  end

  defp wait_for_ra_ready(peer) do
    # Wait for Ra system to be ready on the peer
    # Try up to 30 times with 100ms delay (3 seconds total)
    Enum.reduce_while(1..30, :not_ready, fn _i, _acc ->
      try do
        case :peer.call(peer, :ra_system, :fetch, [:default]) do
          {:ok, _} ->
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
    # This prevents the next test from hitting EPMD name conflicts.
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
end
