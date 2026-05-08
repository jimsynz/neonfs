defmodule NeonFS.TestSupport.ClusterCase do
  @moduledoc """
  ExUnit case template for integration tests requiring a peer cluster.

  ## Design Principles

  - **No `Process.sleep`**: Use event-based synchronisation instead
  - **Supervised lifecycle**: Each peer node supervised via `start_supervised!/2`
  - **Telemetry integration**: Subscribe to events for completion notification

  ## Cluster Lifecycle Modes

  Control the cluster lifecycle with the `@moduletag cluster_mode:` tag:

  - **`:per_test`** (default) — fresh cluster per test via `setup`
  - **`:shared`** — one cluster for the whole module via `setup_all`;
    tests share it and must use unique volume names to avoid collisions

  ## Usage

      # Per-test cluster (default)
      defmodule MyTest do
        use NeonFS.TestSupport.ClusterCase, async: false

        @moduletag nodes: 3

        test "my cluster test", %{cluster: cluster} do
          PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])
        end
      end

      # Shared cluster (one per module)
      defmodule FastTest do
        use NeonFS.TestSupport.ClusterCase, async: false

        @moduletag cluster_mode: :shared
        @moduletag nodes: 3

        test "test A", %{cluster: cluster} do
          # cluster is created once and shared across all tests
        end
      end
  """

  use ExUnit.CaseTemplate

  alias NeonFS.TestSupport.PeerCluster

  using do
    quote do
      alias NeonFS.TestSupport.PeerCluster

      import NeonFS.TestSupport.ClusterCase
    end
  end

  setup_all tags do
    if Map.get(tags, :cluster_mode) == :shared do
      node_count = Map.get(tags, :nodes, 3)
      applications = Map.get(tags, :applications, [:neonfs_core])
      base_dir = create_temp_dir()
      cluster_opts = [applications: applications, base_dir: base_dir]

      cluster_opts =
        case Map.get(tags, :metrics_port) do
          nil -> cluster_opts
          port -> Keyword.put(cluster_opts, :metrics_port, port)
        end

      ensure_clean_node_state()

      cluster = PeerCluster.start_cluster!(node_count, cluster_opts)

      PeerCluster.connect_nodes(cluster)

      on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

      %{cluster: cluster}
    else
      %{}
    end
  end

  setup tags do
    if Map.get(tags, :cluster_mode) == :shared do
      # Cluster already created in setup_all — pass through
      %{}
    else
      # Default: per-test cluster creation
      node_count = Map.get(tags, :nodes, 3)
      applications = Map.get(tags, :applications, [:neonfs_core])
      base_dir = Map.get(tags, :tmp_dir) || create_temp_dir()

      # When a shared cluster exists (from setup_all), skip cleanup that would
      # disconnect it. The per-test cluster uses unique node names so there
      # are no EPMD collisions.
      shared_cluster = tags[:cluster]
      unless shared_cluster, do: ensure_clean_node_state()

      cluster_opts = [applications: applications, base_dir: base_dir]

      cluster_opts =
        case Map.get(tags, :metrics_port) do
          nil -> cluster_opts
          port -> Keyword.put(cluster_opts, :metrics_port, port)
        end

      cluster = PeerCluster.start_cluster!(node_count, cluster_opts)

      PeerCluster.connect_nodes(cluster)

      on_exit(fn ->
        PeerCluster.stop_cluster(cluster)

        # Restore the shared cluster's cookie and reconnect after per-test
        # cleanup. start_cluster! calls :erlang.set_cookie/1, which changes
        # the test runner's cookie and breaks connectivity to shared nodes.
        if shared_cluster do
          :erlang.set_cookie(shared_cluster.cookie)
          PeerCluster.connect_nodes(shared_cluster)
          :global.sync()
        end
      end)

      %{cluster: cluster}
    end
  end

  defp ensure_clean_node_state do
    # Wait for stale peer VMs from a previous test's async on_exit to fully
    # terminate. Check Node.list() for any stale "node*" connections and
    # disconnect them before starting new peers.
    wait_until(
      fn ->
        disconnect_stale_peers()
        no_stale_peer_connections?()
      end,
      timeout: 15_000,
      interval: 200
    )

    # Reconcile global's network view after old peers are gone, before
    # the new cluster connects. This reduces the chance of global acting
    # on stale distribution state from the previous cluster.
    :global.sync()
  end

  defp disconnect_stale_peers do
    for node <- Node.list(),
        String.starts_with?(Atom.to_string(node), "node"),
        do: Node.disconnect(node)
  end

  defp no_stale_peer_connections? do
    not Enum.any?(Node.list(), fn node ->
      String.starts_with?(Atom.to_string(node), "node")
    end)
  end

  defp create_temp_dir do
    dir =
      Path.join(
        System.tmp_dir!(),
        "neonfs_test_#{:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)}"
      )

    File.mkdir_p!(dir)
    dir
  end

  @doc """
  Wait for a condition to become true, with timeout.

  Prefer this over `Process.sleep` when waiting for state changes.
  Uses exponential backoff to reduce polling overhead.

  ## Options
  - `:timeout` - Maximum time to wait in milliseconds (default: 5_000)
  - `:interval` - Initial polling interval in milliseconds (default: 50)
  - `:max_interval` - Maximum polling interval (default: 500)

  ## Example

      :ok = wait_until(fn ->
        case PeerCluster.rpc(cluster, :node1, Module, :status, []) do
          {:ok, status} -> status == :ready
          _ -> false
        end
      end)
  """
  @spec wait_until((-> boolean()), keyword()) :: :ok | {:error, :timeout}
  def wait_until(condition, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    initial_interval = Keyword.get(opts, :interval, 50)
    max_interval = Keyword.get(opts, :max_interval, 500)

    deadline = System.monotonic_time(:millisecond) + timeout
    do_wait_until(deadline, initial_interval, max_interval, condition)
  end

  defp do_wait_until(deadline, interval, max_interval, condition) do
    if System.monotonic_time(:millisecond) > deadline do
      {:error, :timeout}
    else
      if condition.() do
        :ok
      else
        # Exponential backoff, capped at max_interval
        Process.sleep(interval)
        next_interval = min(interval * 2, max_interval)
        do_wait_until(deadline, next_interval, max_interval, condition)
      end
    end
  end

  @doc """
  Start a Bandit HTTP server with automatic retry for transient listener bind
  failures.

  Under heavy load (for example `mix check` running cargo builds in parallel),
  `gen_tcp.listen` port operations inside ThousandIsland can time out with
  `{:inet_async, :timeout}`. The failure is transient — no socket is actually
  in use — and retrying succeeds. This wrapper traps exits so a linked
  supervisor death does not kill the caller, confirms listener readiness via
  `ThousandIsland.listener_info/1`, and retries on transient errors.

  Returns `{server_pid, port}` on success, raises on persistent failure.

  ## Options
  - `:retries` - Maximum attempts (default: 5)
  - `:backoff_ms` - Delay between attempts (default: 200)

  ## Example

      {server, port} =
        start_bandit_with_retry(
          plug: {MyPlug, []},
          port: 0,
          ip: :loopback,
          startup_log: false
        )
  """
  @spec start_bandit_with_retry(keyword(), keyword()) :: {pid(), :inet.port_number()}
  def start_bandit_with_retry(bandit_opts, retry_opts \\ []) do
    retries = Keyword.get(retry_opts, :retries, 5)
    backoff_ms = Keyword.get(retry_opts, :backoff_ms, 200)
    previous_trap = Process.flag(:trap_exit, true)

    try do
      do_start_bandit(bandit_opts, retries, backoff_ms)
    after
      Process.flag(:trap_exit, previous_trap)
    end
  end

  defp do_start_bandit(_bandit_opts, 0, _backoff_ms) do
    raise "Bandit failed to start after repeated :inet_async timeouts"
  end

  defp do_start_bandit(bandit_opts, attempts_left, backoff_ms) do
    case Bandit.start_link(bandit_opts) do
      {:ok, server} ->
        case confirm_listener(server) do
          {:ok, port} ->
            {server, port}

          :transient ->
            Process.sleep(backoff_ms)
            do_start_bandit(bandit_opts, attempts_left - 1, backoff_ms)
        end

      {:error, reason} ->
        if transient_listener_error?(reason) do
          Process.sleep(backoff_ms)
          do_start_bandit(bandit_opts, attempts_left - 1, backoff_ms)
        else
          raise "Bandit.start_link failed: #{inspect(reason)}"
        end
    end
  end

  # After `Bandit.start_link` returns `{:ok, pid}`, the listener can still
  # crash asynchronously with `{:inet_async, :timeout}` — the EXIT arrives at
  # the caller via the Bandit supervisor link. Settle briefly so any delayed
  # EXIT lands in the mailbox, then confirm the listener is reachable.
  defp confirm_listener(server) do
    receive do
      {:EXIT, ^server, reason} ->
        if transient_listener_error?(reason) do
          :transient
        else
          exit(reason)
        end
    after
      150 ->
        try do
          case ThousandIsland.listener_info(server) do
            {:ok, {_ip, port}} -> {:ok, port}
            :error -> :transient
          end
        catch
          :exit, _ -> :transient
        end
    end
  end

  defp transient_listener_error?({:inet_async, :timeout}), do: true

  defp transient_listener_error?({:shutdown, {:failed_to_start_child, _, inner}}),
    do: transient_listener_error?(inner)

  defp transient_listener_error?(_), do: false

  @doc """
  Wait for a telemetry event to be emitted.

  Subscribe to a telemetry event and wait for it to fire.

  ## Options
  - `:timeout` - Maximum time to wait in milliseconds (default: 5_000)

  ## Example

      # Wait for replication to complete
      :ok = wait_for_event([:neonfs, :replication, :complete], timeout: 10_000)
  """
  @spec wait_for_event([atom()], keyword()) :: :ok | {:error, :timeout}
  def wait_for_event(event_name, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 5_000)
    test_pid = self()
    handler_id = make_ref()

    handler = fn _event, _measurements, _metadata, _config ->
      send(test_pid, {:telemetry_event, handler_id})
    end

    :telemetry.attach(handler_id, event_name, handler, nil)

    result =
      receive do
        {:telemetry_event, ^handler_id} -> :ok
      after
        timeout -> {:error, :timeout}
      end

    :telemetry.detach(handler_id)
    result
  end

  @doc """
  Assert that a condition eventually becomes true.

  Raises if the condition is not met within the timeout.

  ## Options
  - `:timeout` - Maximum time to wait in milliseconds (default: 5_000)

  ## Example

      assert_eventually do
        {:ok, nodes} = PeerCluster.rpc(cluster, :node1, NeonFS.Cluster, :members, [])
        length(nodes) == 3
      end
  """
  defmacro assert_eventually(opts \\ [], do: block) do
    timeout = Keyword.get(opts, :timeout, 5_000)

    quote do
      import ExUnit.Assertions

      case wait_until(fn -> unquote(block) end, timeout: unquote(timeout)) do
        :ok ->
          :ok

        {:error, :timeout} ->
          flunk(
            "Condition not met within #{unquote(timeout)}ms: #{unquote(Macro.to_string(block))}"
          )
      end
    end
  end

  # ─── Partition assertion helpers ─────────────────────────────────

  @doc """
  Assert that two groups of nodes are partitioned from each other.

  Each group is a list of node names. Waits (with polling) until no node
  in one group can see any node in the other group.

  ## Options
  - `:timeout` - Maximum time to wait in milliseconds (default: 10_000)
  """
  @spec assert_partitioned(map(), [atom()], [atom()], keyword()) :: :ok
  def assert_partitioned(cluster, group_a, group_b, opts \\ []) do
    import ExUnit.Assertions
    timeout = Keyword.get(opts, :timeout, 10_000)

    node_atoms_a = Enum.map(group_a, &PeerCluster.get_node!(cluster, &1).node)
    node_atoms_b = Enum.map(group_b, &PeerCluster.get_node!(cluster, &1).node)

    case wait_until(
           fn ->
             group_isolated?(cluster, group_a, node_atoms_b) and
               group_isolated?(cluster, group_b, node_atoms_a)
           end,
           timeout: timeout
         ) do
      :ok ->
        :ok

      {:error, :timeout} ->
        flunk("Partition not effective within #{timeout}ms — cross-group nodes still visible")
    end
  end

  @doc """
  Assert that all nodes in a group can see each other.

  Visibility is polled with a bounded timeout — mesh reformation after
  `heal_partition` and other cross-node helpers is asynchronous, and a
  one-shot snapshot can catch the distribution layer mid-reconcile even
  after `wait_for_partition_healed/2` returns (see #438). Callers that
  want a strict snapshot can pass `timeout: 0`.

  ## Options

    * `:timeout` — maximum time to wait for full visibility (default: 5_000ms).
  """
  @spec assert_connected(map(), [atom()], keyword()) :: :ok
  def assert_connected(cluster, node_names, opts \\ []) do
    import ExUnit.Assertions

    timeout = Keyword.get(opts, :timeout, 5_000)

    check = fn ->
      Enum.all?(node_names, fn name ->
        visible = PeerCluster.visible_nodes(cluster, name)

        node_names
        |> Enum.reject(&(&1 == name))
        |> Enum.map(&PeerCluster.get_node!(cluster, &1).node)
        |> Enum.all?(&(&1 in visible))
      end)
    end

    case wait_until(check, timeout: timeout) do
      :ok -> :ok
      {:error, :timeout} -> flunk_missing_links(cluster, node_names)
    end
  end

  # Surface the specific missing visibility link in the failure message —
  # same detail the old one-shot assert emitted, computed against the
  # latest snapshot after timing out.
  defp flunk_missing_links(cluster, node_names) do
    import ExUnit.Assertions

    for name <- node_names, other_atom <- missing_others(cluster, node_names, name) do
      flunk("#{name} cannot see #{inspect(other_atom)} — expected connected")
    end

    :ok
  end

  defp missing_others(cluster, node_names, name) do
    visible = PeerCluster.visible_nodes(cluster, name)

    node_names
    |> Enum.reject(&(&1 == name))
    |> Enum.map(&PeerCluster.get_node!(cluster, &1).node)
    |> Enum.reject(&(&1 in visible))
  end

  @doc """
  Wait until all nodes in the cluster are connected to each other (full mesh).

  ## Options
  - `:timeout` - Maximum time to wait in milliseconds (default: 10_000)
  """
  @spec wait_for_partition_healed(map(), keyword()) :: :ok
  def wait_for_partition_healed(cluster, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 10_000)
    all_names = Enum.map(cluster.nodes, & &1.name)
    expected_count = length(all_names) - 1

    :ok =
      wait_until(
        fn ->
          Enum.all?(all_names, fn name ->
            visible = PeerCluster.visible_nodes(cluster, name)
            length(visible) >= expected_count
          end)
        end,
        timeout: timeout
      )
  end

  # ─── Shared cluster init helpers ──────────────────────────────────

  @doc """
  Initialise a multi-node (3-node) cluster with sequential joins, full mesh
  wait, and quorum ring rebuild. Optionally creates volumes.

  ## Options
  - `:name` - Cluster name (default: "test")
  - `:volumes` - List of `{name, opts}` tuples to create (default: [])

  ## Example

      :ok = init_multi_node_cluster(cluster, name: "my-test", volumes: [{"vol", %{}}])
  """
  @spec init_multi_node_cluster(map(), keyword()) :: :ok
  def init_multi_node_cluster(cluster, opts \\ []) do
    cluster_name = Keyword.get(opts, :name, "test")
    volumes = Keyword.get(opts, :volumes, [])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, [cluster_name])

    {:ok, %{"token" => token}} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_invite, [3600])

    node1_atom = cluster |> PeerCluster.get_node!(:node1) |> Map.get(:node)
    node_names = cluster.nodes |> Enum.map(& &1.name) |> Enum.reject(&(&1 == :node1))

    for node_name <- node_names do
      # Use the direct RPC join flow (not HTTP) since test nodes don't run
      # the metrics HTTP server. This calls accept_join on node1 directly.
      {:ok, _} =
        PeerCluster.rpc(cluster, node_name, NeonFS.Cluster.Join, :join_cluster_rpc, [
          token,
          node1_atom
        ])

      :ok = wait_for_cluster_stable(cluster)
    end

    wait_for_full_mesh(cluster)
    rebuild_quorum_rings(cluster)
    wait_for_all_drives_in_bootstrap(cluster)

    for {name, vol_opts} <- volumes do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [name, vol_opts])
    end

    :ok
  end

  @doc false
  # `Cluster.Join.join_cluster_rpc/3` returns once the synchronous
  # parts of the join finish, but the actual `RaServer.join_cluster`
  # call — and therefore the joining node's drive registration in the
  # bootstrap layer — happens 500 ms later in a spawned process. Tests
  # that read multi-replica metadata (cross-node reads, durability
  # `replicate: factor=N` with N > 1) need every node's drives present
  # before any volume is created, otherwise the volume's
  # `drive_locations` only captures the bootstrap node's drive and the
  # write-side never replicates the metadata segment to peers.
  @spec wait_for_all_drives_in_bootstrap(map()) :: :ok
  def wait_for_all_drives_in_bootstrap(cluster) do
    expected =
      MapSet.new(cluster.nodes, fn ni -> PeerCluster.get_node!(cluster, ni.name).node end)

    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.Core.RaSupervisor, :local_query, [
                 &NeonFS.Core.MetadataStateMachine.get_drives/1
               ]) do
            {:ok, drives_map} when is_map(drives_map) ->
              registered = drives_map |> Map.values() |> MapSet.new(& &1.node)
              MapSet.subset?(expected, registered)

            _ ->
              false
          end
        end,
        timeout: 15_000
      )
  end

  @doc """
  Initialise a mixed-role cluster: one core peer plus zero or more interface
  peers (`:neonfs_s3`, `:neonfs_webdav`, `:neonfs_fuse`, `:neonfs_nfs`).

  The core peer (the first `:neonfs_core` peer in `cluster.nodes`) runs
  `cluster_init`; each interface peer joins via
  `NeonFS.Cluster.Join.join_cluster_rpc/3` with the matching `ServiceType`
  atom so the join flow installs CA + node TLS material on disk. After
  joins complete, `Transport.PoolManager.ensure_pool/2` is invoked
  explicitly on each interface peer with the core peer's listener
  endpoint — interface peers do not run the data-plane `Listener`, so the
  join flow's `activate_data_plane` does not create the outbound pool on
  its own. Without that explicit `ensure_pool/2`, `Router.data_call/4`
  from an interface peer returns `{:error, :no_data_endpoint}` because
  no `PoolManager` ETS row exists for the core target.

  Returns `:ok`. Raises if no core peer is present, or if any join /
  pool establishment step fails.

  ## Options
  - `:name` — cluster name (default: `"test"`)
  - `:volumes` — list of `{name, opts}` tuples to create on the core
    peer after the data plane is up (default: `[]`)

  ## Example

      cluster =
        PeerCluster.start_cluster!(2,
          roles: %{node1: [:neonfs_core], node2: [:neonfs_s3]}
        )

      PeerCluster.connect_nodes(cluster)
      :ok = init_mixed_role_cluster(cluster, name: "smoke")
  """
  @spec init_mixed_role_cluster(map(), keyword()) :: :ok
  def init_mixed_role_cluster(cluster, opts \\ []) do
    cluster_name = Keyword.get(opts, :name, "test")
    volumes = Keyword.get(opts, :volumes, [])

    {core_peer, interface_peers} = split_core_and_interface_peers(cluster)

    {:ok, _} =
      PeerCluster.rpc(cluster, core_peer.name, NeonFS.CLI.Handler, :cluster_init, [cluster_name])

    :ok = wait_for_cluster_stable_on(cluster, core_peer.name)

    {:ok, %{"token" => token}} =
      PeerCluster.rpc(cluster, core_peer.name, NeonFS.CLI.Handler, :create_invite, [3600])

    for peer <- interface_peers do
      type = service_type_for_apps(peer.applications)

      {:ok, _} =
        PeerCluster.rpc(cluster, peer.name, NeonFS.Cluster.Join, :join_cluster_rpc, [
          token,
          core_peer.node,
          type
        ])
    end

    core_endpoint = fetch_core_endpoint(cluster, core_peer)

    for peer <- interface_peers do
      {:ok, _pid} =
        PeerCluster.rpc(cluster, peer.name, NeonFS.Transport.PoolManager, :ensure_pool, [
          core_peer.node,
          core_endpoint
        ])
    end

    for peer <- interface_peers do
      :ok = wait_for_pool(cluster, peer.name, core_peer.node)
      :ok = wait_for_discovery(cluster, peer.name, core_peer.node)
    end

    for {name, vol_opts} <- volumes do
      {:ok, _} =
        PeerCluster.rpc(cluster, core_peer.name, NeonFS.CLI.Handler, :create_volume, [
          name,
          vol_opts
        ])
    end

    :ok
  end

  defp split_core_and_interface_peers(cluster) do
    case Enum.split_with(cluster.nodes, &(:neonfs_core in &1.applications)) do
      {[], _} ->
        raise ArgumentError,
              "init_mixed_role_cluster requires at least one peer running :neonfs_core"

      {[core | _], interfaces} ->
        {core, interfaces}
    end
  end

  defp service_type_for_apps(apps) do
    cond do
      :neonfs_s3 in apps -> :s3
      :neonfs_webdav in apps -> :webdav
      :neonfs_nfs in apps -> :nfs
      :neonfs_fuse in apps -> :fuse
      :neonfs_containerd in apps -> :containerd
      true -> raise ArgumentError, "no recognised interface app in #{inspect(apps)}"
    end
  end

  defp fetch_core_endpoint(cluster, core_peer) do
    port = PeerCluster.rpc(cluster, core_peer.name, NeonFS.Transport.Listener, :get_port, [])

    if not is_integer(port) or port <= 0 do
      raise "core peer #{core_peer.name} has no data-plane listener bound (port=#{inspect(port)})"
    end

    {~c"127.0.0.1", port}
  end

  defp wait_for_cluster_stable_on(cluster, node_name) do
    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, node_name, NeonFS.CLI.Handler, :cluster_status, []) do
            {:ok, _status} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )
  end

  @doc """
  Wait until `NeonFS.Client.Discovery.get_core_nodes/0` on `from_node` includes
  `target_node`. Forces a refresh on each iteration so the test does not have
  to sit through the cache's default `:peer_sync_interval`. Default timeout 15s.
  """
  @spec wait_for_discovery(map(), atom(), node(), keyword()) :: :ok
  def wait_for_discovery(cluster, from_node, target_node, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 15_000)

    :ok =
      wait_until(
        fn ->
          PeerCluster.rpc(cluster, from_node, NeonFS.Client.Discovery, :refresh, [])

          target_node in PeerCluster.rpc(
            cluster,
            from_node,
            NeonFS.Client.Discovery,
            :get_core_nodes,
            []
          )
        end,
        timeout: timeout
      )
  end

  @doc """
  Wait until `NeonFS.Transport.PoolManager.get_pool/1` on `from_node` returns
  `{:ok, _pid}` for `target_node`. Default timeout 30s.
  """
  @spec wait_for_pool(map(), atom(), node(), keyword()) :: :ok
  def wait_for_pool(cluster, from_node, target_node, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 30_000)

    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, from_node, NeonFS.Transport.PoolManager, :get_pool, [
                 target_node
               ]) do
            {:ok, _pool} -> true
            _ -> false
          end
        end,
        timeout: timeout
      )
  end

  @doc """
  Initialise a single-node cluster and optionally create volumes.

  ## Options
  - `:name` - Cluster name (default: "test")
  - `:volumes` - List of `{name, opts}` tuples to create (default: [])
  """
  @spec init_single_node_cluster(map(), keyword()) :: :ok
  def init_single_node_cluster(cluster, opts \\ []) do
    cluster_name = Keyword.get(opts, :name, "test")
    volumes = Keyword.get(opts, :volumes, [])

    {:ok, _} =
      PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, [cluster_name])

    :ok = wait_for_cluster_stable(cluster)

    for {name, vol_opts} <- volumes do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [name, vol_opts])
    end

    :ok
  end

  @doc """
  Wait for cluster_status to return {:ok, _} on node1, indicating Ra is stable.
  """
  @spec wait_for_cluster_stable(map()) :: :ok
  def wait_for_cluster_stable(cluster) do
    :ok =
      wait_until(
        fn ->
          case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
            {:ok, _status} -> true
            _ -> false
          end
        end,
        timeout: 10_000
      )
  end

  @doc """
  Wait for all peer nodes to have fully started (ReadySignal joined `:pg`).

  Uses `:pg.monitor/2` for event-driven detection instead of polling.
  `NeonFS.Core.ReadySignal` is the last child in the supervisor, so its
  presence in the `{:node, :ready}` group guarantees all preceding children
  (including MetadataStore) have started.
  """
  @spec wait_for_full_mesh(map()) :: :ok
  def wait_for_full_mesh(cluster) do
    peer_nodes =
      MapSet.new(cluster.nodes, fn ni -> PeerCluster.get_node!(cluster, ni.name).node end)

    ensure_pg_scope()

    {ref, current_members} = :pg.monitor(:neonfs_events, {:node, :ready})

    ready_nodes =
      current_members
      |> Enum.map(&node/1)
      |> MapSet.new()
      |> MapSet.intersection(peer_nodes)

    unless MapSet.equal?(ready_nodes, peer_nodes) do
      deadline = System.monotonic_time(:millisecond) + 30_000
      await_pg_ready(ref, peer_nodes, ready_nodes, deadline)
    end

    :pg.demonitor(:neonfs_events, ref)
    :ok
  end

  @doc """
  Rebuild quorum rings on all nodes in the cluster.
  """
  @spec rebuild_quorum_rings(map()) :: :ok
  def rebuild_quorum_rings(cluster) do
    for node_info <- cluster.nodes do
      PeerCluster.rpc(cluster, node_info.name, NeonFS.Core.Supervisor, :rebuild_quorum_ring, [])
    end

    :ok
  end

  # ─── Partition helpers ────────────────────────────────────────────

  defp group_isolated?(cluster, group_names, forbidden_atoms) do
    Enum.all?(group_names, fn name ->
      visible = PeerCluster.visible_nodes(cluster, name)
      Enum.all?(forbidden_atoms, &(&1 not in visible))
    end)
  end

  # ─── :pg readiness helpers ─────────────────────────────────────────

  defp ensure_pg_scope do
    case :pg.start(:neonfs_events) do
      {:ok, _pid} -> :ok
      {:error, {:already_started, _pid}} -> :ok
    end
  end

  defp await_pg_ready(ref, expected, ready, deadline) do
    missing = MapSet.difference(expected, ready)

    if MapSet.size(missing) == 0 do
      :ok
    else
      remaining_ms = deadline - System.monotonic_time(:millisecond)

      if remaining_ms <= 0 do
        import ExUnit.Assertions
        flunk("Nodes not ready within timeout: #{inspect(MapSet.to_list(missing))}")
      else
        receive do
          {^ref, :join, {:node, :ready}, pids} ->
            new_ready =
              pids
              |> Enum.map(&node/1)
              |> MapSet.new()
              |> MapSet.intersection(expected)

            await_pg_ready(ref, expected, MapSet.union(ready, new_ready), deadline)
        after
          remaining_ms ->
            import ExUnit.Assertions
            flunk("Nodes not ready within timeout: #{inspect(MapSet.to_list(missing))}")
        end
      end
    end
  end

  # ─── Event-driven wait helpers ────────────────────────────────────

  @doc """
  Subscribe to events on a peer node, then run an action, then wait for an
  event to arrive. Returns the action's result.

  Useful for replacing `assert_eventually timeout: 60_000` polling loops on
  cross-node reads: subscribe first, write, wait for the event, then assert.

  ## Options
  - `:timeout` - How long to wait for the event (default: 10_000)
  - `:match` - Predicate on the envelope (default: always true)
  """
  @spec subscribe_then_act(map(), atom(), binary(), (-> any()), keyword()) :: any()
  def subscribe_then_act(cluster, subscriber_node, volume_id, action_fn, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 10_000)
    match_fn = Keyword.get(opts, :match, fn _envelope -> true end)

    {:ok, collector} =
      PeerCluster.rpc(
        cluster,
        subscriber_node,
        NeonFS.TestSupport.EventCollector,
        :start_with_notify,
        [volume_id, self()]
      )

    result = action_fn.()

    do_receive_event(match_fn, timeout)

    PeerCluster.rpc(cluster, subscriber_node, GenServer, :stop, [collector])
    result
  end

  @doc """
  Subscribe on a peer node and block until an event matching the predicate arrives.

  ## Options
  - `:timeout` - How long to wait (default: 10_000)
  - `:match` - Predicate on the envelope (default: always true)
  """
  @spec wait_for_event_on_node(map(), atom(), binary(), keyword()) :: :ok
  def wait_for_event_on_node(cluster, node_name, volume_id, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 10_000)
    match_fn = Keyword.get(opts, :match, fn _envelope -> true end)

    {:ok, collector} =
      PeerCluster.rpc(
        cluster,
        node_name,
        NeonFS.TestSupport.EventCollector,
        :start_with_notify,
        [volume_id, self()]
      )

    do_receive_event(match_fn, timeout)

    PeerCluster.rpc(cluster, node_name, GenServer, :stop, [collector])
    :ok
  end

  defp do_receive_event(match_fn, timeout) do
    deadline = System.monotonic_time(:millisecond) + timeout

    do_receive_event_loop(match_fn, deadline)
  end

  defp do_receive_event_loop(match_fn, deadline) do
    remaining = max(deadline - System.monotonic_time(:millisecond), 0)

    receive do
      {:neonfs_test_event, envelope} ->
        if match_fn.(envelope) do
          :ok
        else
          do_receive_event_loop(match_fn, deadline)
        end
    after
      remaining ->
        import ExUnit.Assertions
        flunk("No matching event received within timeout")
    end
  end
end
