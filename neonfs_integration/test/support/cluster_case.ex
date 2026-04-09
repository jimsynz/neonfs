defmodule NeonFS.Integration.ClusterCase do
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
        use NeonFS.Integration.ClusterCase, async: false

        @moduletag nodes: 3

        test "my cluster test", %{cluster: cluster} do
          PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])
        end
      end

      # Shared cluster (one per module)
      defmodule FastTest do
        use NeonFS.Integration.ClusterCase, async: false

        @moduletag cluster_mode: :shared
        @moduletag nodes: 3

        test "test A", %{cluster: cluster} do
          # cluster is created once and shared across all tests
        end
      end
  """

  use ExUnit.CaseTemplate

  alias NeonFS.Integration.PeerCluster

  using do
    quote do
      alias NeonFS.Integration.PeerCluster

      import NeonFS.Integration.ClusterCase
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
  """
  @spec assert_connected(map(), [atom()]) :: :ok
  def assert_connected(cluster, node_names) do
    import ExUnit.Assertions

    for name <- node_names do
      visible = PeerCluster.visible_nodes(cluster, name)
      others = Enum.reject(node_names, &(&1 == name))
      other_atoms = Enum.map(others, &PeerCluster.get_node!(cluster, &1).node)

      for other_atom <- other_atoms do
        assert other_atom in visible,
               "#{name} cannot see #{inspect(other_atom)} — expected connected"
      end
    end

    :ok
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

    for {name, vol_opts} <- volumes do
      {:ok, _} =
        PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :create_volume, [name, vol_opts])
    end

    :ok
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
        NeonFS.Integration.EventCollector,
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
        NeonFS.Integration.EventCollector,
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
