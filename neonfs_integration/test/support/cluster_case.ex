defmodule NeonFS.Integration.ClusterCase do
  @moduledoc """
  ExUnit case template for integration tests requiring a peer cluster.

  ## Design Principles

  - **No `Process.sleep`**: Use event-based synchronisation instead
  - **Supervised lifecycle**: Each peer node supervised via `start_supervised!/2`
  - **Telemetry integration**: Subscribe to events for completion notification

  ## Usage

      defmodule MyTest do
        use NeonFS.Integration.ClusterCase, async: false

        @moduletag nodes: 3  # Optional, defaults to 3

        test "my cluster test", %{cluster: cluster} do
          # Use wait_for_event or wait_until, never Process.sleep
          PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_init, ["test"])

          # Wait for cluster to be ready via event or condition
          :ok = wait_until(fn ->
            case PeerCluster.rpc(cluster, :node1, NeonFS.CLI.Handler, :cluster_status, []) do
              {:ok, status} -> status.state == :ready
              _ -> false
            end
          end)
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

  setup tags do
    node_count = Map.get(tags, :nodes, 3)
    applications = Map.get(tags, :applications, [:neonfs_core])

    # Use ExUnit's tmp_dir if available, otherwise create our own
    base_dir = Map.get(tags, :tmp_dir) || create_temp_dir()

    # Ensure no stale peer nodes from previous tests (on_exit runs asynchronously)
    ensure_clean_node_state()

    # Start cluster in the provided directory
    cluster =
      PeerCluster.start_cluster!(node_count, applications: applications, base_dir: base_dir)

    # Connect all nodes to each other
    PeerCluster.connect_nodes(cluster)

    # Force the global name server to reconcile its view of the network.
    # Without this, global may see leftover distribution state from old
    # clusters and disconnect peer nodes to "prevent overlapping partitions".
    :global.sync()

    # Register cleanup for nodes (tmp_dir is cleaned up by ExUnit automatically)
    on_exit(fn -> PeerCluster.stop_cluster(cluster) end)

    %{cluster: cluster}
  end

  defp ensure_clean_node_state do
    # Wait for stale peer VMs from a previous test's async on_exit to fully
    # terminate. We check EPMD directly — any "node*" entry is from a previous
    # test cluster and must be gone before we start new peers to avoid resource
    # contention (CPU/memory from zombie VMs slowing down new cluster startup).
    wait_until(
      fn ->
        disconnect_stale_peers()
        no_stale_epmd_entries?()
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

  defp no_stale_epmd_entries? do
    case :erl_epmd.names() do
      {:ok, names} ->
        not Enum.any?(names, fn {name, _port} ->
          String.starts_with?(to_string(name), "node")
        end)

      {:error, _} ->
        true
    end
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
end
