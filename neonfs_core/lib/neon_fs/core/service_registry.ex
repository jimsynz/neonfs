defmodule NeonFS.Core.ServiceRegistry do
  @moduledoc """
  Registry for cluster service instances.

  Tracks which nodes are running which services (core, fuse, s3, etc.) using
  ETS for concurrent read access and Ra for cluster-wide persistence.

  A single BEAM node may host multiple NeonFS services, so registry entries are
  keyed by `{node, type}` rather than node alone.

  Follows the same dual-path (ETS + Ra) pattern as VolumeRegistry.
  """

  use GenServer
  require Logger

  alias NeonFS.Client.{ServiceInfo, ServiceType}
  alias NeonFS.Cluster.State
  alias NeonFS.Core.{RaServer, RaSupervisor}
  alias NeonFS.Transport.{Listener, PoolManager}

  @core_probe_timeout_ms 1_000

  ## Client API

  @doc """
  Starts the service registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Deregisters all services for a node from the service registry.
  """
  @spec deregister(node()) :: :ok | {:error, term()}
  def deregister(node) do
    GenServer.call(__MODULE__, {:deregister, node, nil}, 10_000)
  end

  @doc """
  Deregisters a specific service for a node from the service registry.
  """
  @spec deregister(node(), ServiceType.t()) :: :ok | {:error, term()}
  def deregister(node, type) do
    GenServer.call(__MODULE__, {:deregister, node, type}, 10_000)
  end

  @doc """
  Re-registers this node's service info with updated metadata.

  Call this after TLS certificates are written and `Listener.rebind/0`
  succeeds, so the new data transfer endpoint is advertised.
  """
  @spec refresh_self() :: :ok
  def refresh_self do
    GenServer.cast(__MODULE__, :refresh_self)
  end

  @doc """
  Gets service info for a specific node.
  """
  @spec get(node()) :: {:ok, ServiceInfo.t()} | {:error, :not_found}
  def get(node) do
    case list_by_node(node) do
      [] -> {:error, :not_found}
      [info] -> {:ok, info}
      services -> {:ok, Enum.find(services, &(&1.type == :core)) || hd(services)}
    end
  end

  @doc """
  Gets service info for a specific node and type.
  """
  @spec get(node(), ServiceType.t()) :: {:ok, ServiceInfo.t()} | {:error, :not_found}
  def get(node, type) do
    key = service_key(node, type)

    case :ets.lookup(:services_by_key, key) do
      [{^key, info}] -> {:ok, info}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Lists all registered services.
  """
  @spec list() :: [ServiceInfo.t()]
  def list do
    :ets.tab2list(:services_by_key)
    |> Enum.map(fn {_key, info} -> info end)
    |> Enum.sort_by(&{&1.node, &1.type})
  end

  @doc """
  Lists services running on a given node.
  """
  @spec list_by_node(node()) :: [ServiceInfo.t()]
  def list_by_node(node) do
    :ets.lookup(:services_by_node, node)
    |> Enum.map(fn {_node, info} -> info end)
    |> Enum.sort_by(& &1.type)
  end

  @doc """
  Lists services of a given type.
  """
  @spec list_by_type(ServiceType.t()) :: [ServiceInfo.t()]
  def list_by_type(type) do
    :ets.lookup(:services_by_type, type)
    |> Enum.map(fn {_type, info} -> info end)
  end

  @doc """
  Lists currently connected remote nodes for a given service type.
  """
  @spec connected_nodes_by_type(ServiceType.t()) :: [node()]
  def connected_nodes_by_type(:core) do
    connected_nodes = Node.list()

    list_by_type(:core)
    |> Enum.map(& &1.node)
    |> Kernel.++(connected_core_fallback_nodes(connected_nodes))
    |> Enum.uniq()
    |> Enum.filter(&(&1 in connected_nodes))
    |> Enum.sort()
  rescue
    ArgumentError -> connected_core_fallback_nodes(Node.list())
  end

  def connected_nodes_by_type(type) do
    connected_nodes = Node.list()

    list_by_type(type)
    |> Enum.map(& &1.node)
    |> Kernel.++(connected_app_nodes(type, connected_nodes))
    |> Enum.uniq()
    |> Enum.filter(&(&1 in connected_nodes))
    |> Enum.sort()
  rescue
    ArgumentError -> connected_app_nodes(type, Node.list())
  end

  @doc """
  Registers a service in the cluster.
  """
  @spec register(ServiceInfo.t()) :: :ok | {:error, term()}
  def register(%ServiceInfo{} = info) do
    GenServer.call(__MODULE__, {:register, info}, 10_000)
  end

  @doc """
  Selects a core node from registered services.
  """
  @spec select_core_node() :: {:ok, node()} | {:error, :no_core_nodes}
  def select_core_node do
    case list_by_type(:core) do
      [first | _] -> {:ok, first.node}
      [] -> {:error, :no_core_nodes}
    end
  end

  @doc """
  Updates metrics for a node.
  """
  @spec update_metrics(node(), map()) :: :ok | {:error, term()}
  def update_metrics(node, metrics) do
    GenServer.call(__MODULE__, {:update_metrics, node, metrics}, 10_000)
  end

  ## Server callbacks

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)

    :ets.new(:services_by_key, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    :ets.new(:services_by_node, [
      :named_table,
      :bag,
      :public,
      read_concurrency: true
    ])

    :ets.new(:services_by_type, [
      :named_table,
      :bag,
      :public,
      read_concurrency: true
    ])

    :net_kernel.monitor_nodes(true, node_type: :visible)

    restored =
      case restore_from_ra() do
        {:ok, count} ->
          Logger.info("ServiceRegistry started, restored services from Ra", count: count)
          true

        {:error, reason} ->
          Logger.debug("ServiceRegistry started but Ra not ready yet, will retry",
            reason: reason
          )

          schedule_restore_retry(1_000)
          false
      end

    {:ok, %{monitors: %{}, restored: restored, restore_backoff: 1_000},
     {:continue, :register_self}}
  end

  @impl true
  def handle_continue(:register_self, state) do
    metadata = build_self_metadata()
    info = ServiceInfo.new(Node.self(), :core, metadata: metadata)
    new_state = do_register(info, state)
    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, _state) do
    Logger.info("ServiceRegistry shutting down, deregistering core service")

    case maybe_ra_command({:deregister_service, Node.self(), :core}, 500) do
      {:ok, :ok} ->
        :ok

      {:error, :ra_not_available} ->
        :ok

      {:error, reason} ->
        Logger.debug("Failed to deregister core service", reason: inspect(reason))
    end

    remove_from_ets(Node.self(), :core)
    :ok
  end

  @impl true
  def handle_cast(:refresh_self, state) do
    metadata = build_self_metadata()
    info = ServiceInfo.new(Node.self(), :core, metadata: metadata)
    new_state = do_register(info, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_call({:register, info}, _from, state) do
    new_state = do_register(info, state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:deregister, node, type}, _from, state) do
    {:ok, new_state} = do_deregister(node, type, state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:update_metrics, node, metrics}, _from, state) do
    reply = do_update_metrics(node, metrics)
    {:reply, reply, state}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    Logger.warning("Service node down, deregistering", node: node)
    {_, new_state} = do_deregister(node, nil, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    Logger.warning("Service node down, deregistering", node: node)
    {_, new_state} = do_deregister(node, nil, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:nodeup, _node, _info}, state) do
    case restore_from_ra() do
      {:ok, _count} -> {:noreply, %{state | restored: true}}
      {:error, _} -> {:noreply, state}
    end
  end

  @impl true
  def handle_info(:retry_restore_from_ra, %{restored: true} = state) do
    {:noreply, state}
  end

  def handle_info(:retry_restore_from_ra, state) do
    case restore_from_ra() do
      {:ok, count} ->
        Logger.info("ServiceRegistry restored services from Ra on retry", count: count)
        {:noreply, %{state | restored: true}}

      {:error, _reason} ->
        next_backoff = min(state.restore_backoff * 2, 30_000)
        schedule_restore_retry(next_backoff)
        {:noreply, %{state | restore_backoff: next_backoff}}
    end
  end

  ## Private helpers

  defp schedule_restore_retry(delay_ms) do
    Process.send_after(self(), :retry_restore_from_ra, delay_ms)
  end

  defp build_self_metadata do
    case Process.whereis(Listener) do
      nil ->
        %{}

      _pid ->
        port = Listener.get_port()

        if port > 0 do
          endpoint = PoolManager.advertise_endpoint(port)
          %{data_endpoint: endpoint}
        else
          %{}
        end
    end
  rescue
    _ -> %{}
  end

  defp connected_app_nodes(type, connected_nodes) do
    Enum.filter(connected_nodes, &service_app_started?(&1, type))
    |> Enum.sort()
  end

  defp connected_core_fallback_nodes(connected_nodes) do
    Enum.filter(connected_nodes, &core_node?/1)
  end

  defp core_node?(node) do
    service_app_started?(node, :core) and cluster_state_exists?(node)
  end

  defp cluster_state_exists?(node) do
    :erpc.call(node, State, :exists?, [], @core_probe_timeout_ms)
  catch
    _, _ -> false
  end

  defp service_app_started?(node, type) do
    app = service_application(type)

    node
    |> :erpc.call(Application, :started_applications, [], @core_probe_timeout_ms)
    |> Enum.any?(fn {started_app, _desc, _vsn} -> started_app == app end)
  catch
    _, _ -> false
  end

  defp service_application(type), do: String.to_atom("neonfs_#{type}")

  defp do_register(info, state) do
    info_map = ServiceInfo.to_map(info)

    # Service registration is best-effort in Ra — the service is always inserted
    # into ETS regardless. Use a short timeout to avoid blocking during cluster
    # transitions (e.g. when a new member has been added but hasn't started yet,
    # quorum is temporarily unreachable).
    case maybe_ra_command({:register_service, info_map}, 500) do
      {:ok, :ok} -> :ok
      {:error, :ra_not_available} -> :ok
      {:error, reason} -> Logger.warning("Ra register_service failed", reason: reason)
    end

    insert_service(info)

    # Monitor the node if it's remote
    state =
      if info.node != Node.self() and not Map.has_key?(state.monitors, info.node) do
        ref = Node.monitor(info.node, true)
        put_in(state.monitors[info.node], ref)
      else
        state
      end

    state
  end

  defp do_deregister(node, type, state) do
    # Best-effort Ra replication — use short timeout like do_register.
    command = if type, do: {:deregister_service, node, type}, else: {:deregister_service, node}

    case maybe_ra_command(command, 500) do
      {:ok, :ok} -> :ok
      {:error, :ra_not_available} -> :ok
      {:error, reason} -> Logger.warning("Ra deregister_service failed", reason: reason)
    end

    remove_from_ets(node, type)

    keep_monitor? = list_by_node(node) != []

    # Demonitor if we were monitoring
    state =
      case {keep_monitor?, Map.pop(state.monitors, node)} do
        {true, {_ref, _new_monitors}} ->
          state

        {_, {nil, _}} ->
          state

        {false, {ref, new_monitors}} when is_reference(ref) ->
          Process.demonitor(ref)
          %{state | monitors: new_monitors}

        {false, {_, new_monitors}} ->
          %{state | monitors: new_monitors}
      end

    {:ok, state}
  end

  defp do_update_metrics(node, metrics) do
    case maybe_ra_command({:update_service_metrics, node, metrics}) do
      {:ok, :ok} -> :ok
      {:error, :ra_not_available} -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  defp insert_service(%ServiceInfo{} = info) do
    key = service_key(info)

    case :ets.lookup(:services_by_key, key) do
      [{^key, existing}] -> remove_service_from_ets(existing)
      [] -> :ok
    end

    :ets.insert(:services_by_key, {key, info})
    :ets.insert(:services_by_node, {info.node, info})
    :ets.insert(:services_by_type, {info.type, info})
  end

  defp remove_from_ets(node, nil) do
    list_by_node(node)
    |> Enum.each(fn info -> remove_service_from_ets(info) end)
  rescue
    ArgumentError -> :ok
  end

  defp remove_from_ets(node, type) do
    case get(node, type) do
      {:ok, info} -> remove_service_from_ets(info)
      {:error, :not_found} -> :ok
    end
  rescue
    ArgumentError -> :ok
  end

  defp remove_service_from_ets(%ServiceInfo{} = info) do
    :ets.delete(:services_by_key, service_key(info))
    :ets.match_delete(:services_by_node, {info.node, info})
    :ets.match_delete(:services_by_type, {info.type, info})
  end

  defp service_key(%ServiceInfo{node: node, type: type}), do: {node, type}
  defp service_key(node, type), do: {node, type}

  defp maybe_ra_command(cmd, timeout \\ 5000) do
    if RaServer.initialized?() do
      maybe_ra_command_impl(cmd, timeout)
    else
      {:error, :ra_not_available}
    end
  end

  defp maybe_ra_command_impl(cmd, timeout) do
    case RaSupervisor.command(cmd, timeout) do
      {:ok, result, _leader} ->
        {:ok, result}

      {:error, :noproc} ->
        if RaServer.initialized?() do
          {:error, :ra_unavailable}
        else
          {:error, :ra_not_available}
        end

      {:error, reason} ->
        {:error, reason}

      {:timeout, _node} ->
        {:error, :timeout}
    end
  catch
    :exit, {:noproc, _} ->
      if RaServer.initialized?() do
        {:error, :ra_unavailable}
      else
        {:error, :ra_not_available}
      end

    kind, reason ->
      Logger.debug("Ra command error", kind: kind, reason: reason)

      if RaServer.initialized?() do
        {:error, {:ra_error, {kind, reason}}}
      else
        {:error, :ra_not_available}
      end
  end

  defp restore_from_ra do
    case RaSupervisor.query(fn state -> Map.get(state, :services, %{}) end) do
      {:ok, services} when is_map(services) ->
        count =
          Enum.reduce(services, 0, fn {_service_key, service_map}, acc ->
            info = ServiceInfo.from_map(service_map)
            insert_service(info)
            acc + 1
          end)

        {:ok, count}

      {:error, reason} ->
        {:error, reason}
    end
  end
end
