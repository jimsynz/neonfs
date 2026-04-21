defmodule NeonFS.Core.ServiceRegistry do
  @moduledoc """
  Registry for cluster service instances.

  Tracks which nodes are running which services (core, fuse, s3, etc.).
  Backed entirely by Ra: reads go through `RaSupervisor.local_query/2`
  against the `MetadataStateMachine`; writes go through Raft consensus
  via `RaSupervisor.command/2`. There is no ETS cache — every query
  hits the locally-committed state, so registrations replicated by the
  leader are immediately visible on every follower.

  A single BEAM node may host multiple NeonFS services, so registry
  entries are keyed by `{node, type}` rather than node alone.

  The GenServer remains in the supervision tree because it still owns:
  self-registration on startup (`handle_continue(:register_self, …)`),
  re-registration on data-plane endpoint changes (`refresh_self/0`),
  node-down monitoring via `:net_kernel.monitor_nodes/2`, and
  best-effort deregistration of the local core service on shutdown.
  """

  use GenServer
  require Logger

  alias NeonFS.Client.{ServiceInfo, ServiceType}
  alias NeonFS.Cluster.State
  alias NeonFS.Core.{MetadataStateMachine, RaServer, RaSupervisor}
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
  Gets service info for a specific node. Returns the `:core` service
  if one is registered, otherwise the first (alphabetical by type) of
  whatever is registered on that node.
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
    case read_service(node, type) do
      {:ok, service_map} when is_map(service_map) -> {:ok, ServiceInfo.from_map(service_map)}
      {:ok, nil} -> {:error, :not_found}
      {:error, _} -> {:error, :not_found}
    end
  end

  @doc """
  Lists all registered services.
  """
  @spec list() :: [ServiceInfo.t()]
  def list do
    case read_services() do
      {:ok, services_map} ->
        services_map
        |> Map.values()
        |> Enum.map(&ServiceInfo.from_map/1)
        |> Enum.sort_by(&{&1.node, &1.type})

      {:error, _} ->
        []
    end
  end

  @doc """
  Lists services running on a given node.
  """
  @spec list_by_node(node()) :: [ServiceInfo.t()]
  def list_by_node(node) do
    list()
    |> Enum.filter(&(&1.node == node))
    |> Enum.sort_by(& &1.type)
  end

  @doc """
  Lists services of a given type.
  """
  @spec list_by_type(ServiceType.t()) :: [ServiceInfo.t()]
  def list_by_type(type) do
    Enum.filter(list(), &(&1.type == type))
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
  end

  def connected_nodes_by_type(type) do
    connected_nodes = Node.list()

    list_by_type(type)
    |> Enum.map(& &1.node)
    |> Kernel.++(connected_app_nodes(type, connected_nodes))
    |> Enum.uniq()
    |> Enum.filter(&(&1 in connected_nodes))
    |> Enum.sort()
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
    :net_kernel.monitor_nodes(true, node_type: :visible)
    {:ok, %{monitors: %{}}, {:continue, :register_self}}
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
    # State lives in Ra — no local restore needed on nodeup.
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  ## Private helpers

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

    case maybe_ra_command({:register_service, info_map}, 500) do
      {:ok, :ok} -> :ok
      {:error, :ra_not_available} -> :ok
      {:error, reason} -> Logger.warning("Ra register_service failed", reason: reason)
    end

    maybe_monitor_node(info.node, state)
  end

  defp maybe_monitor_node(node, state) do
    # Only set up a node monitor if the node is currently connected.
    # `:erlang.monitor_node/2` on an unreachable node fires `:nodedown`
    # immediately, which would synchronously deregister a service we
    # just registered — the entry would disappear from Ra between the
    # caller's `register/1` returning and the next read.
    cond do
      node == Node.self() ->
        state

      Map.has_key?(state.monitors, node) ->
        state

      node not in Node.list() ->
        state

      true ->
        ref = Node.monitor(node, true)
        put_in(state.monitors[node], ref)
    end
  end

  defp do_deregister(node, type, state) do
    command = if type, do: {:deregister_service, node, type}, else: {:deregister_service, node}

    case maybe_ra_command(command, 500) do
      {:ok, :ok} -> :ok
      {:error, :ra_not_available} -> :ok
      {:error, reason} -> Logger.warning("Ra deregister_service failed", reason: reason)
    end

    # Only drop the node-monitor when no other services remain registered
    # for this node — a fuse-only node stays monitored for future nodedowns.
    keep_monitor? = list_by_node(node) != []

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

  defp read_service(node, type) do
    RaSupervisor.local_query(&MetadataStateMachine.get_service(&1, node, type))
  catch
    :exit, _ -> {:error, :ra_not_available}
  end

  defp read_services do
    RaSupervisor.local_query(&MetadataStateMachine.get_services/1)
  catch
    :exit, _ -> {:error, :ra_not_available}
  end

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
end
