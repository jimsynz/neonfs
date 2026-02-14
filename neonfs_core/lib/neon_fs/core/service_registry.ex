defmodule NeonFS.Core.ServiceRegistry do
  @moduledoc """
  Registry for cluster service instances.

  Tracks which nodes are running which services (core, fuse, s3, etc.) using
  ETS for concurrent read access and Ra for cluster-wide persistence.

  Follows the same dual-path (ETS + Ra) pattern as VolumeRegistry.
  """

  use GenServer
  require Logger

  alias NeonFS.Client.{ServiceInfo, ServiceType}
  alias NeonFS.Core.{RaServer, RaSupervisor}

  ## Client API

  @doc """
  Starts the service registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Deregisters a node from the service registry.
  """
  @spec deregister(node()) :: :ok | {:error, term()}
  def deregister(node) do
    GenServer.call(__MODULE__, {:deregister, node}, 10_000)
  end

  @doc """
  Gets service info for a specific node.
  """
  @spec get(node()) :: {:ok, ServiceInfo.t()} | {:error, :not_found}
  def get(node) do
    case :ets.lookup(:services_by_node, node) do
      [{^node, info}] -> {:ok, info}
      [] -> {:error, :not_found}
    end
  end

  @doc """
  Lists all registered services.
  """
  @spec list() :: [ServiceInfo.t()]
  def list do
    :ets.tab2list(:services_by_node)
    |> Enum.map(fn {_node, info} -> info end)
    |> Enum.sort_by(& &1.node)
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

    :ets.new(:services_by_node, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    :ets.new(:services_by_type, [
      :named_table,
      :bag,
      :public,
      read_concurrency: true
    ])

    case restore_from_ra() do
      {:ok, count} ->
        Logger.info("ServiceRegistry started, restored #{count} services from Ra")

      {:error, reason} ->
        Logger.debug("ServiceRegistry started but Ra not ready yet: #{inspect(reason)}")
    end

    {:ok, %{monitors: %{}}, {:continue, :register_self}}
  end

  @impl true
  def handle_continue(:register_self, state) do
    info = ServiceInfo.new(Node.self(), :core)
    new_state = do_register(info, state)
    {:noreply, new_state}
  end

  @impl true
  def terminate(_reason, _state) do
    Logger.info("ServiceRegistry shutting down, deregistering self")
    remove_from_ets(Node.self())
    :ok
  end

  @impl true
  def handle_call({:register, info}, _from, state) do
    new_state = do_register(info, state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:deregister, node}, _from, state) do
    {:ok, new_state} = do_deregister(node, state)
    {:reply, :ok, new_state}
  end

  @impl true
  def handle_call({:update_metrics, node, metrics}, _from, state) do
    reply = do_update_metrics(node, metrics)
    {:reply, reply, state}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    Logger.warning("Service node down: #{node}, deregistering")
    {_, new_state} = do_deregister(node, state)
    {:noreply, new_state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    Logger.warning("Service node down: #{node}, deregistering")
    {_, new_state} = do_deregister(node, state)
    {:noreply, new_state}
  end

  ## Private helpers

  defp do_register(info, state) do
    info_map = ServiceInfo.to_map(info)

    # Service registration is best-effort in Ra — the service is always inserted
    # into ETS regardless. Use a short timeout to avoid blocking during cluster
    # transitions (e.g. when a new member has been added but hasn't started yet,
    # quorum is temporarily unreachable).
    case maybe_ra_command({:register_service, info_map}, 500) do
      {:ok, :ok} -> :ok
      {:error, :ra_not_available} -> :ok
      {:error, reason} -> Logger.warning("Ra register_service failed: #{inspect(reason)}")
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

  defp do_deregister(node, state) do
    # Best-effort Ra replication — use short timeout like do_register.
    case maybe_ra_command({:deregister_service, node}, 500) do
      {:ok, :ok} -> :ok
      {:error, :ra_not_available} -> :ok
      {:error, reason} -> Logger.warning("Ra deregister_service failed: #{inspect(reason)}")
    end

    remove_from_ets(node)

    # Demonitor if we were monitoring
    state =
      case Map.pop(state.monitors, node) do
        {nil, state} ->
          state

        {ref, new_monitors} when is_reference(ref) ->
          Process.demonitor(ref)
          %{state | monitors: new_monitors}

        {_, new_monitors} ->
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
    :ets.insert(:services_by_node, {info.node, info})
    :ets.insert(:services_by_type, {info.type, info})
  end

  defp remove_from_ets(node) do
    case :ets.lookup(:services_by_node, node) do
      [{^node, info}] ->
        :ets.delete(:services_by_node, node)
        :ets.match_delete(:services_by_type, {info.type, info})

      [] ->
        :ok
    end
  rescue
    ArgumentError -> :ok
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
      Logger.debug("Ra command error: #{inspect({kind, reason})}")

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
          Enum.reduce(services, 0, fn {_node, service_map}, acc ->
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
