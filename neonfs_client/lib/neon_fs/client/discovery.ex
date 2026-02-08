defmodule NeonFS.Client.Discovery do
  @moduledoc """
  Discovers and caches service information from the cluster's ServiceRegistry.

  On startup, queries `NeonFS.Core.ServiceRegistry` on a connected core node
  via RPC. Caches results in a local ETS table and periodically refreshes.

  Subscribes to `:nodedown`/`:nodeup` for cache invalidation.
  """

  use GenServer
  require Logger

  alias NeonFS.Client.{Connection, ServiceInfo, ServiceType}

  @default_refresh_ms 5_000
  @ets_table :neonfs_client_services

  ## Client API

  @doc """
  Starts the discovery GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns all known core nodes.
  """
  @spec get_core_nodes() :: [node()]
  def get_core_nodes do
    list_by_type(:core)
    |> Enum.map(& &1.node)
  end

  @doc """
  Returns services of the given type from cache.
  """
  @spec list_by_type(NeonFS.Client.ServiceType.t()) :: [ServiceInfo.t()]
  def list_by_type(type) do
    case :ets.lookup(@ets_table, {:by_type, type}) do
      [{_, services}] -> services
      [] -> []
    end
  rescue
    ArgumentError -> []
  end

  @doc """
  Forces an immediate cache refresh.
  """
  @spec refresh() :: :ok
  def refresh do
    GenServer.cast(__MODULE__, :refresh)
  end

  ## Server callbacks

  @impl true
  def init(opts) do
    refresh_ms = Keyword.get(opts, :refresh_ms, @default_refresh_ms)

    @ets_table =
      :ets.new(@ets_table, [
        :named_table,
        :set,
        :public,
        read_concurrency: true
      ])

    # Monitor node events
    :net_kernel.monitor_nodes(true, [:nodedown_reason])

    state = %{refresh_ms: refresh_ms}

    {:ok, state, {:continue, :initial_refresh}}
  end

  @impl true
  def handle_continue(:initial_refresh, state) do
    do_refresh()
    schedule_refresh(state.refresh_ms)
    {:noreply, state}
  end

  @impl true
  def handle_cast(:refresh, state) do
    do_refresh()
    {:noreply, state}
  end

  @impl true
  def handle_info(:scheduled_refresh, state) do
    do_refresh()
    schedule_refresh(state.refresh_ms)
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodeup, _node, _info}, state) do
    do_refresh()
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    Logger.debug("Node down, invalidating cache for: #{node}")
    invalidate_node(node)
    {:noreply, state}
  end

  ## Private helpers

  defp do_refresh do
    case Connection.connected_core_node() do
      {:ok, core_node} ->
        fetch_and_cache_services(core_node)

      {:error, :no_connection} ->
        Logger.debug("No core node connection, skipping service discovery refresh")
    end
  end

  defp fetch_and_cache_services(core_node) do
    case :rpc.call(core_node, NeonFS.Core.ServiceRegistry, :list, []) do
      services when is_list(services) ->
        # Group by type and cache
        by_type =
          services
          |> Enum.map(&ServiceInfo.from_map/1)
          |> Enum.group_by(& &1.type)

        for {type, type_services} <- by_type do
          :ets.insert(@ets_table, {{:by_type, type}, type_services})
        end

        # Cache individual nodes
        for service <- services do
          info = ServiceInfo.from_map(service)
          :ets.insert(@ets_table, {{:by_node, info.node}, info})
        end

        :telemetry.execute(
          [:neonfs, :client, :discovery, :refresh],
          %{service_count: length(services)},
          %{core_node: core_node}
        )

      {:badrpc, reason} ->
        Logger.debug("Failed to fetch services from #{core_node}: #{inspect(reason)}")
    end
  end

  defp invalidate_node(node) do
    :ets.delete(@ets_table, {:by_node, node})
    # Rebuild type indexes by removing the downed node
    rebuild_type_indexes(node)
  rescue
    ArgumentError -> :ok
  end

  defp rebuild_type_indexes(removed_node) do
    for type <- ServiceType.all() do
      case :ets.lookup(@ets_table, {:by_type, type}) do
        [{_, services}] ->
          filtered = Enum.reject(services, &(&1.node == removed_node))
          :ets.insert(@ets_table, {{:by_type, type}, filtered})

        [] ->
          :ok
      end
    end
  rescue
    ArgumentError -> :ok
  end

  defp schedule_refresh(ms) do
    Process.send_after(self(), :scheduled_refresh, ms)
  end
end
