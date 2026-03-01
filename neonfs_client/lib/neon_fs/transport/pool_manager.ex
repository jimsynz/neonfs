defmodule NeonFS.Transport.PoolManager do
  @moduledoc """
  Manages per-peer `ConnPool` instances for outbound data transfer connections.

  When a new peer's data transfer endpoint is discovered (via
  `NeonFS.Client.Discovery`), PoolManager creates a new ConnPool for that peer.
  When a peer departs, the pool is shut down.

  Pool references are stored in an ETS table (`:neonfs_transport_pools`) for
  fast concurrent lookups from `Router.data_call/4` without serialising through
  the GenServer process.

  ## Options

    * `:pool_size` — connections per peer (default: 4)
    * `:worker_idle_timeout` — idle health-check interval in ms (default: 30_000)
    * `:discovery_refresh_interval` — how often to sync with Discovery in ms (default: 30_000)
    * `:pool_supervisor` — DynamicSupervisor for ConnPool instances
      (default: `NeonFS.Transport.PoolSupervisor`)
    * `:ssl_opts` — TLS options for outbound connections (default: derived from TLS.tls_dir/0)
    * `:service_list_fn` — zero-arity function returning a list of `ServiceInfo` structs.
      Used as the service source for peer discovery. Defaults to querying
      `NeonFS.Client.Discovery` (suitable for non-core nodes). Core nodes should
      pass a function that calls `NeonFS.Core.ServiceRegistry.list/0` directly.
    * `:name` — registered name (default: `NeonFS.Transport.PoolManager`)

  """

  use GenServer

  require Logger

  alias NeonFS.Client.Discovery
  alias NeonFS.Transport.{PoolSupervisor, TLS}

  @ets_table :neonfs_transport_pools
  @default_pool_size 4
  @default_worker_idle_timeout 30_000
  @default_discovery_refresh_interval 30_000

  @type endpoint :: {:ssl.host(), :inet.port_number()}

  # Client API

  @doc """
  Starts the PoolManager linked to the current process.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Looks up the ConnPool for a peer node.

  Reads directly from ETS for fast concurrent access.
  """
  @spec get_pool(node()) :: {:ok, pid()} | {:error, :no_pool}
  def get_pool(node) do
    case :ets.lookup(@ets_table, node) do
      [{^node, pid, _endpoint}] when is_pid(pid) ->
        if Process.alive?(pid), do: {:ok, pid}, else: {:error, :no_pool}

      _ ->
        {:error, :no_pool}
    end
  rescue
    ArgumentError -> {:error, :no_pool}
  end

  @doc """
  Ensures a ConnPool exists for the given peer. Creates one if it doesn't
  already exist.
  """
  @spec ensure_pool(node(), endpoint()) :: {:ok, pid()} | {:error, term()}
  def ensure_pool(node, endpoint) do
    GenServer.call(__MODULE__, {:ensure_pool, node, endpoint})
  end

  @doc """
  Removes and stops the ConnPool for the given peer.
  """
  @spec remove_pool(node()) :: :ok
  def remove_pool(node) do
    GenServer.call(__MODULE__, {:remove_pool, node})
  end

  @doc """
  Returns a `{host, port}` tuple for advertising this node's data transfer endpoint.

  If `:neonfs_client, :data_transfer, :advertise` is configured, uses that address.
  Otherwise auto-detects the first non-loopback IPv4 address.
  """
  @spec advertise_endpoint(:inet.port_number()) :: endpoint()
  def advertise_endpoint(port) do
    host = configured_advertise_address() || auto_detect_address()
    {host, port}
  end

  # GenServer callbacks

  @impl GenServer
  def init(opts) do
    pool_size = Keyword.get(opts, :pool_size, configured_pool_size())
    worker_idle_timeout = Keyword.get(opts, :worker_idle_timeout, @default_worker_idle_timeout)

    refresh_interval =
      Keyword.get(opts, :discovery_refresh_interval, configured_refresh_interval())

    pool_supervisor = Keyword.get(opts, :pool_supervisor, PoolSupervisor)
    ssl_opts = Keyword.get(opts, :ssl_opts)
    service_list_fn = Keyword.get(opts, :service_list_fn)

    @ets_table =
      :ets.new(@ets_table, [
        :named_table,
        :set,
        :public,
        read_concurrency: true
      ])

    :net_kernel.monitor_nodes(true, [:nodedown_reason])

    state = %{
      pool_size: pool_size,
      worker_idle_timeout: worker_idle_timeout,
      refresh_interval: refresh_interval,
      pool_supervisor: pool_supervisor,
      ssl_opts: ssl_opts,
      service_list_fn: service_list_fn,
      monitors: %{}
    }

    schedule_refresh(refresh_interval)

    {:ok, state}
  end

  @impl GenServer
  def handle_call({:ensure_pool, node, endpoint}, _from, state) do
    case :ets.lookup(@ets_table, node) do
      [{^node, pid, _endpoint}] when is_pid(pid) ->
        if Process.alive?(pid) do
          {:reply, {:ok, pid}, state}
        else
          {result, state} = create_pool(node, endpoint, state)
          {:reply, result, state}
        end

      _ ->
        {result, state} = create_pool(node, endpoint, state)
        {:reply, result, state}
    end
  end

  def handle_call({:remove_pool, node}, _from, state) do
    state = do_remove_pool(node, state)
    {:reply, :ok, state}
  end

  @impl GenServer
  def handle_info(:discovery_refresh, state) do
    do_discovery_refresh(state)
    schedule_refresh(state.refresh_interval)
    {:noreply, state}
  end

  def handle_info({:nodedown, node, _info}, state) do
    Logger.debug("PoolManager node down, removing pool", node: node)
    state = do_remove_pool(node, state)
    {:noreply, state}
  end

  def handle_info({:nodeup, _node, _info}, state) do
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, _reason}, state) do
    case Enum.find(state.monitors, fn {_node, mref} -> mref == ref end) do
      {node, _ref} ->
        Logger.debug("PoolManager pool crashed, cleaning up", node: node)
        :ets.delete(@ets_table, node)
        {:noreply, %{state | monitors: Map.delete(state.monitors, node)}}

      nil ->
        {:noreply, state}
    end
  end

  # Private functions

  defp create_pool(node, {host, port} = endpoint, state) do
    # Don't attempt pool creation if TLS certificates aren't available yet.
    # This prevents crash storms during cluster formation when data plane
    # activation happens before all nodes have received their certificates.
    if state.ssl_opts || tls_ready?() do
      do_create_pool(node, {host, port}, endpoint, state)
    else
      Logger.debug("PoolManager deferring pool creation: TLS not ready", node: node)
      {{:error, :tls_not_ready}, state}
    end
  end

  defp tls_ready? do
    ca_path = Path.join(TLS.tls_dir(), "ca.crt")
    File.exists?(ca_path)
  end

  defp do_create_pool(node, {host, port}, endpoint, state) do
    pool_opts = [
      peer: {host, port},
      pool_size: state.pool_size,
      worker_idle_timeout: state.worker_idle_timeout
    ]

    pool_opts =
      if state.ssl_opts do
        Keyword.put(pool_opts, :ssl_opts, state.ssl_opts)
      else
        pool_opts
      end

    case PoolSupervisor.start_pool(state.pool_supervisor, pool_opts) do
      {:ok, pid} ->
        mref = Process.monitor(pid)
        :ets.insert(@ets_table, {node, pid, endpoint})

        # Demonitor any previous monitor for this node
        state =
          case Map.get(state.monitors, node) do
            nil ->
              state

            old_mref ->
              Process.demonitor(old_mref, [:flush])
              state
          end

        {{:ok, pid}, %{state | monitors: Map.put(state.monitors, node, mref)}}

      {:error, reason} = error ->
        Logger.warning("PoolManager failed to create pool",
          node: node,
          reason: inspect(reason)
        )

        {error, state}
    end
  end

  defp do_remove_pool(node, state) do
    case :ets.lookup(@ets_table, node) do
      [{^node, pid, _endpoint}] ->
        :ets.delete(@ets_table, node)

        case Map.pop(state.monitors, node) do
          {nil, monitors} ->
            stop_pool(pid, state.pool_supervisor)
            %{state | monitors: monitors}

          {mref, monitors} ->
            Process.demonitor(mref, [:flush])
            stop_pool(pid, state.pool_supervisor)
            %{state | monitors: monitors}
        end

      _ ->
        state
    end
  rescue
    ArgumentError -> state
  end

  defp stop_pool(pid, supervisor) do
    if Process.alive?(pid) do
      DynamicSupervisor.terminate_child(supervisor, pid)
    end
  catch
    _, _ -> :ok
  end

  defp do_discovery_refresh(state) do
    peers = discover_peer_endpoints(state.service_list_fn)
    peer_nodes = MapSet.new(peers, fn {node, _endpoint} -> node end)

    # Create pools for newly discovered peers
    peers
    |> Enum.reject(&pool_alive?/1)
    |> Enum.each(fn {node, endpoint} -> create_pool(node, endpoint, state) end)

    # Remove pools for departed peers
    existing_nodes =
      :ets.tab2list(@ets_table)
      |> Enum.map(fn {node, _pid, _endpoint} -> node end)
      |> MapSet.new()

    departed = MapSet.difference(existing_nodes, peer_nodes)

    for node <- departed do
      Logger.debug("PoolManager peer departed, removing pool", node: node)
      do_remove_pool(node, state)
    end

    :telemetry.execute(
      [:neonfs, :transport, :pool_manager, :discovery_refresh],
      %{peers_discovered: length(peers)},
      %{}
    )

    :ok
  rescue
    ArgumentError -> :ok
  end

  defp pool_alive?({node, _endpoint}) do
    case :ets.lookup(@ets_table, node) do
      [{^node, pid, _}] when is_pid(pid) -> Process.alive?(pid)
      _ -> false
    end
  rescue
    ArgumentError -> false
  end

  defp discover_peer_endpoints(service_list_fn) do
    services = list_services(service_list_fn)

    for service <- services,
        service.node != Node.self(),
        endpoint = Map.get(service.metadata || %{}, :data_endpoint),
        endpoint != nil do
      {service.node, endpoint}
    end
    |> Enum.uniq_by(fn {node, _} -> node end)
  rescue
    _ -> []
  end

  defp list_services(nil) do
    for type <- [:core, :fuse, :s3, :docker, :cifs],
        service <- Discovery.list_by_type(type) do
      service
    end
  rescue
    _ -> []
  end

  defp list_services(fun) when is_function(fun, 0) do
    fun.()
  rescue
    _ -> []
  end

  defp schedule_refresh(interval) do
    Process.send_after(self(), :discovery_refresh, interval)
  end

  defp configured_pool_size do
    Application.get_env(:neonfs_client, :data_transfer, [])
    |> Keyword.get(:pool_size, @default_pool_size)
  end

  defp configured_refresh_interval do
    Application.get_env(:neonfs_client, :data_transfer, [])
    |> Keyword.get(:discovery_refresh_interval, @default_discovery_refresh_interval)
  end

  defp configured_advertise_address do
    Application.get_env(:neonfs_client, :data_transfer, [])
    |> Keyword.get(:advertise)
  end

  defp auto_detect_address do
    case :inet.getifaddrs() do
      {:ok, ifaddrs} ->
        ifaddrs
        |> Enum.flat_map(fn {_iface, opts} ->
          opts
          |> Keyword.get_values(:addr)
          |> Enum.filter(&ipv4?/1)
          |> Enum.reject(&loopback?/1)
        end)
        |> List.first(~c"127.0.0.1")

      {:error, _} ->
        ~c"127.0.0.1"
    end
  end

  defp ipv4?({_, _, _, _}), do: true
  defp ipv4?(_), do: false

  defp loopback?({127, _, _, _}), do: true
  defp loopback?(_), do: false
end
