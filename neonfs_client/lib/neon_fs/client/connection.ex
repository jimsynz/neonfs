defmodule NeonFS.Client.Connection do
  @moduledoc """
  Manages Erlang distribution connections to bootstrap nodes.

  On init, connects to configured bootstrap nodes via `Node.connect/1`.
  Monitors connections and reconciles toward the desired cluster mesh.

  ## Configuration

  Bootstrap nodes are configured via the `:bootstrap_nodes` option:

      {NeonFS.Client.Connection, bootstrap_nodes: [:neonfs_core@host1]}

  Or via application env:

      config :neonfs_client, bootstrap_nodes: [:neonfs_core@host1]
  """

  use GenServer
  require Logger

  alias NeonFS.Client.ServiceInfo

  @default_reconnect_interval_ms 5_000
  @default_peer_connect_timeout_ms 10_000

  @type state :: %{
          bootstrap_nodes: [node()],
          connected_nodes: MapSet.t(node()),
          connecting: MapSet.t(node()),
          core_nodes: MapSet.t(node()),
          desired_nodes: MapSet.t(node()),
          monitors: %{optional(reference()) => node()},
          peer_connect_timeout: pos_integer(),
          reconnect_interval: pos_integer()
        }

  ## Client API

  @doc """
  Starts the connection manager.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Returns a connected core node, or `{:error, :no_connection}`.
  """
  @spec connected_core_node() :: {:ok, node()} | {:error, :no_connection}
  def connected_core_node do
    GenServer.call(__MODULE__, :connected_core_node)
  end

  @doc """
  Returns the configured bootstrap nodes.
  """
  @spec bootstrap_nodes() :: [node()]
  def bootstrap_nodes do
    GenServer.call(__MODULE__, :bootstrap_nodes)
  end

  @doc """
  Syncs the desired cluster nodes from discovered services.
  """
  @spec sync_services([ServiceInfo.t()]) :: :ok
  def sync_services(services) do
    GenServer.cast(__MODULE__, {:sync_services, services})
  end

  ## Server callbacks

  @impl true
  def init(opts) do
    bootstrap =
      Keyword.get_lazy(opts, :bootstrap_nodes, fn ->
        Application.get_env(:neonfs_client, :bootstrap_nodes, [])
      end)

    peer_connect_timeout =
      Keyword.get_lazy(opts, :peer_connect_timeout, fn ->
        Application.get_env(
          :neonfs_client,
          :peer_connect_timeout,
          @default_peer_connect_timeout_ms
        )
      end)

    reconnect_interval =
      Keyword.get(opts, :reconnect_interval, @default_reconnect_interval_ms)

    bootstrap_set = MapSet.new(bootstrap)

    state = %{
      bootstrap_nodes: bootstrap,
      connected_nodes: MapSet.new(),
      connecting: MapSet.new(),
      core_nodes: bootstrap_set,
      desired_nodes: bootstrap_set,
      monitors: %{},
      peer_connect_timeout: peer_connect_timeout,
      reconnect_interval: reconnect_interval
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    state = reconcile_connections(state)
    schedule_reconcile(state.reconnect_interval)
    {:noreply, state}
  end

  @impl true
  def handle_call(:connected_core_node, _from, state) do
    case connected_core_nodes(state) do
      [node | _] -> {:reply, {:ok, node}, state}
      [] -> {:reply, {:error, :no_connection}, state}
    end
  end

  @impl true
  def handle_call(:bootstrap_nodes, _from, state) do
    {:reply, state.bootstrap_nodes, state}
  end

  @impl true
  def handle_cast({:sync_services, services}, state) do
    state =
      state
      |> sync_discovered_services(services)
      |> reconcile_connections()

    {:noreply, state}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    {:noreply, handle_nodedown(node, state)}
  end

  @impl true
  def handle_info({:nodedown, node, _info}, state) do
    {:noreply, handle_nodedown(node, state)}
  end

  @impl true
  def handle_info(:reconcile, state) do
    state = reconcile_connections(state)
    schedule_reconcile(state.reconnect_interval)
    {:noreply, state}
  end

  @impl true
  def handle_info({:nodeup, _node}, state) do
    {:noreply, reconcile_connections(state)}
  end

  @impl true
  def handle_info({:nodeup, _node, _info}, state) do
    {:noreply, reconcile_connections(state)}
  end

  # Result of an async `Node.connect/1` started by `reconcile_connections/1`.
  # The monitor is set up here (in the GenServer process) rather than in the
  # connect worker — a `Node.monitor/2` is bound to the calling process, so it
  # must live with the GenServer, not the short-lived worker.
  @impl true
  def handle_info({:connect_result, node, true}, state) do
    state = %{state | connecting: MapSet.delete(state.connecting, node)}

    if MapSet.member?(state.connected_nodes, node) do
      {:noreply, state}
    else
      ref = Node.monitor(node, true)

      :telemetry.execute(
        [:neonfs, :client, :connection, :connected],
        %{},
        %{node: node}
      )

      {:noreply,
       %{
         state
         | connected_nodes: MapSet.put(state.connected_nodes, node),
           monitors: Map.put(state.monitors, ref, node)
       }}
    end
  end

  def handle_info({:connect_result, node, result}, state)
      when result in [false, :ignored] do
    Logger.debug("Failed to connect to cluster node (#{inspect(result)})", node: node)
    {:noreply, %{state | connecting: MapSet.delete(state.connecting, node)}}
  end

  ## Private helpers

  defp handle_nodedown(node, state) do
    Logger.warning("Lost connection to cluster node", node: node)

    new_connected = MapSet.delete(state.connected_nodes, node)

    # Remove monitor entry
    new_monitors =
      state.monitors
      |> Enum.reject(fn {_ref, n} -> n == node end)
      |> Map.new()

    %{
      state
      | connected_nodes: new_connected,
        connecting: MapSet.delete(state.connecting, node),
        monitors: new_monitors
    }
  end

  defp connected_core_nodes(state) do
    remote =
      state.connected_nodes
      |> MapSet.intersection(state.core_nodes)
      |> MapSet.to_list()
      |> Enum.sort()

    # Omnibus (and any co-located deployment) runs the core application in
    # the same BEAM node as the client. The local core is a valid RPC
    # target — `:rpc.call/4` works in-node — but it is deliberately kept
    # out of the Node.connect/monitor machinery (you don't connect to
    # yourself), so it never lands in `connected_nodes`. Without surfacing
    # it a single-node cluster reports no reachable core at all and every
    # interface fails with "all core nodes unreachable" (#1049). Use it only
    # as a fallback when no remote core is connected, so multi-node routing
    # is unchanged.
    case remote do
      [] -> if local_core?(), do: [Node.self()], else: []
      _ -> remote
    end
  end

  defp local_core? do
    Process.whereis(NeonFS.Core.ServiceRegistry) != nil
  end

  defp reconcile_connections(state) do
    server = self()

    state.desired_nodes
    |> MapSet.to_list()
    |> Enum.sort()
    |> Enum.reduce(state, fn node, acc ->
      if MapSet.member?(acc.connected_nodes, node) or MapSet.member?(acc.connecting, node) do
        acc
      else
        start_connect(server, node, acc)
      end
    end)
  end

  # `Node.connect/1` blocks the caller while the distribution channel is
  # established (a TCP connect, up to the net-tick window) — running it inline
  # in the GenServer starves concurrent calls like `connected_core_node/0`,
  # which then time out under load (#1072). Run it in a throw-away process and
  # feed the outcome back as `{:connect_result, node, result}`; the node is
  # held in `connecting` until then so reconcile doesn't double-dial it.
  # `Node.connect/1` always returns, so the result message always arrives; if
  # the GenServer restarts mid-dial the message lands on a dead pid and the
  # fresh state simply re-dials on its first reconcile.
  defp start_connect(server, node, state) do
    spawn(fn -> send(server, {:connect_result, node, Node.connect(node)}) end)
    %{state | connecting: MapSet.put(state.connecting, node)}
  end

  defp schedule_reconcile(ms) do
    Process.send_after(self(), :reconcile, ms)
  end

  defp sync_discovered_services(state, services) do
    discovered_nodes =
      services
      |> Enum.reject(&(&1.node == Node.self()))
      |> Enum.map(& &1.node)
      |> MapSet.new()

    discovered_core_nodes =
      services
      |> Enum.filter(&(&1.type == :core and &1.node != Node.self()))
      |> Enum.map(& &1.node)
      |> MapSet.new()

    bootstrap_nodes = MapSet.new(state.bootstrap_nodes)

    %{
      state
      | core_nodes: MapSet.union(bootstrap_nodes, discovered_core_nodes),
        desired_nodes: MapSet.union(bootstrap_nodes, discovered_nodes)
    }
  end
end
