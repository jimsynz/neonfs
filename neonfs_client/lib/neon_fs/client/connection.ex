defmodule NeonFS.Client.Connection do
  @moduledoc """
  Manages Erlang distribution connections to bootstrap nodes.

  On init, connects to configured bootstrap nodes via `Node.connect/1`.
  Monitors connections and attempts reconnection on `:nodedown`.

  ## Configuration

  Bootstrap nodes are configured via the `:bootstrap_nodes` option:

      {NeonFS.Client.Connection, bootstrap_nodes: [:neonfs_core@host1]}

  Or via application env:

      config :neonfs_client, bootstrap_nodes: [:neonfs_core@host1]
  """

  use GenServer
  require Logger

  @default_reconnect_interval_ms 5_000
  @default_peer_connect_timeout_ms 10_000

  @type state :: %{
          bootstrap_nodes: [node()],
          connected_nodes: MapSet.t(node()),
          monitors: %{optional(reference()) => node()},
          peer_connect_timeout: pos_integer()
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

    state = %{
      bootstrap_nodes: bootstrap,
      connected_nodes: MapSet.new(),
      monitors: %{},
      peer_connect_timeout: peer_connect_timeout
    }

    {:ok, state, {:continue, :connect}}
  end

  @impl true
  def handle_continue(:connect, state) do
    {:noreply, connect_to_bootstrap(state)}
  end

  @impl true
  def handle_call(:connected_core_node, _from, state) do
    case MapSet.to_list(state.connected_nodes) do
      [node | _] -> {:reply, {:ok, node}, state}
      [] -> {:reply, {:error, :no_connection}, state}
    end
  end

  @impl true
  def handle_call(:bootstrap_nodes, _from, state) do
    {:reply, state.bootstrap_nodes, state}
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
  def handle_info(:reconnect, state) do
    {:noreply, connect_to_bootstrap(state)}
  end

  ## Private helpers

  defp handle_nodedown(node, state) do
    Logger.warning("Lost connection to bootstrap node", node: node)

    new_connected = MapSet.delete(state.connected_nodes, node)

    # Remove monitor entry
    new_monitors =
      state.monitors
      |> Enum.reject(fn {_ref, n} -> n == node end)
      |> Map.new()

    new_state = %{state | connected_nodes: new_connected, monitors: new_monitors}

    # Schedule reconnection attempt
    Process.send_after(self(), :reconnect, @default_reconnect_interval_ms)
    new_state
  end

  defp connect_to_bootstrap(state) do
    Enum.reduce(state.bootstrap_nodes, state, fn node, acc ->
      if MapSet.member?(acc.connected_nodes, node) do
        acc
      else
        try_connect(node, acc)
      end
    end)
  end

  defp try_connect(node, state) do
    case Node.connect(node) do
      true ->
        ref = Node.monitor(node, true)

        :telemetry.execute(
          [:neonfs, :client, :connection, :connected],
          %{},
          %{node: node}
        )

        %{
          state
          | connected_nodes: MapSet.put(state.connected_nodes, node),
            monitors: Map.put(state.monitors, ref, node)
        }

      false ->
        Logger.debug("Failed to connect to bootstrap node", node: node)
        state

      :ignored ->
        Logger.debug("Node connection ignored (distribution not started)")
        state
    end
  end
end
