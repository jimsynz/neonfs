defmodule NeonFS.Client.CostFunction do
  @moduledoc """
  Calculates routing costs for core nodes based on latency and load.

  Periodically pings known core nodes to measure latency and queries their
  published load metrics. Uses a composite cost function to rank nodes:

      cost = 0.3 * latency_score + 0.4 * load_score + 0.3 * queue_score
  """

  use GenServer
  require Logger

  alias NeonFS.Client.Discovery

  @default_probe_interval_ms 10_000
  @latency_weight 0.3
  @load_weight 0.4
  @queue_weight 0.3

  @type node_cost :: %{
          node: node(),
          latency_ms: non_neg_integer(),
          load_score: float(),
          queue_score: float(),
          total_cost: float()
        }

  ## Client API

  @doc """
  Starts the cost function GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Selects the best core node based on cost.

  ## Options
  - `:prefer_leader` - prefer the Ra leader node if cost is within 20% of cheapest
  """
  @spec select_core_node(keyword()) :: {:ok, node()} | {:error, :no_core_nodes}
  def select_core_node(opts \\ []) do
    GenServer.call(__MODULE__, {:select_core_node, opts})
  end

  @doc """
  Returns the current cost table for all known nodes.
  """
  @spec node_costs() :: [node_cost()]
  def node_costs do
    GenServer.call(__MODULE__, :node_costs)
  end

  ## Server callbacks

  @impl true
  def init(opts) do
    probe_interval = Keyword.get(opts, :probe_interval_ms, @default_probe_interval_ms)

    state = %{
      costs: %{},
      probe_interval: probe_interval
    }

    {:ok, state, {:continue, :probe}}
  end

  @impl true
  def handle_continue(:probe, state) do
    new_state = probe_nodes(state)
    schedule_probe(state.probe_interval)
    {:noreply, new_state}
  end

  @impl true
  def handle_call({:select_core_node, opts}, _from, state) do
    prefer_leader = Keyword.get(opts, :prefer_leader, false)

    case select_cheapest(state.costs, prefer_leader) do
      nil -> {:reply, {:error, :no_core_nodes}, state}
      node -> {:reply, {:ok, node}, state}
    end
  end

  @impl true
  def handle_call(:node_costs, _from, state) do
    costs =
      state.costs
      |> Map.values()
      |> Enum.sort_by(& &1.total_cost)

    {:reply, costs, state}
  end

  @impl true
  def handle_info(:probe, state) do
    new_state = probe_nodes(state)
    schedule_probe(state.probe_interval)
    {:noreply, new_state}
  end

  ## Private helpers

  defp probe_nodes(state) do
    core_nodes = Discovery.get_core_nodes()

    costs =
      core_nodes
      |> Enum.map(&measure_node/1)
      |> Enum.reject(&is_nil/1)
      |> Map.new(fn cost -> {cost.node, cost} end)

    %{state | costs: costs}
  end

  defp measure_node(node) do
    latency = measure_latency(node)

    if latency do
      load_score = fetch_load_score(node)
      queue_score = fetch_queue_score(node)

      # Normalise latency to 0-1 range (cap at 1000ms)
      latency_score = min(latency / 1000.0, 1.0)

      total =
        @latency_weight * latency_score +
          @load_weight * load_score +
          @queue_weight * queue_score

      %{
        node: node,
        latency_ms: latency,
        load_score: load_score,
        queue_score: queue_score,
        total_cost: total
      }
    end
  end

  defp measure_latency(node) do
    start = System.monotonic_time(:millisecond)

    case :rpc.call(node, :erlang, :node, [], 2_000) do
      ^node ->
        System.monotonic_time(:millisecond) - start

      {:badrpc, _} ->
        nil
    end
  end

  defp fetch_load_score(node) do
    case :rpc.call(node, :cpu_sup, :avg1, [], 2_000) do
      load when is_integer(load) ->
        # avg1 returns load * 256, normalise to 0-1 (cap at 4.0 load)
        min(load / 256.0 / 4.0, 1.0)

      {:badrpc, _} ->
        0.5
    end
  rescue
    _ -> 0.5
  end

  defp fetch_queue_score(node) do
    case :rpc.call(node, :erlang, :statistics, [:run_queue], 2_000) do
      queue when is_integer(queue) ->
        # Normalise queue depth: cap at 100
        min(queue / 100.0, 1.0)

      {:badrpc, _} ->
        0.5
    end
  rescue
    _ -> 0.5
  end

  defp select_cheapest(costs, _prefer_leader) when map_size(costs) == 0, do: nil

  defp select_cheapest(costs, _prefer_leader) do
    costs
    |> Map.values()
    |> Enum.min_by(& &1.total_cost)
    |> Map.get(:node)
  end

  defp schedule_probe(ms) do
    Process.send_after(self(), :probe, ms)
  end
end
