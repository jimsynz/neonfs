defmodule NeonFS.Client.PartitionRecovery do
  @moduledoc """
  Monitors core node connections via `:net_kernel.monitor_nodes/2` and
  triggers full cache invalidation on reconnect after a core node was
  unreachable.

  During network partitions, `:pg` broadcasts silently drop messages to
  unreachable nodes. When a partition heals, subscriber caches may be
  arbitrarily stale. This module handles recovery by invalidating all
  local caches, letting them rebuild from fresh RPC calls.

  A debounce mechanism prevents rapid invalidation during connection flapping.

  ## Configuration

      config :neonfs_client, :partition_recovery_debounce_ms, 5_000

  ## Init Options

    * `:core_node?` — predicate `(node() -> boolean())` for testing
    * `:debounce_ms` — override debounce period
    * `:name` — GenServer name (default `__MODULE__`)
  """

  use GenServer

  require Logger

  @default_debounce_ms 5_000

  # -- Public API --

  @doc """
  Start the PartitionRecovery GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  # -- GenServer callbacks --

  @impl true
  def init(opts) do
    :net_kernel.monitor_nodes(true)

    debounce_ms =
      Keyword.get_lazy(opts, :debounce_ms, fn ->
        Application.get_env(:neonfs_client, :partition_recovery_debounce_ms, @default_debounce_ms)
      end)

    core_node_predicate = Keyword.get(opts, :core_node?, &default_core_node?/1)

    {:ok,
     %{
       known_core_nodes: MapSet.new(),
       pending_invalidations: MapSet.new(),
       debounce_ms: debounce_ms,
       core_node?: core_node_predicate
     }}
  end

  @impl true
  def handle_info({:nodedown, node}, state) do
    if state.core_node?.(node) do
      # Remove from known set and cancel pending invalidation (node went away
      # before debounce expired — skip invalidation, will re-trigger on next nodeup)
      {:noreply,
       %{
         state
         | known_core_nodes: MapSet.delete(state.known_core_nodes, node),
           pending_invalidations: MapSet.delete(state.pending_invalidations, node)
       }}
    else
      {:noreply, state}
    end
  end

  def handle_info({:nodeup, node}, state) do
    if state.core_node?.(node) and not MapSet.member?(state.known_core_nodes, node) do
      Process.send_after(self(), {:do_invalidate, node}, state.debounce_ms)

      {:noreply,
       %{
         state
         | known_core_nodes: MapSet.put(state.known_core_nodes, node),
           pending_invalidations: MapSet.put(state.pending_invalidations, node)
       }}
    else
      {:noreply, state}
    end
  end

  def handle_info({:do_invalidate, node}, state) do
    if MapSet.member?(state.pending_invalidations, node) do
      invalidate_all_caches()

      {:noreply,
       %{state | pending_invalidations: MapSet.delete(state.pending_invalidations, node)}}
    else
      # Node went down again before debounce expired — skip
      {:noreply, state}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- Private functions --

  defp invalidate_all_caches do
    # Collect all unique PIDs registered in the events Registry and send
    # each one a single invalidation message. A process may be subscribed
    # to multiple volumes, so deduplication avoids redundant messages.
    pids =
      NeonFS.Events.Registry
      |> Registry.select([{{:_, :"$1", :_}, [], [:"$1"]}])
      |> Enum.uniq()

    for pid <- pids do
      send(pid, :neonfs_invalidate_all)
    end

    Logger.warning("Partition recovery invalidated subscribers", count: length(pids))
  end

  defp default_core_node?(node) do
    case :erpc.call(node, Process, :whereis, [NeonFS.Core.ServiceRegistry], 2_000) do
      pid when is_pid(pid) -> true
      _ -> false
    end
  catch
    _, _ -> false
  end
end
