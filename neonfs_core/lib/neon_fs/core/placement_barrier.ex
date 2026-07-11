defmodule NeonFS.Core.PlacementBarrier do
  @moduledoc """
  Tracks in-flight background chunk placements so a cluster freeze can drain
  them before reporting the cluster durable and quiescent (#1504).

  A `write_ack: :local` volume acknowledges a write once the primary copy is
  on local disk and places the remaining replicas in a fire-and-forget
  background task (`NeonFS.Core.Replication.spawn_background_replication`).
  Between the ack and those placements completing, the acknowledged write is
  not yet at its `min_copies` durable replica set — a power-cycle in that
  window drops the not-yet-placed copies.

  Freeze cuts new write ingress (the #1438 write-gate on `:frozen`) and then
  drains the outstanding placements through this barrier, so everything
  already acknowledged reaches its replica set before the operator powers
  off. Placements run under a dedicated `Task.Supervisor`
  (`NeonFS.Core.PlacementTaskSupervisor`) whose live children *are* the
  outstanding set; `drain/1` monitors them until they exit or the timeout
  fires.
  """

  alias NeonFS.Core.ServiceRegistry

  @supervisor NeonFS.Core.PlacementTaskSupervisor

  @type drain_result :: %{
          drained: non_neg_integer(),
          remaining: non_neg_integer(),
          timed_out: boolean()
        }

  @doc """
  Runs `fun` as a supervised background placement tracked by the barrier.
  """
  @spec run((-> any())) :: {:ok, pid()} | {:error, term()}
  def run(fun) when is_function(fun, 0) do
    Task.Supervisor.start_child(@supervisor, fun)
  end

  @doc """
  Drains the local node's outstanding placements, blocking until they all
  complete or `timeout` milliseconds elapse.
  """
  @spec drain(timeout()) :: drain_result()
  def drain(timeout) do
    case Process.whereis(@supervisor) do
      nil -> %{drained: 0, remaining: 0, timed_out: false}
      _pid -> await_children(Task.Supervisor.children(@supervisor), timeout)
    end
  end

  @doc """
  Drains outstanding placements on every connected core node concurrently,
  aggregating the per-node results. Bounded by `timeout` so a hung placement
  (e.g. a dead target drive) can't wedge the freeze.
  """
  @spec drain_cluster(timeout()) :: drain_result()
  def drain_cluster(timeout) do
    nodes = Enum.uniq([Node.self() | core_nodes()])
    {results, _bad_nodes} = :rpc.multicall(nodes, __MODULE__, :drain, [timeout], timeout + 5_000)

    Enum.reduce(results, %{drained: 0, remaining: 0, timed_out: false}, fn
      %{drained: drained, remaining: remaining, timed_out: timed_out}, acc ->
        %{
          drained: acc.drained + drained,
          remaining: acc.remaining + remaining,
          timed_out: acc.timed_out or timed_out
        }

      _other, acc ->
        acc
    end)
  end

  defp core_nodes do
    ServiceRegistry.connected_nodes_by_type(:core)
  rescue
    _ -> []
  end

  defp await_children([], _timeout), do: %{drained: 0, remaining: 0, timed_out: false}

  defp await_children(pids, timeout) do
    refs = Map.new(pids, fn pid -> {Process.monitor(pid), pid} end)
    deadline = System.monotonic_time(:millisecond) + timeout
    leftover = wait_down(refs, deadline)

    Enum.each(Map.keys(leftover), &Process.demonitor(&1, [:flush]))

    %{
      drained: map_size(refs) - map_size(leftover),
      remaining: map_size(leftover),
      timed_out: map_size(leftover) > 0
    }
  end

  defp wait_down(refs, _deadline) when map_size(refs) == 0, do: refs

  defp wait_down(refs, deadline) do
    remaining_ms = deadline - System.monotonic_time(:millisecond)

    if remaining_ms <= 0 do
      refs
    else
      receive do
        {:DOWN, ref, :process, _pid, _reason} -> wait_down(Map.delete(refs, ref), deadline)
      after
        remaining_ms -> refs
      end
    end
  end
end
