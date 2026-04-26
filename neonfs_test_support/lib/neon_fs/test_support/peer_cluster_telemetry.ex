defmodule NeonFS.TestSupport.PeerClusterTelemetry do
  @moduledoc """
  Attaches telemetry handlers to `NeonFS.TestSupport.PeerCluster`'s per-phase
  spans, accumulates per-phase timings for the whole test run, and prints a
  summary on process exit.

  Started from `test_helper.exs`. The summary shape:

      Cluster setup phase breakdown (accumulated over NN clusters)
      ------------------------------------------------------------
      spawn                  xxx.xs total   (xx.xs avg per node)
      apply_config           xxx.xs total   (xx.xs avg per node)
      start_applications     xxx.xs total   (xx.xs avg per node)
      wait_for_ra            xxx.xs total   (xx.xs avg per node)

  Used by #420 / #423 to figure out which phase is worth optimising.
  """

  use GenServer

  @events [
    [:neonfs, :peer_cluster, :node, :spawn],
    [:neonfs, :peer_cluster, :node, :apply_config],
    [:neonfs, :peer_cluster, :node, :start_applications],
    [:neonfs, :peer_cluster, :node, :wait_for_ra]
  ]

  def start_link(_opts \\ []) do
    GenServer.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc "Print and clear the accumulated timings."
  def print_summary do
    GenServer.call(__MODULE__, :print_summary)
  end

  @impl true
  def init(_) do
    Process.flag(:trap_exit, true)

    for event <- @events do
      :telemetry.attach(
        {__MODULE__, event},
        event ++ [:stop],
        &__MODULE__.handle_event/4,
        nil
      )
    end

    {:ok, %{totals: %{}, counts: %{}}}
  end

  @doc false
  def handle_event(event, measurements, _metadata, _config) do
    phase = List.last(List.delete_at(event, -1))
    duration_native = measurements[:duration] || 0
    duration_us = System.convert_time_unit(duration_native, :native, :microsecond)
    GenServer.cast(__MODULE__, {:record, phase, duration_us})
  end

  @impl true
  def handle_cast({:record, phase, duration_us}, state) do
    totals = Map.update(state.totals, phase, duration_us, &(&1 + duration_us))
    counts = Map.update(state.counts, phase, 1, &(&1 + 1))
    {:noreply, %{state | totals: totals, counts: counts}}
  end

  @impl true
  def handle_call(:print_summary, _from, state) do
    do_print(state)
    {:reply, :ok, %{totals: %{}, counts: %{}}}
  end

  @impl true
  def terminate(_reason, state) do
    do_print(state)
    detach_all()
    :ok
  end

  defp do_print(%{totals: totals}) when map_size(totals) == 0, do: :ok

  defp do_print(%{totals: totals, counts: counts}) do
    IO.puts("")
    IO.puts("Cluster setup phase breakdown (#{total_invocations(counts)} invocations)")
    IO.puts(String.duplicate("-", 60))

    for phase <- [:spawn, :apply_config, :start_applications, :wait_for_ra] do
      total_us = Map.get(totals, phase, 0)
      count = Map.get(counts, phase, 0)

      if count > 0 do
        avg_ms = total_us / count / 1000
        total_s = total_us / 1_000_000

        IO.puts(
          "  #{String.pad_trailing(to_string(phase), 22)} " <>
            "#{format_seconds(total_s)} total   " <>
            "(#{format_ms(avg_ms)} avg per node, #{count}×)"
        )
      end
    end

    IO.puts("")
  end

  defp total_invocations(counts) do
    # Any one phase's count equals the number of node setups; take the max in case
    # some phases were skipped (e.g. wait_for_ra when Ra is disabled).
    counts |> Map.values() |> Enum.max(fn -> 0 end)
  end

  defp format_seconds(s) when s >= 10, do: "#{:erlang.float_to_binary(s, decimals: 1)}s"
  defp format_seconds(s), do: "#{:erlang.float_to_binary(s, decimals: 2)}s"

  defp format_ms(ms) when ms >= 100, do: "#{round(ms)}ms"
  defp format_ms(ms), do: "#{:erlang.float_to_binary(ms, decimals: 1)}ms"

  defp detach_all do
    for event <- @events do
      :telemetry.detach({__MODULE__, event})
    end
  end
end
