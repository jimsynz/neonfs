defmodule NeonFS.Core.Escalation.Ticker do
  @moduledoc """
  Periodic expiry sweep for `NeonFS.Core.Escalation`.

  Schedules a tick every `:expire_interval_ms` (default 60s) and, on each
  tick, asks the stateless `Escalation` module to move overdue pending
  escalations to `:expired` and re-emit pending-count metrics. Owns no
  state beyond the timer reference — all escalation data lives in Ra.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.Escalation

  @default_interval_ms 60_000

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @impl true
  def init(opts) do
    interval = Keyword.get(opts, :expire_interval_ms, @default_interval_ms)
    send(self(), :tick_on_start)
    {:ok, %{interval_ms: interval}}
  end

  @impl true
  def handle_info(:tick_on_start, state) do
    safe_emit_pending_metrics()
    schedule_tick(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(:tick, state) do
    safe_expire_and_emit()
    schedule_tick(state.interval_ms)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp safe_expire_and_emit do
    Escalation.expire_overdue()
    Escalation.emit_pending_metrics()
  rescue
    e ->
      Logger.warning("Escalation expiry tick failed", reason: inspect(e))
      :ok
  catch
    :exit, reason ->
      Logger.warning("Escalation expiry tick exited", reason: inspect(reason))
      :ok
  end

  defp safe_emit_pending_metrics do
    Escalation.emit_pending_metrics()
  rescue
    _ -> :ok
  catch
    :exit, _ -> :ok
  end

  defp schedule_tick(0), do: :ok

  defp schedule_tick(interval_ms) when is_integer(interval_ms) and interval_ms > 0 do
    Process.send_after(self(), :tick, interval_ms)
  end
end
