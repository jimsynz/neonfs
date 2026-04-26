defmodule NeonFS.TestSupport.EventCollector do
  @moduledoc """
  Test helper GenServer that subscribes to volume events and collects
  them for later inspection.

  Started on peer nodes via RPC to verify cross-node event delivery.
  """

  use GenServer

  alias NeonFS.Events.Envelope

  # -- Public API --

  @doc "Start an unlinked collector (survives RPC handler exit)."
  @spec start(binary()) :: GenServer.on_start()
  def start(volume_id) do
    GenServer.start(__MODULE__, {:volume, volume_id, nil})
  end

  @doc "Start an unlinked collector that notifies a pid on each event."
  @spec start_with_notify(binary(), pid()) :: GenServer.on_start()
  def start_with_notify(volume_id, notify_pid) when is_pid(notify_pid) do
    GenServer.start(__MODULE__, {:volume, volume_id, notify_pid})
  end

  @doc "Start an unlinked volume-lifecycle collector."
  @spec start_volumes() :: GenServer.on_start()
  def start_volumes do
    GenServer.start(__MODULE__, {:volumes, nil})
  end

  @doc "Start an unlinked volume-lifecycle collector that notifies a pid."
  @spec start_volumes_with_notify(pid()) :: GenServer.on_start()
  def start_volumes_with_notify(notify_pid) when is_pid(notify_pid) do
    GenServer.start(__MODULE__, {:volumes, notify_pid})
  end

  @doc "Return all collected events in arrival order."
  @spec events(pid()) :: [Envelope.t()]
  def events(pid), do: GenServer.call(pid, :events)

  @doc "Return the count of collected events."
  @spec count(pid()) :: non_neg_integer()
  def count(pid), do: GenServer.call(pid, :count)

  @doc "Clear all collected events."
  @spec clear(pid()) :: :ok
  def clear(pid), do: GenServer.call(pid, :clear)

  @doc "Check whether an :neonfs_invalidate_all was received."
  @spec invalidated?(pid()) :: boolean()
  def invalidated?(pid), do: GenServer.call(pid, :invalidated?)

  # -- GenServer callbacks --

  @impl true
  def init({:volume, volume_id, notify}) do
    NeonFS.Events.subscribe(volume_id)
    {:ok, %{events: [], invalidated: false, notify: notify}}
  end

  def init({:volumes, notify}) do
    NeonFS.Events.subscribe_volumes()
    {:ok, %{events: [], invalidated: false, notify: notify}}
  end

  @impl true
  def handle_call(:events, _from, state) do
    {:reply, Enum.reverse(state.events), state}
  end

  def handle_call(:count, _from, state) do
    {:reply, length(state.events), state}
  end

  def handle_call(:clear, _from, state) do
    {:reply, :ok, %{state | events: [], invalidated: false}}
  end

  def handle_call(:invalidated?, _from, state) do
    {:reply, state.invalidated, state}
  end

  @impl true
  def handle_info({:neonfs_event, %Envelope{} = envelope}, state) do
    if state.notify, do: send(state.notify, {:neonfs_test_event, envelope})
    {:noreply, %{state | events: [envelope | state.events]}}
  end

  def handle_info(:neonfs_invalidate_all, state) do
    {:noreply, %{state | invalidated: true}}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end
end
