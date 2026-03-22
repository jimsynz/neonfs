defmodule NeonFS.Events.Relay do
  @moduledoc """
  Manages `:pg` group membership on behalf of local event subscribers and
  dispatches received events to local processes via `Registry`.

  Each node runs a single Relay process. When the first local process subscribes
  to a volume, the Relay joins the `:pg` group `{:volume, volume_id}`. When the
  last local subscriber unsubscribes (or exits), the Relay leaves the group.

  The Relay always joins the `{:volumes}` group on init for volume lifecycle
  events (VolumeCreated, VolumeUpdated, VolumeDeleted).

  Cross-node messages arrive as `{:neonfs_event, %Envelope{}}` and are fanned
  out locally via `Registry.dispatch/3`.
  """

  use GenServer

  alias NeonFS.Events.{
    DriveAdded,
    DriveRemoved,
    Envelope,
    VolumeCreated,
    VolumeDeleted,
    VolumeUpdated
  }

  @pg_scope :neonfs_events

  # -- Public API --

  @doc """
  Start the Relay GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Ensure the Relay is a member of the `:pg` group for `volume_id`.

  Increments the reference count for the volume. Joins the `:pg` group on the
  first subscriber. Monitors the calling process to decrement the count on exit.
  """
  @spec ensure_volume_group(binary()) :: :ok
  def ensure_volume_group(volume_id) do
    GenServer.call(__MODULE__, {:ensure_volume_group, volume_id, self()})
  end

  @doc """
  Decrement the subscriber count for `volume_id`, leaving the `:pg` group when
  the count reaches zero.
  """
  @spec maybe_leave_volume_group(binary()) :: :ok
  def maybe_leave_volume_group(volume_id) do
    GenServer.call(__MODULE__, {:maybe_leave_volume_group, volume_id, self()})
  end

  # -- GenServer callbacks --

  @impl true
  def init(_opts) do
    :pg.join(@pg_scope, {:volumes}, self())
    :pg.join(@pg_scope, {:drives}, self())

    {:ok,
     %{
       volume_refs: %{},
       monitors: %{}
     }}
  end

  @impl true
  def handle_call({:ensure_volume_group, volume_id, pid}, _from, state) do
    refs = Map.get(state.volume_refs, volume_id, 0)

    if refs == 0 do
      :pg.join(@pg_scope, {:volume, volume_id}, self())
    end

    state = put_in(state.volume_refs[volume_id], refs + 1)
    state = maybe_monitor(state, pid)

    {:reply, :ok, state}
  end

  def handle_call({:maybe_leave_volume_group, volume_id, pid}, _from, state) do
    refs = Map.get(state.volume_refs, volume_id, 0) - 1

    state =
      if refs <= 0 do
        :pg.leave(@pg_scope, {:volume, volume_id}, self())
        %{state | volume_refs: Map.delete(state.volume_refs, volume_id)}
      else
        put_in(state.volume_refs[volume_id], refs)
      end

    state = maybe_demonitor(state, pid)

    {:reply, :ok, state}
  end

  @impl true
  def handle_info({:neonfs_event, %Envelope{event: event} = envelope}, state) do
    group = event_group(event)

    Registry.dispatch(NeonFS.Events.Registry, group, fn entries ->
      for {pid, _value} <- entries do
        send(pid, {:neonfs_event, envelope})
      end
    end)

    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, pid, _reason}, state) do
    state = handle_subscriber_down(state, pid, ref)
    {:noreply, state}
  end

  def handle_info(_msg, state), do: {:noreply, state}

  # -- Private functions --

  defp event_group(%VolumeCreated{}), do: {:volumes}
  defp event_group(%VolumeDeleted{}), do: {:volumes}
  defp event_group(%VolumeUpdated{}), do: {:volumes}
  defp event_group(%DriveAdded{}), do: {:drives}
  defp event_group(%DriveRemoved{}), do: {:drives}
  defp event_group(event), do: {:volume, event.volume_id}

  defp maybe_monitor(state, pid) do
    if Map.has_key?(state.monitors, pid) do
      state
    else
      ref = Process.monitor(pid)
      put_in(state.monitors[pid], ref)
    end
  end

  defp maybe_demonitor(state, pid) do
    # Only demonitor if the pid has no remaining volume subscriptions.
    # Check if any volume still has this pid as a subscriber by looking at
    # the Registry (the canonical source of truth for who is subscribed).
    registrations = Registry.keys(NeonFS.Events.Registry, pid)
    volume_keys = Enum.filter(registrations, &match?({:volume, _}, &1))

    if volume_keys == [] do
      case Map.pop(state.monitors, pid) do
        {nil, monitors} ->
          %{state | monitors: monitors}

        {ref, monitors} ->
          Process.demonitor(ref, [:flush])
          %{state | monitors: monitors}
      end
    else
      state
    end
  end

  defp handle_subscriber_down(state, pid, ref) do
    Process.demonitor(ref, [:flush])
    monitors = Map.delete(state.monitors, pid)
    previous_volumes = Map.keys(state.volume_refs)

    # Find all volumes this pid was subscribed to by checking which volume
    # ref counts need decrementing. We look at Registry to see which keys
    # the dead process had (Registry auto-unregisters on process death, but
    # this callback may fire before or after that cleanup). We decrement
    # all volume ref counts that the pid could have had.
    #
    # Since Registry auto-unregisters the dead pid, we scan volume_refs
    # for any that might need cleanup. The safe approach: for each volume
    # with refs > 0, check if any live processes are still registered.
    volume_refs =
      Enum.reduce(state.volume_refs, state.volume_refs, fn {volume_id, _refs}, acc ->
        registered =
          Registry.lookup(NeonFS.Events.Registry, {:volume, volume_id})
          |> Enum.count(fn {p, _} -> p != pid end)

        if registered == 0 do
          :pg.leave(@pg_scope, {:volume, volume_id}, self())
          Map.delete(acc, volume_id)
        else
          Map.put(acc, volume_id, registered)
        end
      end)

    volumes_left = previous_volumes -- Map.keys(volume_refs)

    :telemetry.execute(
      [:neonfs, :events, :relay, :subscriber_down],
      %{},
      %{pid: pid, volumes_left: volumes_left}
    )

    %{state | volume_refs: volume_refs, monitors: monitors}
  end
end
