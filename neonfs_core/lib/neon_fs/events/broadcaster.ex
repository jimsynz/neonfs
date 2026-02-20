defmodule NeonFS.Events.Broadcaster do
  @moduledoc """
  Broadcasts metadata events to all nodes interested in a volume.

  Called by index modules (FileIndex, VolumeRegistry, etc.) after successful
  metadata writes. Wraps events in `Envelope` structs with source node,
  per-volume sequence number, and HLC timestamp, then sends to all `:pg`
  group members.

  This module is stateless (not a GenServer). Each calling process gets its
  own HLC instance via the process dictionary. Per-volume sequence counters
  use `:atomics` for lock-free atomic increments, stored in `:persistent_term`
  for fast lookups.
  """

  alias NeonFS.Core.HLC
  alias NeonFS.Events.{Envelope, VolumeCreated, VolumeDeleted, VolumeUpdated}

  @pg_scope :neonfs_events

  @volume_events [VolumeCreated, VolumeUpdated, VolumeDeleted]

  @doc """
  Broadcasts an event to all nodes subscribed to the given volume.

  Wraps the event in an `Envelope` with the current node as source, a
  monotonically increasing per-volume sequence number, and an HLC timestamp.
  Sends `{:neonfs_event, envelope}` to all `:pg` members for the volume group.
  Volume lifecycle events are also sent to the `{:volumes}` group.

  Fire-and-forget — returns `:ok` regardless of whether any subscribers exist.
  """
  @spec broadcast(binary(), NeonFS.Events.event()) :: :ok
  def broadcast(volume_id, event) do
    sequence = next_sequence(volume_id)
    hlc_timestamp = next_hlc_timestamp()

    envelope = %Envelope{
      event: event,
      source_node: node(),
      sequence: sequence,
      hlc_timestamp: hlc_timestamp
    }

    for pid <- :pg.get_members(@pg_scope, {:volume, volume_id}) do
      send(pid, {:neonfs_event, envelope})
    end

    if event.__struct__ in @volume_events do
      for pid <- :pg.get_members(@pg_scope, {:volumes}) do
        send(pid, {:neonfs_event, envelope})
      end
    end

    :ok
  end

  @doc """
  Gets or creates a per-volume `:atomics` counter.

  The counter ref is stored in `:persistent_term` under
  `{NeonFS.Events.Broadcaster, :counter, volume_id}`. Creates a new
  `:atomics` ref (1 element, signed) on first call. Idempotent — subsequent
  calls return the same ref.
  """
  @spec get_or_create_counter(binary()) :: :atomics.atomics_ref()
  def get_or_create_counter(volume_id) do
    key = {__MODULE__, :counter, volume_id}

    case safe_persistent_term_get(key) do
      {:ok, counter} ->
        counter

      :error ->
        counter = :atomics.new(1, signed: false)

        try do
          :persistent_term.put(key, counter)
        rescue
          # Another process may have raced us — use theirs
          ArgumentError ->
            case safe_persistent_term_get(key) do
              {:ok, existing} -> existing
              :error -> counter
            end
        end

        # Return whatever is in persistent_term now (handles race)
        case safe_persistent_term_get(key) do
          {:ok, result} -> result
          :error -> counter
        end
    end
  end

  @doc """
  Returns the next sequence number for a volume.

  Atomically increments the per-volume counter and returns the new value.
  Counters start at 0; the first event gets sequence 1.
  """
  @spec next_sequence(binary()) :: pos_integer()
  def next_sequence(volume_id) do
    counter = get_or_create_counter(volume_id)
    :atomics.add_get(counter, 1, 1)
  end

  # Generates an HLC timestamp using per-process state stored in the
  # process dictionary. Each calling process maintains its own HLC instance,
  # avoiding a global bottleneck while ensuring monotonicity within each process.
  defp next_hlc_timestamp do
    hlc_state =
      case Process.get({__MODULE__, :hlc}) do
        nil -> HLC.new(node())
        state -> state
      end

    {timestamp, new_state} = HLC.now(hlc_state)
    Process.put({__MODULE__, :hlc}, new_state)
    timestamp
  end

  defp safe_persistent_term_get(key) do
    {:ok, :persistent_term.get(key)}
  rescue
    ArgumentError -> :error
  end
end
