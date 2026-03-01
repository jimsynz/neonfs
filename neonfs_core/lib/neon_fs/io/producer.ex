defmodule NeonFS.IO.Producer do
  @moduledoc """
  GenStage producer with Weighted Fair Queuing (WFQ) dispatch.

  Receives I/O operations from the scheduler facade and dispatches them
  to drive worker consumers. Each volume maintains a virtual finish time;
  the producer selects the operation with the lowest virtual finish time
  when a worker requests demand, ensuring volumes with higher `io_weight`
  get proportionally more I/O bandwidth.

  Within a volume's queue, operations are sorted by `Priority.weight/1`
  (highest first). Per-priority-class counters are maintained for the
  `status/0` API.

  ## WFQ Algorithm

  1. Each volume has a virtual finish time (VFT), initialised to 0.
  2. On enqueue for volume V: `VFT_V = max(VFT_V, current_virtual_time) + (1 / io_weight)`
  3. Dispatch selects the operation whose volume has the lowest VFT.
  4. `current_virtual_time` advances to the VFT of the dispatched volume.

  Volumes with higher `io_weight` advance their VFT more slowly,
  getting dispatched more frequently.
  """

  use GenStage
  require Logger

  alias NeonFS.IO.{Operation, Priority}

  @default_io_weight 100

  @type weight_overrides :: %{
          global: %{optional(Priority.t()) => pos_integer()},
          per_volume: %{optional(String.t()) => %{optional(Priority.t()) => pos_integer()}}
        }

  @type t :: %__MODULE__{
          volume_registry_mod: module() | nil,
          cancelled: MapSet.t(),
          counters: %{Priority.t() => non_neg_integer()},
          current_virtual_time: float(),
          operations: %{String.t() => Operation.t()},
          pending_demand: non_neg_integer(),
          volume_queues: %{String.t() => [Operation.t()]},
          volume_vfts: %{String.t() => float()},
          weight_cache: %{String.t() => pos_integer()},
          weight_overrides: weight_overrides()
        }

  defstruct [
    :volume_registry_mod,
    cancelled: MapSet.new(),
    counters: %{},
    current_virtual_time: 0.0,
    operations: %{},
    pending_demand: 0,
    volume_queues: %{},
    volume_vfts: %{},
    weight_cache: %{},
    weight_overrides: %{global: %{}, per_volume: %{}}
  ]

  ## Client API

  @doc """
  Starts the I/O producer.

  ## Options

    * `:name` — process name (default: `__MODULE__`)
    * `:volume_registry_mod` — module for looking up volume io_weight
      (default: `NeonFS.Core.VolumeRegistry`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenStage.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Enqueues an I/O operation for WFQ dispatch.
  """
  @spec enqueue(GenStage.stage(), Operation.t()) :: :ok
  def enqueue(producer \\ __MODULE__, %Operation{} = op) do
    GenStage.cast(producer, {:enqueue, op})
  end

  @doc """
  Cancels a previously enqueued operation.
  """
  @spec cancel(GenStage.stage(), String.t()) :: :ok
  def cancel(producer \\ __MODULE__, operation_id) when is_binary(operation_id) do
    GenStage.cast(producer, {:cancel, operation_id})
  end

  @doc """
  Returns current queue depths and total pending count.
  """
  @spec status(GenStage.stage()) :: %{queue_depths: map(), total_pending: non_neg_integer()}
  def status(producer \\ __MODULE__) do
    GenStage.call(producer, :status)
  end

  ## GenStage Callbacks

  @impl true
  def init(opts) do
    volume_registry_mod =
      Keyword.get(opts, :volume_registry_mod, NeonFS.Core.VolumeRegistry)

    dispatcher = Keyword.get(opts, :dispatcher, nil)

    counters = Map.new(Priority.all(), fn p -> {p, 0} end)

    state = %__MODULE__{
      counters: counters,
      volume_registry_mod: volume_registry_mod
    }

    producer_opts = if dispatcher, do: [dispatcher: dispatcher], else: []
    {:producer, state, producer_opts}
  end

  @impl true
  def handle_demand(demand, state) when demand > 0 do
    state = %{state | pending_demand: state.pending_demand + demand}
    {events, state} = dispatch_events(state)
    {:noreply, events, state}
  end

  @impl true
  def handle_cast({:enqueue, %Operation{} = op}, state) do
    state = do_enqueue(state, op)
    {events, state} = dispatch_events(state)
    {:noreply, events, state}
  end

  def handle_cast({:cancel, operation_id}, state) do
    state = do_cancel(state, operation_id)
    {:noreply, [], state}
  end

  def handle_cast({:adjust_weights, overrides}, state) do
    state = %{state | weight_overrides: overrides}
    # Re-sort all volume queues since effective priorities may have changed
    state = resort_all_queues(state)
    {events, state} = dispatch_events(state)
    {:noreply, events, state}
  end

  @impl true
  def handle_call(:status, _from, state) do
    result = %{
      queue_depths: Map.new(state.counters),
      total_pending: Enum.reduce(state.counters, 0, fn {_k, v}, acc -> acc + v end)
    }

    {:reply, result, [], state}
  end

  ## Private — Enqueue

  defp do_enqueue(state, %Operation{} = op) do
    volume_id = op.volume_id

    volume_queues =
      Map.update(state.volume_queues, volume_id, [op], fn queue ->
        insert_by_priority(queue, op, state.weight_overrides)
      end)

    {io_weight, state} = resolve_io_weight(state, volume_id)
    cost = 1.0 / io_weight

    volume_vfts =
      Map.update(
        state.volume_vfts,
        volume_id,
        state.current_virtual_time + cost,
        fn vft -> max(vft, state.current_virtual_time) + cost end
      )

    counters = Map.update!(state.counters, op.priority, &(&1 + 1))
    operations = Map.put(state.operations, op.id, op)

    :telemetry.execute(
      [:neonfs, :io, :producer, :enqueue],
      %{},
      %{operation_id: op.id, priority: op.priority, volume_id: volume_id}
    )

    %{
      state
      | counters: counters,
        operations: operations,
        volume_queues: volume_queues,
        volume_vfts: volume_vfts
    }
  end

  defp insert_by_priority(queue, op, overrides) do
    volume_id = op.volume_id
    op_weight = effective_weight(op.priority, volume_id, overrides)

    {before, after_ops} =
      Enum.split_while(queue, fn queued ->
        effective_weight(queued.priority, volume_id, overrides) >= op_weight
      end)

    before ++ [op] ++ after_ops
  end

  ## Private — Cancel

  defp do_cancel(state, operation_id) do
    case Map.pop(state.operations, operation_id) do
      {nil, _} ->
        # Operation not yet enqueued or already dispatched — record for
        # later skipping during dispatch
        %{state | cancelled: MapSet.put(state.cancelled, operation_id)}

      {op, operations} ->
        volume_queues = remove_from_volume_queue(state.volume_queues, op)
        counters = Map.update!(state.counters, op.priority, &max(&1 - 1, 0))

        %{
          state
          | counters: counters,
            operations: operations,
            volume_queues: volume_queues
        }
    end
  end

  defp remove_from_volume_queue(volume_queues, op) do
    case Map.get(volume_queues, op.volume_id) do
      nil ->
        volume_queues

      queue ->
        filtered = Enum.reject(queue, &(&1.id == op.id))

        if filtered == [] do
          Map.delete(volume_queues, op.volume_id)
        else
          Map.put(volume_queues, op.volume_id, filtered)
        end
    end
  end

  ## Private — Dispatch

  defp dispatch_events(state) do
    dispatch_events(state, [])
  end

  defp dispatch_events(%{pending_demand: 0} = state, acc) do
    {Enum.reverse(acc), state}
  end

  defp dispatch_events(state, acc) do
    case select_next_operation(state) do
      nil ->
        {Enum.reverse(acc), state}

      {op, state} ->
        if MapSet.member?(state.cancelled, op.id) do
          state = %{state | cancelled: MapSet.delete(state.cancelled, op.id)}
          dispatch_events(state, acc)
        else
          state = %{state | pending_demand: state.pending_demand - 1}
          dispatch_events(state, [op | acc])
        end
    end
  end

  defp select_next_operation(state) do
    non_empty =
      Enum.filter(state.volume_queues, fn {_vid, queue} -> queue != [] end)

    case non_empty do
      [] ->
        nil

      volumes ->
        {selected_vid, _queue} =
          Enum.min_by(volumes, fn {vid, queue} ->
            vft = Map.get(state.volume_vfts, vid, 0.0)
            top_op = hd(queue)
            top_weight = effective_weight(top_op.priority, vid, state.weight_overrides)
            # Sort by lowest VFT first, then highest priority weight as tiebreaker
            {vft, -top_weight}
          end)

        [op | rest] = Map.fetch!(state.volume_queues, selected_vid)

        volume_queues =
          if rest == [] do
            Map.delete(state.volume_queues, selected_vid)
          else
            Map.put(state.volume_queues, selected_vid, rest)
          end

        vft = Map.get(state.volume_vfts, selected_vid, 0.0)

        operations = Map.delete(state.operations, op.id)
        counters = Map.update!(state.counters, op.priority, &max(&1 - 1, 0))

        :telemetry.execute(
          [:neonfs, :io, :producer, :dispatch],
          %{},
          %{operation_id: op.id, priority: op.priority, volume_id: op.volume_id}
        )

        state = %{
          state
          | counters: counters,
            current_virtual_time: max(state.current_virtual_time, vft),
            operations: operations,
            volume_queues: volume_queues
        }

        {op, state}
    end
  end

  ## Private — Effective Weight

  defp effective_weight(priority, volume_id, %{per_volume: per_volume, global: global}) do
    per_vol = Map.get(per_volume, volume_id, %{})

    case Map.get(per_vol, priority) do
      nil ->
        case Map.get(global, priority) do
          nil -> Priority.weight(priority)
          weight -> weight
        end

      weight ->
        weight
    end
  end

  defp effective_weight(priority, _volume_id, _overrides) do
    Priority.weight(priority)
  end

  ## Private — Queue Resorting

  defp resort_all_queues(state) do
    volume_queues =
      Map.new(state.volume_queues, fn {volume_id, queue} ->
        sorted =
          Enum.sort_by(queue, fn op ->
            -effective_weight(op.priority, volume_id, state.weight_overrides)
          end)

        {volume_id, sorted}
      end)

    %{state | volume_queues: volume_queues}
  end

  ## Private — Weight Lookup

  defp resolve_io_weight(state, volume_id) do
    case Map.fetch(state.weight_cache, volume_id) do
      {:ok, weight} ->
        {weight, state}

      :error ->
        weight = fetch_io_weight(state.volume_registry_mod, volume_id)
        state = %{state | weight_cache: Map.put(state.weight_cache, volume_id, weight)}
        {weight, state}
    end
  end

  defp fetch_io_weight(volume_registry_mod, volume_id) do
    case volume_registry_mod.get(volume_id) do
      {:ok, volume} -> volume.io_weight
      {:error, _} -> @default_io_weight
    end
  rescue
    _ -> @default_io_weight
  catch
    :exit, _ -> @default_io_weight
  end
end
