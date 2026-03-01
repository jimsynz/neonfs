defmodule NeonFS.IO.DriveWorker do
  @moduledoc """
  GenStage consumer that executes I/O operations for a specific physical drive.

  Each drive in `DriveRegistry` gets its own worker process. The worker
  subscribes to `NeonFS.IO.Producer` using the partition dispatcher, receiving
  only operations targeted at its drive.

  Operations are passed through the appropriate `DriveStrategy` (HDD elevator
  scheduling or SSD parallel FIFO) before execution. Callbacks run within
  `Task.async_stream` for controlled parallelism.

  ## Demand Configuration

  `max_demand` is set based on strategy type:
  - **HDD**: `batch_size` (default 32) — small batches preserve sort benefits
  - **SSD**: `max_concurrent` (default 64) — high parallelism, no seek penalty
  """

  use GenStage
  require Logger

  alias NeonFS.IO.DriveStrategy.{HDD, SSD}
  alias NeonFS.IO.Operation

  @default_hdd_batch_size 32
  @default_ssd_max_concurrent 64

  @type t :: %__MODULE__{
          drive_id: String.t(),
          drive_type: :hdd | :ssd,
          strategy_mod: module(),
          strategy_state: term(),
          in_flight: non_neg_integer(),
          max_concurrent: pos_integer()
        }

  defstruct [
    :drive_id,
    :drive_type,
    :strategy_mod,
    :strategy_state,
    in_flight: 0,
    max_concurrent: 64
  ]

  ## Client API

  @doc """
  Starts a drive worker for the specified drive.

  ## Options

    * `:drive_id` (required) — unique identifier for the drive
    * `:drive_type` — `:hdd` or `:ssd` (default: `:ssd`)
    * `:producer` — producer process to subscribe to (default: `NeonFS.IO.Producer`)
    * `:strategy_config` — keyword options passed to the drive strategy's `init/1`
    * `:name` — process name
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts) do
    {name, opts} = Keyword.pop(opts, :name)
    start_opts = if name, do: [name: name], else: []
    GenStage.start_link(__MODULE__, opts, start_opts)
  end

  ## GenStage Callbacks

  @impl true
  def init(opts) do
    drive_id = Keyword.fetch!(opts, :drive_id)
    drive_type = Keyword.get(opts, :drive_type, :ssd)
    producer = Keyword.get(opts, :producer, NeonFS.IO.Producer)
    strategy_config = Keyword.get(opts, :strategy_config, [])

    {strategy_mod, max_concurrent} = strategy_for_type(drive_type, strategy_config)
    strategy_state = strategy_mod.init(strategy_config)

    state = %__MODULE__{
      drive_id: drive_id,
      drive_type: drive_type,
      strategy_mod: strategy_mod,
      strategy_state: strategy_state,
      max_concurrent: max_concurrent
    }

    Logger.info("DriveWorker started",
      drive_id: drive_id,
      drive_type: drive_type,
      max_concurrent: max_concurrent
    )

    subscription_opts = [
      partition: drive_id,
      max_demand: max_concurrent,
      min_demand: max(div(max_concurrent, 2), 1)
    ]

    {:consumer, state, subscribe_to: [{producer, subscription_opts}]}
  end

  @impl true
  def handle_events(events, _from, state) do
    strategy_state =
      Enum.reduce(events, state.strategy_state, fn op, strat ->
        state.strategy_mod.enqueue(strat, op)
      end)

    available = max(state.max_concurrent - state.in_flight, 1)
    {batch, strategy_state} = state.strategy_mod.next_batch(strategy_state, available)

    batch_size = length(batch)
    state = %{state | strategy_state: strategy_state, in_flight: state.in_flight + batch_size}

    state = execute_batch(batch, state)

    {:noreply, [], state}
  end

  ## Private

  defp strategy_for_type(:hdd, config) do
    batch_size = Keyword.get(config, :batch_size, @default_hdd_batch_size)
    {HDD, batch_size}
  end

  defp strategy_for_type(:ssd, config) do
    max_concurrent = Keyword.get(config, :max_concurrent, @default_ssd_max_concurrent)
    {SSD, max_concurrent}
  end

  defp execute_batch([], state), do: state

  defp execute_batch(batch, state) do
    batch
    |> Task.async_stream(&execute_operation/1,
      max_concurrency: state.max_concurrent,
      timeout: :infinity,
      ordered: false
    )
    |> Stream.run()

    %{state | in_flight: max(state.in_flight - length(batch), 0)}
  end

  defp execute_operation(%Operation{} = op) do
    start_time = System.monotonic_time()

    try do
      result = op.callback.()

      :telemetry.execute(
        [:neonfs, :io, :complete],
        %{duration: System.monotonic_time() - start_time},
        %{operation_id: op.id, priority: op.priority, drive_id: op.drive_id, type: op.type}
      )

      {:ok, result}
    rescue
      error ->
        :telemetry.execute(
          [:neonfs, :io, :error],
          %{duration: System.monotonic_time() - start_time},
          %{
            operation_id: op.id,
            priority: op.priority,
            drive_id: op.drive_id,
            type: op.type,
            reason: Exception.message(error)
          }
        )

        {:error, error}
    catch
      kind, reason ->
        :telemetry.execute(
          [:neonfs, :io, :error],
          %{duration: System.monotonic_time() - start_time},
          %{
            operation_id: op.id,
            priority: op.priority,
            drive_id: op.drive_id,
            type: op.type,
            reason: inspect({kind, reason})
          }
        )

        {:error, {kind, reason}}
    end
  end
end
