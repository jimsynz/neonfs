defmodule NeonFS.IO.Scheduler do
  @moduledoc """
  Public API facade for the I/O scheduler subsystem.

  Accepts I/O operations, validates them, and routes them through the
  GenStage-backed WFQ producer for dispatch to drive workers.

  `BackgroundWorker` remains the scheduler for non-I/O background jobs
  (key rotation, GC scheduling, etc.). This module handles only
  disk-bound operations.
  """

  use GenServer
  require Logger

  alias NeonFS.IO.{Operation, Producer, WorkerSupervisor}

  ## Client API

  @doc """
  Starts the I/O scheduler.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Submits an I/O operation for scheduling.

  The operation is validated before being accepted. Returns
  `{:ok, operation_id}` on success.

  ## Examples

      op = Operation.new(
        priority: :user_read,
        volume_id: "vol-1",
        drive_id: "nvme0",
        type: :read,
        callback: fn -> File.read!("/data/chunk") end
      )
      {:ok, id} = Scheduler.submit(op)
  """
  @spec submit(Operation.t()) :: {:ok, String.t()} | {:error, String.t()}
  def submit(%Operation{} = op) do
    submit(op, [])
  end

  @doc """
  Submits an I/O operation with additional options.

  ## Options

  Reserved for future use (e.g., `:timeout`, `:coalesce`).
  """
  @spec submit(Operation.t(), keyword()) :: {:ok, String.t()} | {:error, String.t()}
  def submit(%Operation{} = op, opts) when is_list(opts) do
    case Operation.validate(op) do
      {:ok, op} -> GenServer.call(name(), {:submit, op, opts})
      {:error, _} = error -> error
    end
  end

  @doc """
  Cancels a previously submitted operation.

  Returns `:ok` regardless — the cancellation is asynchronous.
  """
  @spec cancel(String.t()) :: :ok
  def cancel(operation_id) when is_binary(operation_id) do
    GenServer.call(name(), {:cancel, operation_id})
  end

  @doc """
  Returns current queue depths per priority class and total pending.
  """
  @spec status() :: map()
  def status do
    GenServer.call(name(), :status)
  end

  @doc """
  Submits an I/O operation and blocks until the result is available.

  Wraps the operation's callback so the result is sent back to the calling
  process. Falls back to direct execution when the scheduler isn't running,
  which avoids breaking tests that don't start the IO subsystem.

  ## Options

    * `:timeout` — maximum wait time in milliseconds (default: 30_000)
  """
  @spec submit_sync(Operation.t(), keyword()) :: term()
  def submit_sync(%Operation{} = op, opts \\ []) do
    timeout = Keyword.get(opts, :timeout, 30_000)

    if scheduler_available?() do
      caller = self()
      ref = make_ref()

      wrapped_callback = fn ->
        try do
          result = op.callback.()
          send(caller, {:io_result, ref, result})
          result
        rescue
          e ->
            send(caller, {:io_result, ref, {:error, {:callback_exception, Exception.message(e)}}})
            reraise e, __STACKTRACE__
        catch
          kind, reason ->
            send(caller, {:io_result, ref, {:error, {:callback_exception, {kind, reason}}}})
            :erlang.raise(kind, reason, __STACKTRACE__)
        end
      end

      wrapped_op = %{op | callback: wrapped_callback}

      case submit(wrapped_op) do
        {:ok, _id} ->
          receive do
            {:io_result, ^ref, result} -> result
          after
            timeout -> {:error, :io_scheduler_timeout}
          end

        {:error, _} = error ->
          error
      end
    else
      op.callback.()
    end
  end

  @doc """
  Submits an I/O operation without awaiting the result.

  For fire-and-forget callers (repair, cache). Falls back to spawning
  the callback directly when the scheduler isn't running.
  """
  @spec submit_async(Operation.t()) :: :ok
  def submit_async(%Operation{} = op) do
    if scheduler_available?() do
      case submit(op) do
        {:ok, _id} -> :ok
        {:error, _} -> spawn(fn -> op.callback.() end)
      end
    else
      spawn(fn -> op.callback.() end)
    end

    :ok
  end

  @doc """
  Returns whether the I/O scheduler process is running.
  """
  @spec scheduler_available?() :: boolean()
  def scheduler_available? do
    Process.whereis(__MODULE__) != nil
  end

  @doc """
  Starts a drive worker for the given drive.

  ## Options

    * `:drive_id` (required) — unique identifier for the drive
    * `:drive_type` — `:hdd` or `:ssd` (default: `:ssd`)
    * `:producer` — producer to subscribe to (default: `NeonFS.IO.Producer`)
    * `:strategy_config` — keyword options passed to the drive strategy
  """
  @spec start_worker(keyword()) :: DynamicSupervisor.on_start_child()
  def start_worker(opts) do
    WorkerSupervisor.start_worker(opts)
  end

  @doc """
  Stops the drive worker for the given drive ID.
  """
  @spec stop_worker(String.t()) :: :ok | {:error, :not_found}
  def stop_worker(drive_id) do
    WorkerSupervisor.stop_worker(drive_id)
  end

  defp name, do: __MODULE__

  ## Server Callbacks

  @impl true
  def init(opts) do
    producer = Keyword.get(opts, :producer, Producer)

    state = %{
      producer: producer,
      operations: %{}
    }

    {:ok, state}
  end

  @impl true
  def handle_call({:submit, op, _opts}, _from, state) do
    Producer.enqueue(state.producer, op)
    operations = Map.put(state.operations, op.id, true)

    :telemetry.execute(
      [:neonfs, :io, :scheduler, :submit],
      %{},
      %{operation_id: op.id, priority: op.priority, drive_id: op.drive_id}
    )

    {:reply, {:ok, op.id}, %{state | operations: operations}}
  end

  def handle_call({:cancel, operation_id}, _from, state) do
    Producer.cancel(state.producer, operation_id)
    operations = Map.delete(state.operations, operation_id)

    :telemetry.execute(
      [:neonfs, :io, :scheduler, :cancel],
      %{},
      %{operation_id: operation_id}
    )

    {:reply, :ok, %{state | operations: operations}}
  end

  def handle_call(:status, _from, state) do
    result = Producer.status(state.producer)
    {:reply, result, state}
  end
end
