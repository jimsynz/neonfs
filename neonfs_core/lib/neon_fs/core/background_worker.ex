defmodule NeonFS.Core.BackgroundWorker do
  @moduledoc """
  Node-local background task runner with priority queues, rate limiting,
  and load-aware yielding.

  Provides centralised scheduling for maintenance operations like tier
  migrations, scrubbing, and rebalancing. Uses `Task.Supervisor` for crash
  isolation so failed tasks don't bring down the worker.

  ## Configuration

    * `:max_concurrent` — maximum tasks running at once (default: 2)
    * `:max_per_minute` — rate limit on task starts (default: 10)
    * `:load_threshold` — scheduler utilisation above which new work pauses (default: 0.8)
    * `:check_interval_ms` — how often to poll the queue (default: 500)
    * `:task_supervisor` — name of the `Task.Supervisor` (default: `NeonFS.Core.BackgroundTaskSupervisor`)

  ## Telemetry Events

    * `[:neonfs, :background_worker, :submit]` — work submitted
    * `[:neonfs, :background_worker, :start]` — work started
    * `[:neonfs, :background_worker, :complete]` — work completed
    * `[:neonfs, :background_worker, :fail]` — work failed
    * `[:neonfs, :background_worker, :cancel]` — work cancelled
  """

  use GenServer
  require Logger

  @type priority :: :high | :normal | :low
  @type work_id :: String.t()
  @type work_status :: :queued | :running | :completed | :cancelled | :failed

  @priority_order [:high, :normal, :low]

  ## Client API

  @doc """
  Starts the BackgroundWorker GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Submits a function for background execution.

  ## Options

    * `:priority` — `:high`, `:normal` (default), or `:low`
    * `:label` — human-readable label for logging

  ## Returns

    * `{:ok, work_id}` — the submitted work's unique identifier
  """
  @spec submit((-> term()), keyword()) :: {:ok, work_id()}
  def submit(work_fn, opts \\ []) when is_function(work_fn, 0) do
    GenServer.call(__MODULE__, {:submit, work_fn, opts})
  end

  @doc """
  Cancels a queued or running work item.

  Queued items are removed from the queue. Running items are left to
  complete but marked as cancelled (their result is discarded).

  ## Returns

    * `:ok` — work found and cancelled/marked
    * `{:error, :not_found}` — no work with this ID
  """
  @spec cancel(work_id()) :: :ok | {:error, :not_found}
  def cancel(work_id) do
    GenServer.call(__MODULE__, {:cancel, work_id})
  end

  @doc """
  Returns overall worker status.
  """
  @spec status() :: %{
          queued: non_neg_integer(),
          running: non_neg_integer(),
          completed: non_neg_integer(),
          by_priority: %{priority() => non_neg_integer()}
        }
  def status do
    GenServer.call(__MODULE__, :status)
  end

  @doc """
  Returns the status of a specific work item.
  """
  @spec status(work_id()) :: work_status() | {:error, :not_found}
  def status(work_id) do
    GenServer.call(__MODULE__, {:status, work_id})
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)

    # Enable scheduler wall time statistics
    :erlang.system_flag(:scheduler_wall_time, true)

    task_supervisor =
      Keyword.get(opts, :task_supervisor, NeonFS.Core.BackgroundTaskSupervisor)

    state = %{
      queues: %{high: :queue.new(), normal: :queue.new(), low: :queue.new()},
      running: %{},
      work_registry: %{},
      max_concurrent: Keyword.get(opts, :max_concurrent, 2),
      max_per_minute: Keyword.get(opts, :max_per_minute, 10),
      load_threshold: Keyword.get(opts, :load_threshold, 0.8),
      check_interval_ms: Keyword.get(opts, :check_interval_ms, 500),
      task_supervisor: task_supervisor,
      completed_count: 0,
      starts_this_minute: [],
      last_wall_time: nil
    }

    schedule_check(state)

    Logger.info("BackgroundWorker started (max_concurrent=#{state.max_concurrent})")
    {:ok, state}
  end

  @impl true
  def handle_call({:submit, work_fn, opts}, _from, state) do
    priority = Keyword.get(opts, :priority, :normal)
    label = Keyword.get(opts, :label, "background_work")
    work_id = generate_work_id()

    work = %{
      id: work_id,
      fn: work_fn,
      priority: priority,
      label: label,
      submitted_at: System.monotonic_time(:millisecond)
    }

    state = enqueue(state, priority, work)
    state = put_in(state.work_registry[work_id], :queued)

    :telemetry.execute(
      [:neonfs, :background_worker, :submit],
      %{},
      %{work_id: work_id, priority: priority, label: label}
    )

    {:reply, {:ok, work_id}, state}
  end

  def handle_call({:cancel, work_id}, _from, state) do
    case Map.get(state.work_registry, work_id) do
      nil ->
        {:reply, {:error, :not_found}, state}

      :queued ->
        state = remove_from_queues(state, work_id)
        state = put_in(state.work_registry[work_id], :cancelled)

        :telemetry.execute(
          [:neonfs, :background_worker, :cancel],
          %{},
          %{work_id: work_id}
        )

        {:reply, :ok, state}

      :running ->
        # Mark as cancelled; the task completion handler will discard the result
        state = put_in(state.work_registry[work_id], :cancelled)

        :telemetry.execute(
          [:neonfs, :background_worker, :cancel],
          %{},
          %{work_id: work_id}
        )

        {:reply, :ok, state}

      _terminal ->
        {:reply, {:error, :not_found}, state}
    end
  end

  def handle_call(:status, _from, state) do
    queued = count_queued(state)
    running = map_size(state.running)

    by_priority =
      Map.new(@priority_order, fn p ->
        {p, :queue.len(state.queues[p])}
      end)

    result = %{
      queued: queued,
      running: running,
      completed: state.completed_count,
      by_priority: by_priority
    }

    {:reply, result, state}
  end

  def handle_call({:status, work_id}, _from, state) do
    case Map.get(state.work_registry, work_id) do
      nil -> {:reply, {:error, :not_found}, state}
      status -> {:reply, status, state}
    end
  end

  @impl true
  def handle_info(:check_queue, state) do
    state = maybe_start_work(state)
    schedule_check(state)
    {:noreply, state}
  end

  def handle_info({ref, result}, state) when is_reference(ref) do
    # Task completed successfully
    Process.demonitor(ref, [:flush])
    state = handle_task_result(state, ref, {:ok, result})
    {:noreply, state}
  end

  def handle_info({:DOWN, ref, :process, _pid, reason}, state) do
    # Task crashed
    state = handle_task_result(state, ref, {:error, reason})
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, state) do
    # Cancel all queued work
    Enum.each(@priority_order, fn priority ->
      drain_queue(state.queues[priority])
    end)

    # Wait briefly for running tasks
    if map_size(state.running) > 0 do
      Logger.info(
        "BackgroundWorker shutting down, waiting for #{map_size(state.running)} running tasks"
      )

      Process.sleep(min(map_size(state.running) * 1000, 5000))
    end

    :ok
  end

  ## Private — Queue management

  defp enqueue(state, priority, work) do
    queue = state.queues[priority]
    put_in(state.queues[priority], :queue.in(work, queue))
  end

  defp dequeue_next(state) do
    Enum.find_value(@priority_order, fn priority ->
      queue = state.queues[priority]

      case :queue.out(queue) do
        {{:value, work}, new_queue} ->
          {work, put_in(state.queues[priority], new_queue)}

        {:empty, _} ->
          nil
      end
    end)
  end

  defp remove_from_queues(state, work_id) do
    queues =
      Map.new(@priority_order, fn priority ->
        queue = state.queues[priority]
        filtered = :queue.filter(fn work -> work.id != work_id end, queue)
        {priority, filtered}
      end)

    %{state | queues: queues}
  end

  defp count_queued(state) do
    Enum.reduce(@priority_order, 0, fn p, acc ->
      acc + :queue.len(state.queues[p])
    end)
  end

  defp drain_queue(queue) do
    case :queue.out(queue) do
      {{:value, _work}, rest} -> drain_queue(rest)
      {:empty, _} -> :ok
    end
  end

  ## Private — Work execution

  defp maybe_start_work(state) do
    cond do
      map_size(state.running) >= state.max_concurrent ->
        state

      rate_limited?(state) ->
        state

      load_too_high?(state) ->
        state

      true ->
        case dequeue_next(state) do
          nil ->
            state

          {work, state} ->
            start_work(state, work)
        end
    end
  end

  defp start_work(state, work) do
    # Check if this work was cancelled while queued
    case Map.get(state.work_registry, work.id) do
      :queued ->
        do_start_work(state, work)

      _other ->
        # Skip cancelled work, try next
        maybe_start_work(state)
    end
  end

  defp do_start_work(state, work) do
    task =
      Task.Supervisor.async_nolink(state.task_supervisor, fn ->
        work.fn.()
      end)

    state = put_in(state.running[task.ref], work)
    state = put_in(state.work_registry[work.id], :running)
    state = record_start(state)

    :telemetry.execute(
      [:neonfs, :background_worker, :start],
      %{},
      %{work_id: work.id, priority: work.priority, label: work.label}
    )

    # Try to start more work if slots available
    maybe_start_work(state)
  end

  defp handle_task_result(state, ref, result) do
    case Map.pop(state.running, ref) do
      {nil, _running} ->
        state

      {work, running} ->
        state = %{state | running: running}
        handle_work_completion(state, work, result)
    end
  end

  defp handle_work_completion(state, work, result) do
    current_status = Map.get(state.work_registry, work.id)

    case {current_status, result} do
      {:cancelled, _} ->
        # Work was cancelled while running, discard result
        state

      {_, {:ok, _value}} ->
        state = put_in(state.work_registry[work.id], :completed)
        state = %{state | completed_count: state.completed_count + 1}

        :telemetry.execute(
          [:neonfs, :background_worker, :complete],
          %{},
          %{work_id: work.id, label: work.label}
        )

        state

      {_, {:error, reason}} ->
        state = put_in(state.work_registry[work.id], :failed)

        Logger.warning("Background work #{work.id} (#{work.label}) failed: #{inspect(reason)}")

        :telemetry.execute(
          [:neonfs, :background_worker, :fail],
          %{},
          %{work_id: work.id, label: work.label, reason: reason}
        )

        state
    end
  end

  ## Private — Rate limiting

  defp rate_limited?(state) do
    now = System.monotonic_time(:millisecond)
    one_minute_ago = now - 60_000
    recent_starts = Enum.count(state.starts_this_minute, &(&1 > one_minute_ago))
    recent_starts >= state.max_per_minute
  end

  defp record_start(state) do
    now = System.monotonic_time(:millisecond)
    one_minute_ago = now - 60_000
    # Prune old entries and add new
    starts = [now | Enum.filter(state.starts_this_minute, &(&1 > one_minute_ago))]
    %{state | starts_this_minute: starts}
  end

  ## Private — Load awareness

  defp load_too_high?(state) do
    utilisation = scheduler_utilisation(state)
    utilisation > state.load_threshold
  end

  defp scheduler_utilisation(state) do
    current = :erlang.statistics(:scheduler_wall_time_all)

    case state.last_wall_time do
      nil ->
        0.0

      prev ->
        calculate_utilisation(prev, current)
    end
  rescue
    _ -> 0.0
  end

  defp calculate_utilisation(prev, current) do
    pairs = Enum.zip(Enum.sort(prev), Enum.sort(current))

    {total_active, total_total} =
      Enum.reduce(pairs, {0, 0}, fn {{_id, a1, t1}, {_id2, a2, t2}}, {acc_a, acc_t} ->
        {acc_a + (a2 - a1), acc_t + (t2 - t1)}
      end)

    if total_total > 0 do
      total_active / total_total
    else
      0.0
    end
  end

  ## Private — Helpers

  defp schedule_check(state) do
    Process.send_after(self(), :check_queue, state.check_interval_ms)
  end

  defp generate_work_id do
    :crypto.strong_rand_bytes(8) |> Base.encode16(case: :lower)
  end
end
