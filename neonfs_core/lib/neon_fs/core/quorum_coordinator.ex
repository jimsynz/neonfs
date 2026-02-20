defmodule NeonFS.Core.QuorumCoordinator do
  @moduledoc """
  Leaderless quorum coordinator for metadata operations.

  Routes metadata operations through the consistent hashing ring, dispatches
  to replica sets, and enforces quorum requirements (R+W>N). Stale replicas
  detected during quorum reads trigger async read repair via the
  BackgroundWorker infrastructure.

  ## Quorum Configuration

  Default: N=3, R=2, W=2. Override per-call via opts or per-volume via
  `volume.metadata_consistency`.

  ## Degraded Reads

  When quorum is unreachable, falls back to a local MetadataStore replica
  if available. Returns `{:ok, value, :possibly_stale}` so callers can
  decide whether to serve stale data. Degraded reads are enabled by default
  and never used for writes.

  ## Dependency Injection

  All external dependencies are injectable via opts for testability:

    * `:write_fn` — `(node, segment_id, key, value) -> :ok | {:error, reason}`
    * `:read_fn` — `(node, segment_id, key) -> {:ok, value, timestamp} | {:error, :not_found}`
    * `:delete_fn` — `(node, segment_id, key) -> :ok | {:error, reason}`
    * `:quarantine_checker` — `(node) -> boolean()`
    * `:read_repair_fn` — `((() -> term()), keyword()) -> {:ok, term()}`
    * `:local_node` — `node()` override for testing
  """

  require Logger

  alias NeonFS.Core.BackgroundWorker
  alias NeonFS.Core.ClockMonitor
  alias NeonFS.Core.HLC
  alias NeonFS.Core.MetadataRing
  alias NeonFS.Core.MetadataStore

  @default_replicas 3
  @default_read_quorum 2
  @default_write_quorum 2
  @default_timeout_ms 5_000

  @type key :: String.t()
  @type quorum_config :: %{
          replicas: pos_integer(),
          read_quorum: pos_integer(),
          write_quorum: pos_integer()
        }

  ## Public API

  @doc """
  Writes a value via quorum.

  Hashes the key to locate segment and replica set via MetadataRing, then
  dispatches writes to all replicas in parallel and awaits `write_quorum`
  acknowledgements.

  ## Options

    * `:ring` — MetadataRing for routing (required)
    * `:replicas` — N (default: #{@default_replicas})
    * `:read_quorum` — R (default: #{@default_read_quorum})
    * `:write_quorum` — W (default: #{@default_write_quorum})
    * `:timeout` — per-replica timeout in ms (default: #{@default_timeout_ms})
    * `:write_fn` — custom write dispatcher for testing
    * `:quarantine_checker` — custom quarantine check for testing
    * `:local_node` — local node override for testing

  ## Returns

    * `{:ok, :written}` — write quorum met
    * `{:error, :quorum_unavailable}` — fewer than W replicas acknowledged
    * `{:error, :node_quarantined}` — local node is quarantined
  """
  @spec quorum_write(key(), term(), keyword()) :: {:ok, :written} | {:error, term()}
  def quorum_write(key, value, opts \\ []) do
    with :ok <- check_not_quarantined(opts) do
      ring = Keyword.fetch!(opts, :ring)
      {segment_id, replicas} = MetadataRing.locate(ring, key)
      config = resolve_quorum_config(opts, length(replicas))
      timeout = Keyword.get(opts, :timeout, @default_timeout_ms)

      # Generate a coordinator-level timestamp BEFORE dispatching to replicas.
      # All replicas use this same timestamp, preventing late-arriving writes
      # from an earlier quorum_write (whose task was killed after W met) from
      # overwriting a logically-later write with a higher per-node HLC timestamp.
      caller_timestamp = generate_write_timestamp(opts)
      write_opts = Keyword.put(opts, :caller_timestamp, caller_timestamp)

      start_time = System.monotonic_time(:millisecond)

      write_satisfied? = fn results, pending ->
        ok_count = Enum.count(results, &match?(:ok, &1))
        ok_count >= config.write_quorum or ok_count + pending < config.write_quorum
      end

      responses =
        dispatch_parallel(
          replicas,
          &dispatch_write(&1, segment_id, key, value, write_opts),
          timeout,
          write_satisfied?
        )

      latency = System.monotonic_time(:millisecond) - start_time
      result = evaluate_write_quorum(responses, config.write_quorum)

      :telemetry.execute(
        [:neonfs, :quorum, :write],
        %{latency_ms: latency},
        %{segment_id: segment_id, quorum_size: config.write_quorum}
      )

      result
    end
  end

  @doc """
  Reads a value via quorum.

  Hashes the key to locate segment and replica set, reads from R replicas,
  returns the value with the highest HLC timestamp, and triggers async read
  repair for any stale replicas detected.

  ## Options

  Same as `quorum_write/3`, plus:

    * `:degraded_reads` — allow fallback to local replica (default: true)
    * `:read_fn` — custom read dispatcher for testing
    * `:read_repair_fn` — custom read repair submission for testing

  ## Returns

    * `{:ok, value}` — read quorum met, returning latest value
    * `{:ok, value, :possibly_stale}` — degraded read from local replica
    * `{:error, :not_found}` — majority of replicas report not found
    * `{:error, :quorum_unavailable}` — quorum unreachable and no local fallback
  """
  @spec quorum_read(key(), keyword()) ::
          {:ok, term()}
          | {:ok, term(), :possibly_stale}
          | {:error, :not_found}
          | {:error, :quorum_unavailable}
  def quorum_read(key, opts \\ []) do
    ring = Keyword.fetch!(opts, :ring)
    {segment_id, replicas} = MetadataRing.locate(ring, key)
    config = resolve_quorum_config(opts, length(replicas))
    timeout = Keyword.get(opts, :timeout, @default_timeout_ms)

    start_time = System.monotonic_time(:millisecond)

    read_satisfied? = fn results, pending ->
      {successes, not_founds, _} = categorise_read_responses(results)
      sc = length(successes)
      nfc = length(not_founds)

      sc >= config.read_quorum or
        nfc >= config.read_quorum or
        (sc + pending < config.read_quorum and nfc + pending < config.read_quorum)
    end

    responses =
      dispatch_parallel(
        replicas,
        fn node -> {node, dispatch_read(node, segment_id, key, opts)} end,
        timeout,
        read_satisfied?
      )

    latency = System.monotonic_time(:millisecond) - start_time
    result = evaluate_read_quorum(responses, config, key, segment_id, opts)

    :telemetry.execute(
      [:neonfs, :quorum, :read],
      %{latency_ms: latency},
      %{segment_id: segment_id, quorum_size: config.read_quorum}
    )

    result
  end

  @doc """
  Deletes a key via quorum (tombstone write).

  Dispatches MetadataStore.delete to all replicas and awaits write quorum.
  Same options and return values as `quorum_write/3`.
  """
  @spec quorum_delete(key(), keyword()) :: {:ok, :written} | {:error, term()}
  def quorum_delete(key, opts \\ []) do
    with :ok <- check_not_quarantined(opts) do
      ring = Keyword.fetch!(opts, :ring)
      {segment_id, replicas} = MetadataRing.locate(ring, key)
      config = resolve_quorum_config(opts, length(replicas))
      timeout = Keyword.get(opts, :timeout, @default_timeout_ms)

      start_time = System.monotonic_time(:millisecond)

      delete_satisfied? = fn results, pending ->
        ok_count = Enum.count(results, &match?(:ok, &1))
        ok_count >= config.write_quorum or ok_count + pending < config.write_quorum
      end

      responses =
        dispatch_parallel(
          replicas,
          &dispatch_delete(&1, segment_id, key, opts),
          timeout,
          delete_satisfied?
        )

      latency = System.monotonic_time(:millisecond) - start_time
      result = evaluate_write_quorum(responses, config.write_quorum)

      :telemetry.execute(
        [:neonfs, :quorum, :write],
        %{latency_ms: latency},
        %{segment_id: segment_id, quorum_size: config.write_quorum}
      )

      result
    end
  end

  ## Private — Quorum config resolution

  defp resolve_quorum_config(opts, replica_count) do
    n = Keyword.get(opts, :replicas, @default_replicas)
    r = Keyword.get(opts, :read_quorum, @default_read_quorum)
    w = Keyword.get(opts, :write_quorum, @default_write_quorum)

    effective_n = min(n, replica_count)
    effective_r = min(r, effective_n)
    effective_w = min(w, effective_n)

    %{replicas: effective_n, read_quorum: effective_r, write_quorum: effective_w}
  end

  ## Private — Parallel dispatch

  defp dispatch_parallel(replicas, fun, timeout, satisfied?) do
    tasks = Enum.map(replicas, fn node -> Task.async(fn -> fun.(node) end) end)
    deadline = System.monotonic_time(:millisecond) + timeout + 500
    collect_until_satisfied(tasks, satisfied?, [], deadline)
  end

  defp collect_until_satisfied([], _satisfied?, results, _deadline), do: results

  defp collect_until_satisfied(remaining_tasks, satisfied?, results, deadline) do
    remaining_ms = deadline - System.monotonic_time(:millisecond)

    if remaining_ms <= 0 do
      shutdown_remaining(remaining_tasks, results)
    else
      poll_and_collect(remaining_tasks, satisfied?, results, deadline, remaining_ms)
    end
  end

  defp poll_and_collect(remaining_tasks, satisfied?, results, deadline, remaining_ms) do
    poll_ms = min(100, remaining_ms)
    batch = Task.yield_many(remaining_tasks, timeout: poll_ms)

    {yielded, still_pending} = Enum.split_with(batch, fn {_t, r} -> r != nil end)
    new_results = Enum.map(yielded, &extract_yield_result/1)
    all_results = results ++ new_results
    pending_tasks = Enum.map(still_pending, fn {task, nil} -> task end)

    if satisfied?.(all_results, length(pending_tasks)) or pending_tasks == [] do
      shutdown_remaining(pending_tasks, all_results)
    else
      collect_until_satisfied(pending_tasks, satisfied?, all_results, deadline)
    end
  end

  defp shutdown_remaining(tasks, results) do
    Enum.each(tasks, &Task.shutdown(&1, :brutal_kill))
    results ++ List.duplicate({:error, :timeout}, length(tasks))
  end

  defp extract_yield_result({_task, {:ok, result}}), do: result

  defp extract_yield_result({task, nil}) do
    Task.shutdown(task, :brutal_kill)
    {:error, :timeout}
  end

  defp extract_yield_result({_task, {:exit, reason}}), do: {:error, reason}

  ## Private — Write dispatch

  defp dispatch_write(node, segment_id, key, value, opts) do
    case Keyword.get(opts, :write_fn) do
      nil -> default_write(node, segment_id, key, value, opts)
      write_fn -> write_fn.(node, segment_id, key, value)
    end
  rescue
    e -> {:error, Exception.message(e)}
  catch
    :exit, reason -> {:error, reason}
  end

  defp default_write(node, segment_id, key, value, opts) do
    local_node = Keyword.get(opts, :local_node, Node.self())
    write_opts = build_write_opts(opts)

    if node == local_node do
      MetadataStore.write(segment_id, key, value, write_opts)
    else
      timeout = Keyword.get(opts, :timeout, @default_timeout_ms)
      :erpc.call(node, MetadataStore, :write, [segment_id, key, value, write_opts], timeout)
    end
  end

  ## Private — Read dispatch

  defp dispatch_read(node, segment_id, key, opts) do
    case Keyword.get(opts, :read_fn) do
      nil -> default_read(node, segment_id, key, opts)
      read_fn -> read_fn.(node, segment_id, key)
    end
  rescue
    e -> {:error, Exception.message(e)}
  catch
    :exit, reason -> {:error, reason}
  end

  defp default_read(node, segment_id, key, opts) do
    local_node = Keyword.get(opts, :local_node, Node.self())

    if node == local_node do
      MetadataStore.read(segment_id, key, metadata_store_opts(opts))
    else
      timeout = Keyword.get(opts, :timeout, @default_timeout_ms)
      :erpc.call(node, MetadataStore, :read, [segment_id, key, []], timeout)
    end
  end

  ## Private — Delete dispatch

  defp dispatch_delete(node, segment_id, key, opts) do
    case Keyword.get(opts, :delete_fn) do
      nil -> default_delete(node, segment_id, key, opts)
      delete_fn -> delete_fn.(node, segment_id, key)
    end
  rescue
    e -> {:error, Exception.message(e)}
  catch
    :exit, reason -> {:error, reason}
  end

  defp default_delete(node, segment_id, key, opts) do
    local_node = Keyword.get(opts, :local_node, Node.self())

    if node == local_node do
      MetadataStore.delete(segment_id, key, metadata_store_opts(opts))
    else
      timeout = Keyword.get(opts, :timeout, @default_timeout_ms)
      :erpc.call(node, MetadataStore, :delete, [segment_id, key, []], timeout)
    end
  end

  defp metadata_store_opts(opts) do
    case Keyword.get(opts, :metadata_store) do
      nil -> []
      server -> [server: server]
    end
  end

  defp generate_write_timestamp(opts) do
    case Keyword.get(opts, :write_fn) do
      nil -> MetadataStore.generate_timestamp(metadata_store_opts(opts))
      _custom_fn -> {System.system_time(:millisecond), 0, Node.self()}
    end
  end

  defp build_write_opts(opts) do
    base = metadata_store_opts(opts)

    case Keyword.get(opts, :caller_timestamp) do
      nil -> base
      ts -> Keyword.put(base, :caller_timestamp, ts)
    end
  end

  ## Private — Write quorum evaluation

  defp evaluate_write_quorum(responses, write_quorum) do
    successes = Enum.count(responses, &match?(:ok, &1))

    if successes >= write_quorum do
      {:ok, :written}
    else
      {:error, :quorum_unavailable}
    end
  end

  ## Private — Read quorum evaluation

  defp evaluate_read_quorum(responses, config, key, segment_id, opts) do
    {successes, not_founds, _failures} = categorise_read_responses(responses)

    cond do
      length(successes) >= config.read_quorum ->
        handle_successful_quorum(successes, key, segment_id, opts)

      length(not_founds) >= config.read_quorum ->
        {:error, :not_found}

      true ->
        handle_degraded_read(successes, opts)
    end
  end

  defp categorise_read_responses(responses) do
    Enum.reduce(responses, {[], [], []}, fn
      {node, {:ok, value, timestamp}}, {s, nf, f} ->
        {[{node, value, timestamp} | s], nf, f}

      {node, {:error, :not_found}}, {s, nf, f} ->
        {s, [node | nf], f}

      {_node, {:error, _reason}}, {s, nf, f} ->
        {s, nf, [true | f]}

      {:error, _reason}, {s, nf, f} ->
        {s, nf, [true | f]}
    end)
  end

  defp handle_successful_quorum(successes, key, segment_id, opts) do
    {_node, latest_value, latest_timestamp} =
      Enum.max_by(successes, fn {_node, _value, ts} -> ts end, fn ->
        {nil, nil, {0, 0, nil}}
      end)

    stale_replicas =
      successes
      |> Enum.filter(fn {_node, _value, ts} -> HLC.compare(ts, latest_timestamp) == :lt end)
      |> Enum.map(fn {node, _value, _ts} -> node end)

    if stale_replicas != [] do
      trigger_read_repair(key, segment_id, latest_value, stale_replicas, opts)
    end

    {:ok, latest_value}
  end

  defp handle_degraded_read(successes, opts) do
    degraded_enabled = Keyword.get(opts, :degraded_reads, true)
    local_node = Keyword.get(opts, :local_node, Node.self())

    local_success =
      Enum.find(successes, fn {node, _v, _ts} -> node == local_node end)

    if degraded_enabled && local_success != nil do
      {_node, value, _ts} = local_success
      {:ok, value, :possibly_stale}
    else
      {:error, :quorum_unavailable}
    end
  end

  ## Private — Read repair

  defp trigger_read_repair(key, segment_id, latest_value, stale_replicas, opts) do
    :telemetry.execute(
      [:neonfs, :quorum, :stale_detected],
      %{stale_count: length(stale_replicas)},
      %{segment_id: segment_id, key: key}
    )

    read_repair_fn =
      Keyword.get(opts, :read_repair_fn, fn work_fn, work_opts ->
        BackgroundWorker.submit(work_fn, work_opts)
      end)

    Enum.each(stale_replicas, fn node ->
      read_repair_fn.(
        fn -> dispatch_write(node, segment_id, key, latest_value, opts) end,
        priority: :high,
        label: "read_repair"
      )
    end)
  end

  ## Private — Quarantine check

  defp check_not_quarantined(opts) do
    checker = Keyword.get(opts, :quarantine_checker, &ClockMonitor.quarantined?/1)
    local_node = Keyword.get(opts, :local_node, Node.self())

    if checker.(local_node) do
      {:error, :node_quarantined}
    else
      :ok
    end
  end
end
