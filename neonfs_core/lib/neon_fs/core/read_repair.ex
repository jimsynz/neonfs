defmodule NeonFS.Core.ReadRepair do
  @moduledoc """
  Async read repair for stale metadata replicas.

  When a quorum read detects that some replicas have older values than the
  latest, ReadRepair submits background jobs to update them. Repairs are
  coalesced: multiple requests for the same key within a short window are
  merged into a single repair operation.

  Repairs are fire-and-forget from the caller's perspective — failures are
  logged but never block reads. If a stale replica is unreachable, the
  anti-entropy process (task 0090) will catch it later.

  ## Telemetry Events

    * `[:neonfs, :read_repair, :submitted]` — repair job submitted
    * `[:neonfs, :read_repair, :completed]` — repair job succeeded
    * `[:neonfs, :read_repair, :failed]` — repair job failed
  """

  use GenServer
  require Logger

  alias NeonFS.Core.BackgroundWorker
  alias NeonFS.Core.MetadataStore

  @default_coalesce_window_ms 1_000

  @type key :: String.t()
  @type segment_id :: binary()

  ## Client API

  @doc """
  Starts the ReadRepair GenServer.

  ## Options

    * `:coalesce_window_ms` — time window for coalescing duplicate repairs
      (default: #{@default_coalesce_window_ms})
    * `:name` — GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Submits an async repair for stale replicas.

  Checks the coalescing window — if a repair for the same key was recently
  submitted, this request is silently dropped. Otherwise, submits one
  BackgroundWorker job per stale replica.

  ## Parameters

    * `key` — the metadata key being repaired
    * `repair_context` — map with `:segment_id`, `:latest_value`, and `:opts`
      (QuorumCoordinator write opts for dispatching)
    * `stale_replicas` — list of nodes that need updating
  """
  @spec submit(key(), map(), [node()]) :: :ok
  def submit(key, repair_context, stale_replicas) do
    GenServer.cast(__MODULE__, {:submit, key, repair_context, stale_replicas})
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    coalesce_window_ms = Keyword.get(opts, :coalesce_window_ms, @default_coalesce_window_ms)

    {:ok,
     %{
       recent_repairs: %{},
       coalesce_window_ms: coalesce_window_ms
     }}
  end

  @impl true
  def handle_cast({:submit, key, repair_context, stale_replicas}, state) do
    now = System.monotonic_time(:millisecond)
    state = prune_expired(state, now)

    if recently_repaired?(state, key, now) do
      {:noreply, state}
    else
      do_submit_repairs(key, repair_context, stale_replicas)
      new_recent = Map.put(state.recent_repairs, key, now + state.coalesce_window_ms)
      {:noreply, %{state | recent_repairs: new_recent}}
    end
  end

  @impl true
  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## Private

  defp recently_repaired?(state, key, now) do
    case Map.get(state.recent_repairs, key) do
      nil -> false
      expires_at -> now < expires_at
    end
  end

  defp prune_expired(state, now) do
    pruned =
      state.recent_repairs
      |> Enum.reject(fn {_key, expires_at} -> now >= expires_at end)
      |> Map.new()

    %{state | recent_repairs: pruned}
  end

  defp do_submit_repairs(key, repair_context, stale_replicas) do
    segment_id = Map.get(repair_context, :segment_id)
    latest_value = Map.get(repair_context, :latest_value)
    write_opts = Map.get(repair_context, :opts, [])

    Enum.each(stale_replicas, fn node ->
      work_fn = build_repair_fn(node, segment_id, key, latest_value, write_opts)

      {:ok, _work_id} =
        BackgroundWorker.submit(work_fn, priority: :high, label: "read_repair:#{key}")

      :telemetry.execute(
        [:neonfs, :read_repair, :submitted],
        %{stale_count: length(stale_replicas)},
        %{key: key, node: node}
      )
    end)
  end

  defp build_repair_fn(node, segment_id, key, latest_value, write_opts) do
    fn ->
      result = dispatch_repair_write(node, segment_id, key, latest_value, write_opts)
      emit_repair_result_telemetry(result, key, node)
      result
    end
  end

  defp emit_repair_result_telemetry(:ok, key, node) do
    :telemetry.execute([:neonfs, :read_repair, :completed], %{}, %{key: key, node: node})
  end

  defp emit_repair_result_telemetry({:error, reason}, key, node) do
    Logger.debug("Read repair failed", node: node, reason: inspect(reason))

    :telemetry.execute([:neonfs, :read_repair, :failed], %{}, %{
      key: key,
      node: node,
      reason: reason
    })
  end

  defp dispatch_repair_write(node, segment_id, key, value, opts) do
    local_node = Keyword.get(opts, :local_node, Node.self())

    if node == local_node do
      MetadataStore.write(segment_id, key, value)
    else
      timeout = Keyword.get(opts, :timeout, 5_000)

      try do
        :erpc.call(node, MetadataStore, :write, [segment_id, key, value, []], timeout)
      catch
        :exit, reason -> {:error, reason}
      end
    end
  end
end
