defmodule NeonFS.Core.AntiEntropy do
  @moduledoc """
  Periodic anti-entropy for metadata segments.

  While quorum reads with read repair handle consistency for actively accessed
  data, rarely-accessed metadata can drift between replicas. Anti-entropy
  periodically compares Merkle trees between segment replicas and repairs any
  differences, ensuring all replicas eventually converge.

  ## How It Works

  1. Enumerate all segments where the local node is a replica
  2. For each segment, compute Merkle tree root hashes on all replicas
  3. If roots match, skip (no divergence)
  4. If roots differ, fetch full record lists from all replicas and reconcile:
     - Missing keys are replicated to the replicas that lack them
     - Stale values are updated with the latest (highest HLC timestamp)
  5. Tombstone cleanup: if all replicas agree a key is tombstoned, purge it

  ## Configuration

    * `:sync_interval_ms` — how often to run a full sync pass
      (default: 21_600_000 = 6 hours)
    * `:segments_per_cycle` — max segments to sync per cycle (default: 100)

  ## Telemetry Events

    * `[:neonfs, :anti_entropy, :started]` — sync started for a segment
    * `[:neonfs, :anti_entropy, :completed]` — sync completed for a segment
    * `[:neonfs, :anti_entropy, :tombstones_cleaned]` — tombstones purged
  """

  use GenServer
  require Logger

  alias NeonFS.Core.BackgroundWorker
  alias NeonFS.Core.BlobStore
  alias NeonFS.Core.DriveRegistry
  alias NeonFS.Core.HLC
  alias NeonFS.Core.MetadataCodec
  alias NeonFS.Core.MetadataRing
  alias NeonFS.Core.MetadataStore

  @default_sync_interval_ms 6 * 60 * 60 * 1_000
  @default_segments_per_cycle 100
  @sync_concurrency 8
  # Per-segment cap for the parallel sync pass: covers up to ~3 remote
  # `compare_merkle_trees` RPCs at the default 10s timeout each, plus
  # reconciliation headroom. A segment that exceeds this is killed and
  # accounted as `skipped: true` rather than blocking the pass (#668).
  @sync_segment_timeout_ms 60_000

  ## Client API

  @doc """
  Starts the AntiEntropy GenServer.

  ## Options

    * `:sync_interval_ms` — interval between full sync passes
      (default: #{@default_sync_interval_ms})
    * `:segments_per_cycle` — max segments to process per cycle
      (default: #{@default_segments_per_cycle})
    * `:ring_fn` — function returning the current MetadataRing (for testing)
    * `:local_node` — override local node identity (for testing)
    * `:name` — GenServer name (default: `__MODULE__`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Triggers an immediate sync pass.

  Returns a summary of the sync results.
  """
  @spec sync_now() :: map()
  def sync_now do
    GenServer.call(__MODULE__, :sync_now, 60_000)
  end

  @doc """
  Syncs a single segment across all its replicas.

  Compares Merkle trees, repairs divergent keys, and cleans up tombstones.

  ## Options

    * `:ring` — MetadataRing to use for replica lookup
    * `:local_node` — override local node identity
    * `:timeout` — RPC timeout in milliseconds (default: 10_000)
    * `:metadata_store_opts` — keyword list passed to MetadataStore calls
  """
  @spec sync_segment(MetadataRing.segment_id(), keyword()) :: map()
  def sync_segment(segment_id, opts \\ []) do
    do_sync_segment(segment_id, opts)
  end

  @doc """
  Returns current status and configuration.
  """
  @spec status() :: map()
  def status do
    GenServer.call(__MODULE__, :status)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    state = %{
      sync_interval_ms: Keyword.get(opts, :sync_interval_ms, @default_sync_interval_ms),
      segments_per_cycle: Keyword.get(opts, :segments_per_cycle, @default_segments_per_cycle),
      ring_fn: Keyword.get(opts, :ring_fn, &default_ring/0),
      local_node: Keyword.get(opts, :local_node, Node.self()),
      last_sync: nil,
      syncs_completed: 0,
      total_keys_repaired: 0,
      total_tombstones_cleaned: 0
    }

    schedule_sync(state)

    Logger.info(
      "AntiEntropy started (interval=#{state.sync_interval_ms}ms, segments_per_cycle=#{state.segments_per_cycle})"
    )

    {:ok, state}
  end

  @impl true
  def handle_call(:sync_now, _from, state) do
    result = run_sync_pass(state)
    state = update_stats(state, result)
    {:reply, result, state}
  end

  def handle_call(:status, _from, state) do
    result = %{
      sync_interval_ms: state.sync_interval_ms,
      segments_per_cycle: state.segments_per_cycle,
      last_sync: state.last_sync,
      syncs_completed: state.syncs_completed,
      total_keys_repaired: state.total_keys_repaired,
      total_tombstones_cleaned: state.total_tombstones_cleaned
    }

    {:reply, result, state}
  end

  @impl true
  def handle_info(:sync, state) do
    submit_background_sync(state)
    schedule_sync(state)
    {:noreply, state}
  end

  def handle_info({:sync_completed, result}, state) do
    state = update_stats(state, result)
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## Private — Scheduling

  defp schedule_sync(state) do
    Process.send_after(self(), :sync, state.sync_interval_ms)
  end

  defp submit_background_sync(state) do
    anti_entropy_pid = self()

    work_fn = fn ->
      result = run_sync_pass(state)
      send(anti_entropy_pid, {:sync_completed, result})
      result
    end

    BackgroundWorker.submit(work_fn, priority: :low, label: "anti_entropy:full_pass")
  end

  ## Private — Sync pass

  defp run_sync_pass(state) do
    ring = state.ring_fn.()
    local_node = state.local_node

    local_segments =
      ring
      |> MetadataRing.segments()
      |> Enum.filter(fn {_seg_id, replicas} -> local_node in replicas end)
      |> Enum.take(state.segments_per_cycle)

    # Process segments in parallel so a single slow / stalled remote RPC
    # cannot serialise the whole pass. Each segment has its own RPC
    # budget inside `do_sync_segment`; `Task.async_stream` gives the
    # outer pass a hard wall-clock cap independent of segment count
    # (#668). Tasks that time out are accounted as skipped rather than
    # bringing down the GenServer.
    results =
      local_segments
      |> Task.async_stream(
        fn {segment_id, _replicas} ->
          do_sync_segment(segment_id, ring: ring, local_node: local_node)
        end,
        max_concurrency: @sync_concurrency,
        timeout: @sync_segment_timeout_ms,
        on_timeout: :kill_task,
        ordered: false
      )
      |> Enum.map(fn
        {:ok, result} -> result
        {:exit, _reason} -> %{keys_repaired: 0, tombstones_cleaned: 0, skipped: true}
      end)

    %{
      segments_synced: length(results),
      keys_repaired: Enum.sum(Enum.map(results, & &1.keys_repaired)),
      tombstones_cleaned: Enum.sum(Enum.map(results, & &1.tombstones_cleaned)),
      segments_skipped: Enum.count(results, & &1.skipped),
      timestamp: System.system_time(:second)
    }
  end

  defp update_stats(state, result) do
    %{
      state
      | last_sync: result.timestamp,
        syncs_completed: state.syncs_completed + 1,
        total_keys_repaired: state.total_keys_repaired + result.keys_repaired,
        total_tombstones_cleaned: state.total_tombstones_cleaned + result.tombstones_cleaned
    }
  end

  ## Private — Single segment sync

  defp do_sync_segment(segment_id, opts) do
    ring = Keyword.get(opts, :ring) || default_ring()
    local_node = Keyword.get(opts, :local_node, Node.self())
    timeout = Keyword.get(opts, :timeout, 10_000)
    store_opts = Keyword.get(opts, :metadata_store_opts, [])

    {_seg, replicas} = find_segment_replicas(ring, segment_id)

    :telemetry.execute(
      [:neonfs, :anti_entropy, :started],
      %{},
      %{segment_id: segment_id, replicas: length(replicas)}
    )

    start_time = System.monotonic_time(:millisecond)

    result =
      case compare_merkle_trees(segment_id, replicas, local_node, timeout, store_opts) do
        {:all_match, _} ->
          # Check tombstones even when trees match — all replicas may agree on tombstones
          tombstones_cleaned =
            cleanup_tombstones(segment_id, replicas, local_node, timeout, store_opts)

          %{keys_repaired: 0, tombstones_cleaned: tombstones_cleaned, skipped: false}

        {:diverged, trees} ->
          repair_result =
            reconcile_segment(segment_id, replicas, trees, local_node, timeout, store_opts)

          repair_result

        {:error, _reason} ->
          %{keys_repaired: 0, tombstones_cleaned: 0, skipped: true}
      end

    duration_ms = System.monotonic_time(:millisecond) - start_time

    :telemetry.execute(
      [:neonfs, :anti_entropy, :completed],
      %{duration_ms: duration_ms},
      %{
        segment_id: segment_id,
        keys_repaired: result.keys_repaired,
        tombstones_cleaned: result.tombstones_cleaned
      }
    )

    result
  end

  ## Private — Merkle tree comparison

  defp compare_merkle_trees(segment_id, replicas, local_node, timeout, store_opts) do
    trees =
      replicas
      |> Enum.map(fn node ->
        {node, fetch_merkle_tree(node, segment_id, local_node, timeout, store_opts)}
      end)
      |> Enum.filter(fn {_node, result} -> match?({:ok, _, _}, result) end)

    case trees do
      [] ->
        {:error, :no_replicas_reachable}

      [{_node, {:ok, root, _count}} | rest] ->
        all_match = Enum.all?(rest, fn {_n, {:ok, r, _c}} -> r == root end)

        if all_match do
          {:all_match, trees}
        else
          {:diverged, trees}
        end
    end
  end

  defp fetch_merkle_tree(node, segment_id, local_node, timeout, store_opts) do
    if node == local_node do
      MetadataStore.merkle_tree(segment_id, store_opts)
    else
      try do
        :erpc.call(node, MetadataStore, :merkle_tree, [segment_id, store_opts], timeout)
      catch
        :exit, reason -> {:error, reason}
      end
    end
  end

  ## Private — Reconciliation

  defp reconcile_segment(segment_id, replicas, _trees, local_node, timeout, store_opts) do
    replica_records = fetch_replica_records(replicas, segment_id, local_node, timeout, store_opts)

    case replica_records do
      [] ->
        %{keys_repaired: 0, tombstones_cleaned: 0, skipped: true}

      _ ->
        {keys_repaired, tombstones_cleaned} =
          do_reconcile(segment_id, replica_records, local_node, timeout, store_opts)

        %{keys_repaired: keys_repaired, tombstones_cleaned: tombstones_cleaned, skipped: false}
    end
  end

  defp do_reconcile(segment_id, replica_records, local_node, timeout, store_opts) do
    # Build a union of all keys across all replicas
    all_keys =
      replica_records
      |> Enum.flat_map(fn {_node, records} -> Map.keys(records) end)
      |> Enum.uniq()

    {repaired, tombstones} =
      Enum.reduce(all_keys, {0, 0}, fn key_hash, {rep_acc, tomb_acc} ->
        records_per_node =
          Enum.map(replica_records, fn {node, records} ->
            {node, Map.get(records, key_hash)}
          end)

        handle_key_reconciliation(
          segment_id,
          key_hash,
          records_per_node,
          local_node,
          timeout,
          store_opts,
          {rep_acc, tomb_acc}
        )
      end)

    if tombstones > 0 do
      :telemetry.execute(
        [:neonfs, :anti_entropy, :tombstones_cleaned],
        %{count: tombstones},
        %{segment_id: segment_id}
      )
    end

    {repaired, tombstones}
  end

  defp handle_key_reconciliation(
         segment_id,
         key_hash,
         records_per_node,
         local_node,
         timeout,
         store_opts,
         {rep_acc, tomb_acc}
       ) do
    present = Enum.filter(records_per_node, fn {_node, rec} -> rec != nil end)
    missing = Enum.filter(records_per_node, fn {_node, rec} -> rec == nil end)

    # Find the latest record across all replicas
    {_winner_node, latest} = find_latest_record(present)

    cond do
      # All replicas agree this is tombstoned — purge it
      all_tombstoned?(records_per_node) ->
        purge_from_all(segment_id, key_hash, records_per_node, local_node, timeout, store_opts)
        {rep_acc, tomb_acc + 1}

      # Some replicas are missing this key entirely
      missing != [] ->
        repair_count =
          repair_missing_replicas(segment_id, key_hash, latest, missing, local_node, timeout)

        {rep_acc + repair_count, tomb_acc}

      # All have the key but some are stale
      true ->
        stale =
          Enum.filter(present, fn {_node, rec} ->
            HLC.compare(rec.hlc_timestamp, latest.hlc_timestamp) == :lt
          end)

        repair_count =
          repair_stale_replicas(segment_id, key_hash, latest, stale, local_node, timeout)

        {rep_acc + repair_count, tomb_acc}
    end
  end

  defp find_latest_record(present_records) do
    Enum.max_by(present_records, fn {_node, rec} -> rec.hlc_timestamp end, fn ->
      {nil, nil}
    end)
  end

  defp all_tombstoned?(records_per_node) do
    Enum.all?(records_per_node, fn
      {_node, nil} -> false
      {_node, rec} -> rec.tombstone == true
    end)
  end

  ## Private — Tombstone cleanup (when trees match)

  defp cleanup_tombstones(segment_id, replicas, local_node, timeout, store_opts) do
    replica_records = fetch_replica_records(replicas, segment_id, local_node, timeout, store_opts)

    case replica_records do
      [] ->
        0

      _ ->
        tombstone_keys = find_agreed_tombstones(replica_records)

        purge_agreed_tombstones(
          segment_id,
          tombstone_keys,
          replica_records,
          local_node,
          timeout,
          store_opts
        )
    end
  end

  defp find_agreed_tombstones(replica_records) do
    replica_records
    |> Enum.flat_map(fn {_node, records} -> Map.keys(records) end)
    |> Enum.uniq()
    |> Enum.filter(&unanimously_tombstoned?(&1, replica_records))
  end

  defp unanimously_tombstoned?(key_hash, replica_records) do
    replica_records
    |> Enum.map(fn {_node, records} -> Map.get(records, key_hash) end)
    |> Enum.all?(fn
      nil -> false
      rec -> rec.tombstone == true
    end)
  end

  defp purge_agreed_tombstones(
         segment_id,
         tombstone_keys,
         replica_records,
         local_node,
         timeout,
         store_opts
       ) do
    node_list = Enum.map(replica_records, fn {node, _} -> {node, nil} end)

    Enum.each(tombstone_keys, fn key_hash ->
      purge_from_all(segment_id, key_hash, node_list, local_node, timeout, store_opts)
    end)

    count = length(tombstone_keys)

    if count > 0 do
      :telemetry.execute(
        [:neonfs, :anti_entropy, :tombstones_cleaned],
        %{count: count},
        %{segment_id: segment_id}
      )
    end

    count
  end

  ## Private — Repair helpers

  defp repair_missing_replicas(segment_id, key_hash, latest, missing_nodes, local_node, timeout) do
    Enum.count(missing_nodes, fn {node, _} ->
      write_record_to_node(node, segment_id, key_hash, latest, local_node, timeout) == :ok
    end)
  end

  defp repair_stale_replicas(segment_id, key_hash, latest, stale_nodes, local_node, timeout) do
    Enum.count(stale_nodes, fn {node, _rec} ->
      write_record_to_node(node, segment_id, key_hash, latest, local_node, timeout) == :ok
    end)
  end

  defp write_record_to_node(node, segment_id, key_hash, record, local_node, timeout) do
    if node == local_node do
      write_raw_record(segment_id, key_hash, record)
    else
      try do
        :erpc.call(
          node,
          __MODULE__,
          :write_raw_record,
          [segment_id, key_hash, record],
          timeout
        )
      catch
        :exit, reason -> {:error, reason}
      end
    end
  end

  @doc false
  # Used internally by anti-entropy for raw record writes that preserve key_hash.
  @spec write_raw_record(binary(), binary(), map()) :: :ok | {:error, term()}
  def write_raw_record(segment_id, key_hash, record) do
    with {:ok, serialised} <- MetadataCodec.encode_record(record),
         {:ok, {}} <- BlobStore.write_metadata(segment_id, key_hash, serialised, select_drive()) do
      :ok
    end
  end

  defp purge_from_all(segment_id, key_hash, records_per_node, local_node, timeout, _store_opts) do
    Enum.each(records_per_node, fn {node, _rec} ->
      purge_key_on_node(node, segment_id, key_hash, local_node, timeout)
    end)
  end

  defp purge_key_on_node(node, segment_id, key_hash, local_node, timeout) do
    if node == local_node do
      purge_raw_tombstone(segment_id, key_hash)
    else
      try do
        :erpc.call(
          node,
          __MODULE__,
          :purge_raw_tombstone,
          [segment_id, key_hash],
          timeout
        )
      catch
        :exit, _ -> :ok
      end
    end
  end

  @doc false
  # Used internally by anti-entropy for raw tombstone purge by key_hash.
  @spec purge_raw_tombstone(binary(), binary()) :: :ok | {:error, term()}
  def purge_raw_tombstone(segment_id, key_hash) do
    case BlobStore.delete_metadata(segment_id, key_hash, select_drive()) do
      result when result in [:ok, {:ok, {}}] -> :ok
      {:error, reason} -> {:error, reason}
    end
  end

  ## Private — Data fetching

  defp fetch_replica_records(replicas, segment_id, local_node, timeout, store_opts) do
    replicas
    |> Enum.map(fn node ->
      {node, fetch_all_records(node, segment_id, local_node, timeout, store_opts)}
    end)
    |> Enum.filter(fn {_node, result} -> match?({:ok, _}, result) end)
    |> Enum.map(fn {node, {:ok, records}} -> {node, Map.new(records)} end)
  end

  defp fetch_all_records(node, segment_id, local_node, timeout, store_opts) do
    if node == local_node do
      MetadataStore.list_segment_all(segment_id, store_opts)
    else
      try do
        :erpc.call(node, MetadataStore, :list_segment_all, [segment_id, store_opts], timeout)
      catch
        :exit, reason -> {:error, reason}
      end
    end
  end

  ## Private — Ring lookup

  defp find_segment_replicas(ring, segment_id) do
    # Find the replica set for this specific segment_id
    case Enum.find(MetadataRing.segments(ring), fn {seg, _} -> seg == segment_id end) do
      nil -> {segment_id, []}
      found -> found
    end
  end

  defp default_ring do
    # Try to get ring from persistent_term (set by Supervisor)
    case :persistent_term.get({NeonFS.Core.ChunkIndex, :quorum_opts}, nil) do
      nil ->
        MetadataRing.new([Node.self()], virtual_nodes_per_physical: 4, replicas: 1)

      quorum_opts ->
        Keyword.fetch!(quorum_opts, :ring)
    end
  end

  defp select_drive do
    with {:error, _} <- DriveRegistry.select_drive(:hot),
         {:error, _} <- DriveRegistry.select_drive(:warm),
         {:error, _} <- DriveRegistry.select_drive(:cold) do
      "default"
    else
      {:ok, drive} -> drive.id
    end
  end
end
