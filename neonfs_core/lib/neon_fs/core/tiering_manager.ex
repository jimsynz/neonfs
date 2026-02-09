defmodule NeonFS.Core.TieringManager do
  @moduledoc """
  Per-node tiering manager that periodically evaluates chunks for promotion and demotion.

  Promotion: chunks on cold/warm tiers with access count exceeding the volume's
  `tiering.promotion_threshold` in a 24h window are promoted to the next tier.

  Demotion: chunks on hot tier with zero accesses for longer than the volume's
  `tiering.demotion_delay` seconds are demoted to the next tier.

  Eviction: when a tier exceeds 90% capacity, demote least-accessed chunks
  regardless of delay.

  Tier progression is always one step: hot <-> warm <-> cold (no direct hot -> cold jumps).

  Submits migration work to `BackgroundWorker` with `:low` priority.

  ## Configuration

    * `:eval_interval_ms` — evaluation cycle interval (default: 300_000 = 5 min)
    * `:max_chunks_per_cycle` — max chunks to evaluate per cycle (default: 1000)
    * `:eviction_threshold` — tier usage ratio above which eviction kicks in (default: 0.9)
    * `:queue_full_threshold` — skip evaluation when BackgroundWorker queued items exceed this (default: 50)
    * `:dry_run` — log decisions without submitting work (default: false)

  ## Telemetry Events

    * `[:neonfs, :tiering_manager, :evaluation]` — cycle completed
    * `[:neonfs, :tiering_manager, :promotion]` — promotion scheduled
    * `[:neonfs, :tiering_manager, :demotion]` — demotion scheduled
  """

  use GenServer
  require Logger

  alias NeonFS.Core.BackgroundWorker
  alias NeonFS.Core.ChunkAccessTracker
  alias NeonFS.Core.ChunkIndex
  alias NeonFS.Core.DriveRegistry
  alias NeonFS.Core.VolumeRegistry

  @tier_order [:hot, :warm, :cold]

  ## Client API

  @doc """
  Starts the TieringManager GenServer.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Triggers an immediate evaluation cycle.

  Returns the evaluation result summary.
  """
  @spec evaluate_now() :: map()
  def evaluate_now do
    GenServer.call(__MODULE__, :evaluate_now, 30_000)
  end

  @doc """
  Returns the current configuration and last evaluation stats.
  """
  @spec status() :: map()
  def status do
    GenServer.call(__MODULE__, :status)
  end

  ## Server Callbacks

  @impl true
  def init(opts) do
    state = %{
      eval_interval_ms: Keyword.get(opts, :eval_interval_ms, 300_000),
      max_chunks_per_cycle: Keyword.get(opts, :max_chunks_per_cycle, 1000),
      eviction_threshold: Keyword.get(opts, :eviction_threshold, 0.9),
      queue_full_threshold: Keyword.get(opts, :queue_full_threshold, 50),
      dry_run: Keyword.get(opts, :dry_run, false),
      last_evaluation: nil,
      # Injected modules for testing
      chunk_index_mod: Keyword.get(opts, :chunk_index_mod, ChunkIndex),
      access_tracker_mod: Keyword.get(opts, :access_tracker_mod, ChunkAccessTracker),
      drive_registry_mod: Keyword.get(opts, :drive_registry_mod, DriveRegistry),
      volume_registry_mod: Keyword.get(opts, :volume_registry_mod, VolumeRegistry),
      background_worker_mod: Keyword.get(opts, :background_worker_mod, BackgroundWorker)
    }

    schedule_evaluation(state)

    Logger.info(
      "TieringManager started (interval=#{state.eval_interval_ms}ms, dry_run=#{state.dry_run})"
    )

    {:ok, state}
  end

  @impl true
  def handle_call(:evaluate_now, _from, state) do
    result = run_evaluation(state)
    state = %{state | last_evaluation: result}
    {:reply, result, state}
  end

  def handle_call(:status, _from, state) do
    result = %{
      eval_interval_ms: state.eval_interval_ms,
      max_chunks_per_cycle: state.max_chunks_per_cycle,
      eviction_threshold: state.eviction_threshold,
      queue_full_threshold: state.queue_full_threshold,
      dry_run: state.dry_run,
      last_evaluation: state.last_evaluation
    }

    {:reply, result, state}
  end

  @impl true
  def handle_info(:evaluate, state) do
    result = run_evaluation(state)
    state = %{state | last_evaluation: result}
    schedule_evaluation(state)
    {:noreply, state}
  end

  def handle_info(_msg, state) do
    {:noreply, state}
  end

  ## Private — Evaluation

  defp run_evaluation(state) do
    worker_status = state.background_worker_mod.status()

    if worker_status.queued > state.queue_full_threshold do
      Logger.debug(
        "TieringManager skipping evaluation: BackgroundWorker queue full (#{worker_status.queued})"
      )

      %{skipped: true, reason: :queue_full, promotions: 0, demotions: 0}
    else
      do_evaluation(state)
    end
  end

  defp do_evaluation(state) do
    chunks = state.chunk_index_mod.list_by_node(Node.self())
    chunks = Enum.take(chunks, state.max_chunks_per_cycle)

    # Build tier capacity map
    tier_usage = build_tier_usage(state)

    # Group chunks by volume for config lookups
    volume_configs = load_volume_configs(chunks, state)

    # Evaluate each chunk
    {promotions, demotions} =
      Enum.reduce(chunks, {[], []}, fn chunk, {promos, demos} ->
        evaluate_chunk(chunk, volume_configs, tier_usage, state, {promos, demos})
      end)

    # Check for eviction pressure
    eviction_demotions = check_eviction_pressure(chunks, tier_usage, state)

    # Deduplicate: eviction demotions take priority
    eviction_hashes = MapSet.new(eviction_demotions, fn {hash, _, _} -> hash end)

    demotions =
      Enum.reject(demotions, fn {hash, _, _} -> MapSet.member?(eviction_hashes, hash) end)

    all_demotions = eviction_demotions ++ demotions

    # Submit work
    submit_promotions(promotions, state)
    submit_demotions(all_demotions, state)

    result = %{
      skipped: false,
      chunks_evaluated: length(chunks),
      promotions: length(promotions),
      demotions: length(all_demotions),
      evictions: length(eviction_demotions)
    }

    :telemetry.execute(
      [:neonfs, :tiering_manager, :evaluation],
      %{
        chunks_evaluated: result.chunks_evaluated,
        promotions: result.promotions,
        demotions: result.demotions
      },
      %{}
    )

    result
  end

  defp evaluate_chunk(chunk, volume_configs, _tier_usage, state, {promos, demos}) do
    # Determine the chunk's current tier from its local location
    local_location = find_local_location(chunk)

    case local_location do
      nil ->
        {promos, demos}

      location ->
        current_tier = location.tier
        stats = state.access_tracker_mod.get_stats(chunk.hash)
        config = Map.get(volume_configs, chunk_volume_id(chunk))

        promos = maybe_promote(chunk.hash, current_tier, stats, config, promos)
        demos = maybe_demote(chunk.hash, current_tier, stats, config, demos)

        {promos, demos}
    end
  end

  defp maybe_promote(hash, current_tier, stats, config, promos) do
    promotion_threshold = get_promotion_threshold(config)
    target = promote_tier(current_tier)

    if target != nil and stats.daily >= promotion_threshold do
      [{hash, current_tier, target} | promos]
    else
      promos
    end
  end

  defp maybe_demote(hash, current_tier, stats, config, demos) do
    demotion_delay = get_demotion_delay(config)
    target = demote_tier(current_tier)

    if target != nil and should_demote?(stats, demotion_delay) do
      [{hash, current_tier, target} | demos]
    else
      demos
    end
  end

  defp should_demote?(stats, demotion_delay) do
    case stats.last_accessed do
      nil ->
        # Never accessed — demote
        true

      %DateTime{} = last ->
        seconds_since = DateTime.diff(DateTime.utc_now(), last, :second)
        seconds_since > demotion_delay
    end
  end

  defp check_eviction_pressure(chunks, tier_usage, state) do
    Enum.flat_map(@tier_order, fn tier ->
      usage = Map.get(tier_usage, tier, 0.0)

      if usage > state.eviction_threshold and demote_tier(tier) != nil do
        evict_from_tier(chunks, tier, state)
      else
        []
      end
    end)
  end

  defp evict_from_tier(chunks, tier, state) do
    target = demote_tier(tier)

    chunks
    |> Enum.filter(fn chunk ->
      loc = find_local_location(chunk)
      loc != nil and loc.tier == tier
    end)
    |> Enum.map(fn chunk ->
      stats = state.access_tracker_mod.get_stats(chunk.hash)
      {chunk.hash, stats}
    end)
    |> Enum.sort_by(fn {_hash, stats} -> stats.daily end, :asc)
    |> Enum.take(div(state.max_chunks_per_cycle, 10))
    |> Enum.map(fn {hash, _stats} -> {hash, tier, target} end)
  end

  ## Private — Tier helpers

  defp promote_tier(:cold), do: :warm
  defp promote_tier(:warm), do: :hot
  defp promote_tier(:hot), do: nil

  defp demote_tier(:hot), do: :warm
  defp demote_tier(:warm), do: :cold
  defp demote_tier(:cold), do: nil

  defp find_local_location(chunk) do
    local_node = Node.self()

    Enum.find(chunk.locations, fn loc ->
      loc.node == local_node or loc[:node] == local_node
    end)
  end

  defp chunk_volume_id(_chunk) do
    # ChunkMeta doesn't carry volume_id directly.
    # Use a default config for now — the migration system (task 0055)
    # will resolve volume association via FileIndex.
    :default
  end

  ## Private — Config helpers

  defp load_volume_configs(_chunks, state) do
    volumes = state.volume_registry_mod.list()

    volume_map =
      Map.new(volumes, fn vol ->
        {vol.id, vol.tiering}
      end)

    # Add default config for chunks without volume association
    Map.put(volume_map, :default, %{
      initial_tier: :hot,
      promotion_threshold: 10,
      demotion_delay: 86_400
    })
  end

  defp get_promotion_threshold(nil), do: 10
  defp get_promotion_threshold(config), do: Map.get(config, :promotion_threshold, 10)

  defp get_demotion_delay(nil), do: 86_400
  defp get_demotion_delay(config), do: Map.get(config, :demotion_delay, 86_400)

  defp build_tier_usage(state) do
    drives = state.drive_registry_mod.list_drives()
    local_node = Node.self()

    # Group local drives by tier and compute aggregate usage ratio
    drives
    |> Enum.filter(&(&1.node == local_node))
    |> Enum.group_by(& &1.tier)
    |> Map.new(fn {tier, tier_drives} ->
      total_capacity = Enum.sum(Enum.map(tier_drives, & &1.capacity_bytes))
      total_used = Enum.sum(Enum.map(tier_drives, & &1.used_bytes))

      ratio =
        if total_capacity > 0 do
          total_used / total_capacity
        else
          0.0
        end

      {tier, ratio}
    end)
  end

  ## Private — Work submission

  defp submit_promotions(promotions, state) do
    Enum.each(promotions, fn move -> submit_move(move, :promotion, state) end)
  end

  defp submit_demotions(demotions, state) do
    Enum.each(demotions, fn move -> submit_move(move, :demotion, state) end)
  end

  defp submit_move({hash, from_tier, to_tier}, direction, state) do
    action = if direction == :promotion, do: "promote", else: "demote"
    label = "#{action}:#{hash_prefix(hash)}:#{from_tier}->#{to_tier}"

    unless state.dry_run do
      state.background_worker_mod.submit(
        fn -> {String.to_existing_atom(action), hash, from_tier, to_tier} end,
        priority: :low,
        label: label
      )
    end

    if state.dry_run do
      Logger.info("TieringManager [dry-run] would #{action} #{label}")
    end

    :telemetry.execute(
      [:neonfs, :tiering_manager, direction],
      %{},
      %{hash: hash, from_tier: from_tier, to_tier: to_tier, dry_run: state.dry_run}
    )
  end

  defp hash_prefix(hash) when is_binary(hash) and byte_size(hash) >= 4 do
    hash |> binary_part(0, 4) |> Base.encode16(case: :lower)
  end

  defp hash_prefix(hash) when is_binary(hash), do: Base.encode16(hash, case: :lower)

  ## Private — Scheduling

  defp schedule_evaluation(state) do
    Process.send_after(self(), :evaluate, state.eval_interval_ms)
  end
end
