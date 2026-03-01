defmodule NeonFS.IO.PriorityAdjuster do
  @moduledoc """
  Periodically checks storage utilisation and volume health, publishing
  adjusted priority weights to the I/O producer.

  Under normal conditions the static `Priority.weight/1` values apply.
  When storage pressure or degraded redundancy is detected, scrub and
  repair weights are boosted so maintenance I/O competes more fairly
  with user traffic.

  ## Pressure Levels

    * **normal** (< 85%) — no adjustment
    * **boost** (85–95%) — scrub and repair weights increased by 50%
    * **critical** (> 95%) — scrub and repair weights match replication

  ## Per-Volume Repair Promotion

  When a volume has degraded redundancy (actual replicas < durability
  target), repair operations for that volume are promoted to
  `user_read` weight.

  The adjuster is a read-only observer — it never modifies queues
  directly, only publishes weight overrides consumed by the producer's
  dispatch logic.
  """

  use GenServer
  require Logger

  alias NeonFS.IO.Priority

  @default_check_interval :timer.seconds(30)
  @default_boost_threshold 0.85
  @default_critical_threshold 0.95

  @type pressure_level :: :normal | :boost | :critical

  @type t :: %__MODULE__{
          check_interval: pos_integer(),
          boost_threshold: float(),
          critical_threshold: float(),
          producer: GenServer.name(),
          storage_metrics_mod: module(),
          volume_registry_mod: module(),
          chunk_index_mod: module(),
          current_level: pressure_level(),
          degraded_volumes: MapSet.t()
        }

  defstruct [
    :check_interval,
    :boost_threshold,
    :critical_threshold,
    :producer,
    :storage_metrics_mod,
    :volume_registry_mod,
    :chunk_index_mod,
    current_level: :normal,
    degraded_volumes: MapSet.new()
  ]

  ## Client API

  @doc """
  Starts the priority adjuster.

  ## Options

    * `:name` — process name (default: `__MODULE__`)
    * `:producer` — producer process name (default: `NeonFS.IO.Producer`)
    * `:check_interval` — milliseconds between checks (default: 30_000)
    * `:boost_threshold` — utilisation fraction to trigger boost (default: 0.85)
    * `:critical_threshold` — utilisation fraction to trigger critical (default: 0.95)
    * `:storage_metrics_mod` — module for storage queries (default: `NeonFS.Core.StorageMetrics`)
    * `:volume_registry_mod` — module for volume queries (default: `NeonFS.Core.VolumeRegistry`)
    * `:chunk_index_mod` — module for chunk queries (default: `NeonFS.Core.ChunkIndex`)
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    {name, opts} = Keyword.pop(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  ## GenServer Callbacks

  @impl true
  def init(opts) do
    state = %__MODULE__{
      check_interval: Keyword.get(opts, :check_interval, @default_check_interval),
      boost_threshold: Keyword.get(opts, :boost_threshold, @default_boost_threshold),
      critical_threshold: Keyword.get(opts, :critical_threshold, @default_critical_threshold),
      producer: Keyword.get(opts, :producer, NeonFS.IO.Producer),
      storage_metrics_mod: Keyword.get(opts, :storage_metrics_mod, NeonFS.Core.StorageMetrics),
      volume_registry_mod: Keyword.get(opts, :volume_registry_mod, NeonFS.Core.VolumeRegistry),
      chunk_index_mod: Keyword.get(opts, :chunk_index_mod, NeonFS.Core.ChunkIndex)
    }

    schedule_check(state)
    {:ok, state}
  end

  @impl true
  def handle_info(:check, state) do
    state = evaluate_and_publish(state)
    schedule_check(state)
    {:noreply, state}
  end

  ## Private — Evaluation

  defp evaluate_and_publish(state) do
    level = compute_pressure_level(state)
    degraded = find_degraded_volumes(state)

    changed = level != state.current_level or degraded != state.degraded_volumes

    :telemetry.execute(
      [:neon_fs, :io, :priority_check],
      %{},
      %{level: level, changed: changed}
    )

    if changed do
      overrides = build_weight_overrides(level, degraded)
      GenStage.cast(state.producer, {:adjust_weights, overrides})

      :telemetry.execute(
        [:neon_fs, :io, :priority_adjusted],
        %{},
        %{level: level, degraded_volumes: MapSet.to_list(degraded)}
      )

      Logger.info("PriorityAdjuster: level=#{level}, degraded_volumes=#{MapSet.size(degraded)}")
    end

    %{state | current_level: level, degraded_volumes: degraded}
  end

  defp compute_pressure_level(state) do
    capacity = state.storage_metrics_mod.cluster_capacity()
    utilisation = compute_utilisation(capacity)

    cond do
      utilisation >= state.critical_threshold -> :critical
      utilisation >= state.boost_threshold -> :boost
      true -> :normal
    end
  end

  defp compute_utilisation(%{total_capacity: :unlimited}), do: 0.0

  defp compute_utilisation(%{total_capacity: 0}), do: 0.0

  defp compute_utilisation(%{total_capacity: cap, total_used: used}) do
    used / cap
  end

  defp find_degraded_volumes(state) do
    volumes = state.volume_registry_mod.list()

    Enum.reduce(volumes, MapSet.new(), fn volume, acc ->
      if volume_degraded?(state, volume) do
        MapSet.put(acc, volume.id)
      else
        acc
      end
    end)
  rescue
    _ -> MapSet.new()
  catch
    :exit, _ -> MapSet.new()
  end

  defp volume_degraded?(state, volume) do
    target = durability_target(volume)
    chunks = state.chunk_index_mod.get_chunks_for_volume(volume.id)

    Enum.any?(chunks, fn chunk ->
      length(chunk.locations) < target
    end)
  rescue
    _ -> false
  catch
    :exit, _ -> false
  end

  defp durability_target(%{durability: %{type: :replicate, factor: factor}}), do: factor

  defp durability_target(%{durability: %{type: :erasure, data_chunks: d, parity_chunks: p}}),
    do: d + p

  defp durability_target(_), do: 3

  ## Private — Weight Override Computation

  defp build_weight_overrides(level, degraded_volumes) do
    base = build_pressure_overrides(level)
    volume_overrides = build_volume_overrides(degraded_volumes)

    %{global: base, per_volume: volume_overrides}
  end

  defp build_pressure_overrides(:normal), do: %{}

  defp build_pressure_overrides(:boost) do
    %{
      scrub: round(Priority.weight(:scrub) * 1.5),
      repair: round(Priority.weight(:repair) * 1.5)
    }
  end

  defp build_pressure_overrides(:critical) do
    replication_weight = Priority.weight(:replication)

    %{
      scrub: replication_weight,
      repair: replication_weight
    }
  end

  defp build_volume_overrides(degraded_volumes) do
    user_read_weight = Priority.weight(:user_read)

    Map.new(degraded_volumes, fn volume_id ->
      {volume_id, %{repair: user_read_weight}}
    end)
  end

  ## Private — Scheduling

  defp schedule_check(state) do
    Process.send_after(self(), :check, state.check_interval)
  end
end
