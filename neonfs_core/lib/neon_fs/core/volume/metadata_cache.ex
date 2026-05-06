defmodule NeonFS.Core.Volume.MetadataCache do
  @moduledoc """
  ETS-backed cache for per-volume metadata reads (#816).

  Caches the values that `NeonFS.Core.Volume.MetadataReader` (#820,
  #821) returns from `get/4` and `range/5`. Keyed by
  `{volume_id, root_chunk_hash, key}`:

  - `volume_id` makes per-volume eviction efficient on bootstrap
    `:update_volume_root` / `:unregister_volume_root` events
    (handled in #823).
  - `root_chunk_hash` is the natural CoW versioning bound — a
    different hash means different bytes, so stale entries that
    survive an eviction are correct-by-construction; they just
    take memory until cleaned up.

  Subscribes to bootstrap-layer telemetry events from
  `MetadataStateMachine.apply/3` (#779) and evicts a volume's
  entries on `:update_volume_root` / `:unregister_volume_root`. The
  read-through wiring in `MetadataReader` is the follow-up #824.

  No LRU bound for now: the cache is unbounded until we have
  telemetry data showing real-world working sets.

  ## Telemetry

  - `[:neonfs, :volume, :metadata_cache, :hit]` — `%{}`,
    `%{volume_id, root_chunk_hash}`.
  - `[:neonfs, :volume, :metadata_cache, :miss]` — `%{}`,
    `%{volume_id, root_chunk_hash}`.
  - `[:neonfs, :volume, :metadata_cache, :put]` — `%{}`,
    `%{volume_id, root_chunk_hash}`.
  - `[:neonfs, :volume, :metadata_cache, :evict_volume]` —
    `%{count: pos_integer()}`, `%{volume_id}`.
  """

  use GenServer

  @type volume_id :: binary()
  @type root_chunk_hash :: binary()
  @type cache_key :: {volume_id(), root_chunk_hash(), key :: term()}

  @table __MODULE__

  ## Client API

  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    name = Keyword.get(opts, :name, __MODULE__)
    GenServer.start_link(__MODULE__, opts, name: name)
  end

  @doc """
  Look up a cached value. Returns `{:ok, value}` on hit, `:miss`
  otherwise. Emits a `:hit` or `:miss` telemetry event so callers
  can build hit-rate dashboards without instrumenting every callsite.
  """
  @spec get(volume_id(), root_chunk_hash(), term()) :: {:ok, term()} | :miss
  def get(volume_id, root_chunk_hash, key) do
    case :ets.lookup(@table, {volume_id, root_chunk_hash, key}) do
      [{_, value}] ->
        emit(:hit, %{}, %{volume_id: volume_id, root_chunk_hash: root_chunk_hash})
        {:ok, value}

      [] ->
        emit(:miss, %{}, %{volume_id: volume_id, root_chunk_hash: root_chunk_hash})
        :miss
    end
  end

  @doc """
  Insert a value. Overwrites any prior entry for the same
  `{volume_id, root_chunk_hash, key}`.
  """
  @spec put(volume_id(), root_chunk_hash(), term(), term()) :: :ok
  def put(volume_id, root_chunk_hash, key, value) do
    :ets.insert(@table, {{volume_id, root_chunk_hash, key}, value})
    emit(:put, %{}, %{volume_id: volume_id, root_chunk_hash: root_chunk_hash})
    :ok
  end

  @doc """
  Wipe every entry whose key starts with `volume_id`. Driven by the
  bootstrap-layer event subscription (#823) on
  `:update_volume_root` / `:unregister_volume_root`.

  Returns the count of evicted entries (mainly useful in tests
  + telemetry metadata).
  """
  @spec evict_volume(volume_id()) :: non_neg_integer()
  def evict_volume(volume_id) do
    # `match_delete` with a guard on the first tuple element evicts
    # every entry whose key tuple starts with `volume_id`. ETS
    # match-spec form: `{{$1, $2, $3}, $4} when $1 == volume_id`.
    spec = [
      {
        {{:"$1", :"$2", :"$3"}, :"$4"},
        [{:==, :"$1", volume_id}],
        [true]
      }
    ]

    count = :ets.select_delete(@table, spec)
    emit(:evict_volume, %{count: count}, %{volume_id: volume_id})
    count
  end

  @doc "Returns the underlying ETS table size — for tests and metrics."
  @spec size() :: non_neg_integer()
  def size do
    :ets.info(@table, :size) || 0
  end

  @doc false
  # Telemetry handler for bootstrap-layer events that change a
  # volume's root pointer. Driven by the events `MetadataStateMachine`
  # emits from `:update_volume_root` and `:unregister_volume_root`
  # apply clauses (#779). Attached from `init/1` and detached from
  # `terminate/2`; the handler dispatches to `evict_volume/1` so the
  # next read for that volume goes through the full bootstrap → root
  # segment → index tree walk.
  #
  # `:register_volume_root` is intentionally not handled — there's
  # nothing to evict for a brand-new volume.
  def handle_bootstrap_event(_event, _measurements, %{volume_id: volume_id}, _config)
      when is_binary(volume_id) do
    evict_volume(volume_id)
    :ok
  end

  def handle_bootstrap_event(_event, _measurements, _metadata, _config), do: :ok

  ## GenServer Callbacks

  @impl true
  def init(_opts) do
    :ets.new(@table, [
      :named_table,
      :public,
      :set,
      read_concurrency: true,
      write_concurrency: true
    ])

    handler_id = handler_id()

    :telemetry.attach_many(
      handler_id,
      [
        [:neonfs, :ra, :command, :update_volume_root],
        [:neonfs, :ra, :command, :unregister_volume_root]
      ],
      &__MODULE__.handle_bootstrap_event/4,
      nil
    )

    {:ok, %{handler_id: handler_id}}
  end

  @impl true
  def terminate(_reason, %{handler_id: handler_id}) do
    :telemetry.detach(handler_id)
    :ok
  end

  def terminate(_reason, _state), do: :ok

  ## Internals

  defp handler_id do
    "metadata-cache-#{:erlang.phash2(self())}"
  end

  defp emit(event, measurements, metadata) do
    :telemetry.execute(
      [:neonfs, :volume, :metadata_cache, event],
      measurements,
      metadata
    )
  end
end
