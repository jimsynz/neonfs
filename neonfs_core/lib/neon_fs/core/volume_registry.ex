defmodule NeonFS.Core.VolumeRegistry do
  @moduledoc """
  Registry for managing storage volumes.

  Uses ETS for concurrent read access with serialized writes through GenServer.
  Maintains two tables for fast lookups: by ID and by name.
  """

  use GenServer
  require Logger

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.DriveRegistry
  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.MetadataStateMachine
  alias NeonFS.Core.Persistence
  alias NeonFS.Core.RaServer
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.Core.Volume
  alias NeonFS.Core.Volume.Deprovisioner
  alias NeonFS.Core.Volume.DriveSelector
  alias NeonFS.Core.Volume.Provisioner
  alias NeonFS.Core.VolumeEncryption
  alias NeonFS.Error.AlreadyExists
  alias NeonFS.Error.Invalid, as: InvalidError
  alias NeonFS.Error.Unavailable
  alias NeonFS.Events
  alias NeonFS.Events.Broadcaster
  alias NeonFS.Events.Envelope
  alias NeonFS.Events.{VolumeCreated, VolumeDeleted, VolumeUpdated}

  @type volume_id :: binary()
  @type volume_name :: String.t()

  @system_volume_name "_system"
  @system_volume_protected_fields [:encryption, :nfs_export, :owner, :system]

  # Client API

  @doc """
  Adjusts the system volume replication factor to match cluster size.

  The adjustment persists the volume through Ra, so the call budget
  reflects a cluster-wide replicated write on a busy system — not a
  local lookup (#1154).
  """
  @spec adjust_system_volume_replication(pos_integer()) :: {:ok, Volume.t()} | {:error, term()}
  def adjust_system_volume_replication(new_cluster_size)
      when is_integer(new_cluster_size) and new_cluster_size >= 1 do
    GenServer.call(__MODULE__, {:adjust_system_volume_replication, new_cluster_size}, 30_000)
  end

  @doc """
  Creates a new volume with the given name and configuration.

  Returns `{:ok, volume}` if successful, or `{:error, reason}` if:
  - Name already exists
  - Name starts with `_` (reserved namespace)
  - Configuration is invalid
  """
  @spec create(String.t(), keyword()) :: {:ok, Volume.t()} | {:error, term()}
  def create(name, opts \\ []) do
    GenServer.call(__MODULE__, {:create, name, opts}, 30_000)
  end

  @doc """
  Creates the system volume. Called once during cluster init.

  The system volume uses a deterministic ID derived from the cluster name
  and has special properties: replicated to all nodes, cannot be deleted
  or renamed, hidden from default volume listings.
  """
  @spec create_system_volume(keyword()) :: {:ok, Volume.t()} | {:error, term()}
  def create_system_volume(opts \\ []) do
    GenServer.call(__MODULE__, {:create_system_volume, opts}, 30_000)
  end

  @doc """
  Deletes a volume.

  Returns `:ok` if successful, or `{:error, reason}` if:
  - Volume not found
  - Volume contains files (must be empty)
  - Volume is a system volume
  """
  @spec delete(volume_id()) :: :ok | {:error, term()}
  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id}, 30_000)
  end

  @doc """
  Gets a volume by its ID.

  First checks local ETS cache, then falls back to Ra if not found locally.
  Returns `{:ok, volume}` if found, or `{:error, :not_found}` otherwise.
  """
  @spec get(volume_id()) :: {:ok, Volume.t()} | {:error, :not_found}
  def get(id) do
    case :ets.lookup(:volumes_by_id, id) do
      [{^id, volume}] ->
        {:ok, volume}

      [] ->
        # Not in local cache, try Ra
        get_from_ra(id)
    end
  end

  @doc """
  Gets a volume by its name.

  First checks local ETS cache, then falls back to Ra if not found locally.
  Returns `{:ok, volume}` if found, or `{:error, :not_found}` otherwise.
  """
  @spec get_by_name(volume_name()) :: {:ok, Volume.t()} | {:error, :not_found}
  def get_by_name(name) do
    case :ets.lookup(:volumes_by_name, name) do
      [{^name, volume_id}] ->
        get(volume_id)

      [] ->
        # Not in local cache, try Ra
        get_by_name_from_ra(name)
    end
  end

  @doc """
  Returns the system volume, or `{:error, :not_found}` if it doesn't exist.
  """
  @spec get_system_volume() :: {:ok, Volume.t()} | {:error, :not_found}
  def get_system_volume do
    get_by_name(@system_volume_name)
  end

  @doc """
  Lists volumes.

  By default, system volumes are excluded. Pass `include_system: true` to
  include them.
  """
  @spec list(keyword()) :: [Volume.t()]
  def list(opts \\ []) do
    restored? = :persistent_term.get({__MODULE__, :restored}, false)

    local_volumes =
      :ets.tab2list(:volumes_by_id)
      |> Enum.map(fn {_id, volume} -> volume end)

    # Sync from Ra when ETS is empty or when the initial Ra restore hasn't
    # completed yet (ETS may have stale data from DETS restore)
    volumes =
      if Enum.empty?(local_volumes) or not restored? do
        case sync_from_ra() do
          {:ok, synced_volumes} when synced_volumes != [] -> synced_volumes
          _ -> local_volumes
        end
      else
        local_volumes
      end

    volumes =
      if Keyword.get(opts, :include_system, false) do
        volumes
      else
        Enum.reject(volumes, fn v -> Map.get(v, :system, false) end)
      end

    Enum.sort_by(volumes, & &1.name)
  end

  @doc """
  Starts the volume registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Updates a volume's configuration.

  Returns `{:ok, updated_volume}` if successful, or `{:error, reason}` if:
  - Volume not found
  - New configuration is invalid
  - System volume protected fields are being changed
  """
  @spec update(volume_id(), keyword()) :: {:ok, Volume.t()} | {:error, term()}
  def update(id, opts) do
    GenServer.call(__MODULE__, {:update, id, opts}, 30_000)
  end

  @doc """
  Updates a volume's statistics (size and chunk count).

  Returns `{:ok, updated_volume}` if successful, or `{:error, :not_found}` if not found.
  """
  @spec update_stats(volume_id(), keyword()) :: {:ok, Volume.t()} | {:error, term()}
  def update_stats(id, stats) do
    GenServer.call(__MODULE__, {:update_stats, id, stats}, 30_000)
  end

  @doc """
  Atomically adjusts a volume's usage counters by the given deltas.

  Unlike `update_stats/2` (a read-modify-write that loses concurrent
  updates), this issues a single atomic Ra command, so simultaneous
  writes to the same volume accumulate correctly. Deltas may be
  negative (deletes, truncations); counters clamp at zero.

  Accepts `:logical_size`, `:physical_size`, `:chunk_count` (defaulting
  to `0`). Returns `{:ok, updated_volume}` or `{:error, :not_found}`.
  """
  @spec adjust_stats(volume_id(), keyword()) :: {:ok, Volume.t()} | {:error, term()}
  def adjust_stats(id, deltas) do
    GenServer.call(__MODULE__, {:adjust_stats, id, Map.new(deltas)}, 30_000)
  end

  @doc """
  Recomputes a volume's usage counters from the file index and writes
  the authoritative absolutes back through Ra, correcting any drift.

  This is the reconcile backstop for the incremental counters (#1462):
  a crash between a file-index write and the counter adjustment, or a
  streamed overwrite, can leave `logical_size` out of step with reality.
  """
  @spec reconcile_stats(volume_id()) :: {:ok, Volume.t()} | {:error, term()}
  def reconcile_stats(id) do
    GenServer.call(__MODULE__, {:reconcile_stats, id}, 60_000)
  end

  # Server callbacks

  @impl true
  def init(_opts) do
    # Trap exits so terminate/2 is called during shutdown
    Process.flag(:trap_exit, true)

    # Create ETS tables for fast lookups
    :ets.new(:volumes_by_id, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    :ets.new(:volumes_by_name, [
      :named_table,
      :set,
      :public,
      read_concurrency: true
    ])

    # Subscribe to volume lifecycle events so ETS stays in sync when
    # volumes are created/updated/deleted on other nodes
    Events.subscribe_volumes()

    # Try to restore volumes from Ra state into ETS
    # If Ra is not ready yet (e.g., during startup), schedule retries
    restored =
      case restore_from_ra() do
        {:ok, count} ->
          Logger.info("VolumeRegistry started, restored volumes from Ra", count: count)
          :persistent_term.put({__MODULE__, :restored}, true)
          true

        {:error, reason} ->
          Logger.debug("VolumeRegistry started but Ra not ready yet, will retry",
            reason: reason
          )

          schedule_restore_retry(1_000)
          false
      end

    {:ok, %{restored: restored, restore_backoff: 1_000}}
  end

  @impl true
  def terminate(_reason, _state) do
    # Save ETS tables to DETS before shutdown
    Logger.info("VolumeRegistry shutting down, saving tables...")
    meta_dir = Persistence.meta_dir()

    Persistence.snapshot_table(
      :volumes_by_id,
      Path.join(meta_dir, "volume_registry_by_id.dets")
    )

    Persistence.snapshot_table(
      :volumes_by_name,
      Path.join(meta_dir, "volume_registry_by_name.dets")
    )

    Logger.info("VolumeRegistry tables saved")
    :ok
  end

  @impl true
  def handle_call({:adjust_system_volume_replication, new_size}, _from, state) do
    reply = do_adjust_system_volume_replication(new_size)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:create, name, opts}, _from, state) do
    reply = do_create_volume(name, opts)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:create_system_volume, opts}, _from, state) do
    reply = do_create_system_volume(opts)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:delete, id}, _from, state) do
    reply = do_delete_volume(id)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:update, id, opts}, _from, state) do
    reply = do_update_volume(id, opts)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:update_stats, id, stats}, _from, state) do
    reply = do_update_stats(id, stats)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:adjust_stats, id, deltas}, _from, state) do
    reply = do_adjust_stats(id, deltas)
    {:reply, reply, state}
  end

  @impl true
  def handle_call({:reconcile_stats, id}, _from, state) do
    reply = do_reconcile_stats(id)
    {:reply, reply, state}
  end

  @impl true
  def handle_info(
        {:neonfs_event, %Envelope{source_node: source, event: event}},
        state
      )
      when source != node() do
    sync_event_to_ets(event)
    {:noreply, state}
  end

  def handle_info({:neonfs_event, %Envelope{}}, state), do: {:noreply, state}

  def handle_info(:retry_restore_from_ra, %{restored: true} = state) do
    {:noreply, state}
  end

  def handle_info(:retry_restore_from_ra, state) do
    case restore_from_ra() do
      {:ok, count} ->
        Logger.info("VolumeRegistry restored volumes from Ra on retry", count: count)
        :persistent_term.put({__MODULE__, :restored}, true)
        {:noreply, %{state | restored: true}}

      {:error, _reason} ->
        next_backoff = min(state.restore_backoff * 2, 30_000)
        schedule_restore_retry(next_backoff)
        {:noreply, %{state | restore_backoff: next_backoff}}
    end
  end

  def handle_info(_msg, state), do: {:noreply, state}

  defp sync_event_to_ets(%VolumeCreated{volume_id: id}), do: get_from_ra(id)
  defp sync_event_to_ets(%VolumeUpdated{volume_id: id}), do: get_from_ra(id)

  defp sync_event_to_ets(%VolumeDeleted{volume_id: id}) do
    case :ets.lookup(:volumes_by_id, id) do
      [{^id, volume}] -> delete_volume_from_ets(volume)
      [] -> :ok
    end
  end

  defp do_adjust_system_volume_replication(new_size) do
    with {:ok, volume} <- get_by_name(@system_volume_name) do
      current = volume.durability
      new_min = min(new_size, current.min_copies)
      updated_durability = %{current | factor: new_size, min_copies: new_min}
      updated = %{volume | durability: updated_durability, updated_at: DateTime.utc_now()}

      case persist_volume(updated) do
        :ok -> {:ok, updated}
        error -> error
      end
    end
  end

  defp do_create_system_volume(opts) do
    with {:ok, cluster_name} <- load_cluster_name(),
         {:error, :not_found} <- get_by_name(@system_volume_name) do
      volume = build_system_volume(cluster_name, opts)

      with :ok <- persist_volume(volume),
           :ok <- provision_system_volume(volume) do
        {:ok, volume}
      else
        {:error, _reason} = error -> error
      end
    else
      {:ok, _existing} -> {:error, AlreadyExists.from_reason(:already_exists)}
      {:error, reason} -> {:error, reason}
    end
  end

  # System volume is always `replicate: factor=1, min_copies=1`, so
  # any single registered drive is sufficient. Provision it
  # eagerly when drives are available in the bootstrap layer —
  # without per-volume metadata the bootstrap → root segment →
  # index tree read path returns `:not_found` for every
  # system-volume read, which breaks `SystemVolume.read` for cluster
  # identity / TLS / audit-log files.
  #
  # When the bootstrap layer is empty (Ra not running yet, or no
  # drives registered — common in unit-test setups), provisioning is
  # deferred to the first metadata write through #785.
  defp provision_system_volume(%Volume{} = volume) do
    if sufficient_drives_for?(volume.durability) do
      case Provisioner.provision(volume) do
        {:ok, _root_chunk_hash} -> :ok
        {:error, _reason} = err -> err
      end
    else
      :ok
    end
  end

  defp do_create_volume(name, opts) do
    with :ok <- check_reserved_name(name),
         {:error, :not_found} <- get_by_name(name),
         volume = Volume.new(name, opts),
         :ok <- Volume.validate(volume),
         :ok <- persist_volume(volume) do
      case provision_metadata(volume, opts) do
        :ok ->
          safe_broadcast(volume.id, %VolumeCreated{volume_id: volume.id})
          {:ok, volume}

        {:error, reason} ->
          # Provisioning failed (insufficient drives / replicas / Ra
          # error). Roll back the volume registration so we don't
          # leave a half-provisioned state — the volume's chunks (if
          # any landed) become unreferenced and GC reaps them.
          _ = delete_volume_persisted(volume.id, volume)
          {:error, reason}
      end
    else
      {:ok, _} ->
        {:error, AlreadyExists.from_reason(:already_exists, name)}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Builds a fresh `RootSegment` for the volume, writes the chunk to
  # replica drives, and registers the bootstrap-layer entry (#779).
  #
  # Skipped when the bootstrap layer doesn't have enough drives to
  # satisfy the volume's `min_copies` (replicate) or `data_chunks`
  # (erasure). The under-provisioned volume still exists in the
  # registry — its first metadata write through #785 will allocate
  # a root segment lazily once enough drives come online.
  #
  # Tests can pass `:provisioner` opt to override the default
  # `Volume.Provisioner` for stubbing.
  defp provision_metadata(volume, opts) do
    cond do
      Keyword.get(opts, :skip_provisioning?, false) -> :ok
      not sufficient_drives_for?(volume.durability) -> :ok
      true -> run_provisioner(volume, opts)
    end
  end

  defp run_provisioner(volume, opts) do
    provisioner = Keyword.get(opts, :provisioner, Provisioner)

    case provisioner.provision(volume) do
      {:ok, _root_chunk_hash} -> :ok
      {:error, _reason} = err -> err
    end
  end

  defp sufficient_drives_for?(durability) do
    case RaSupervisor.local_query(&MetadataStateMachine.get_drives/1) do
      {:ok, drives_map} when is_map(drives_map) ->
        match?({:ok, _}, DriveSelector.select_replicas(durability, drives_map))

      _ ->
        false
    end
  end

  defp do_delete_volume(id) do
    with {:ok, volume} <- get(id),
         :ok <- check_not_system_volume(volume),
         :ok <- check_volume_empty(volume),
         :ok <- delete_volume_persisted(id, volume) do
      deprovision_bootstrap_entry(id)
      safe_broadcast(id, %VolumeDeleted{volume_id: id})
      :ok
    end
  end

  # Tells the bootstrap layer (#779) to forget the volume's root
  # pointer. The Ra `:unregister_volume_root` command is idempotent,
  # so this is safe even on volumes whose `create` predates the
  # provisioner wiring (#810). On Ra failure we log and continue —
  # the volume is already gone from the registry, and a stale
  # bootstrap entry is recoverable but not blocking.
  defp deprovision_bootstrap_entry(id) do
    case Deprovisioner.deprovision(id) do
      {:ok, :unregistered} ->
        :ok

      {:error, {:bootstrap_unregister_failed, reason}} ->
        Logger.warning("Failed to unregister volume root from bootstrap layer",
          volume_id: id,
          reason: inspect(reason)
        )

        :ok
    end
  end

  defp do_update_stats(id, stats) do
    with {:ok, volume} <- get(id),
         updated = Volume.update_stats(volume, stats),
         :ok <- persist_volume(updated) do
      {:ok, updated}
    end
  end

  defp do_adjust_stats(id, deltas) do
    case maybe_ra_command({:adjust_volume_stats, id, deltas}) do
      {:ok, {:ok, volume_map}} ->
        cache_and_return_volume(volume_map)

      # The volume is known to the ETS cache but not (yet) to the Ra state
      # machine — the pre-Ra single-node bootstrap case. Fall back to a
      # read-modify-write that upserts through `persist_volume`, matching
      # the old `update_stats` resilience. Serialised through this
      # GenServer, so it's safe in the single-node mode where it applies.
      {:ok, {:error, :not_found}} ->
        adjust_stats_via_upsert(id, deltas)

      {:ok, {:error, reason}} ->
        {:error, reason}

      {:error, %Unavailable{details: %{reason: :ra_not_available}}} ->
        adjust_stats_via_upsert(id, deltas)

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp adjust_stats_via_upsert(id, deltas) do
    with {:ok, volume} <- get(id),
         updated =
           Volume.update_stats(volume,
             logical_size: max(0, volume.logical_size + Map.get(deltas, :logical_size, 0)),
             physical_size: max(0, volume.physical_size + Map.get(deltas, :physical_size, 0)),
             chunk_count: max(0, volume.chunk_count + Map.get(deltas, :chunk_count, 0)),
             file_count: max(0, volume.file_count + Map.get(deltas, :file_count, 0))
           ),
         :ok <- persist_volume(updated) do
      {:ok, updated}
    end
  end

  defp do_reconcile_stats(id) do
    with {:ok, _volume} <- get(id),
         {:ok, %{logical_size: logical, file_count: files}} <- FileIndex.volume_usage(id) do
      absolutes = %{logical_size: logical, file_count: files}

      case maybe_ra_command({:set_volume_stats, id, absolutes}) do
        {:ok, {:ok, volume_map}} ->
          cache_and_return_volume(volume_map)

        {:ok, {:error, :not_found}} ->
          set_stats_via_upsert(id, absolutes)

        {:ok, {:error, reason}} ->
          {:error, reason}

        {:error, %Unavailable{details: %{reason: :ra_not_available}}} ->
          set_stats_via_upsert(id, absolutes)

        {:error, reason} ->
          {:error, reason}
      end
    end
  end

  defp set_stats_via_upsert(id, absolutes) do
    with {:ok, volume} <- get(id),
         updated = Volume.update_stats(volume, Keyword.new(absolutes)),
         :ok <- persist_volume(updated) do
      {:ok, updated}
    end
  end

  defp do_update_volume(id, opts) do
    with {:ok, volume} <- get(id),
         :ok <- check_system_volume_update(volume, opts),
         updated = Volume.update(volume, opts),
         :ok <- Volume.validate(updated),
         :ok <- persist_volume(updated) do
      safe_broadcast(id, %VolumeUpdated{volume_id: id})
      {:ok, updated}
    end
  end

  # The system volume needs a concrete tier so its first chunk write —
  # typically `/cluster/identity.json` from
  # `Cluster.Init.write_cluster_identity/1` — can find a candidate
  # drive. Hard-coding `:hot` here regressed operators bootstrapping a
  # cluster with only non-hot drives (e.g.
  # `neonfs cluster init --drive ... --tier cold` on a single-disk
  # deployment).
  #
  # Picks the highest-priority tier (hot > warm > cold) that has at
  # least one drive registered. Defaults to `:hot` when no drives are
  # registered yet — `do_create_system_volume/0` already short-circuits
  # provisioning when `sufficient_drives_for?/1` returns false, so the
  # tier value is inconsequential in that case. Same default applies
  # when DriveRegistry's ETS table doesn't exist (unit tests that
  # exercise the volume registry in isolation).
  defp pick_system_volume_tier do
    present_tiers =
      try do
        DriveRegistry.list_drives() |> Enum.map(& &1.tier)
      rescue
        ArgumentError -> []
      end

    Enum.find([:hot, :warm, :cold], :hot, &(&1 in present_tiers))
  end

  # Defaults to factor=1 when the caller doesn't specify, matching the
  # historical single-node bootstrap. `neonfs cluster init
  # --system-replicas N` plumbs `N` through here so operators can seed a
  # multi-replica system volume on a cluster they intend to scale up.
  # Any value < 1 falls back to 1.
  defp system_volume_factor(opts) do
    case Keyword.get(opts, :replicas, 1) do
      n when is_integer(n) and n >= 1 -> n
      _ -> 1
    end
  end

  defp build_system_volume(cluster_name, opts) do
    now = DateTime.utc_now()
    factor = system_volume_factor(opts)

    %Volume{
      id: system_volume_id(cluster_name),
      name: @system_volume_name,
      owner: :system,
      durability: %{type: :replicate, factor: factor, min_copies: factor},
      write_ack: :quorum,
      tiering: %{
        initial_tier: pick_system_volume_tier(),
        promotion_threshold: 1,
        demotion_delay: 1
      },
      caching: %{
        transformed_chunks: false,
        reconstructed_stripes: false,
        remote_chunks: false
      },
      io_weight: 100,
      compression: %{algorithm: :zstd, level: 3, min_size: 0},
      verification: %{on_read: :always, sampling_rate: nil, scrub_interval: 2_592_000},
      encryption: VolumeEncryption.new(),
      metadata_consistency: nil,
      logical_size: 0,
      physical_size: 0,
      chunk_count: 0,
      file_count: 0,
      created_at: now,
      updated_at: now,
      system: true
    }
  end

  defp check_not_system_volume(%Volume{} = volume) do
    if Map.get(volume, :system, false) do
      {:error,
       InvalidError.exception(
         message: "Cannot delete system volume",
         details: %{volume_id: volume.id, volume_name: volume.name}
       )}
    else
      :ok
    end
  end

  defp check_reserved_name("_" <> _ = name) do
    {:error,
     InvalidError.exception(
       message: "Volume names starting with '_' are reserved",
       details: %{volume_name: name}
     )}
  end

  defp check_reserved_name(_), do: :ok

  defp check_system_volume_update(%Volume{} = volume, opts) do
    if Map.get(volume, :system, false) do
      has_protected_field =
        Enum.any?(@system_volume_protected_fields, &Keyword.has_key?(opts, &1))

      durability_type_changed =
        case Keyword.get(opts, :durability) do
          %{type: type} when type != :replicate -> true
          _ -> false
        end

      if has_protected_field or durability_type_changed do
        {:error,
         InvalidError.exception(
           message: "Cannot modify protected fields on system volume",
           details: %{volume_id: volume.id}
         )}
      else
        :ok
      end
    else
      :ok
    end
  end

  defp check_volume_empty(volume) do
    files = FileIndex.list_volume(volume.id)

    if Enum.empty?(files) do
      :ok
    else
      {:error,
       InvalidError.exception(
         message: "Volume contains #{length(files)} file(s), cannot delete",
         details: %{volume_id: volume.id, file_count: length(files)}
       )}
    end
  end

  defp load_cluster_name do
    case ClusterState.load() do
      {:ok, state} -> {:ok, state.cluster_name}
      {:error, reason} -> {:error, {:no_cluster_state, reason}}
    end
  end

  defp system_volume_id(cluster_name) do
    :crypto.hash(:sha256, "neonfs:system_volume:#{cluster_name}")
    |> Base.encode16(case: :lower)
    |> binary_part(0, 32)
  end

  defp persist_volume(volume) do
    case maybe_ra_command({:put_volume, volume_to_map(volume)}) do
      {:ok, :ok} ->
        insert_volume(volume)
        :ok

      {:error, %Unavailable{details: %{reason: :ra_not_available}}} ->
        insert_volume(volume)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp delete_volume_persisted(id, volume) do
    case maybe_ra_command({:delete_volume, id}) do
      {:ok, :ok} ->
        delete_volume_from_ets(volume)
        :ok

      {:error, %Unavailable{details: %{reason: :ra_not_available}}} ->
        delete_volume_from_ets(volume)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp schedule_restore_retry(delay_ms) do
    Process.send_after(self(), :retry_restore_from_ra, delay_ms)
  end

  # Event broadcasting

  defp safe_broadcast(volume_id, event) do
    Broadcaster.broadcast(volume_id, event)
  rescue
    _ ->
      Logger.warning("Event broadcast failed", event_type: inspect(event.__struct__))
      :ok
  catch
    :exit, _ ->
      Logger.warning("Event broadcast failed", event_type: inspect(event.__struct__))
      :ok
  end

  # Private helpers

  defp insert_volume(%Volume{} = volume) do
    :ets.insert(:volumes_by_id, {volume.id, volume})
    :ets.insert(:volumes_by_name, {volume.name, volume.id})
  end

  defp delete_volume_from_ets(%Volume{} = volume) do
    :ets.delete(:volumes_by_id, volume.id)
    :ets.delete(:volumes_by_name, volume.name)
  end

  # Query Ra for a volume by ID, caching the result locally if found
  defp get_from_ra(id) do
    query_fn = fn state ->
      state
      |> Map.get(:volumes, %{})
      |> Map.get(id)
    end

    case RaSupervisor.query(query_fn) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, volume_map} -> cache_and_return_volume(volume_map)
      {:error, _} -> {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :not_found}
  end

  # Query Ra for a volume by name, caching the result locally if found
  defp get_by_name_from_ra(name) do
    query_fn = fn state ->
      state
      |> Map.get(:volumes, %{})
      |> find_volume_by_name(name)
    end

    case RaSupervisor.query(query_fn) do
      {:ok, nil} -> {:error, :not_found}
      {:ok, volume_map} -> cache_and_return_volume(volume_map)
      {:error, _} -> {:error, :not_found}
    end
  catch
    :exit, _ -> {:error, :not_found}
  end

  defp find_volume_by_name(volumes, name) do
    Enum.find_value(volumes, fn {_id, volume_map} ->
      if volume_map[:name] == name, do: volume_map
    end)
  end

  defp cache_and_return_volume(volume_map) do
    volume = map_to_volume(volume_map)
    insert_volume(volume)
    {:ok, volume}
  end

  # Try to execute a Ra command, but gracefully handle Ra not being available
  # Returns {:ok, result} | {:error, :ra_not_available} | {:error, reason}
  #
  # IMPORTANT: Only returns :ra_not_available when Ra has not been initialized yet
  # (Phase 1 single-node mode). Once Ra is initialized, errors are propagated
  # so that quorum loss is properly detected.
  # Ra commands are cluster-wide replicated writes; the 5 s
  # `RaSupervisor.command/2` default is a local-lookup budget that times
  # out under load (#1165). 20 s nests inside the 30 s GenServer.call
  # timeouts on this module's public API, so the clean `Unavailable`
  # error still fires before the caller's exit.
  @ra_command_timeout 20_000

  defp maybe_ra_command(cmd) do
    case RaSupervisor.command(cmd, @ra_command_timeout) do
      {:ok, result, _leader} ->
        {:ok, result}

      {:error, :noproc} ->
        # Ra server not running - check if it was ever initialized
        if RaServer.initialized?() do
          {:error, Unavailable.from_reason(:ra_unavailable)}
        else
          {:error, Unavailable.from_reason(:ra_not_available)}
        end

      {:error, reason} ->
        {:error, reason}

      {:timeout, _node} ->
        {:error, Unavailable.from_reason(:timeout)}
    end
  catch
    :exit, {:noproc, _} ->
      if RaServer.initialized?() do
        {:error, Unavailable.from_reason(:ra_unavailable)}
      else
        {:error, Unavailable.from_reason(:ra_not_available)}
      end

    kind, reason ->
      Logger.debug("Ra command error", kind: kind, reason: reason)

      if RaServer.initialized?() do
        {:error, {:ra_error, {kind, reason}}}
      else
        {:error, Unavailable.from_reason(:ra_not_available)}
      end
  end

  # Restore volumes from Ra state into ETS
  defp restore_from_ra do
    case RaSupervisor.query(fn state -> Map.get(state, :volumes, %{}) end) do
      {:ok, volumes} when is_map(volumes) ->
        count =
          Enum.reduce(volumes, 0, fn {_id, volume_map}, acc ->
            volume = map_to_volume(volume_map)
            insert_volume(volume)
            acc + 1
          end)

        {:ok, count}

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Sync volumes from Ra into ETS and return the list
  # Used when local ETS is empty but Ra might have data
  # NOTE: No logging here - this can be called during RPC from CLI,
  # and Logger output causes RegSend messages that crash erl_rpc
  defp sync_from_ra do
    case RaSupervisor.query(fn state -> Map.get(state, :volumes, %{}) end) do
      {:ok, volumes} when is_map(volumes) and map_size(volumes) > 0 ->
        volume_list =
          Enum.map(volumes, fn {_id, volume_map} ->
            volume = map_to_volume(volume_map)
            insert_volume(volume)
            volume
          end)

        {:ok, volume_list}

      {:ok, _} ->
        {:ok, []}

      {:error, reason} ->
        {:error, reason}
    end
  catch
    _kind, _reason ->
      {:error, :ra_not_available}
  end

  # Convert Volume struct to map for Ra storage
  defp volume_to_map(%Volume{} = volume) do
    %{
      id: volume.id,
      name: volume.name,
      owner: volume.owner,
      atime_mode: volume.atime_mode,
      nfs_export: volume.nfs_export,
      nfs_allowed_ips: volume.nfs_allowed_ips,
      nfs_root_squash: volume.nfs_root_squash,
      durability: volume.durability,
      write_ack: volume.write_ack,
      tiering: volume.tiering,
      caching: volume.caching,
      io_weight: volume.io_weight,
      compression: volume.compression,
      verification: volume.verification,
      encryption: encryption_to_map(volume.encryption),
      max_size: volume.max_size,
      max_files: volume.max_files,
      logical_size: volume.logical_size,
      physical_size: volume.physical_size,
      chunk_count: volume.chunk_count,
      file_count: volume.file_count,
      created_at: volume.created_at,
      updated_at: volume.updated_at,
      system: volume.system
    }
  end

  # Convert map from Ra storage to Volume struct
  # Handles backward compatibility with pre-Phase 3 volumes that have
  # initial_tier instead of tiering map
  defp map_to_volume(volume_map) when is_map(volume_map) do
    tiering = resolve_tiering(volume_map)
    caching = resolve_caching(volume_map)

    %Volume{
      id: volume_map.id,
      name: volume_map.name,
      owner: volume_map[:owner],
      atime_mode: volume_map[:atime_mode] || :noatime,
      nfs_export: volume_map[:nfs_export] || false,
      nfs_allowed_ips: volume_map[:nfs_allowed_ips] || [],
      # `false` is meaningful (no_root_squash), so default only on a
      # missing key — `|| true` would clobber an explicit `false`.
      nfs_root_squash: Map.get(volume_map, :nfs_root_squash, true),
      durability: volume_map.durability,
      write_ack: volume_map.write_ack,
      tiering: tiering,
      caching: caching,
      io_weight: volume_map[:io_weight] || 100,
      compression: volume_map.compression,
      verification: resolve_verification(volume_map.verification),
      encryption: map_to_encryption(volume_map[:encryption]),
      max_size: volume_map[:max_size],
      max_files: volume_map[:max_files],
      created_at: volume_map.created_at,
      updated_at: volume_map.updated_at,
      system: volume_map[:system] || false
    }
    |> struct(usage_counters_from_map(volume_map))
  end

  defp usage_counters_from_map(volume_map) do
    %{
      logical_size: volume_map[:logical_size] || 0,
      physical_size: volume_map[:physical_size] || 0,
      chunk_count: volume_map[:chunk_count] || 0,
      file_count: volume_map[:file_count] || 0
    }
  end

  defp resolve_caching(volume_map) do
    volume_map
    |> Map.get(:caching, %{
      transformed_chunks: true,
      reconstructed_stripes: true,
      remote_chunks: true
    })
    |> Map.drop([:max_memory, "max_memory"])
  end

  defp encryption_to_map(%NeonFS.Core.VolumeEncryption{} = enc) do
    %{
      mode: enc.mode,
      current_key_version: enc.current_key_version,
      rotation: enc.rotation
    }
  end

  defp map_to_encryption(nil) do
    %NeonFS.Core.VolumeEncryption{
      mode: :none,
      current_key_version: 0,
      keys: %{},
      rotation: nil
    }
  end

  defp map_to_encryption(enc_map) when is_map(enc_map) do
    %NeonFS.Core.VolumeEncryption{
      mode: enc_map[:mode] || :none,
      current_key_version: enc_map[:current_key_version] || 0,
      keys: enc_map[:keys] || %{},
      rotation: enc_map[:rotation]
    }
  end

  # Resolve tiering config from either new tiering map or legacy initial_tier field
  # Ensure verification map includes scrub_interval (backward compat with pre-0123 volumes)
  defp resolve_verification(%{scrub_interval: _} = verification), do: verification

  defp resolve_verification(verification) when is_map(verification) do
    Map.put(verification, :scrub_interval, 2_592_000)
  end

  defp resolve_tiering(volume_map) do
    case volume_map[:tiering] do
      %{initial_tier: _} = tiering ->
        tiering

      _ ->
        # Legacy format: use initial_tier field with defaults
        initial_tier = volume_map[:initial_tier] || :hot
        %{initial_tier: initial_tier, promotion_threshold: 10, demotion_delay: 86_400}
    end
  end
end
