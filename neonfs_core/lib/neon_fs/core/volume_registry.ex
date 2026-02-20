defmodule NeonFS.Core.VolumeRegistry do
  @moduledoc """
  Registry for managing storage volumes.

  Uses ETS for concurrent read access with serialized writes through GenServer.
  Maintains two tables for fast lookups: by ID and by name.
  """

  use GenServer
  require Logger

  alias NeonFS.Cluster.State, as: ClusterState
  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.Persistence
  alias NeonFS.Core.RaServer
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.Core.Volume
  alias NeonFS.Core.VolumeEncryption
  alias NeonFS.Events.Broadcaster
  alias NeonFS.Events.{VolumeCreated, VolumeDeleted, VolumeUpdated}

  @type volume_id :: binary()
  @type volume_name :: String.t()

  @system_volume_name "_system"
  @system_volume_protected_fields [:encryption, :owner, :system]

  # Client API

  @doc """
  Adjusts the system volume replication factor to match cluster size.
  """
  @spec adjust_system_volume_replication(pos_integer()) :: {:ok, Volume.t()} | {:error, term()}
  def adjust_system_volume_replication(new_cluster_size)
      when is_integer(new_cluster_size) and new_cluster_size >= 1 do
    GenServer.call(__MODULE__, {:adjust_system_volume_replication, new_cluster_size}, 10_000)
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
    GenServer.call(__MODULE__, {:create, name, opts}, 10_000)
  end

  @doc """
  Creates the system volume. Called once during cluster init.

  The system volume uses a deterministic ID derived from the cluster name
  and has special properties: replicated to all nodes, cannot be deleted
  or renamed, hidden from default volume listings.
  """
  @spec create_system_volume() :: {:ok, Volume.t()} | {:error, term()}
  def create_system_volume do
    GenServer.call(__MODULE__, :create_system_volume, 10_000)
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
    GenServer.call(__MODULE__, {:delete, id}, 10_000)
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
    local_volumes =
      :ets.tab2list(:volumes_by_id)
      |> Enum.map(fn {_id, volume} -> volume end)

    # If local ETS is empty, try to sync from Ra (handles case where
    # VolumeRegistry started before Ra was ready)
    volumes =
      if Enum.empty?(local_volumes) do
        case sync_from_ra() do
          {:ok, synced_volumes} -> synced_volumes
          {:error, _} -> local_volumes
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
    GenServer.call(__MODULE__, {:update, id, opts}, 10_000)
  end

  @doc """
  Updates a volume's statistics (size and chunk count).

  Returns `{:ok, updated_volume}` if successful, or `{:error, :not_found}` if not found.
  """
  @spec update_stats(volume_id(), keyword()) :: {:ok, Volume.t()} | {:error, term()}
  def update_stats(id, stats) do
    GenServer.call(__MODULE__, {:update_stats, id, stats}, 10_000)
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

    # Try to restore volumes from Ra state into ETS
    # If Ra is not ready yet (e.g., during startup), that's okay - the index
    # will be populated as operations occur or when Ra becomes available
    case restore_from_ra() do
      {:ok, count} ->
        Logger.info("VolumeRegistry started, restored #{count} volumes from Ra")

      {:error, reason} ->
        Logger.debug("VolumeRegistry started but Ra not ready yet: #{inspect(reason)}")
    end

    {:ok, %{}}
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
  def handle_call(:create_system_volume, _from, state) do
    reply = do_create_system_volume()
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

  defp do_create_system_volume do
    with {:ok, cluster_name} <- load_cluster_name(),
         {:error, :not_found} <- get_by_name(@system_volume_name) do
      volume = build_system_volume(cluster_name)

      case persist_volume(volume) do
        :ok -> {:ok, volume}
        error -> error
      end
    else
      {:ok, _existing} -> {:error, :already_exists}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_create_volume(name, opts) do
    with :ok <- check_reserved_name(name),
         {:error, :not_found} <- get_by_name(name),
         volume = Volume.new(name, opts),
         :ok <- Volume.validate(volume),
         :ok <- persist_volume(volume) do
      safe_broadcast(volume.id, %VolumeCreated{volume_id: volume.id})
      {:ok, volume}
    else
      {:ok, _} -> {:error, "volume with name '#{name}' already exists"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_delete_volume(id) do
    with {:ok, volume} <- get(id),
         :ok <- check_not_system_volume(volume),
         :ok <- check_volume_empty(volume),
         :ok <- delete_volume_persisted(id, volume) do
      safe_broadcast(id, %VolumeDeleted{volume_id: id})
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

  defp build_system_volume(cluster_name) do
    now = DateTime.utc_now()

    %Volume{
      id: system_volume_id(cluster_name),
      name: @system_volume_name,
      owner: :system,
      durability: %{type: :replicate, factor: 1, min_copies: 1},
      write_ack: :quorum,
      tiering: %{initial_tier: :hot, promotion_threshold: 1, demotion_delay: 1},
      caching: %{
        transformed_chunks: false,
        reconstructed_stripes: false,
        remote_chunks: false,
        max_memory: 1
      },
      io_weight: 100,
      compression: %{algorithm: :zstd, level: 3, min_size: 0},
      verification: %{on_read: :always, sampling_rate: nil},
      encryption: VolumeEncryption.new(),
      metadata_consistency: nil,
      logical_size: 0,
      physical_size: 0,
      chunk_count: 0,
      created_at: now,
      updated_at: now,
      system: true
    }
  end

  defp check_not_system_volume(%Volume{} = volume) do
    if Map.get(volume, :system, false) do
      {:error, :system_volume}
    else
      :ok
    end
  end

  defp check_reserved_name("_" <> _), do: {:error, :reserved_name}
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
        {:error, :system_volume_protected}
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
      {:error, "volume contains #{length(files)} file(s), cannot delete"}
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

      {:error, :ra_not_available} ->
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

      {:error, :ra_not_available} ->
        delete_volume_from_ets(volume)
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  # Event broadcasting

  defp safe_broadcast(volume_id, event) do
    Broadcaster.broadcast(volume_id, event)
  rescue
    _ ->
      Logger.warning("Event broadcast failed for #{inspect(event.__struct__)}")
      :ok
  catch
    :exit, _ ->
      Logger.warning("Event broadcast failed for #{inspect(event.__struct__)}")
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
  defp maybe_ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, result, _leader} ->
        {:ok, result}

      {:error, :noproc} ->
        # Ra server not running - check if it was ever initialized
        if RaServer.initialized?() do
          {:error, :ra_unavailable}
        else
          {:error, :ra_not_available}
        end

      {:error, reason} ->
        {:error, reason}

      {:timeout, _node} ->
        {:error, :timeout}
    end
  catch
    :exit, {:noproc, _} ->
      if RaServer.initialized?() do
        {:error, :ra_unavailable}
      else
        {:error, :ra_not_available}
      end

    kind, reason ->
      Logger.debug("Ra command error: #{inspect({kind, reason})}")

      if RaServer.initialized?() do
        {:error, {:ra_error, {kind, reason}}}
      else
        {:error, :ra_not_available}
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
      durability: volume.durability,
      write_ack: volume.write_ack,
      tiering: volume.tiering,
      caching: volume.caching,
      io_weight: volume.io_weight,
      compression: volume.compression,
      verification: volume.verification,
      encryption: encryption_to_map(volume.encryption),
      logical_size: volume.logical_size,
      physical_size: volume.physical_size,
      chunk_count: volume.chunk_count,
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

    %Volume{
      id: volume_map.id,
      name: volume_map.name,
      owner: volume_map[:owner],
      durability: volume_map.durability,
      write_ack: volume_map.write_ack,
      tiering: tiering,
      caching:
        volume_map[:caching] ||
          %{
            transformed_chunks: true,
            reconstructed_stripes: true,
            remote_chunks: true,
            max_memory: 268_435_456
          },
      io_weight: volume_map[:io_weight] || 100,
      compression: volume_map.compression,
      verification: volume_map.verification,
      encryption: map_to_encryption(volume_map[:encryption]),
      logical_size: volume_map[:logical_size] || 0,
      physical_size: volume_map[:physical_size] || 0,
      chunk_count: volume_map[:chunk_count] || 0,
      created_at: volume_map.created_at,
      updated_at: volume_map.updated_at,
      system: volume_map[:system] || false
    }
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
