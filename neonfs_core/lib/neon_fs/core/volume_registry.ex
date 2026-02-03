defmodule NeonFS.Core.VolumeRegistry do
  @moduledoc """
  Registry for managing storage volumes.

  Uses ETS for concurrent read access with serialized writes through GenServer.
  Maintains two tables for fast lookups: by ID and by name.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.FileIndex
  alias NeonFS.Core.Persistence
  alias NeonFS.Core.RaServer
  alias NeonFS.Core.RaSupervisor
  alias NeonFS.Core.Volume

  @type volume_id :: binary()
  @type volume_name :: String.t()

  # Client API

  @doc """
  Starts the volume registry.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates a new volume with the given name and configuration.

  Returns `{:ok, volume}` if successful, or `{:error, reason}` if:
  - Name already exists
  - Configuration is invalid
  """
  @spec create(String.t(), keyword()) :: {:ok, Volume.t()} | {:error, term()}
  def create(name, opts \\ []) do
    GenServer.call(__MODULE__, {:create, name, opts}, 10_000)
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
  Lists all volumes.

  Returns a list of all volume structs.
  """
  @spec list() :: [Volume.t()]
  def list do
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

    Enum.sort_by(volumes, & &1.name)
  end

  @doc """
  Updates a volume's configuration.

  Returns `{:ok, updated_volume}` if successful, or `{:error, reason}` if:
  - Volume not found
  - New configuration is invalid
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

  @doc """
  Deletes a volume.

  Returns `:ok` if successful, or `{:error, reason}` if:
  - Volume not found
  - Volume contains files (must be empty)
  """
  @spec delete(volume_id()) :: :ok | {:error, term()}
  def delete(id) do
    GenServer.call(__MODULE__, {:delete, id}, 10_000)
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
  def handle_call({:create, name, opts}, _from, state) do
    reply = do_create_volume(name, opts)
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
  def handle_call({:delete, id}, _from, state) do
    reply = do_delete_volume(id)
    {:reply, reply, state}
  end

  defp do_create_volume(name, opts) do
    with {:error, :not_found} <- get_by_name(name),
         volume = Volume.new(name, opts),
         :ok <- Volume.validate(volume),
         :ok <- persist_volume(volume) do
      {:ok, volume}
    else
      {:ok, _} -> {:error, "volume with name '#{name}' already exists"}
      {:error, reason} -> {:error, reason}
    end
  end

  defp do_update_volume(id, opts) do
    with {:ok, volume} <- get(id),
         updated = Volume.update(volume, opts),
         :ok <- Volume.validate(updated),
         :ok <- persist_volume(updated) do
      {:ok, updated}
    end
  end

  defp do_update_stats(id, stats) do
    with {:ok, volume} <- get(id),
         updated = Volume.update_stats(volume, stats),
         :ok <- persist_volume(updated) do
      {:ok, updated}
    end
  end

  defp do_delete_volume(id) do
    with {:ok, volume} <- get(id),
         :ok <- check_volume_empty(volume) do
      delete_volume_persisted(id, volume)
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
      initial_tier: volume.initial_tier,
      compression: volume.compression,
      verification: volume.verification,
      logical_size: volume.logical_size,
      physical_size: volume.physical_size,
      chunk_count: volume.chunk_count,
      created_at: volume.created_at,
      updated_at: volume.updated_at
    }
  end

  # Convert map from Ra storage to Volume struct
  defp map_to_volume(volume_map) when is_map(volume_map) do
    %Volume{
      id: volume_map.id,
      name: volume_map.name,
      owner: volume_map[:owner],
      durability: volume_map.durability,
      write_ack: volume_map.write_ack,
      initial_tier: volume_map.initial_tier,
      compression: volume_map.compression,
      verification: volume_map.verification,
      logical_size: volume_map[:logical_size] || 0,
      physical_size: volume_map[:physical_size] || 0,
      chunk_count: volume_map[:chunk_count] || 0,
      created_at: volume_map.created_at,
      updated_at: volume_map.updated_at
    }
  end
end
