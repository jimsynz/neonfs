defmodule NeonFS.Core.ACLManager do
  @moduledoc """
  Manages volume ACLs with ETS cache and Ra-backed durability.

  Provides fast permission lookups via a public ETS table (`:volume_acls`),
  with all mutations persisted through Ra consensus. Restores state from Ra
  on startup.

  Follows the VolumeRegistry pattern: serialised writes through GenServer,
  concurrent reads directly from ETS.
  """

  use GenServer
  require Logger

  alias NeonFS.Core.{AuditLog, FileIndex, Persistence, RaSupervisor, VolumeACL}
  alias NeonFS.Events.Broadcaster
  alias NeonFS.Events.{FileAclChanged, VolumeAclChanged}

  @ets_table :volume_acls

  # Client API

  @doc """
  Starts the ACL manager.
  """
  @spec start_link(keyword()) :: GenServer.on_start()
  def start_link(opts \\ []) do
    GenServer.start_link(__MODULE__, opts, name: __MODULE__)
  end

  @doc """
  Creates or replaces the ACL for a volume.
  """
  @spec set_volume_acl(binary(), VolumeACL.t()) :: :ok | {:error, term()}
  def set_volume_acl(volume_id, %VolumeACL{} = acl) do
    GenServer.call(__MODULE__, {:set_volume_acl, volume_id, acl})
  end

  @doc """
  Grants permissions to a principal on a volume.

  If the principal already has an entry, it is replaced.
  """
  @spec grant(binary(), VolumeACL.principal(), [VolumeACL.permission()]) :: :ok | {:error, term()}
  def grant(volume_id, principal, permissions) do
    GenServer.call(__MODULE__, {:grant, volume_id, principal, permissions})
  end

  @doc """
  Revokes all permissions for a principal on a volume.
  """
  @spec revoke(binary(), VolumeACL.principal()) :: :ok | {:error, term()}
  def revoke(volume_id, principal) do
    GenServer.call(__MODULE__, {:revoke, volume_id, principal})
  end

  @doc """
  Gets the ACL for a volume from ETS cache.

  This is a direct ETS read — no GenServer call, no Ra round-trip.
  """
  @spec get_volume_acl(binary()) :: {:ok, VolumeACL.t()} | {:error, :not_found}
  def get_volume_acl(volume_id) do
    case :ets.whereis(@ets_table) do
      :undefined ->
        {:error, :not_found}

      _ref ->
        case :ets.lookup(@ets_table, volume_id) do
          [{^volume_id, acl}] -> {:ok, acl}
          [] -> {:error, :not_found}
        end
    end
  end

  @doc """
  Deletes the ACL for a volume from ETS cache and Ra.
  """
  @spec delete_volume_acl(binary()) :: :ok
  def delete_volume_acl(volume_id) do
    GenServer.call(__MODULE__, {:delete_volume_acl, volume_id})
  end

  @doc """
  Sets extended ACL entries on a file or directory.

  Updates the file's `acl_entries` field via FileIndex.
  """
  @spec set_file_acl(binary(), String.t(), [map()]) :: :ok | {:error, term()}
  def set_file_acl(volume_id, path, acl_entries) when is_list(acl_entries) do
    GenServer.call(__MODULE__, {:set_file_acl, volume_id, path, acl_entries})
  end

  @doc """
  Retrieves the file ACL (mode + extended entries) for a file or directory.

  This is a direct read from FileIndex — no GenServer call needed.
  """
  @spec get_file_acl(binary(), String.t()) ::
          {:ok,
           %{
             mode: non_neg_integer(),
             uid: non_neg_integer(),
             gid: non_neg_integer(),
             acl_entries: [map()],
             default_acl: [map()] | nil
           }}
          | {:error, :not_found}
  def get_file_acl(volume_id, path) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, file_meta} ->
        {:ok,
         %{
           mode: file_meta.mode,
           uid: file_meta.uid,
           gid: file_meta.gid,
           acl_entries: file_meta.acl_entries,
           default_acl: file_meta.default_acl
         }}

      {:error, :not_found} ->
        {:error, :not_found}
    end
  end

  @doc """
  Sets the default ACL for a directory.

  The default ACL is inherited by newly created children.
  """
  @spec set_default_acl(binary(), String.t(), [map()]) :: :ok | {:error, term()}
  def set_default_acl(volume_id, path, default_acl) when is_list(default_acl) do
    GenServer.call(__MODULE__, {:set_default_acl, volume_id, path, default_acl})
  end

  # Server Callbacks

  @impl true
  def init(_opts) do
    Process.flag(:trap_exit, true)
    :ets.new(@ets_table, [:set, :named_table, :public, read_concurrency: true])
    {:ok, %{}, {:continue, :load_from_ra}}
  end

  @impl true
  def handle_continue(:load_from_ra, state) do
    load_acls_from_ra()
    {:noreply, state}
  end

  @impl true
  def handle_call({:set_volume_acl, volume_id, acl}, _from, state) do
    reply = persist_and_cache(volume_id, acl)

    if reply == :ok do
      AuditLog.log_event(
        event_type: :volume_acl_changed,
        actor_uid: acl.owner_uid,
        resource: volume_id,
        details: %{action: :set, owner_uid: acl.owner_uid, owner_gid: acl.owner_gid}
      )

      safe_broadcast(volume_id, %VolumeAclChanged{volume_id: volume_id})
    end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:grant, volume_id, principal, permissions}, _from, state) do
    case get_volume_acl(volume_id) do
      {:ok, acl} ->
        entry = %{principal: principal, permissions: MapSet.new(permissions)}
        updated_entries = upsert_entry(acl.entries, principal, entry)
        updated_acl = %{acl | entries: updated_entries}
        reply = persist_and_cache(volume_id, updated_acl)

        if reply == :ok do
          AuditLog.log_event(
            event_type: :volume_acl_changed,
            actor_uid: 0,
            resource: volume_id,
            details: %{action: :grant, principal: principal, permissions: permissions}
          )

          safe_broadcast(volume_id, %VolumeAclChanged{volume_id: volume_id})
        end

        {:reply, reply, state}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:revoke, volume_id, principal}, _from, state) do
    case get_volume_acl(volume_id) do
      {:ok, acl} ->
        updated_entries = Enum.reject(acl.entries, &(&1.principal == principal))
        updated_acl = %{acl | entries: updated_entries}
        reply = persist_and_cache(volume_id, updated_acl)

        if reply == :ok do
          AuditLog.log_event(
            event_type: :volume_acl_changed,
            actor_uid: 0,
            resource: volume_id,
            details: %{action: :revoke, principal: principal}
          )

          safe_broadcast(volume_id, %VolumeAclChanged{volume_id: volume_id})
        end

        {:reply, reply, state}

      {:error, :not_found} ->
        {:reply, {:error, :not_found}, state}
    end
  end

  @impl true
  def handle_call({:delete_volume_acl, volume_id}, _from, state) do
    :ets.delete(@ets_table, volume_id)
    safe_broadcast(volume_id, %VolumeAclChanged{volume_id: volume_id})
    {:reply, :ok, state}
  end

  @impl true
  def handle_call({:set_file_acl, volume_id, path, acl_entries}, _from, state) do
    reply =
      case FileIndex.get_by_path(volume_id, path) do
        {:ok, file_meta} ->
          case FileIndex.update(file_meta.id, acl_entries: acl_entries) do
            {:ok, _updated} ->
              AuditLog.log_event(
                event_type: :file_acl_changed,
                actor_uid: 0,
                resource: volume_id,
                details: %{path: path, entries_count: length(acl_entries)}
              )

              safe_broadcast(volume_id, %FileAclChanged{volume_id: volume_id, path: path})
              :ok

            {:error, reason} ->
              {:error, reason}
          end

        {:error, :not_found} ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  @impl true
  def handle_call({:set_default_acl, volume_id, path, default_acl}, _from, state) do
    reply =
      case FileIndex.get_by_path(volume_id, path) do
        {:ok, file_meta} ->
          case FileIndex.update(file_meta.id, default_acl: default_acl) do
            {:ok, _updated} -> :ok
            {:error, reason} -> {:error, reason}
          end

        {:error, :not_found} ->
          {:error, :not_found}
      end

    {:reply, reply, state}
  end

  @impl true
  def terminate(_reason, _state) do
    meta_dir = Persistence.meta_dir()
    dets_path = Path.join(meta_dir, "volume_acls.dets")
    Persistence.snapshot_table(@ets_table, dets_path)
    Logger.info("ACLManager table saved")
    :ok
  rescue
    _ -> :ok
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

  # Private

  defp persist_and_cache(volume_id, acl) do
    acl_data = acl_to_map(acl)

    case maybe_ra_command({:put_volume_acl, volume_id, acl_data}) do
      {:ok, :ok} ->
        :ets.insert(@ets_table, {volume_id, acl})
        :ok

      {:error, :ra_not_available} ->
        :ets.insert(@ets_table, {volume_id, acl})
        :ok

      {:error, reason} ->
        {:error, reason}
    end
  end

  defp upsert_entry(entries, principal, new_entry) do
    case Enum.find_index(entries, &(&1.principal == principal)) do
      nil -> entries ++ [new_entry]
      idx -> List.replace_at(entries, idx, new_entry)
    end
  end

  defp load_acls_from_ra do
    query_fn = fn state -> Map.get(state, :volume_acls, %{}) end

    case RaSupervisor.query(query_fn) do
      {:ok, acl_map} when is_map(acl_map) ->
        Enum.each(acl_map, fn {volume_id, acl_data} ->
          acl = map_to_acl(volume_id, acl_data)
          :ets.insert(@ets_table, {volume_id, acl})
        end)

        count = map_size(acl_map)
        if count > 0, do: Logger.info("Loaded volume ACLs from Ra", count: count)

      {:error, reason} ->
        Logger.debug("Could not load ACLs from Ra", reason: reason)
    end
  rescue
    e -> Logger.debug("Could not load ACLs from Ra", reason: e)
  catch
    :exit, reason -> Logger.debug("Could not load ACLs from Ra (exit)", reason: reason)
  end

  defp acl_to_map(%VolumeACL{} = acl) do
    %{
      owner_uid: acl.owner_uid,
      owner_gid: acl.owner_gid,
      entries: Enum.map(acl.entries, &entry_to_map/1)
    }
  end

  defp entry_to_map(%{principal: principal, permissions: permissions}) do
    %{principal: principal, permissions: MapSet.to_list(permissions)}
  end

  defp map_to_acl(volume_id, acl_data) do
    entries = Enum.map(Map.get(acl_data, :entries, []), &map_to_entry/1)

    %VolumeACL{
      volume_id: volume_id,
      owner_uid: Map.get(acl_data, :owner_uid, 0),
      owner_gid: Map.get(acl_data, :owner_gid, 0),
      entries: entries
    }
  end

  defp map_to_entry(entry) do
    %{
      principal: Map.get(entry, :principal),
      permissions: MapSet.new(Map.get(entry, :permissions, []))
    }
  end

  # Follow VolumeRegistry pattern for Ra interaction
  defp maybe_ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, result, _leader} -> {:ok, result}
      {:error, :noproc} -> {:error, :ra_not_available}
      {:error, reason} -> {:error, reason}
      {:timeout, _node} -> {:error, :timeout}
    end
  catch
    :exit, _ -> {:error, :ra_not_available}
  end
end
