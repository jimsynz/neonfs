defmodule NeonFS.Core.ACLManager do
  @moduledoc """
  Stateless facade over Ra-backed volume and file ACLs.

  All volume ACL reads go through `RaSupervisor.local_query/2` against
  the `MetadataStateMachine`; all writes go through `RaSupervisor.command/2`.
  `grant/3` and `revoke/2` are implemented as dedicated Ra commands so the
  read-modify-write is atomic on the state machine — concurrent grants on
  the same volume can't lose entries to a race.

  File ACLs live on `FileMeta` and are read/written through `FileIndex`,
  which already serialises mutations.
  """

  require Logger

  alias NeonFS.Core.{AuditLog, FileIndex, MetadataStateMachine, RaSupervisor, VolumeACL}
  alias NeonFS.Events.Broadcaster
  alias NeonFS.Events.{FileAclChanged, VolumeAclChanged}

  @doc """
  Creates or replaces the ACL for a volume.
  """
  @spec set_volume_acl(binary(), VolumeACL.t()) :: :ok | {:error, term()}
  def set_volume_acl(volume_id, %VolumeACL{} = acl) do
    case ra_command({:put_volume_acl, volume_id, acl_to_map(acl)}) do
      :ok ->
        AuditLog.log_event(
          event_type: :volume_acl_changed,
          actor_uid: acl.owner_uid,
          resource: volume_id,
          details: %{action: :set, owner_uid: acl.owner_uid, owner_gid: acl.owner_gid}
        )

        safe_broadcast(volume_id, %VolumeAclChanged{volume_id: volume_id})
        :ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Grants permissions to a principal on a volume.

  Atomic on Ra: if the principal already has an entry it's replaced,
  otherwise it's appended. Returns `{:error, :not_found}` if the volume
  has no ACL.
  """
  @spec grant(binary(), VolumeACL.principal(), [VolumeACL.permission()]) ::
          :ok | {:error, term()}
  def grant(volume_id, principal, permissions) do
    case ra_command({:grant_volume_acl_entry, volume_id, principal, Enum.to_list(permissions)}) do
      :ok ->
        AuditLog.log_event(
          event_type: :volume_acl_changed,
          actor_uid: 0,
          resource: volume_id,
          details: %{action: :grant, principal: principal, permissions: permissions}
        )

        safe_broadcast(volume_id, %VolumeAclChanged{volume_id: volume_id})
        :ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Revokes all permissions for a principal on a volume.

  No-op if the principal has no entry. Returns `{:error, :not_found}` if
  the volume has no ACL.
  """
  @spec revoke(binary(), VolumeACL.principal()) :: :ok | {:error, term()}
  def revoke(volume_id, principal) do
    case ra_command({:revoke_volume_acl_entry, volume_id, principal}) do
      :ok ->
        AuditLog.log_event(
          event_type: :volume_acl_changed,
          actor_uid: 0,
          resource: volume_id,
          details: %{action: :revoke, principal: principal}
        )

        safe_broadcast(volume_id, %VolumeAclChanged{volume_id: volume_id})
        :ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Gets the ACL for a volume from the local Ra replica.

  Reads go directly to Ra with no process hop. The `apply/3` callback
  runs on every cluster member as commands commit, so the local
  replica's view is the committed state.
  """
  @spec get_volume_acl(binary()) :: {:ok, VolumeACL.t()} | {:error, :not_found}
  def get_volume_acl(volume_id) do
    case read_volume_acl(volume_id) do
      {:ok, acl_data} when is_map(acl_data) ->
        {:ok, map_to_acl(volume_id, acl_data)}

      {:ok, nil} ->
        {:error, :not_found}

      {:error, _} ->
        {:error, :not_found}
    end
  end

  @doc """
  Deletes the ACL for a volume via Ra.
  """
  @spec delete_volume_acl(binary()) :: :ok | {:error, term()}
  def delete_volume_acl(volume_id) do
    case ra_command({:delete_volume_acl, volume_id}) do
      :ok ->
        safe_broadcast(volume_id, %VolumeAclChanged{volume_id: volume_id})
        :ok

      {:error, _} = error ->
        error
    end
  end

  @doc """
  Sets extended ACL entries on a file or directory.

  Updates the file's `acl_entries` field via FileIndex.
  """
  @spec set_file_acl(binary(), String.t(), [map()]) :: :ok | {:error, term()}
  def set_file_acl(volume_id, path, acl_entries) when is_list(acl_entries) do
    with {:ok, file_meta} <- FileIndex.get_by_path(volume_id, path),
         {:ok, _updated} <- FileIndex.update(file_meta.id, acl_entries: acl_entries) do
      AuditLog.log_event(
        event_type: :file_acl_changed,
        actor_uid: 0,
        resource: volume_id,
        details: %{path: path, entries_count: length(acl_entries)}
      )

      safe_broadcast(volume_id, %FileAclChanged{volume_id: volume_id, path: path})
      :ok
    end
  end

  @doc """
  Retrieves the file ACL (mode + extended entries) for a file or directory.

  This is a direct read from FileIndex.
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
    with {:ok, file_meta} <- FileIndex.get_by_path(volume_id, path),
         {:ok, _updated} <- FileIndex.update(file_meta.id, default_acl: default_acl) do
      :ok
    end
  end

  # Private

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

  defp read_volume_acl(volume_id) do
    RaSupervisor.local_query(&MetadataStateMachine.get_volume_acl(&1, volume_id))
  catch
    :exit, _ -> {:error, :ra_not_available}
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

  defp ra_command(cmd) do
    case RaSupervisor.command(cmd) do
      {:ok, :ok, _leader} -> :ok
      {:ok, {:error, reason}, _leader} -> {:error, reason}
      {:ok, other, _leader} -> {:error, {:unexpected_reply, other}}
      {:error, :noproc} -> {:error, :ra_not_available}
      {:error, reason} -> {:error, reason}
      {:timeout, _node} -> {:error, :timeout}
    end
  catch
    :exit, _ -> {:error, :ra_not_available}
  end
end
