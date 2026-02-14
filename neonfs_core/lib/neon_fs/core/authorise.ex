defmodule NeonFS.Core.Authorise do
  @moduledoc """
  Authorisation checks for volume and file-level access control.

  Supports two resource types:
  - `{:volume, volume_id}` — volume-level ACL check via `VolumeACL`
  - `{:file, volume_id, path}` — file-level POSIX mode + extended ACL check via `FileACL`

  UID 0 (root) bypasses all checks. For volume resources, the volume owner UID
  has implicit full control. For file resources, POSIX mode bits and extended
  ACL entries are evaluated per POSIX.1e semantics.

  Permission inheritance (volume-level): `:admin` implies `:write` implies `:read`.

  ## Actions

  - `:read` — read file data from a volume
  - `:write` — write file data to a volume
  - `:admin` — administrative operations (delete volume, modify ACLs)
  - `:mount` — mount a volume (requires `:read` permission)
  """

  alias NeonFS.Core.{ACLManager, AuditLog, FileACL, FileIndex, VolumeACL}

  @type action :: :read | :write | :admin | :mount

  @doc """
  Checks if a UID is authorised for an action on a resource.

  Returns `:ok` if authorised, `{:error, :forbidden}` otherwise.
  UID 0 (root) bypasses all permission checks.
  """
  @spec check(non_neg_integer(), action(), term()) :: :ok | {:error, :forbidden}
  def check(uid, action, resource) do
    check(uid, [], action, resource)
  end

  @doc """
  Checks if a UID is authorised for an action on a resource,
  considering supplementary GIDs.
  """
  @spec check(non_neg_integer(), [non_neg_integer()], action(), term()) ::
          :ok | {:error, :forbidden}
  def check(0, _gids, _action, _resource), do: :ok

  def check(uid, gids, action, {:volume, _volume_id} = resource) do
    case resolve_volume_acl(resource) do
      {:ok, acl} ->
        permission = action_to_permission(action)

        if VolumeACL.has_permission?(acl, uid, gids, permission) do
          emit_granted(uid, action, resource)
          :ok
        else
          emit_denied(uid, action, resource)
          {:error, :forbidden}
        end

      :no_acl ->
        emit_denied(uid, action, resource)
        {:error, :forbidden}
    end
  end

  def check(uid, gids, action, {:file, volume_id, path} = resource) do
    case resolve_file_acl(volume_id, path) do
      {:ok, file_acl} ->
        permission = action_to_file_permission(action)

        case FileACL.check_access(file_acl, uid, gids |> List.first(0), gids, permission) do
          :ok ->
            emit_granted(uid, action, resource)
            :ok

          {:error, :forbidden} ->
            emit_denied(uid, action, resource)
            {:error, :forbidden}
        end

      :no_acl ->
        # No file found — fall through to volume-level check
        check(uid, gids, action, {:volume, volume_id})
    end
  end

  # Private

  defp resolve_volume_acl({:volume, volume_id}) do
    case ACLManager.get_volume_acl(volume_id) do
      {:ok, acl} -> {:ok, acl}
      {:error, :not_found} -> :no_acl
    end
  end

  defp resolve_file_acl(volume_id, path) do
    case FileIndex.get_by_path(volume_id, path) do
      {:ok, file_meta} ->
        {:ok,
         FileACL.new(
           mode: file_meta.mode,
           uid: file_meta.uid,
           gid: file_meta.gid,
           acl_entries: file_meta.acl_entries
         )}

      {:error, :not_found} ->
        :no_acl
    end
  rescue
    _ -> :no_acl
  catch
    :exit, _ -> :no_acl
  end

  defp action_to_permission(:read), do: :read
  defp action_to_permission(:write), do: :write
  defp action_to_permission(:admin), do: :admin
  defp action_to_permission(:mount), do: :read

  defp action_to_file_permission(:read), do: :r
  defp action_to_file_permission(:write), do: :w
  defp action_to_file_permission(:admin), do: :w
  defp action_to_file_permission(:mount), do: :r

  defp emit_granted(uid, action, resource) do
    :telemetry.execute(
      [:neonfs, :authorise, :granted],
      %{},
      %{uid: uid, action: action, resource: resource}
    )
  end

  defp emit_denied(uid, action, resource) do
    :telemetry.execute(
      [:neonfs, :authorise, :denied],
      %{},
      %{uid: uid, action: action, resource: resource}
    )

    AuditLog.log_event(
      event_type: :authorisation_denied,
      actor_uid: uid,
      resource: format_resource(resource),
      outcome: :denied,
      details: %{action: action, resource: resource}
    )
  end

  defp format_resource({:volume, volume_id}), do: volume_id
  defp format_resource({:file, volume_id, path}), do: "#{volume_id}:#{path}"
end
