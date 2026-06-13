defmodule NeonFS.CLI.Handler.ACL do
  @moduledoc """
  CLI command handlers for access control: per-file POSIX/extended
  ACLs, directory default ACLs, volume-level grants/revokes, and the
  audit-log query (which aggregates events across core nodes).

  Extracted from `NeonFS.CLI.Handler` (#1203). `NeonFS.CLI.Handler`
  delegates its `handle_*_acl`, `handle_acl_*` and `handle_audit_list`
  RPC entry points here, so the CLI wire contract is unchanged.
  """

  import NeonFS.CLI.Handler.Common

  alias NeonFS.Core.{ACLManager, AuditLog, ServiceRegistry, VolumeACL, VolumeRegistry}
  alias NeonFS.Error.{Invalid, VolumeNotFound}

  @doc """
  Sets extended ACL entries on a file or directory.
  """
  @spec handle_set_file_acl(String.t(), String.t(), [map()]) :: {:ok, map()} | {:error, term()}
  def handle_set_file_acl(volume_name, path, acl_entries) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         :ok <- ACLManager.set_file_acl(volume.id, path, acl_entries) do
      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Gets the file ACL (mode + extended entries) for a file or directory.
  """
  @spec handle_get_file_acl(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def handle_get_file_acl(volume_name, path) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, result} <- ACLManager.get_file_acl(volume.id, path) do
      {:ok, result}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Sets the default ACL for a directory.
  """
  @spec handle_set_default_acl(String.t(), String.t(), [map()]) :: {:ok, map()} | {:error, term()}
  def handle_set_default_acl(volume_name, path, default_acl) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         :ok <- ACLManager.set_default_acl(volume.id, path, default_acl) do
      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Lists audit log events with optional filters, aggregated across all
  connected core nodes.
  """
  @spec handle_audit_list(map()) :: {:ok, [map()]}
  def handle_audit_list(filters \\ %{}) do
    set_cli_metadata()

    with :ok <- require_cluster() do
      query_opts = parse_audit_filters(filters)
      local_events = AuditLog.query(query_opts)
      remote_events = collect_remote_audit_events(query_opts)

      events =
        (local_events ++ remote_events)
        |> Enum.sort_by(& &1.timestamp, {:desc, DateTime})
        |> maybe_limit_events(query_opts)
        |> Enum.map(&audit_event_to_map/1)

      {:ok, events}
    end
  end

  @doc """
  Grants permissions to a principal on a volume.
  """
  @spec handle_acl_grant(String.t(), String.t(), [String.t()]) :: {:ok, map()} | {:error, term()}
  def handle_acl_grant(volume_name, principal_str, permissions) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, principal} <- parse_principal(principal_str),
         {:ok, perms} <- parse_permissions(permissions),
         :ok <- ACLManager.grant(volume.id, principal, perms) do
      AuditLog.log_event(
        event_type: :acl_grant,
        actor_uid: 0,
        resource: volume.id,
        details: %{principal: principal_str, permissions: permissions}
      )

      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Revokes all permissions for a principal on a volume.
  """
  @spec handle_acl_revoke(String.t(), String.t()) :: {:ok, map()} | {:error, term()}
  def handle_acl_revoke(volume_name, principal_str) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, principal} <- parse_principal(principal_str),
         :ok <- ACLManager.revoke(volume.id, principal) do
      AuditLog.log_event(
        event_type: :acl_revoke,
        actor_uid: 0,
        resource: volume.id,
        details: %{principal: principal_str}
      )

      {:ok, %{}}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  @doc """
  Shows the volume-level ACL (owner uid/gid + entries).
  """
  @spec handle_acl_show(String.t()) :: {:ok, map()} | {:error, term()}
  def handle_acl_show(volume_name) do
    set_cli_metadata()

    with :ok <- require_cluster(),
         {:ok, volume} <- VolumeRegistry.get_by_name(volume_name),
         {:ok, acl} <- ACLManager.get_volume_acl(volume.id) do
      {:ok, volume_acl_to_map(acl)}
    else
      {:error, :not_found} ->
        {:error, VolumeNotFound.exception(volume_name: volume_name)}

      {:error, reason} ->
        {:error, wrap_error(reason)}
    end
  end

  # Private

  defp audit_event_to_map(%NeonFS.Core.AuditEvent{} = event) do
    %{
      id: event.id,
      timestamp: DateTime.to_iso8601(event.timestamp),
      event_type: Atom.to_string(event.event_type),
      actor_uid: event.actor_uid,
      actor_node: Atom.to_string(event.actor_node),
      resource: event.resource,
      details: event.details,
      outcome: Atom.to_string(event.outcome)
    }
  end

  defp parse_audit_filters(filters) do
    []
    |> maybe_add_filter(:event_type, parse_event_type(Map.get(filters, "type")))
    |> maybe_add_filter(:actor_uid, Map.get(filters, "actor_uid"))
    |> maybe_add_filter(:resource, Map.get(filters, "resource"))
    |> maybe_add_filter(:since, parse_datetime(Map.get(filters, "since")))
    |> maybe_add_filter(:until, parse_datetime(Map.get(filters, "until")))
    |> maybe_add_filter(:limit, Map.get(filters, "limit"))
  end

  defp maybe_add_filter(opts, _key, nil), do: opts
  defp maybe_add_filter(opts, key, value), do: Keyword.put(opts, key, value)

  defp parse_event_type(nil), do: nil

  defp parse_event_type(type) when is_binary(type) do
    String.to_existing_atom(type)
  rescue
    ArgumentError -> nil
  end

  defp parse_datetime(nil), do: nil

  defp parse_datetime(dt_string) when is_binary(dt_string) do
    case DateTime.from_iso8601(dt_string) do
      {:ok, dt, _offset} -> dt
      _ -> nil
    end
  end

  defp collect_remote_audit_events(query_opts) do
    for node <- ServiceRegistry.connected_nodes_by_type(:core), reduce: [] do
      acc ->
        case safe_remote_audit_query(node, query_opts) do
          events when is_list(events) -> events ++ acc
          _ -> acc
        end
    end
  end

  defp safe_remote_audit_query(node, query_opts) do
    :erpc.call(node, AuditLog, :query, [query_opts], 5_000)
  catch
    :exit, _ -> []
  end

  defp maybe_limit_events(events, query_opts) do
    limit = Keyword.get(query_opts, :limit, 100)
    Enum.take(events, limit)
  end

  defp volume_acl_to_map(%VolumeACL{} = acl) do
    %{
      volume_id: acl.volume_id,
      owner_uid: acl.owner_uid,
      owner_gid: acl.owner_gid,
      entries:
        Enum.map(acl.entries, fn entry ->
          {type, id} = entry.principal

          %{
            principal: "#{type}:#{id}",
            permissions: entry.permissions |> MapSet.to_list() |> Enum.map(&Atom.to_string/1)
          }
        end)
    }
  end

  defp parse_principal("uid:" <> uid_str) do
    case Integer.parse(uid_str) do
      {uid, ""} when uid >= 0 -> {:ok, {:uid, uid}}
      _ -> {:error, Invalid.exception(message: "Invalid UID: #{uid_str}")}
    end
  end

  defp parse_principal("gid:" <> gid_str) do
    case Integer.parse(gid_str) do
      {gid, ""} when gid >= 0 -> {:ok, {:gid, gid}}
      _ -> {:error, Invalid.exception(message: "Invalid GID: #{gid_str}")}
    end
  end

  defp parse_principal(other) do
    {:error, Invalid.exception(message: "Invalid principal format: #{other}. Use uid:N or gid:N")}
  end

  defp parse_permissions(perm_strings) when is_list(perm_strings) do
    perms = Enum.map(perm_strings, &String.to_existing_atom/1)
    valid = [:read, :write, :admin]
    invalid = Enum.reject(perms, &(&1 in valid))

    if invalid == [] do
      {:ok, perms}
    else
      {:error, Invalid.exception(message: "Invalid permissions: #{inspect(invalid)}")}
    end
  rescue
    ArgumentError ->
      {:error, Invalid.exception(message: "Invalid permission name. Valid: read, write, admin")}
  end
end
