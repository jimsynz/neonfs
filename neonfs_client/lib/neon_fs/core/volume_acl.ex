defmodule NeonFS.Core.VolumeACL do
  @moduledoc """
  Volume-level access control using numeric POSIX UIDs/GIDs.

  NeonFS does not manage user identities — it stores and enforces numeric
  UIDs/GIDs, exactly like NFS AUTH_SYS. Name resolution (UID → username) is the
  responsibility of each interface layer (FUSE clients, S3 gateway, CIFS/Samba).

  Permission inheritance: `:admin` implies `:write` implies `:read`.
  The volume owner UID always has all permissions (implicit admin).
  """

  alias __MODULE__

  @type permission :: :read | :write | :admin

  @type principal :: {:uid, non_neg_integer()} | {:gid, non_neg_integer()}

  @type entry :: %{
          principal: principal(),
          permissions: MapSet.t(permission())
        }

  @type t :: %__MODULE__{
          volume_id: String.t(),
          owner_uid: non_neg_integer(),
          owner_gid: non_neg_integer(),
          entries: [entry()]
        }

  defstruct [:volume_id, :owner_uid, :owner_gid, entries: []]

  @doc """
  Creates a new VolumeACL.

  ## Options

  - `:volume_id` - Volume identifier (required)
  - `:owner_uid` - Owner UID (required)
  - `:owner_gid` - Owner GID (required)
  - `:entries` - List of ACL entries (default: [])

  ## Examples

      iex> VolumeACL.new(volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000)
      %VolumeACL{volume_id: "vol-1", owner_uid: 1000, owner_gid: 1000, entries: []}
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    %VolumeACL{
      volume_id: Keyword.fetch!(opts, :volume_id),
      owner_uid: Keyword.fetch!(opts, :owner_uid),
      owner_gid: Keyword.fetch!(opts, :owner_gid),
      entries: Keyword.get(opts, :entries, [])
    }
  end

  @doc """
  Checks if a UID has a specific permission on this volume.

  The owner UID always has all permissions. For non-owners, checks direct UID
  entries and GID entries matching the owner GID.

  See `has_permission?/4` for supplementary GID support.
  """
  @spec has_permission?(t(), non_neg_integer(), permission()) :: boolean()
  def has_permission?(%VolumeACL{} = acl, uid, permission) do
    has_permission?(acl, uid, [], permission)
  end

  @doc """
  Checks if a UID has a specific permission, considering supplementary GIDs.

  Evaluation order:
  1. Owner UID always has all permissions
  2. Direct UID entry match
  3. GID match (owner_gid or any supplementary GID)

  Permission inheritance: `:admin` implies `:write` implies `:read`.
  """
  @spec has_permission?(t(), non_neg_integer(), [non_neg_integer()], permission()) :: boolean()
  def has_permission?(%VolumeACL{owner_uid: uid}, uid, _supplementary_gids, _permission), do: true

  def has_permission?(%VolumeACL{} = acl, uid, supplementary_gids, permission) do
    effective = effective_permissions(acl, uid, supplementary_gids)
    permission_satisfied?(effective, permission)
  end

  @doc """
  Validates a VolumeACL configuration.

  Returns `:ok` if valid, or `{:error, reason}` if invalid.
  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(%VolumeACL{volume_id: id}) when not is_binary(id) or byte_size(id) == 0,
    do: {:error, "volume_id must be a non-empty string"}

  def validate(%VolumeACL{owner_uid: uid}) when not is_integer(uid) or uid < 0,
    do: {:error, "owner_uid must be a non-negative integer"}

  def validate(%VolumeACL{owner_gid: gid}) when not is_integer(gid) or gid < 0,
    do: {:error, "owner_gid must be a non-negative integer"}

  def validate(%VolumeACL{entries: entries}) when not is_list(entries),
    do: {:error, "entries must be a list"}

  def validate(%VolumeACL{entries: entries}) do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {entry, idx}, :ok ->
      case validate_entry(entry) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, "entry #{idx}: #{reason}"}}
      end
    end)
  end

  # --- Private ---

  @all_permissions MapSet.new([:read, :write, :admin])

  defp effective_permissions(%VolumeACL{entries: entries}, uid, supplementary_gids) do
    entries
    |> Enum.filter(fn entry -> matches_principal?(entry.principal, uid, supplementary_gids) end)
    |> Enum.reduce(MapSet.new(), fn entry, acc -> MapSet.union(acc, entry.permissions) end)
  end

  defp matches_principal?({:uid, entry_uid}, uid, _gids), do: entry_uid == uid
  defp matches_principal?({:gid, entry_gid}, _uid, gids), do: entry_gid in gids

  defp permission_satisfied?(effective, :read) do
    MapSet.member?(effective, :read) or
      MapSet.member?(effective, :write) or
      MapSet.member?(effective, :admin)
  end

  defp permission_satisfied?(effective, :write) do
    MapSet.member?(effective, :write) or
      MapSet.member?(effective, :admin)
  end

  defp permission_satisfied?(effective, :admin) do
    MapSet.member?(effective, :admin)
  end

  defp validate_entry(%{principal: {:uid, uid}, permissions: perms})
       when is_integer(uid) and uid >= 0 do
    validate_permissions(perms)
  end

  defp validate_entry(%{principal: {:gid, gid}, permissions: perms})
       when is_integer(gid) and gid >= 0 do
    validate_permissions(perms)
  end

  defp validate_entry(%{principal: principal}) do
    {:error, "invalid principal: #{inspect(principal)}"}
  end

  defp validate_entry(_), do: {:error, "must be a map with :principal and :permissions keys"}

  defp validate_permissions(%MapSet{} = perms) do
    if MapSet.subset?(perms, @all_permissions) do
      :ok
    else
      invalid = MapSet.difference(perms, @all_permissions)
      {:error, "invalid permissions: #{inspect(MapSet.to_list(invalid))}"}
    end
  end

  defp validate_permissions(_), do: {:error, "permissions must be a MapSet"}
end
