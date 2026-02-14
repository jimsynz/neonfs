defmodule NeonFS.Core.FileACL do
  @moduledoc """
  File-level access control using POSIX mode bits and extended ACL entries.

  Implements standard POSIX ACL evaluation order:
  1. If requesting_uid == owner_uid, use owner permission bits
  2. If extended ACL entries exist and a named user entry matches, use it masked
     by the mask entry
  3. If requesting_gid == owner_gid or any supplementary GID matches the owning
     group, use group permission bits (masked if extended ACLs exist)
  4. If a named group entry matches any of the requesting GIDs, use it masked
  5. Use other permission bits

  The mask entry limits effective permissions of named users and named groups,
  but does NOT affect owner or other permissions.
  """

  alias __MODULE__

  @type permission :: :r | :w | :x

  @type acl_type :: :user | :group | :mask | :other

  @type acl_entry :: %{
          type: acl_type(),
          id: non_neg_integer() | nil,
          permissions: MapSet.t(permission())
        }

  @type t :: %__MODULE__{
          mode: non_neg_integer(),
          uid: non_neg_integer(),
          gid: non_neg_integer(),
          acl_entries: [acl_entry()]
        }

  defstruct [:mode, :uid, :gid, acl_entries: []]

  @doc """
  Creates a new FileACL.

  ## Options

  - `:mode` - POSIX mode bits as integer (required), e.g. 0o755
  - `:uid` - Owner UID (required)
  - `:gid` - Owner GID (required)
  - `:acl_entries` - List of extended ACL entries (default: [])

  ## Examples

      iex> FileACL.new(mode: 0o755, uid: 1000, gid: 1000)
      %FileACL{mode: 0o755, uid: 1000, gid: 1000, acl_entries: []}
  """
  @spec new(keyword()) :: t()
  def new(opts) do
    %FileACL{
      mode: Keyword.fetch!(opts, :mode),
      uid: Keyword.fetch!(opts, :uid),
      gid: Keyword.fetch!(opts, :gid),
      acl_entries: Keyword.get(opts, :acl_entries, [])
    }
  end

  @doc """
  Checks whether a requesting user has the specified permission on this file.

  Returns `:ok` if access is granted, or `{:error, :forbidden}` if denied.

  See `check_access/5` for supplementary GID support.
  """
  @spec check_access(t(), non_neg_integer(), non_neg_integer(), permission()) ::
          :ok | {:error, :forbidden}
  def check_access(%FileACL{} = acl, requesting_uid, requesting_gid, permission) do
    check_access(acl, requesting_uid, requesting_gid, [], permission)
  end

  @doc """
  Checks access with supplementary GIDs.

  Implements POSIX ACL evaluation order:
  1. Owner match — use owner bits from mode
  2. Named user ACL entry — use entry permissions masked by mask entry
  3. Owning group match — use group bits from mode (masked if extended ACLs)
  4. Named group ACL entry — use entry permissions masked by mask entry
  5. Other — use other bits from mode

  Root (UID 0) is NOT given special treatment — this is a storage system,
  not an OS kernel. The interface layer (FUSE) handles root mapping.
  """
  @spec check_access(t(), non_neg_integer(), non_neg_integer(), [non_neg_integer()], permission()) ::
          :ok | {:error, :forbidden}
  def check_access(
        %FileACL{} = acl,
        requesting_uid,
        requesting_gid,
        supplementary_gids,
        permission
      ) do
    all_gids = [requesting_gid | supplementary_gids]

    cond do
      # 1. Owner match
      requesting_uid == acl.uid ->
        if has_owner_permission?(acl.mode, permission), do: :ok, else: {:error, :forbidden}

      # 2. Named user ACL entry
      named_user = find_named_user(acl.acl_entries, requesting_uid) ->
        effective = apply_mask(named_user.permissions, acl.acl_entries)
        if MapSet.member?(effective, permission), do: :ok, else: {:error, :forbidden}

      # 3-4. Group matching (owning group + named group entries)
      group_result = check_group_access(acl, all_gids, permission) ->
        group_result

      # 5. Other
      true ->
        if has_other_permission?(acl.mode, permission), do: :ok, else: {:error, :forbidden}
    end
  end

  @doc """
  Validates a FileACL configuration.

  Returns `:ok` if valid, or `{:error, reason}` if invalid.
  """
  @spec validate(t()) :: :ok | {:error, String.t()}
  def validate(%FileACL{mode: mode}) when not is_integer(mode) or mode < 0 or mode > 0o7777,
    do: {:error, "mode must be an integer between 0 and 0o7777"}

  def validate(%FileACL{uid: uid}) when not is_integer(uid) or uid < 0,
    do: {:error, "uid must be a non-negative integer"}

  def validate(%FileACL{gid: gid}) when not is_integer(gid) or gid < 0,
    do: {:error, "gid must be a non-negative integer"}

  def validate(%FileACL{acl_entries: entries}) when not is_list(entries),
    do: {:error, "acl_entries must be a list"}

  def validate(%FileACL{acl_entries: entries}) do
    entries
    |> Enum.with_index()
    |> Enum.reduce_while(:ok, fn {entry, idx}, :ok ->
      case validate_acl_entry(entry) do
        :ok -> {:cont, :ok}
        {:error, reason} -> {:halt, {:error, "acl_entry #{idx}: #{reason}"}}
      end
    end)
  end

  # --- Private ---

  @all_permissions MapSet.new([:r, :w, :x])

  # Owner permission bits (bits 8-6)
  defp has_owner_permission?(mode, :r), do: Bitwise.band(mode, 0o400) != 0
  defp has_owner_permission?(mode, :w), do: Bitwise.band(mode, 0o200) != 0
  defp has_owner_permission?(mode, :x), do: Bitwise.band(mode, 0o100) != 0

  # Group permission bits (bits 5-3)
  defp has_group_permission?(mode, :r), do: Bitwise.band(mode, 0o040) != 0
  defp has_group_permission?(mode, :w), do: Bitwise.band(mode, 0o020) != 0
  defp has_group_permission?(mode, :x), do: Bitwise.band(mode, 0o010) != 0

  # Other permission bits (bits 2-0)
  defp has_other_permission?(mode, :r), do: Bitwise.band(mode, 0o004) != 0
  defp has_other_permission?(mode, :w), do: Bitwise.band(mode, 0o002) != 0
  defp has_other_permission?(mode, :x), do: Bitwise.band(mode, 0o001) != 0

  defp find_named_user(acl_entries, uid) do
    Enum.find(acl_entries, fn
      %{type: :user, id: id} when id == uid -> true
      _ -> false
    end)
  end

  defp find_named_groups(acl_entries, gids) do
    Enum.filter(acl_entries, fn
      %{type: :group, id: id} when not is_nil(id) -> id in gids
      _ -> false
    end)
  end

  defp find_mask(acl_entries) do
    Enum.find(acl_entries, fn
      %{type: :mask} -> true
      _ -> false
    end)
  end

  defp apply_mask(permissions, acl_entries) do
    case find_mask(acl_entries) do
      nil -> permissions
      %{permissions: mask_perms} -> MapSet.intersection(permissions, mask_perms)
    end
  end

  defp check_group_access(%FileACL{} = acl, all_gids, permission) do
    has_extended_acls = acl.acl_entries != []
    owning_group_match = acl.gid in all_gids
    named_groups = find_named_groups(acl.acl_entries, all_gids)

    # Collect all matching group permissions
    group_permissions =
      collect_group_permissions(acl, owning_group_match, named_groups, has_extended_acls)

    case group_permissions do
      # No group matched at all — fall through to other
      nil -> nil
      # At least one group matched — check if permission is in the union
      perms -> if MapSet.member?(perms, permission), do: :ok, else: {:error, :forbidden}
    end
  end

  defp collect_group_permissions(acl, owning_group_match, named_groups, has_extended_acls) do
    owning_perms =
      if owning_group_match do
        group_mode_perms = mode_group_permissions(acl.mode)

        if has_extended_acls do
          apply_mask(group_mode_perms, acl.acl_entries)
        else
          group_mode_perms
        end
      end

    named_perms =
      case named_groups do
        [] ->
          nil

        groups ->
          groups
          |> Enum.map(fn entry -> apply_mask(entry.permissions, acl.acl_entries) end)
          |> Enum.reduce(MapSet.new(), &MapSet.union/2)
      end

    case {owning_perms, named_perms} do
      {nil, nil} -> nil
      {perms, nil} -> perms
      {nil, perms} -> perms
      {p1, p2} -> MapSet.union(p1, p2)
    end
  end

  defp mode_group_permissions(mode) do
    perms = []
    perms = if has_group_permission?(mode, :r), do: [:r | perms], else: perms
    perms = if has_group_permission?(mode, :w), do: [:w | perms], else: perms
    perms = if has_group_permission?(mode, :x), do: [:x | perms], else: perms
    MapSet.new(perms)
  end

  defp validate_acl_entry(%{type: type, id: id, permissions: perms})
       when type in [:user, :group] do
    with :ok <- validate_acl_id(id, type) do
      validate_acl_permissions(perms)
    end
  end

  defp validate_acl_entry(%{type: :mask, id: nil, permissions: perms}) do
    validate_acl_permissions(perms)
  end

  defp validate_acl_entry(%{type: :mask, id: id}) when not is_nil(id),
    do: {:error, "mask entry must have nil id"}

  defp validate_acl_entry(%{type: :other, id: nil, permissions: perms}) do
    validate_acl_permissions(perms)
  end

  defp validate_acl_entry(%{type: :other, id: id}) when not is_nil(id),
    do: {:error, "other entry must have nil id"}

  defp validate_acl_entry(%{type: type}),
    do: {:error, "invalid ACL type: #{inspect(type)}"}

  defp validate_acl_entry(_),
    do: {:error, "must be a map with :type, :id, and :permissions keys"}

  defp validate_acl_id(id, _type) when is_integer(id) and id >= 0, do: :ok
  defp validate_acl_id(nil, type), do: {:error, "#{type} entry requires a non-nil id"}

  defp validate_acl_id(id, _type),
    do: {:error, "id must be a non-negative integer, got: #{inspect(id)}"}

  defp validate_acl_permissions(%MapSet{} = perms) do
    if MapSet.subset?(perms, @all_permissions) do
      :ok
    else
      invalid = MapSet.difference(perms, @all_permissions)
      {:error, "invalid permissions: #{inspect(MapSet.to_list(invalid))}"}
    end
  end

  defp validate_acl_permissions(_), do: {:error, "permissions must be a MapSet"}
end
