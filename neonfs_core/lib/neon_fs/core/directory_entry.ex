defmodule NeonFS.Core.DirectoryEntry do
  @moduledoc """
  Represents a directory in the distributed filesystem.

  DirectoryEntry is the path→ID mapping for efficient directory listings.
  Sharded by `hash(parent_path)` so that a directory listing is always a
  single-segment quorum read.

  ## Key Format

  Stored with key `"dir:<volume_id>:<parent_path>"`.

  ## Children Map

  Each child is `%{name => %{type: :file | :dir, id: binary()}}`:

      %DirectoryEntry{
        parent_path: "/documents",
        volume_id: "vol_123",
        children: %{
          "report.pdf" => %{type: :file, id: "f47ac10b-..."},
          "drafts" => %{type: :dir, id: "9c7d8e6f-..."}
        }
      }
  """

  @type child_info :: %{type: :file | :dir, id: binary()}

  @type t :: %__MODULE__{
          parent_path: String.t(),
          volume_id: String.t(),
          children: %{String.t() => child_info()},
          mode: non_neg_integer(),
          uid: non_neg_integer(),
          gid: non_neg_integer(),
          hlc_timestamp: term()
        }

  @enforce_keys [:parent_path, :volume_id]
  defstruct [
    :parent_path,
    :volume_id,
    :hlc_timestamp,
    children: %{},
    mode: 0o755,
    uid: 0,
    gid: 0
  ]

  @doc """
  Creates a new DirectoryEntry.

  ## Options

    * `:mode` — POSIX directory mode (default: 0o755)
    * `:uid` — owner user ID (default: 0)
    * `:gid` — owner group ID (default: 0)
  """
  @spec new(String.t(), String.t(), keyword()) :: t()
  def new(volume_id, parent_path, opts \\ []) do
    %__MODULE__{
      parent_path: parent_path,
      volume_id: volume_id,
      children: Keyword.get(opts, :children, %{}),
      mode: Keyword.get(opts, :mode, 0o755),
      uid: Keyword.get(opts, :uid, 0),
      gid: Keyword.get(opts, :gid, 0)
    }
  end

  @doc """
  Adds a child entry to the directory.

  Returns the updated DirectoryEntry.
  """
  @spec add_child(t(), String.t(), :file | :dir, binary()) :: t()
  def add_child(%__MODULE__{} = entry, name, type, id)
      when type in [:file, :dir] and is_binary(name) and is_binary(id) do
    child = %{type: type, id: id}
    %{entry | children: Map.put(entry.children, name, child)}
  end

  @doc """
  Removes a child entry from the directory.

  Returns the updated DirectoryEntry.
  """
  @spec remove_child(t(), String.t()) :: t()
  def remove_child(%__MODULE__{} = entry, name) when is_binary(name) do
    %{entry | children: Map.delete(entry.children, name)}
  end

  @doc """
  Renames a child within the directory.

  Returns `{:ok, updated_entry}` if the old name exists, or
  `{:error, :not_found}` otherwise.
  """
  @spec rename_child(t(), String.t(), String.t()) ::
          {:ok, t()} | {:error, :not_found} | {:error, :already_exists}
  def rename_child(%__MODULE__{} = entry, old_name, new_name)
      when is_binary(old_name) and is_binary(new_name) do
    cond do
      not Map.has_key?(entry.children, old_name) ->
        {:error, :not_found}

      old_name != new_name and Map.has_key?(entry.children, new_name) ->
        {:error, :already_exists}

      true ->
        {child, remaining} = Map.pop(entry.children, old_name)
        {:ok, %{entry | children: Map.put(remaining, new_name, child)}}
    end
  end

  @doc """
  Returns true if the directory contains a child with the given name.
  """
  @spec has_child?(t(), String.t()) :: boolean()
  def has_child?(%__MODULE__{} = entry, name) when is_binary(name) do
    Map.has_key?(entry.children, name)
  end

  @doc """
  Gets a child entry by name.
  """
  @spec get_child(t(), String.t()) :: {:ok, child_info()} | {:error, :not_found}
  def get_child(%__MODULE__{} = entry, name) when is_binary(name) do
    case Map.fetch(entry.children, name) do
      {:ok, child} -> {:ok, child}
      :error -> {:error, :not_found}
    end
  end

  @doc """
  Converts to a storable map for quorum serialisation.
  """
  @spec to_storable_map(t()) :: map()
  def to_storable_map(%__MODULE__{} = entry) do
    %{
      parent_path: entry.parent_path,
      volume_id: entry.volume_id,
      children: entry.children,
      mode: entry.mode,
      uid: entry.uid,
      gid: entry.gid,
      hlc_timestamp: entry.hlc_timestamp
    }
  end

  @doc """
  Restores from a storable map (quorum deserialisation).
  """
  @spec from_storable_map(map()) :: t()
  def from_storable_map(map) when is_map(map) do
    %__MODULE__{
      parent_path: get_field(map, :parent_path),
      volume_id: get_field(map, :volume_id),
      children: decode_children(get_field(map, :children, %{})),
      mode: get_field(map, :mode, 0o755),
      uid: get_field(map, :uid, 0),
      gid: get_field(map, :gid, 0),
      hlc_timestamp: get_field(map, :hlc_timestamp)
    }
  end

  defp get_field(map, key, default \\ nil) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || default
  end

  defp decode_children(children) when is_map(children) do
    Map.new(children, fn {name, info} ->
      name = if is_atom(name), do: Atom.to_string(name), else: name

      decoded_info = %{
        type: decode_type(get_field(info, :type)),
        id: get_field(info, :id)
      }

      {name, decoded_info}
    end)
  end

  defp decode_children(_), do: %{}

  defp decode_type(:file), do: :file
  defp decode_type(:dir), do: :dir
  defp decode_type("file"), do: :file
  defp decode_type("dir"), do: :dir
  defp decode_type(_), do: :file
end
