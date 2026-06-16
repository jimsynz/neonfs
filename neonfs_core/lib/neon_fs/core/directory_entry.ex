defmodule NeonFS.Core.DirectoryEntry do
  @moduledoc """
  The metadata record for a directory in the distributed filesystem.

  Holds a directory's own POSIX attributes (mode/uid/gid) and HLC
  timestamp. Children are **not** stored here — each child is its own
  keyed `dirent:` entry in the `:file_index` tree (see
  `NeonFS.Core.FileIndex`), so adding or removing a child is a single
  small keyed write rather than a read-modify-write of a growing blob.

  ## Key Format

  Stored with key `"dir:<volume_id>:<parent_path>"`.

      %DirectoryEntry{
        parent_path: "/documents",
        volume_id: "vol_123",
        mode: 0o755,
        uid: 0,
        gid: 0
      }
  """

  @type t :: %__MODULE__{
          parent_path: String.t(),
          volume_id: String.t(),
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
      mode: Keyword.get(opts, :mode, 0o755),
      uid: Keyword.get(opts, :uid, 0),
      gid: Keyword.get(opts, :gid, 0)
    }
  end

  @doc """
  Converts to a storable map for quorum serialisation.
  """
  @spec to_storable_map(t()) :: map()
  def to_storable_map(%__MODULE__{} = entry) do
    %{
      parent_path: entry.parent_path,
      volume_id: entry.volume_id,
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
      mode: get_field(map, :mode, 0o755),
      uid: get_field(map, :uid, 0),
      gid: get_field(map, :gid, 0),
      hlc_timestamp: get_field(map, :hlc_timestamp)
    }
  end

  defp get_field(map, key, default \\ nil) do
    Map.get(map, key) || Map.get(map, Atom.to_string(key)) || default
  end
end
