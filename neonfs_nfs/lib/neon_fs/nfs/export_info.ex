defmodule NeonFS.NFS.ExportInfo do
  @moduledoc """
  Information about an exported NFS volume.

  Tracks the metadata for a volume that has been made available via NFS.
  Unlike FUSE mounts (one handler per mount point), NFS uses a single
  listener serving all exported volumes through the virtual root.
  """

  @enforce_keys [:id, :volume_name, :exported_at]
  defstruct [:id, :volume_name, :exported_at]

  @type t :: %__MODULE__{
          id: String.t(),
          volume_name: String.t(),
          exported_at: DateTime.t()
        }

  @spec new(keyword()) :: t()
  def new(attrs) do
    struct!(__MODULE__, attrs)
  end
end
