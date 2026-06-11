defmodule NeonFS.NFS.ExportInfo do
  @moduledoc """
  Information about an exported NFS volume.

  Exports are cluster state (#1175): a volume is exported when its
  `nfs_export` flag is set in the core volume registry. This struct is
  the local mirror `NeonFS.NFS.ExportManager` keeps of one such volume.
  Carrying `volume_id` lets the MOUNT/NFSv3 paths build filehandles
  without a per-lookup RPC to core.
  """

  @enforce_keys [:volume_id, :volume_name, :exported_at]
  defstruct [:volume_id, :volume_name, :exported_at]

  @type t :: %__MODULE__{
          volume_id: String.t(),
          volume_name: String.t(),
          exported_at: DateTime.t()
        }

  @doc """
  Create a new ExportInfo from the given keyword attributes.
  """
  @spec new(keyword()) :: t()
  def new(attrs) do
    struct!(__MODULE__, attrs)
  end
end
