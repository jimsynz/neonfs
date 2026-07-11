defmodule NeonFS.NFS.ExportInfo do
  @moduledoc """
  Information about an exported NFS volume.

  Exports are cluster state (#1175): a volume is exported when its
  `nfs_export` flag is set in the core volume registry. This struct is
  the local mirror `NeonFS.NFS.ExportManager` keeps of one such volume.
  Carrying `volume_id` lets the MOUNT/NFSv3 paths build filehandles
  without a per-lookup RPC to core. Carrying `write_ack` lets the WRITE
  path decide the RFC 1813 `committed` level (`:file_sync` vs `:unstable`)
  without a per-WRITE `get_volume` RPC (#1509) — the mirror is resynced on
  volume lifecycle events, so this stays current.
  """

  @enforce_keys [:volume_id, :volume_name, :exported_at]
  defstruct [
    :volume_id,
    :volume_name,
    :exported_at,
    allowed_ips: [],
    root_squash: true,
    write_ack: :local
  ]

  @type t :: %__MODULE__{
          volume_id: String.t(),
          volume_name: String.t(),
          exported_at: DateTime.t(),
          allowed_ips: [String.t()],
          root_squash: boolean(),
          write_ack: :local | :quorum | :all
        }

  @doc """
  Create a new ExportInfo from the given keyword attributes.
  """
  @spec new(keyword()) :: t()
  def new(attrs) do
    struct!(__MODULE__, attrs)
  end
end
