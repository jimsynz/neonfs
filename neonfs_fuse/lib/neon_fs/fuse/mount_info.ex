defmodule NeonFS.FUSE.MountInfo do
  @moduledoc """
  Information about an active FUSE mount.

  Tracks the lifecycle and resources associated with a mounted volume.
  """

  @enforce_keys [:id, :volume_name, :mount_point, :started_at, :mount_session, :handler_pid]
  defstruct [
    :id,
    :volume_name,
    :mount_point,
    :started_at,
    :mount_session,
    :handler_pid
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          volume_name: String.t(),
          mount_point: String.t(),
          started_at: DateTime.t(),
          mount_session: reference(),
          handler_pid: pid()
        }

  @doc """
  Create a new MountInfo struct.
  """
  @spec new(keyword()) :: t()
  def new(attrs) do
    struct!(__MODULE__, attrs)
  end
end
