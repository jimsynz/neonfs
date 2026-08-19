defmodule NeonFS.FUSE.MountInfo do
  @moduledoc """
  Information about an active FUSE mount.

  Tracks the lifecycle and resources associated with a mounted volume.

  `:opts` are the options the mount was made with. They are kept because a
  mount that has to be re-established after a restart has to come back with the
  same ones — a read-only mount that returns writable is a worse outcome than
  one that does not return at all.
  """

  @enforce_keys [:id, :volume_name, :mount_point, :started_at, :mount_session]
  defstruct [
    :id,
    :volume_name,
    :mount_point,
    :started_at,
    :mount_session,
    :handler_pid,
    :session_pid,
    :cache_pid,
    opts: []
  ]

  @type t :: %__MODULE__{
          id: String.t(),
          volume_name: String.t(),
          mount_point: String.t(),
          started_at: DateTime.t(),
          mount_session: reference(),
          handler_pid: pid() | nil,
          session_pid: pid() | nil,
          cache_pid: pid() | nil,
          opts: keyword()
        }

  @doc """
  Create a new MountInfo struct.
  """
  @spec new(keyword()) :: t()
  def new(attrs) do
    struct!(__MODULE__, attrs)
  end
end
