defmodule NeonFS.Events.FileAclChanged do
  @moduledoc """
  Emitted when POSIX ACL changes on a specific file or directory.
  """

  @enforce_keys [:volume_id, :path]
  defstruct [:volume_id, :path]

  @type t :: %__MODULE__{
          volume_id: binary(),
          path: String.t()
        }
end
