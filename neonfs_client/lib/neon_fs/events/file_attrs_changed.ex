defmodule NeonFS.Events.FileAttrsChanged do
  @moduledoc """
  Emitted when file metadata changes without content modification.

  Covers chmod, chown, utimens, and extended attribute (xattr) modifications.
  These are not split into separate event types because the subscriber response
  is the same: invalidate the cached stat for this file.
  """

  @enforce_keys [:volume_id, :file_id, :path]
  defstruct [:volume_id, :file_id, :path]

  @type t :: %__MODULE__{
          volume_id: binary(),
          file_id: binary(),
          path: String.t()
        }
end
