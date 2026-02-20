defmodule NeonFS.Events.DirDeleted do
  @moduledoc """
  Emitted when a directory is deleted from a volume.
  """

  @enforce_keys [:volume_id, :path]
  defstruct [:volume_id, :path]

  @type t :: %__MODULE__{
          volume_id: binary(),
          path: String.t()
        }
end
