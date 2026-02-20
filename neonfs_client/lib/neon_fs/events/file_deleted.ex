defmodule NeonFS.Events.FileDeleted do
  @moduledoc """
  Emitted when a file is deleted from a volume.
  """

  @enforce_keys [:volume_id, :file_id, :path]
  defstruct [:volume_id, :file_id, :path]

  @type t :: %__MODULE__{
          volume_id: binary(),
          file_id: binary(),
          path: String.t()
        }
end
