defmodule NeonFS.Events.FileContentUpdated do
  @moduledoc """
  Emitted when a file's content is modified (write operation).
  """

  @enforce_keys [:volume_id, :file_id, :path]
  defstruct [:volume_id, :file_id, :path]

  @type t :: %__MODULE__{
          volume_id: binary(),
          file_id: binary(),
          path: String.t()
        }
end
