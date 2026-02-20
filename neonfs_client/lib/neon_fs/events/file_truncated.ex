defmodule NeonFS.Events.FileTruncated do
  @moduledoc """
  Emitted when a file is truncated.
  """

  @enforce_keys [:volume_id, :file_id, :path]
  defstruct [:volume_id, :file_id, :path]

  @type t :: %__MODULE__{
          volume_id: binary(),
          file_id: binary(),
          path: String.t()
        }
end
