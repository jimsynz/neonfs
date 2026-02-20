defmodule NeonFS.Events.FileRenamed do
  @moduledoc """
  Emitted when a file is renamed or moved within a volume.

  Carries both paths so subscribers can invalidate the old path's cache entry
  and the parent directory listings for both old and new locations.
  """

  @enforce_keys [:volume_id, :file_id, :old_path, :new_path]
  defstruct [:volume_id, :file_id, :old_path, :new_path]

  @type t :: %__MODULE__{
          volume_id: binary(),
          file_id: binary(),
          old_path: String.t(),
          new_path: String.t()
        }
end
