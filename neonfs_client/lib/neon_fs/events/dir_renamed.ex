defmodule NeonFS.Events.DirRenamed do
  @moduledoc """
  Emitted when a directory is renamed or moved within a volume.
  """

  @enforce_keys [:volume_id, :old_path, :new_path]
  defstruct [:volume_id, :old_path, :new_path]

  @type t :: %__MODULE__{
          volume_id: binary(),
          old_path: String.t(),
          new_path: String.t()
        }
end
