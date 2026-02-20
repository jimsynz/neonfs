defmodule NeonFS.Events.VolumeDeleted do
  @moduledoc """
  Emitted when a volume is deleted.
  """

  @enforce_keys [:volume_id]
  defstruct [:volume_id]

  @type t :: %__MODULE__{volume_id: binary()}
end
