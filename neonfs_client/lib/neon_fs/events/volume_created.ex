defmodule NeonFS.Events.VolumeCreated do
  @moduledoc """
  Emitted when a new volume is created.
  """

  @enforce_keys [:volume_id]
  defstruct [:volume_id]

  @type t :: %__MODULE__{volume_id: binary()}
end
