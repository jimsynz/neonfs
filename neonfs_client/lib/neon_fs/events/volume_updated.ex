defmodule NeonFS.Events.VolumeUpdated do
  @moduledoc """
  Emitted when an existing volume's configuration is updated.
  """

  @enforce_keys [:volume_id]
  defstruct [:volume_id]

  @type t :: %__MODULE__{volume_id: binary()}
end
