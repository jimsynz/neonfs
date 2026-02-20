defmodule NeonFS.Events.VolumeAclChanged do
  @moduledoc """
  Emitted when a volume's access control list is modified.

  Subscribers should re-evaluate cached permission decisions for the volume.
  """

  @enforce_keys [:volume_id]
  defstruct [:volume_id]

  @type t :: %__MODULE__{volume_id: binary()}
end
