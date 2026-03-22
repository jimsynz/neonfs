defmodule NeonFS.Events.DriveRemoved do
  @moduledoc """
  Emitted when a drive is removed from a node.
  """

  @enforce_keys [:node, :drive_id]
  defstruct [:node, :drive_id]

  @type t :: %__MODULE__{
          node: node(),
          drive_id: String.t()
        }
end
