defmodule NeonFS.Events.DriveAdded do
  @moduledoc """
  Emitted when a drive is added to a node.

  Carries the full drive data as a map so that receiving nodes can insert
  directly into their local DriveRegistry ETS without an additional RPC.
  """

  @enforce_keys [:node, :drive_id, :drive]
  defstruct [:node, :drive_id, :drive]

  @type t :: %__MODULE__{
          node: node(),
          drive_id: String.t(),
          drive: map()
        }
end
