defmodule NeonFS.Events.Envelope do
  @moduledoc """
  Wrapper for event notification messages.

  Every broadcast event is wrapped in an envelope that carries metadata for
  ordering, gap detection, and debugging:

  - `event` — the event struct (volume-scoped or cluster-scoped)
  - `source_node` — the core node that originated the event
  - `sequence` — monotonic counter (per-volume for volume events, shared for drive events)
  - `hlc_timestamp` — Hybrid Logical Clock timestamp `{wall_ms, counter, node_id}`
  """

  @enforce_keys [:event, :source_node, :sequence, :hlc_timestamp]
  defstruct [:event, :source_node, :sequence, :hlc_timestamp]

  @type t :: %__MODULE__{
          event: NeonFS.Events.event(),
          source_node: node(),
          sequence: non_neg_integer(),
          hlc_timestamp: {non_neg_integer(), non_neg_integer(), node()}
        }
end
