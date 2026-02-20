defmodule NeonFS.Client.EventHandler do
  @moduledoc """
  Behaviour for processes that handle NeonFS event notifications.

  Subscribers can implement this behaviour for documentation and Dialyzer
  support, or handle `{:neonfs_event, envelope}` messages directly in
  their `handle_info/2` without implementing the behaviour.
  """

  @callback handle_event(envelope :: NeonFS.Events.Envelope.t()) :: :ok
end
