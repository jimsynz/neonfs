defmodule NeonFS.Error.Unavailable do
  @moduledoc """
  Error class for service availability failures.

  Used when quorum is lost, all nodes are unreachable, or the
  cluster cannot serve requests.
  """
  use Splode.Error, fields: [message: nil, details: %{}], class: :unavailable

  @impl true
  def message(%{message: message}) when is_binary(message), do: message
  def message(_), do: "Service unavailable"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
