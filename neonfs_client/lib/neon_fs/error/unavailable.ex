defmodule NeonFS.Error.Unavailable do
  @moduledoc """
  Error class for service availability failures.

  Used when quorum is lost, all nodes are unreachable, or the
  cluster cannot serve requests.
  """
  use Splode.Error, fields: [message: nil, details: %{}], class: :unavailable

  @type t :: %__MODULE__{}

  @doc """
  Builds an `Unavailable` error from one of the ad-hoc unavailability tags
  the cluster paths historically returned (`:ra_not_available`,
  `:ra_unavailable`, `:coordinator_unavailable`, `:timeout`).

  The originating tag is preserved under `details.reason` so callers that
  need to distinguish "cluster not yet initialised" from "cluster
  unreachable" can still branch on it.
  """
  @spec from_reason(atom()) :: t()
  def from_reason(reason) when is_atom(reason) do
    exception(message: message_for(reason), details: %{reason: reason})
  end

  defp message_for(:ra_not_available), do: "Cluster not yet initialised"
  defp message_for(:ra_unavailable), do: "Cluster unavailable"
  defp message_for(:coordinator_unavailable), do: "Namespace coordinator unavailable"
  defp message_for(:timeout), do: "Cluster request timed out"
  defp message_for(_), do: "Service unavailable"

  @impl true
  def message(%{message: message}) when is_binary(message), do: message
  def message(_), do: "Service unavailable"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
