defmodule NeonFS.S3.ClusterFrozenError do
  @moduledoc """
  Raised when a write is attempted while the cluster is `:frozen` (#1378).

  `HealthPlug` rescues it and returns `503 Service Unavailable` with a
  `Retry-After` hint. The `Plug.Exception` implementation gives the same
  status if the error ever reaches Bandit directly.
  """

  defexception message: "Cluster frozen for maintenance"
end

defimpl Plug.Exception, for: NeonFS.S3.ClusterFrozenError do
  def status(_exception), do: 503
  def actions(_exception), do: []
end
