defmodule NeonFS.Error.Unsupported do
  @moduledoc """
  The requested operation is not supported for this resource's
  configuration — e.g. streaming writes against an erasure-coded volume.

  Distinct from `NeonFS.Error.Invalid` (bad input the caller could
  correct): the input is well-formed, but the capability isn't available
  here. Interface layers map it to `ENOTSUP`/`EOPNOTSUPP` (FUSE) or
  HTTP 501. Shares the `:invalid` class so callers that bucket all
  client-correctable 4xx-style errors still match on `class`.

  `capability` names the unsupported operation for callers/logs.
  """
  use Splode.Error, fields: [:capability, message: nil], class: :invalid

  @type t :: %__MODULE__{}

  @doc """
  Builds an `Unsupported` error for the named capability.
  """
  @spec for_capability(atom() | String.t()) :: t()
  def for_capability(capability) do
    exception(capability: capability)
  end

  @impl true
  def message(%{message: message}) when is_binary(message), do: message

  def message(%{capability: capability}) when not is_nil(capability) do
    "Operation not supported: #{capability}"
  end

  def message(_), do: "Operation not supported"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
