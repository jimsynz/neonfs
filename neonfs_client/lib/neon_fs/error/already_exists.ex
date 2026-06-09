defmodule NeonFS.Error.AlreadyExists do
  @moduledoc """
  A resource (file, directory, volume, bucket) already exists.

  Distinct from the transient `NeonFS.Error.Conflict` — this is a
  permanent collision the caller cannot retry away. Interface layers map
  it to `EEXIST` (FUSE/CIFS), `NFS3ERR_EXIST`, or HTTP 409/405/412
  depending on the operation.

  `reason` preserves the originating tag (`:already_exists`, `:exists`,
  `:eexist`) for callsites that still need to distinguish it.
  """
  use Splode.Error, fields: [:resource, reason: :already_exists], class: :conflict

  @type t :: %__MODULE__{}

  @doc """
  Builds an `AlreadyExists` error, preserving the originating tag under
  `reason` and an optional `resource` (path/name) for context.
  """
  @spec from_reason(atom(), term()) :: t()
  def from_reason(reason, resource \\ nil) when is_atom(reason) do
    exception(reason: reason, resource: resource)
  end

  @impl true
  def message(%{resource: resource}) when not is_nil(resource) do
    "Already exists: #{inspect(resource)}"
  end

  def message(_), do: "Already exists"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
