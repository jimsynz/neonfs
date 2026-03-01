defmodule NeonFS.Error.VolumeNotFound do
  @moduledoc """
  The requested volume does not exist in the cluster.
  """
  use Splode.Error, fields: [:volume_name, :volume_id], class: :not_found

  @impl true
  def message(%{volume_name: name}) when is_binary(name), do: "Volume '#{name}' not found"
  def message(%{volume_id: id}) when not is_nil(id), do: "Volume with ID '#{id}' not found"
  def message(_), do: "Volume not found"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
