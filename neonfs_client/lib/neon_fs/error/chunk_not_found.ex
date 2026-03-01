defmodule NeonFS.Error.ChunkNotFound do
  @moduledoc """
  The requested chunk does not exist in any known location.
  """
  use Splode.Error, fields: [:chunk_hash, :volume_id], class: :not_found

  @impl true
  def message(%{chunk_hash: hash}) when is_binary(hash), do: "Chunk not found: #{hash}"
  def message(_), do: "Chunk not found"

  defimpl String.Chars do
    def to_string(error), do: Exception.message(error)
  end
end
