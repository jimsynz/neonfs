defmodule NeonFS.Core.Application do
  @moduledoc """
  Application callback module for NeonFS Core.

  Starts the core supervision tree which manages:
  - Blob storage (content-addressed storage)
  - Chunk metadata index
  - File metadata index
  - Volume registry
  """

  use Application

  @impl true
  def start(_type, _args) do
    # Start the core supervision tree
    NeonFS.Core.Supervisor.start_link([])
  end
end
