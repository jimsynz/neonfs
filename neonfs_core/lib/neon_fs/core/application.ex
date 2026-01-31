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
  require Logger

  @impl true
  def start(_type, _args) do
    # Start the core supervision tree
    NeonFS.Core.Supervisor.start_link([])
  end

  @impl true
  def stop(_state) do
    # Snapshot all metadata tables before the supervision tree shuts down.
    # This must happen here because ETS tables are destroyed when their
    # owning GenServers terminate, and the supervisor shuts down children
    # in reverse order (Persistence last, but ETS tables are already gone).
    Logger.info("Application stopping, triggering final snapshot...")

    case NeonFS.Core.Persistence.snapshot_now() do
      :ok ->
        Logger.info("Final snapshot completed successfully")

      {:error, reason} ->
        Logger.error("Final snapshot failed: #{inspect(reason)}")
    end

    :ok
  end
end
