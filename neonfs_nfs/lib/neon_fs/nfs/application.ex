defmodule NeonFS.NFS.Application do
  @moduledoc """
  OTP Application for neonfs_nfs.

  Starts the NFS supervision tree and handles graceful shutdown.
  """

  use Application
  require Logger

  alias NeonFS.NFS.{ExportManager, Supervisor}

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    # In test mode, don't start the supervisor - tests use start_supervised
    # for the specific components they need
    result =
      if Application.get_env(:neonfs_nfs, :start_supervisor, true) do
        Supervisor.start_link()
      else
        # Return a minimal supervisor for test mode
        Elixir.Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
      end

    result
  end

  @impl true
  def stop(_state) do
    # Graceful shutdown: unexport all volumes
    case Process.whereis(ExportManager) do
      nil ->
        :ok

      _pid ->
        Enum.each(ExportManager.list_exports(), fn export ->
          Logger.debug("Unexporting volume before shutdown", volume_name: export.volume_name)
          ExportManager.unexport(export.id)
        end)
    end

    :ok
  end
end
