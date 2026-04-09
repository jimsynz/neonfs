defmodule NeonFS.NFS.Application do
  @moduledoc """
  OTP Application for neonfs_nfs.

  Starts the NFS supervision tree and handles graceful shutdown.
  """

  use Application
  require Logger

  alias NeonFS.NFS.{ExportManager, HealthCheck, Supervisor}

  @impl true
  def start(_type, _args) do
    Logger.metadata(node_name: node())

    start_supervisor? = Application.get_env(:neonfs_nfs, :start_supervisor, true)

    result =
      if start_supervisor? do
        Supervisor.start_link()
      else
        Elixir.Supervisor.start_link([], strategy: :one_for_one, name: __MODULE__)
      end

    if start_supervisor? do
      HealthCheck.register_checks()
    end

    unless Application.spec(:neonfs_omnibus) do
      NeonFS.Systemd.notify_ready()
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
